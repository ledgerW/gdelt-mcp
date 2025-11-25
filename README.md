# GDELT 2.0 MCP Server

Model Context Protocol server providing AI agents with access to GDELT 2.0 (Global Database of Events, Language, and Tone) via Google BigQuery. Query global events, news coverage, themes, and visual content with cost-optimized workflows.

## Why This MCP Rocks 🚀

- **🎯 Cost-Optimized Workflows**: Built-in materialization tools make iterative analysis 50-100x cheaper
- **📊 4 Massive Datasets**: Events, mentions, themes, and visual analysis - all queryable
- **🔐 Secure Architecture**: Your credentials, your queries, your control
- **⚡ Production-Ready**: Partition pruning, date filters, and cost estimation built-in
- **🎓 CAMEO Taxonomy**: Built-in event/actor code lookups for precise queries

## Quick Start

### Prerequisites

- Python 3.11+
- `uv` package manager ([install](https://github.com/astral-sh/uv))
- GCP project with BigQuery API enabled
- Service account with BigQuery permissions

### Installation

```bash
cd gdelt-mcp
uv sync
```

### Getting GCP Credentials

1. **Create GCP Project** and enable BigQuery API
2. **Create Service Account** in IAM & Admin
3. **Grant Roles**: BigQuery Data Viewer + BigQuery Job User
4. **Generate Key**: Download JSON key file
5. **Extract Values**: `project_id`, `private_key`, `client_email`

### Running the Server

```bash
# Development
uv run python server.py

# With FastMCP CLI
uv run fastmcp run server.py

# Interactive testing
uv run fastmcp dev server.py
```

## Authentication

Bearer token format (pipe-delimited):
```
project_id|private_key|client_email
```

**Example in Python:**
```python
from fastmcp import Client
from fastmcp.client.auth import BearerAuth

token = f"{project_id}|{private_key}|{client_email}"
client = Client("https://your-server/mcp", auth=BearerAuth(token=token))
```


### LangChain Agent with Remote Server

Create a LangChain agent that uses the usage guide in its system prompt and organizes tools by their tags:

```python
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain.agents import create_agent
import httpx

async def create_gdelt_agent():
    """Create a LangChain agent with GDELT tools organized by category."""
    
    # Bearer token format: project_id|private_key|client_email
    token = f"{project_id}|{private_key}|{client_email}"
    
    # Initialize MultiServerMCPClient with remote server
    client = MultiServerMCPClient({
        "gdelt": {
            "transport": "streamable_http",
            "url": "https://your-server/mcp",
            "auth": httpx.BasicAuth(username="", password=token),
        }
    })
    
    # Get usage guide resource
    usage_guide_blobs = await client.get_resources(
        "gdelt",
        uris="gdelt://guide/usage"
    )
    usage_guide = usage_guide_blobs[0].as_string()
    
    # Get LangChain tools (these include metadata with tags)
    tools = await client.get_tools(server_name="gdelt")
    
    # Group tools by FastMCP tags
    def group_tools_by_tag(tools):
        categories = {
            'schema': [],
            'query': [],
            'cost': [],
            'cameo': [],
            'guide': []
        }
        for tool in tools:
            # Extract tags from LangChain tool metadata
            if hasattr(tool, 'metadata') and tool.metadata:
                tags = tool.metadata.get('tags', [])
                for tag in tags:
                    if tag in categories:
                        categories[tag].append(tool.name)
        return categories
    
    tool_categories = group_tools_by_tag(tools)
    
    # Format categorized tool list for system prompt
    tools_by_category = "\n\n".join([
        f"**{category.upper()} TOOLS**:\n" + 
        "\n".join([f"- {tool}" for tool in tools])
        for category, tools in tool_categories.items()
        if tools
    ])
    
    # Build comprehensive system prompt
    system_prompt = f"""You are a GDELT data analysis assistant with access to the complete GDELT 2.0 dataset via BigQuery.

IMPORTANT USAGE GUIDE:
{usage_guide}

AVAILABLE TOOLS BY CATEGORY:
{tools_by_category}

Follow the recommended workflow from the usage guide above. Always:
1. Check existing materialized subsets first (list_materialized_subsets)
2. Create materialized subsets for iterative analysis (create_materialized_subset)
3. Use appropriate date filters in all queries
4. Start with Events table (smallest) before querying larger tables"""
    
    # Create agent with gpt-5.1
    agent = create_agent(
        "openai:gpt-5.1",
        tools,
        system_prompt=system_prompt
    )
    
    return agent

# Use the agent
agent = await create_gdelt_agent()
result = await agent.ainvoke({
    "messages": "Analyze protests in the United States during the first week of January 2025"
})
print(result)
```

**Key Features:**

- **Remote server with authentication**: Uses bearer token auth (project_id|private_key|client_email)
- **Usage guide integration**: Fetches and includes the usage guide in system prompt
- **Tag-based tool grouping**: Organizes tools by category (schema, query, cost, cameo, guide)
- **Comprehensive system prompt**: Integrates best practices and tool categories
- **GPT-5.1 model**: Latest OpenAI model for optimal performance

### MCP Client with Remote Server

Connect to the server using MCP's client API to access tools and resources directly:

```python
from mcp import Client
import httpx

# Bearer token format: project_id|private_key|client_email
token = f"{project_id}|{private_key}|{client_email}"
server_url = "https://gdelt-mcp.fastmcp.app/mcp"
auth = httpx.BasicAuth(username="", password=token)

client = Client(server_url, auth=auth)

async with client:
    # Get usage guide resource
    usage_guide = await client.read_resource("gdelt://guide/usage")
    print(f"Usage guide: {usage_guide[:200]}...")
    
    # List all tools
    tools = await client.list_tools()
    print(f"Available tools: {[t.name for t in tools]}")
    
    # Filter tools by tag
    schema_tools = [
        tool for tool in tools 
        if hasattr(tool, 'meta') and tool.meta and
           tool.meta.get('_fastmcp', {}) and
           'schema' in tool.meta.get('_fastmcp', {}).get('tags', [])
    ]
    
    query_tools = [
        tool for tool in tools 
        if hasattr(tool, 'meta') and tool.meta and
           tool.meta.get('_fastmcp', {}) and
           'query' in tool.meta.get('_fastmcp', {}).get('tags', [])
    ]
    
    cost_tools = [
        tool for tool in tools 
        if hasattr(tool, 'meta') and tool.meta and
           tool.meta.get('_fastmcp', {}) and
           'cost' in tool.meta.get('_fastmcp', {}).get('tags', [])
    ]
    
    print(f"Found {len(schema_tools)} schema tools")
    print(f"Found {len(query_tools)} query tools")
    print(f"Found {len(cost_tools)} cost optimization tools")
```

**Key Features:**

- **Remote server with authentication**: Uses bearer token auth
- **Direct resource access**: Read usage guides and schemas
- **Tool listing and filtering**: Filter tools by FastMCP tags
- **Simple API**: Straightforward client interface for tool discovery

**Available Tool Tags:**
- `schema`: Schema documentation tools
- `query`: Direct query tools (events, eventmentions, gkg, cloudvision)
- `cost`: Cost optimization tools (estimate, materialize, list, query subsets)
- `cameo`: CAMEO taxonomy lookup tools
- `guide`: Usage guide and best practices

## MCP Resources

Schema documentation and best practices:

- `gdelt://events/schema` - Events table (who/what/where/when)
- `gdelt://eventmentions/schema` - Media mentions and sources
- `gdelt://gkg/schema` - Global Knowledge Graph (themes, entities)
- `gdelt://cloudvision/schema` - Visual analysis of news images
- `gdelt://guide/usage` - **Cost-effective querying guide** 🚨

## MCP Tools

### Query Tools

**`query_events`** - Query the Events table (smallest, query first)
- Parameters: `where_clause`, `select_fields`, `limit`, `order_by`
- ⚠️ **REQUIRED**: Include `SQLDATE >= YYYYMMDD` filter

**`query_eventmentions`** - Query media mentions of events
- Parameters: `where_clause`, `select_fields`, `limit`
- 💡 **RECOMMENDED**: Use `GLOBALEVENTID` from Events queries

**`query_gkg`** - Query Global Knowledge Graph (largest/most expensive)
- Parameters: `where_clause`, `select_fields`, `limit`
- ⚠️ **REQUIRED**: Include `DATE >= YYYYMMDDhhmmss` filter

**`query_cloudvision`** - Query visual analysis of news images
- Parameters: `where_clause`, `select_fields`, `limit`
- 💡 **RECOMMENDED**: Include `timestamp` filters

### Cost Optimization Tools

**`estimate_query_cost`** - Check query cost before execution (dry-run)
- Prevents expensive accidents
- Get cost warnings for >1GB scans

**`create_materialized_subset`** - Create filtered subset with auto-expiration
- Filter once, query many times (50-100x cheaper)
- Auto-expires in 48 hours (configurable)
- Must include date filters in where_clause

**`list_materialized_subsets`** - View your materialized subsets
- Shows expiration status, size, row count

**`query_materialized_subset`** - Query subsets (near-free!)
- ~$0.00001 per query vs $0.01+ on full tables
- Perfect for iterative analysis

### CAMEO Taxonomy Tools

**`get_cameo_event_codes`** - Get CAMEO event code taxonomy
- Parameters: `category` (e.g., "19"), `search_keyword` (e.g., "protest")
- 300+ hierarchical event codes

**`get_cameo_actor_codes`** - Get CAMEO actor code taxonomy
- Parameters: `code_type` ("countries", "types", or "all")
- Country codes and actor type classifications

## Cost-Optimized Workflow

🎯 **The recommended approach for all analysis:**

```python
# Step 1: Create subset once (filters data, small cost)
create_materialized_subset(
    source_table="events",
    subset_name="ukraine_jan2025",
    where_clause="SQLDATE BETWEEN 20250101 AND 20250131 AND (Actor1CountryCode = 'UKR' OR Actor2CountryCode = 'UKR')",
    select_fields="SQLDATE, Actor1Name, Actor2Name, EventCode",
    description="Ukraine events January 2025"
)

# Step 2: Query subset multiple times (near-free)
query_materialized_subset(
    subset_name="ukraine_jan2025",
    where_clause="EventCode LIKE '19%'",  # Military events
    limit=1000
)

query_materialized_subset(
    subset_name="ukraine_jan2025",
    where_clause="GoldsteinScale < -5",  # High conflict
    limit=500
)
```

**Cost Comparison:**
- Direct queries (3x): 3 × $0.01 = **$0.03**
- Materialized workflow: $0.01 + 3 × $0.00001 = **$0.01**
- **Savings: 66%** (scales with more queries!)

📖 **Read the `gdelt://guide/usage` resource for complete guidance.**

## Example Queries

### Find Recent Military Conflicts
```python
query_events(
    where_clause="EventRootCode = '19' AND SQLDATE >= 20250101",
    select_fields="SQLDATE, Actor1Name, Actor2Name, EventCode",
    limit=100
)
```

### Track Protests in a Country
```python
query_events(
    where_clause="EventRootCode = '14' AND Actor1CountryCode = 'USA' AND SQLDATE >= 20250101",
    limit=50
)
```

### Analyze Media Coverage
```python
# First get events
events = query_events(
    where_clause="Actor1Name LIKE '%Biden%' AND SQLDATE >= 20250101",
    limit=10
)

# Then get mentions for specific event
mentions = query_eventmentions(
    where_clause=f"GLOBALEVENTID = {events[0]['GLOBALEVENTID']}"
)
```

### Find Articles About Climate Change
```python
query_gkg(
    where_clause="Themes LIKE '%ENV_CLIMATECHANGE%' AND DATE >= 20250101000000",
    select_fields="DATE, Themes, V2Locations, V2Tone",
    limit=100
)
```

## GDELT Tables Overview

| Table | Size | Update Frequency | Best For |
|-------|------|------------------|----------|
| **Events** | Small (~100-200MB/day) | 15 minutes | Event tracking, actor analysis |
| **EventMentions** | Medium (~1-2GB/day) | 15 minutes | Source analysis, media coverage |
| **GKG** | Large (~5-10GB/day) | 15 minutes | Themes, entities, sentiment |
| **CloudVision** | Variable | 15 minutes | Visual content analysis |

**Query Priority:** Events → Mentions → GKG → CloudVision

## Best Practices

1. **Always use date filters** to enable partition pruning
2. **Start with Events table** - it's the smallest
3. **Use `estimate_query_cost`** before expensive queries
4. **Create materialized subsets** for iterative analysis
5. **Select specific fields** instead of `*` when possible
6. **Check CAMEO codes** before querying events


## Troubleshooting

**BigQuery Access Denied**
- Verify service account has BigQuery Data Viewer + Job User roles
- Check that BigQuery API is enabled in your GCP project

**Expensive Queries**
- Add date filters (SQLDATE or DATE)
- Select fewer fields (not `*`)
- Use materialization for repeated queries
- Check costs with `estimate_query_cost` first

**Query Timeout**
- Reduce date range
- Limit number of results
- Select fewer fields

## Resources

- [GDELT Project](https://www.gdeltproject.org/)
- [GDELT 2.0 Documentation](https://www.gdeltproject.org/data.html)
- [CAMEO Event Codes](https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt)
- [FastMCP Documentation](https://gofastmcp.com/)
- [Google BigQuery Pricing](https://cloud.google.com/bigquery/pricing)

## License

This MCP server implementation is provided as-is. GDELT data is freely available for research and analysis.
