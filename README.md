# GDELT 2.0 MCP Server

A Model Context Protocol (MCP) server providing access to the GDELT 2.0 (Global Database of Events, Language, and Tone) dataset via Google BigQuery.

## Overview

This MCP server exposes four GDELT 2.0 tables along with CAMEO taxonomy lookups to enable AI agents to query and analyze global events data:

- **Events Table**: Distinct events with actors, actions, and locations
- **EventMentions Table**: Every mention of every event across news sources  
- **GKG Table**: Global Knowledge Graph with themes, entities, and metadata
- **CloudVision Table**: Visual analysis of images in news articles

## Features

- ✅ **4 MCP Resources**: Schema documentation for each table
- ✅ **6 Query Tools**: Execute SQL queries on all 4 tables + CAMEO taxonomy lookups
- ✅ **Bearer Token Authentication**: Secure token-based auth using GCP credentials
- ✅ **BigQuery Integration**: Direct access to GDELT's partitioned BigQuery tables
- ✅ **User Pays Model**: Each user queries with their own GCP credentials

## Prerequisites

- Python 3.11+
- `uv` package manager ([install instructions](https://github.com/astral-sh/uv))
- Google Cloud Platform account
- GCP project with BigQuery API enabled
- Service account with BigQuery read access to GDELT dataset

## Getting Your GCP Credentials

Before using the GDELT MCP server, you need to obtain Google Cloud Platform credentials with access to the GDELT BigQuery dataset.

### Step 1: Create a GCP Project

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Note your **Project ID** (you'll need this later)

### Step 2: Enable BigQuery API

1. In your GCP project, go to **APIs & Services > Library**
2. Search for "BigQuery API"
3. Click **Enable**

### Step 3: Create a Service Account

1. Go to **IAM & Admin > Service Accounts**
2. Click **Create Service Account**
3. Name it (e.g., "gdelt-mcp-access")
4. Click **Create and Continue**

### Step 4: Grant BigQuery Permissions

Add these roles to your service account:
- **BigQuery Data Viewer** - Read access to data
- **BigQuery Job User** - Ability to run queries

### Step 5: Create and Download Key

1. Click on your newly created service account
2. Go to the **Keys** tab
3. Click **Add Key > Create New Key**
4. Choose **JSON** format
5. Click **Create** (this downloads the key file)

### Step 6: Extract Credentials

Open the downloaded JSON file. You'll need these three values:
- `project_id`
- `private_key` (keep the `\n` characters as-is)
- `client_email`

### Step 7: Verify Access to GDELT

The GDELT dataset (`gdelt-bq.gdeltv2`) is publicly accessible, but you still need valid GCP credentials to query it. Your queries will be billed to your GCP project.

**Important**: GDELT tables are large. Always use date filters in your queries to minimize costs.

---

## Installation

### 1. Clone or Download

```bash
cd gdelt-mcp
```

### 2. Install Dependencies

Using uv (automatically creates virtual environment):

```bash
uv sync
```

This will:
- Create a virtual environment
- Install all dependencies from `pyproject.toml`
- Lock dependencies in `uv.lock`

## Authentication

The GDELT MCP server uses **Bearer token authentication**. Each user provides their own GCP credentials, which are used to execute BigQuery queries on their behalf. This means:

✅ **You control your data**: Queries run under your GCP project  
✅ **You pay for usage**: BigQuery costs are billed to your account  
✅ **Secure**: Your credentials are never stored server-side  

### Token Format

The Bearer token is a pipe-delimited (`|`) concatenation of your three GCP credentials:

```
project_id|private_key|client_email
```

**Example**:
```
my-project-123|-----BEGIN PRIVATE KEY-----\nMIIE...-----END PRIVATE KEY-----\n|my-service@my-project.iam.gserviceaccount.com
```

### Generating Your Token

Using Python:
```python
project_id = "your-project-id"
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "your-service@your-project.iam.gserviceaccount.com"

token = f"{project_id}|{private_key}|{client_email}"
```

Using shell:
```bash
# Extract from your service account JSON file
PROJECT_ID=$(cat service-account.json | jq -r '.project_id')
PRIVATE_KEY=$(cat service-account.json | jq -r '.private_key')
CLIENT_EMAIL=$(cat service-account.json | jq -r '.client_email')

TOKEN="${PROJECT_ID}|${PRIVATE_KEY}|${CLIENT_EMAIL}"
echo $TOKEN
```

### Security Best Practices

- 🔒 **Never commit tokens to git**
- 🔒 **Store tokens in environment variables** or secure vaults
- 🔒 **Rotate service account keys** regularly
- 🔒 **Use least-privilege IAM roles** (BigQuery Data Viewer + Job User only)
- 🔒 **Monitor your GCP billing** for unexpected usage

## Running the Server

### Local Development

```bash
uv run python server.py
```

### With FastMCP CLI

```bash
uv run fastmcp run server.py
```

### Testing the Server

```bash
uv run fastmcp dev server.py
```

This opens an interactive inspector to test tools and resources.

## Client Setup Examples

### FastMCP Client (Python)

```python
from fastmcp import Client
from fastmcp.client.auth import BearerAuth
import os
import json

# Load your service account credentials
with open('service-account.json') as f:
    creds = json.load(f)

# Create Bearer token
token = f"{creds['project_id']}|{creds['private_key']}|{creds['client_email']}"

# Connect to the MCP server
async with Client(
    "https://your-server-url/mcp",
    auth=BearerAuth(token=token)
) as client:
    # Use the tools
    result = await client.call_tool("query_events", {
        "where_clause": "SQLDATE >= 20240101",
        "limit": 10
    })
    print(result)
```

### LangChain MCP Client (Python)

```python
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain.agents import create_agent
import json

# Load your credentials
with open('service-account.json') as f:
    creds = json.load(f)

# Create Bearer token
token = f"{creds['project_id']}|{creds['private_key']}|{creds['client_email']}"

# Configure MCP client
client = MultiServerMCPClient({
    "gdelt": {
        "transport": "streamable_http",
        "url": "https://your-server-url/mcp",
        "headers": {
            "Authorization": f"Bearer {token}",
        },
    }
})

# Get tools and create agent
tools = await client.get_tools()
agent = create_agent("openai:gpt-4", tools)

# Use the agent
response = await agent.ainvoke({
    "messages": "Find recent military conflicts in Ukraine"
})
print(response)
```

### HTTP Request (cURL)

```bash
# Set your credentials
PROJECT_ID="your-project-id"
PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
CLIENT_EMAIL="your-service@your-project.iam.gserviceaccount.com"

# Create token
TOKEN="${PROJECT_ID}|${PRIVATE_KEY}|${CLIENT_EMAIL}"

# Make request
curl -X POST https://your-server-url/mcp \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "query_events",
      "arguments": {
        "where_clause": "SQLDATE >= 20240101",
        "limit": 10
      }
    },
    "id": 1
  }'
```

### Environment Variable Method

For production use, store your token in an environment variable:

```bash
# Generate and export token
export GDELT_MCP_TOKEN="$(cat service-account.json | jq -r '[.project_id, .private_key, .client_email] | join("|")')"
```

Then in your Python code:
```python
import os
from fastmcp import Client
from fastmcp.client.auth import BearerAuth

token = os.getenv("GDELT_MCP_TOKEN")
async with Client("https://your-server-url/mcp", auth=BearerAuth(token=token)) as client:
    # Use the client
    pass
```

---

## MCP Resources

The server exposes 4 resources with schema documentation:

- `gdelt://events/schema` - Events table schema
- `gdelt://eventmentions/schema` - EventMentions table schema
- `gdelt://gkg/schema` - GKG table schema
- `gdelt://cloudvision/schema` - CloudVision table schema

## MCP Tools

### Query Tools

#### `query_events`
Query the GDELT Events table.

**Parameters:**
- `where_clause` (optional): SQL WHERE clause without "WHERE" keyword
- `select_fields` (optional): Comma-separated field list (default: "*")
- `limit` (optional): Max rows to return (default: 100, max: 10000)

**Example:**
```python
query_events(
    where_clause="EventRootCode = '19' AND SQLDATE >= 20240101",
    select_fields="GLOBALEVENTID, Actor1Name, Actor2Name, EventCode",
    limit=50
)
```

#### `query_eventmentions`
Query the EventMentions table to get all mentions of events.

#### `query_gkg`
Query the Global Knowledge Graph table.

**Example:**
```python
query_gkg(
    where_clause="Themes LIKE '%PROTEST%' AND DATE >= 20240101000000",
    limit=100
)
```

#### `query_cloudvision`
Query visual analysis data from images.

### CAMEO Taxonomy Tools

#### `get_cameo_event_codes`
Get CAMEO event code taxonomy (300+ event types).

**Parameters:**
- `category` (optional): Two-digit category code (e.g., "19" for military force)
- `search_keyword` (optional): Keyword to search in descriptions

**Examples:**
```python
# Get all military force events
get_cameo_event_codes(category="19")

# Search for protest-related events
get_cameo_event_codes(search_keyword="protest")
```

#### `get_cameo_actor_codes`
Get CAMEO actor code taxonomy (countries, actor types).

**Parameters:**
- `code_type`: "countries", "types", or "all" (default: "all")

## GDELT Tables Overview

### Events Table
- 61 fields capturing event details
- Includes: actors, event codes, Goldstein scale, tone, locations
- Updated every 15 minutes
- Use for: Event analysis, actor tracking, conflict monitoring

### EventMentions Table
- Links events to specific article mentions
- Includes: confidence scores, character offsets, document tone
- Use for: Source analysis, event verification, media coverage tracking

### GKG (Global Knowledge Graph)
- Rich metadata extracted from articles
- Includes: themes, locations, persons, organizations, counts, quotations
- Use for: Topic analysis, entity tracking, sentiment analysis

### CloudVision Table
- Visual analysis from Google Cloud Vision API
- Includes: labels, faces, text (OCR), landmarks, logos
- Use for: Image analysis, visual content discovery

## CAMEO Taxonomy

### Event Codes
300+ hierarchical codes classifying events:
- **01-09**: Verbal cooperation (statements, appeals, cooperation intents)
- **10-16**: Material cooperation (demands, disapproval, rejection)
- **17-20**: Conflict (threats, protests, force, violence)

Examples:
- `01` - Make public statement
- `19` - Fight (military force)
- `14` - Protest
- `20` - Use unconventional mass violence

### Actor Codes
- **Country Codes**: 3-character codes (USA, CHN, RUS, etc.)
- **Actor Types**: GOV (Government), MIL (Military), CVI (Civilian), etc.

## Example Usage

### Find Recent Military Conflicts

```python
events = query_events(
    where_clause="EventRootCode = '19' AND SQLDATE >= 20240101",
    limit=100
)
```

### Track Protests by Location

```python
events = query_events(
    where_clause="EventRootCode = '14' AND ActionGeo_CountryCode = 'USA'",
    limit=50
)
```

### Analyze Media Coverage

```python
mentions = query_eventmentions(
    where_clause="GLOBALEVENTID = 1234567890"
)
```

### Find Articles About Climate Change

```python
articles = query_gkg(
    where_clause="Themes LIKE '%ENV_CLIMATECHANGE%'",
    limit=100
)
```

## Query Best Practices

1. **Always use date filters** to reduce BigQuery scan size:
   ```sql
   SQLDATE >= 20240101  -- For Events
   DATE >= 20240101000000  -- For GKG
   ```

2. **Limit result sets** appropriately (max 10,000 rows)

3. **Select specific fields** when possible instead of `*`

4. **Use indexed fields** in WHERE clauses:
   - Events: `SQLDATE`, `Actor1CountryCode`, `Actor2CountryCode`, `EventCode`
   - GKG: `DATE`, `SourceCommonName`

5. **Be mindful of BigQuery costs**:
   - Basic queries scan significant data
   - Use partitioned tables when available
   - Test with small date ranges first

## Project Structure

```
gdelt-mcp/
├── server.py              # Main MCP server
├── bigquery_client.py     # BigQuery connection wrapper
├── schema_docs.py         # Table schema documentation
├── cameo_lookups.py       # CAMEO taxonomy data
├── pyproject.toml         # Project configuration and dependencies
├── .env                   # Environment variables (not in git)
├── .gitignore            # Git ignore rules
└── README.md             # This file
```

**Note**: Files marked "not in git" are excluded via `.gitignore` for security.

## Troubleshooting

### Dependency Installation Issues

If `uv sync` encounters build errors:

1. Clear the cache and try again:
   ```bash
   uv cache clean
   uv sync
   ```

2. Check that you have the latest uv version:
   ```bash
   uv self update
   ```

### BigQuery Access Denied

Ensure your service account has:
- BigQuery Data Viewer role
- BigQuery Job User role

### Query Timeout

For large queries:
- Reduce the date range
- Limit the number of results
- Select fewer fields

## Resources

- [GDELT Project](https://www.gdeltproject.org/)
- [GDELT 2.0 Documentation](https://www.gdeltproject.org/data.html)
- [CAMEO Event Codes](https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt)
- [FastMCP Documentation](https://gofastmcp.com/)
- [Google BigQuery](https://cloud.google.com/bigquery)

## License

This MCP server implementation is provided as-is. GDELT data is freely available for research and analysis.

## Support

For issues with:
- **GDELT data**: See [GDELT Project](https://www.gdeltproject.org/)
- **FastMCP**: See [FastMCP docs](https://gofastmcp.com/)
- **This server**: Check logs and ensure credentials are correctly configured
