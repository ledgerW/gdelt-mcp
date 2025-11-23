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
- ✅ **8 Query Tools**: Execute SQL queries on all 4 tables + schema retrieval tools
- ✅ **2 CAMEO Taxonomy Tools**: Access 300+ event codes and actor classifications
- ✅ **BigQuery Integration**: Direct access to GDELT's BigQuery tables
- ✅ **No Authentication Required**: Designed for cloud service integration

## Prerequisites

- Python 3.11+
- Google Cloud Project with BigQuery API enabled
- Service account credentials with BigQuery access
- `uv` package manager ([install instructions](https://github.com/astral-sh/uv))

## Installation

### 1. Clone or Download

```bash
cd gdelt-mcp
```

### 2. Set Up Environment Variables

Create a `.env` file with your GCP service account credentials:

```env
# GCP Service Account Credentials (for BigQuery access)
GCP_PROJECT_ID=your-project-id
GCP_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
GCP_CLIENT_EMAIL=your-service-account@your-project.iam.gserviceaccount.com
```

**Important**: Only the actual secrets are stored in `.env`. The client uses these credentials directly without creating a JSON file.

### 3. Install Dependencies

Using uv (automatically creates virtual environment):

```bash
uv sync
```

This will:
- Create a virtual environment
- Install all dependencies from `pyproject.toml`
- Lock dependencies in `uv.lock`

## Configuration

### GCP Credentials

The server uses environment variables from `.env` for authentication. This approach:
- ✅ Keeps secrets out of version control
- ✅ Works seamlessly in containerized/cloud environments  
- ✅ Minimal configuration - only 3 variables needed

**To set up credentials:**

1. Create a GCP service account with these permissions:
   - `bigquery.jobs.create`
   - `bigquery.tables.getData`
   - `bigquery.tables.get`

2. Download the service account JSON key file

3. Copy only the secret values from the JSON into your `.env` file:
   - `project_id` → `GCP_PROJECT_ID`
   - `private_key` → `GCP_PRIVATE_KEY` (keep the `\n` newline characters)
   - `client_email` → `GCP_CLIENT_EMAIL`

4. The server will use these credentials directly (no JSON file created)

**Note**: The `.gitignore` file is configured to exclude `.env` from version control for security.

The service account needs read access to the GDELT BigQuery dataset: `gdelt-bq.gdeltv2.*`

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

### Schema Tools

- `get_events_schema()` - Returns complete Events table schema
- `get_eventmentions_schema()` - Returns EventMentions table schema
- `get_gkg_schema()` - Returns GKG table schema
- `get_cloudvision_schema()` - Returns CloudVision table schema

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
