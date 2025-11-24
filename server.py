"""GDELT 2.0 MCP Server - Provides access to GDELT BigQuery tables and CAMEO taxonomies."""

import os
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.server.dependencies import get_http_headers

from bigquery_client import GDELTBigQueryClient
from schema_docs import get_table_schema, get_all_schemas, get_sample_queries
from cameo_lookups import (
    CAMEO_EVENT_CODES,
    CAMEO_COUNTRY_CODES,
    CAMEO_TYPE_CODES,
    get_event_code_description,
    get_country_name,
    get_actor_type_description,
    search_event_codes,
    get_event_codes_by_category,
    get_all_cameo_data,
)

# Load environment variables
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP("GDELT 2.0")

# Initialize BigQuery client (will be lazy-loaded)
_bq_client: Optional[GDELTBigQueryClient] = None



def get_credentials_from_token() -> Optional[Tuple[str, str, str]]:
    """
    Extract and validate GCP credentials from Bearer token.
    
    Token format: project_id|private_key|client_email
    
    Returns:
        Tuple of (project_id, private_key, client_email) or None if invalid
    """
    headers = get_http_headers()
    auth_header = headers.get("authorization", "")
    
    if not auth_header.startswith("Bearer "):
        return None
    
    token = auth_header[7:]  # Remove "Bearer " prefix
    parts = token.split("|")
    
    if len(parts) != 3:
        return None
    
    project_id, private_key, client_email = parts
    
    # Basic validation
    if not project_id or not private_key.startswith("-----BEGIN PRIVATE KEY-----") or "@" not in client_email:
        return None
    
    return (project_id, private_key, client_email)


# ============================================================================
# RESOURCES - Provide access to table schemas and documentation
# ============================================================================

@mcp.resource("gdelt://events/schema")
def get_events_schema_resource() -> str:
    """Schema and documentation for the GDELT Events table."""
    schema = get_table_schema("events")
    return f"""# GDELT Events Table Schema

**Table:** {schema['table_name']}

**Description:** {schema['description']}

## Fields

{chr(10).join([f"- **{field['name']}** ({field['type']}): {field['description']}" for field in schema['fields']])}

## Sample Queries

{chr(10).join([f"### {q['description']}{chr(10)}```sql{chr(10)}{q['query']}{chr(10)}```{chr(10)}" for q in schema['sample_queries']])}
"""


@mcp.resource("gdelt://eventmentions/schema")
def get_eventmentions_schema_resource() -> str:
    """Schema and documentation for the GDELT EventMentions table."""
    schema = get_table_schema("eventmentions")
    return f"""# GDELT EventMentions Table Schema

**Table:** {schema['table_name']}

**Description:** {schema['description']}

## Fields

{chr(10).join([f"- **{field['name']}** ({field['type']}): {field['description']}" for field in schema['fields']])}

## Sample Queries

{chr(10).join([f"### {q['description']}{chr(10)}```sql{chr(10)}{q['query']}{chr(10)}```{chr(10)}" for q in schema['sample_queries']])}
"""


@mcp.resource("gdelt://gkg/schema")
def get_gkg_schema_resource() -> str:
    """Schema and documentation for the GDELT GKG (Global Knowledge Graph) table."""
    schema = get_table_schema("gkg")
    return f"""# GDELT GKG (Global Knowledge Graph) Table Schema

**Table:** {schema['table_name']}

**Description:** {schema['description']}

## Fields

{chr(10).join([f"- **{field['name']}** ({field['type']}): {field['description']}" for field in schema['fields']])}

## Sample Queries

{chr(10).join([f"### {q['description']}{chr(10)}```sql{chr(10)}{q['query']}{chr(10)}```{chr(10)}" for q in schema['sample_queries']])}
"""


@mcp.resource("gdelt://cloudvision/schema")
def get_cloudvision_schema_resource() -> str:
    """Schema and documentation for the GDELT CloudVision table."""
    schema = get_table_schema("cloudvision")
    return f"""# GDELT CloudVision Table Schema

**Table:** {schema['table_name']}

**Description:** {schema['description']}

## Fields

{chr(10).join([f"- **{field['name']}** ({field['type']}): {field['description']}" for field in schema['fields']])}

## Sample Queries

{chr(10).join([f"### {q['description']}{chr(10)}```sql{chr(10)}{q['query']}{chr(10)}```{chr(10)}" for q in schema['sample_queries']])}
"""


# ============================================================================
# QUERY TOOLS - Execute queries on GDELT tables
# ============================================================================

@mcp.tool()
def query_events(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100,
    order_by: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Query the GDELT Events table for structured event data about interactions between actors worldwide.
    
    Use this tool when you need to analyze:
    - Global events and actions between countries, organizations, or individuals
    - Who did what to whom (Actor1 -> Event -> Actor2 relationships)
    - Event locations, dates, and geographic coordinates
    - Event impact measures (Goldstein scale, number of mentions, sources, articles)
    - Specific event types using CAMEO codes (e.g., military actions, protests, negotiations)
    
    This is the primary table for event-level analysis. Each row represents a unique event with actors, 
    action codes, and contextual information.

    ! This table is very large - ALWAYS use a WHERE clause !
    
    PERFORMANCE OPTIMIZATION: Queries automatically use partition pruning when you include SQLDATE filters
    (e.g., "SQLDATE >= 20240101"). Always include date filters to make queries faster and cheaper.
    
    Args:
        where_clause: SQL WHERE clause without the WHERE keyword (e.g., "Actor1CountryCode = 'USA'")
        select_fields: Comma-separated list of fields to select (default: all fields)
        limit: Maximum number of rows to return (default: 100, max: 10000)
        order_by: ORDER BY clause without the ORDER BY keyword (e.g., "SQLDATE DESC")
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_events(where_clause="EventRootCode = '19' AND SQLDATE >= 20240101", limit=50)
    """
    # Get credentials from Bearer token
    credentials = get_credentials_from_token()
    
    if not credentials:
        return [{
            "error": "Authentication required",
            "message": "Please provide a valid Bearer token in the Authorization header",
            "format": "project_id|private_key|client_email",
            "instructions": "See README for instructions on obtaining and formatting your GCP credentials"
        }]
    
    project_id, private_key, client_email = credentials
    
    try:
        # Create client with user's credentials
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        limit = min(limit, 10000)  # Cap at 10000
        
        results = client.query(
            table=client.EVENTS_TABLE,
            where_clause=where_clause,
            select_fields=select_fields,
            limit=limit
        )
        
        return results
    except Exception as e:
        return [{
            "error": "Query failed",
            "message": str(e),
            "help": "Verify your GCP credentials have BigQuery access to gdelt-bq.gdeltv2 dataset"
        }]


@mcp.tool()
def query_eventmentions(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query the GDELT EventMentions table for detailed source information about how events are mentioned in media.
    
    Use this tool when you need to:
    - Find all news articles and sources that mention a specific event (by GLOBALEVENTID)
    - Analyze media coverage and sentiment for events
    - Track which news sources reported on an event
    - Get confidence scores for event mentions
    - Find URLs and document identifiers for event coverage
    
    This table contains one row for each time an event appears in media, linking back to events via GLOBALEVENTID.
    Use this after querying the Events table to drill into media mentions and sources.

    ! This table is very large - ALWAYS use a WHERE clause !
    
    Args:
        where_clause: SQL WHERE clause without the WHERE keyword
        select_fields: Comma-separated list of fields to select (default: all fields)
        limit: Maximum number of rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_eventmentions(where_clause="GLOBALEVENTID = 1234567890", limit=50)
    """
    # Get credentials from Bearer token
    credentials = get_credentials_from_token()
    
    if not credentials:
        return [{
            "error": "Authentication required",
            "message": "Please provide a valid Bearer token in the Authorization header",
            "format": "project_id|private_key|client_email",
            "instructions": "See README for instructions on obtaining and formatting your GCP credentials"
        }]
    
    project_id, private_key, client_email = credentials
    
    try:
        # Create client with user's credentials
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        limit = min(limit, 10000)
        
        results = client.query(
            table=client.EVENTMENTIONS_TABLE,
            where_clause=where_clause,
            select_fields=select_fields,
            limit=limit
        )
        
        return results
    except Exception as e:
        return [{
            "error": "Query failed",
            "message": str(e),
            "help": "Verify your GCP credentials have BigQuery access to gdelt-bq.gdeltv2 dataset"
        }]


@mcp.tool()
def query_gkg(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query the GDELT GKG (Global Knowledge Graph) table for rich contextual information from news documents.
    
    Use this tool when you need to analyze:
    - Themes and topics in news coverage (PROTEST, ECONOMY, TERRORISM, etc.)
    - Named entities: persons, organizations, and locations mentioned in articles
    - Geographic locations and coordinates from news content
    - Emotional tone and sentiment of articles
    - Counts and measures extracted from news text
    - Source URLs and document metadata
    
    The GKG provides deeper semantic analysis of news content beyond just events. Each row represents 
    a processed news document with extracted knowledge. Use this for topic analysis, sentiment tracking,
    and entity extraction from global news.

    ! This table is very large - ALWAYS use a WHERE clause !
    
    PERFORMANCE OPTIMIZATION: Queries automatically use partition pruning when you include DATE filters
    (e.g., "DATE >= 20240101000000"). Always include date filters to make queries faster and cheaper.
    
    Args:
        where_clause: SQL WHERE clause without the WHERE keyword
        select_fields: Comma-separated list of fields to select (default: all fields)
        limit: Maximum number of rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_gkg(where_clause="Themes LIKE '%PROTEST%' AND DATE >= 20240101000000", limit=50)
    """
    # Get credentials from Bearer token
    credentials = get_credentials_from_token()
    
    if not credentials:
        return [{
            "error": "Authentication required",
            "message": "Please provide a valid Bearer token in the Authorization header",
            "format": "project_id|private_key|client_email",
            "instructions": "See README for instructions on obtaining and formatting your GCP credentials"
        }]
    
    project_id, private_key, client_email = credentials
    
    try:
        # Create client with user's credentials
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        limit = min(limit, 10000)
        
        results = client.query(
            table=client.GKG_TABLE,
            where_clause=where_clause,
            select_fields=select_fields,
            limit=limit
        )
        
        return results
    except Exception as e:
        return [{
            "error": "Query failed",
            "message": str(e),
            "help": "Verify your GCP credentials have BigQuery access to gdelt-bq.gdeltv2 dataset"
        }]


@mcp.tool()
def query_cloudvision(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query the GDELT CloudVision table for visual analysis of images embedded in news articles.
    
    Use this tool when you need to analyze:
    - Visual content from news images (detected objects, labels, scenes)
    - Facial recognition and emotion detection in news photos
    - Optical Character Recognition (OCR) text from images
    - Logo and brand detection in visual content
    - Safe search classifications (violence, adult content, etc.)
    - Image metadata and source URLs
    
    This table contains computer vision analysis powered by Google Cloud Vision API. Each row represents
    an image from news coverage with detailed visual analysis. Use this to understand visual narratives,
    detect objects/people in news imagery, or extract text from photos.

    ! This table is very large - ALWAYS use a WHERE clause !
    
    Args:
        where_clause: SQL WHERE clause without the WHERE keyword
        select_fields: Comma-separated list of fields to select (default: all fields)
        limit: Maximum number of rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_cloudvision(where_clause="labels LIKE '%protest%'", limit=50)
    """
    # Get credentials from Bearer token
    credentials = get_credentials_from_token()
    
    if not credentials:
        return [{
            "error": "Authentication required",
            "message": "Please provide a valid Bearer token in the Authorization header",
            "format": "project_id|private_key|client_email",
            "instructions": "See README for instructions on obtaining and formatting your GCP credentials"
        }]
    
    project_id, private_key, client_email = credentials
    
    try:
        # Create client with user's credentials
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        limit = min(limit, 10000)
        
        results = client.query(
            table=client.CLOUDVISION_TABLE,
            where_clause=where_clause,
            select_fields=select_fields,
            limit=limit
        )
        
        return results
    except Exception as e:
        return [{
            "error": "Query failed",
            "message": str(e),
            "help": "Verify your GCP credentials have BigQuery access to gdelt-bq.gdeltv2 dataset"
        }]


# ============================================================================
# CAMEO TAXONOMY TOOLS - Access event and actor code lookups
# ============================================================================

@mcp.tool()
def get_cameo_event_codes(
    category: Optional[str] = None,
    search_keyword: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get CAMEO event code taxonomy for understanding and constructing event queries.
    
    Use this tool to:
    - Lookup event code definitions before querying the Events table
    - Find the right EventRootCode or EventCode for your analysis
    - Discover what types of events are available (protests, military actions, negotiations, etc.)
    - Search for event codes by keyword (e.g., "protest", "conflict", "agreement")
    - Browse event codes by category (01-20, each representing a class of events)
    
    CAMEO codes are hierarchical: root codes (e.g., "19") represent broad categories, while specific 
    codes (e.g., "193") represent detailed event types. Always use this before querying events to 
    ensure you're using the correct codes.
    
    Args:
        category: Optional two-digit category code to filter by (e.g., "01", "19")
        search_keyword: Optional keyword to search in event descriptions
    
    Returns:
        Dictionary of event codes and their descriptions
    
    Examples:
        get_cameo_event_codes(category="19")  # Get all military force events
        get_cameo_event_codes(search_keyword="protest")  # Search for protest-related events
    """
    if search_keyword:
        codes = search_event_codes(search_keyword)
        return {
            "search_keyword": search_keyword,
            "count": len(codes),
            "codes": codes
        }
    elif category:
        codes = get_event_codes_by_category(category)
        return {
            "category": category,
            "count": len(codes),
            "codes": codes
        }
    else:
        return {
            "count": len(CAMEO_EVENT_CODES),
            "codes": CAMEO_EVENT_CODES
        }


@mcp.tool()
def get_cameo_actor_codes(code_type: str = "all") -> Dict[str, Any]:
    """
    Get CAMEO actor code taxonomy for understanding and filtering by actors in GDELT data.
    
    Use this tool to:
    - Lookup country codes (3-letter ISO codes like 'USA', 'CHN', 'RUS') for Actor queries
    - Find actor type codes (GOV=Government, MIL=Military, COP=Police, etc.) 
    - Understand actor classification before querying Events or other tables
    - Construct WHERE clauses with the correct actor identifiers
    
    Actor codes help identify WHO is involved in events. Country codes identify national actors,
    while type codes classify the kind of actor (government, military, rebel, media, etc.).
    Use this reference before filtering by Actor1CountryCode, Actor2CountryCode, or actor types.
    
    Args:
        code_type: Type of codes to return - "countries", "types", or "all" (default: "all")
    
    Returns:
        Dictionary of actor codes and their descriptions
    
    Examples:
        get_cameo_actor_codes(code_type="countries")  # Get all country codes
        get_cameo_actor_codes(code_type="types")  # Get actor type codes (GOV, MIL, etc.)
    """
    if code_type == "countries":
        return {
            "type": "country_codes",
            "count": len(CAMEO_COUNTRY_CODES),
            "codes": CAMEO_COUNTRY_CODES
        }
    elif code_type == "types":
        return {
            "type": "actor_type_codes",
            "count": len(CAMEO_TYPE_CODES),
            "codes": CAMEO_TYPE_CODES
        }
    else:
        return {
            "type": "all",
            "country_codes": CAMEO_COUNTRY_CODES,
            "actor_type_codes": CAMEO_TYPE_CODES,
            "total_count": len(CAMEO_COUNTRY_CODES) + len(CAMEO_TYPE_CODES)
        }


# ============================================================================
# Run the server
# ============================================================================

if __name__ == "__main__":
    mcp.run()
