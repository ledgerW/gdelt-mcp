"""GDELT 2.0 MCP Server - Provides access to GDELT BigQuery tables and CAMEO taxonomies."""

import os
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from fastmcp import FastMCP

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


def get_bq_client() -> GDELTBigQueryClient:
    """Get or create BigQuery client instance."""
    global _bq_client
    if _bq_client is None:
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp_bq.json")
        project_id = os.getenv("GCP_PROJECT_ID")
        _bq_client = GDELTBigQueryClient(credentials_path=credentials_path, project_id=project_id)
    return _bq_client


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
# SCHEMA TOOLS - Get schema information for tables
# ============================================================================

@mcp.tool()
def get_events_schema() -> Dict[str, Any]:
    """
    Get the complete schema for the GDELT Events table.
    
    Returns:
        Dictionary containing table name, description, fields, and sample queries
    """
    return get_table_schema("events")


@mcp.tool()
def get_eventmentions_schema() -> Dict[str, Any]:
    """
    Get the complete schema for the GDELT EventMentions table.
    
    Returns:
        Dictionary containing table name, description, fields, and sample queries
    """
    return get_table_schema("eventmentions")


@mcp.tool()
def get_gkg_schema() -> Dict[str, Any]:
    """
    Get the complete schema for the GDELT GKG (Global Knowledge Graph) table.
    
    Returns:
        Dictionary containing table name, description, fields, and sample queries
    """
    return get_table_schema("gkg")


@mcp.tool()
def get_cloudvision_schema() -> Dict[str, Any]:
    """
    Get the complete schema for the GDELT CloudVision table.
    
    Returns:
        Dictionary containing table name, description, fields, and sample queries
    """
    return get_table_schema("cloudvision")


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
    Query the GDELT Events table.
    
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
    client = get_bq_client()
    limit = min(limit, 10000)  # Cap at 10000
    
    # Build complete WHERE clause with ORDER BY if provided
    full_where = where_clause
    if order_by:
        # We'll append ORDER BY after the query is built in the client
        pass
    
    results = client.query(
        table=client.EVENTS_TABLE,
        where_clause=where_clause,
        select_fields=select_fields,
        limit=limit
    )
    
    return results


@mcp.tool()
def query_eventmentions(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query the GDELT EventMentions table.
    
    Args:
        where_clause: SQL WHERE clause without the WHERE keyword
        select_fields: Comma-separated list of fields to select (default: all fields)
        limit: Maximum number of rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_eventmentions(where_clause="GLOBALEVENTID = 1234567890", limit=50)
    """
    client = get_bq_client()
    limit = min(limit, 10000)
    
    results = client.query(
        table=client.EVENTMENTIONS_TABLE,
        where_clause=where_clause,
        select_fields=select_fields,
        limit=limit
    )
    
    return results


@mcp.tool()
def query_gkg(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query the GDELT GKG (Global Knowledge Graph) table.
    
    Args:
        where_clause: SQL WHERE clause without the WHERE keyword
        select_fields: Comma-separated list of fields to select (default: all fields)
        limit: Maximum number of rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_gkg(where_clause="Themes LIKE '%PROTEST%' AND DATE >= 20240101000000", limit=50)
    """
    client = get_bq_client()
    limit = min(limit, 10000)
    
    results = client.query(
        table=client.GKG_TABLE,
        where_clause=where_clause,
        select_fields=select_fields,
        limit=limit
    )
    
    return results


@mcp.tool()
def query_cloudvision(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query the GDELT CloudVision table.
    
    Args:
        where_clause: SQL WHERE clause without the WHERE keyword
        select_fields: Comma-separated list of fields to select (default: all fields)
        limit: Maximum number of rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_cloudvision(where_clause="labels LIKE '%protest%'", limit=50)
    """
    client = get_bq_client()
    limit = min(limit, 10000)
    
    results = client.query(
        table=client.CLOUDVISION_TABLE,
        where_clause=where_clause,
        select_fields=select_fields,
        limit=limit
    )
    
    return results


# ============================================================================
# CAMEO TAXONOMY TOOLS - Access event and actor code lookups
# ============================================================================

@mcp.tool()
def get_cameo_event_codes(
    category: Optional[str] = None,
    search_keyword: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get CAMEO event code taxonomy. These codes classify types of events (e.g., '19' for military force).
    
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
    Get CAMEO actor code taxonomy. Includes country codes, actor types, and other classifications.
    
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
