"""GDELT 2.0 MCP Server - Provides access to GDELT BigQuery tables and CAMEO taxonomies."""

import os
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from fastmcp import FastMCP

from utils.auth import get_credentials_from_token
from resources.schemas import (
    get_events_schema_resource_impl,
    get_eventmentions_schema_resource_impl,
    get_gkg_schema_resource_impl,
    get_cloudvision_schema_resource_impl,
    get_cost_optimization_guide_impl,
)
from tools.query_tools import (
    query_events_impl,
    query_eventmentions_impl,
    query_gkg_impl,
    query_cloudvision_impl,
)
from tools.cost_optimization import (
    estimate_query_cost_impl,
    create_materialized_subset_impl,
    list_materialized_subsets_impl,
    query_materialized_subset_impl,
    extend_subset_expiration_impl,
    delete_materialized_subset_impl,
)
from tools.cameo_tools import (
    get_cameo_event_codes_impl,
    get_cameo_actor_codes_impl,
)

# Load environment variables
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP("GDELT 2.0")


# ============================================================================
# RESOURCES
# ============================================================================

@mcp.resource("gdelt://events/schema")
def get_events_schema_resource() -> str:
    """Schema and documentation for the GDELT Events table."""
    return get_events_schema_resource_impl()


@mcp.resource("gdelt://eventmentions/schema")
def get_eventmentions_schema_resource() -> str:
    """Schema and documentation for the GDELT EventMentions table."""
    return get_eventmentions_schema_resource_impl()


@mcp.resource("gdelt://gkg/schema")
def get_gkg_schema_resource() -> str:
    """Schema and documentation for the GDELT GKG (Global Knowledge Graph) table."""
    return get_gkg_schema_resource_impl()


@mcp.resource("gdelt://cloudvision/schema")
def get_cloudvision_schema_resource() -> str:
    """Schema and documentation for the GDELT CloudVision table."""
    return get_cloudvision_schema_resource_impl()


@mcp.resource("gdelt://best-practices/cost-optimization")
def get_cost_optimization_guide() -> str:
    """REQUIRED READING: Cost-effective querying workflow for GDELT BigQuery data."""
    return get_cost_optimization_guide_impl()


# ============================================================================
# QUERY TOOLS
# ============================================================================

@mcp.tool()
def query_events(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100,
    order_by: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Query the GDELT Events table for structured event data."""
    credentials = get_credentials_from_token()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token"}]
    return query_events_impl(credentials, where_clause, select_fields, limit, order_by)


@mcp.tool()
def query_eventmentions(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Query the GDELT EventMentions table."""
    credentials = get_credentials_from_token()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token"}]
    return query_eventmentions_impl(credentials, where_clause, select_fields, limit)


@mcp.tool()
def query_gkg(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Query the GDELT GKG (Global Knowledge Graph) table."""
    credentials = get_credentials_from_token()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token"}]
    return query_gkg_impl(credentials, where_clause, select_fields, limit)


@mcp.tool()
def query_cloudvision(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Query the GDELT CloudVision table."""
    credentials = get_credentials_from_token()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token"}]
    return query_cloudvision_impl(credentials, where_clause, select_fields, limit)


# ============================================================================
# COST OPTIMIZATION TOOLS
# ============================================================================

@mcp.tool()
def estimate_query_cost(
    table: str,
    where_clause: Optional[str] = None,
    select_fields: str = "*"
) -> Dict[str, Any]:
    """Estimate the cost of a query before executing it."""
    credentials = get_credentials_from_token()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token"}
    return estimate_query_cost_impl(credentials, table, where_clause, select_fields)


@mcp.tool()
def create_materialized_subset(
    source_table: str,
    subset_name: str,
    where_clause: str,
    select_fields: str = "*",
    description: Optional[str] = None,
    expiration_hours: int = 48
) -> Dict[str, Any]:
    """Create a materialized subset table from GDELT data."""
    credentials = get_credentials_from_token()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token"}
    return create_materialized_subset_impl(
        credentials, source_table, subset_name, where_clause, select_fields, description, expiration_hours
    )


@mcp.tool()
def list_materialized_subsets() -> List[Dict[str, Any]]:
    """List all materialized subset tables in your project."""
    credentials = get_credentials_from_token()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token"}]
    return list_materialized_subsets_impl(credentials)


@mcp.tool()
def query_materialized_subset(
    subset_name: str,
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """Query a materialized subset table."""
    credentials = get_credentials_from_token()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token"}]
    return query_materialized_subset_impl(credentials, subset_name, where_clause, select_fields, limit)


@mcp.tool()
def extend_subset_expiration(
    subset_name: str,
    additional_hours: int = 48
) -> Dict[str, Any]:
    """Extend the expiration time of a materialized subset."""
    credentials = get_credentials_from_token()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token"}
    return extend_subset_expiration_impl(credentials, subset_name, additional_hours)


@mcp.tool()
def delete_materialized_subset(subset_name: str) -> Dict[str, Any]:
    """Delete a materialized subset table."""
    credentials = get_credentials_from_token()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token"}
    return delete_materialized_subset_impl(credentials, subset_name)


# ============================================================================
# CAMEO TAXONOMY TOOLS
# ============================================================================

@mcp.tool()
def get_cameo_event_codes(
    category: Optional[str] = None,
    search_keyword: Optional[str] = None
) -> Dict[str, Any]:
    """Get CAMEO event code taxonomy."""
    return get_cameo_event_codes_impl(category, search_keyword)


@mcp.tool()
def get_cameo_actor_codes(code_type: str = "all") -> Dict[str, Any]:
    """Get CAMEO actor code taxonomy."""
    return get_cameo_actor_codes_impl(code_type)


# ============================================================================
# Run the server
# ============================================================================

if __name__ == "__main__":
    mcp.run()
