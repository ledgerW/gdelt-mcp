"""GDELT 2.0 MCP Server - Provides access to GDELT BigQuery tables and CAMEO taxonomies."""

import os
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from fastmcp import FastMCP

from utils.auth import get_credentials_from_token, get_credentials_from_env
from resources import (
    get_events_schema_resource_impl,
    get_eventmentions_schema_resource_impl,
    get_gkg_schema_resource_impl,
    get_cloudvision_schema_resource_impl,
    get_usage_guide_impl,
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


@mcp.resource("gdelt://guide/usage")
def get_usage_guide_resource() -> str:
    """REQUIRED READING: GDELT BigQuery usage guide - cost-effective querying workflows."""
    return get_usage_guide_impl()


# ============================================================================
# RESOURCE FETCHING TOOLS
# ============================================================================

@mcp.tool(tags=["schema"])
def get_events_schema() -> str:
    """
    Get the GDELT Events table schema documentation.
    
    Use this tool to understand the Events table structure, available fields, and sample queries
    before querying event data.
    
    Returns:
        Complete schema documentation for the Events table
    """
    return get_events_schema_resource_impl()


@mcp.tool(tags=["schema"])
def get_eventmentions_schema() -> str:
    """
    Get the GDELT EventMentions table schema documentation.
    
    Use this tool to understand the EventMentions table structure, available fields, and sample
    queries before querying media mentions data.
    
    Returns:
        Complete schema documentation for the EventMentions table
    """
    return get_eventmentions_schema_resource_impl()


@mcp.tool(tags=["schema"])
def get_gkg_schema() -> str:
    """
    Get the GDELT GKG (Global Knowledge Graph) table schema documentation.
    
    Use this tool to understand the GKG table structure, available fields, and sample queries
    before querying semantic content, themes, and entities.
    
    Returns:
        Complete schema documentation for the GKG table
    """
    return get_gkg_schema_resource_impl()


@mcp.tool(tags=["schema"])
def get_cloudvision_schema() -> str:
    """
    Get the GDELT CloudVision table schema documentation.
    
    Use this tool to understand the CloudVision table structure, available fields, and sample
    queries before querying visual analysis data.
    
    Returns:
        Complete schema documentation for the CloudVision table
    """
    return get_cloudvision_schema_resource_impl()


@mcp.tool(tags=["guide"])
def get_usage_guide() -> str:
    """
    ⚠️ REQUIRED READING BEFORE ANY GDELT QUERY ⚠️
    
    Get the comprehensive GDELT usage guide with the recommended workflow.
    
    READ THIS FIRST to avoid expensive queries and runaway costs. The GDELT dataset is MASSIVE
    (terabytes on BigQuery). A single poorly-formed query can cost $1-50+. This guide presents
    the ONE SAFE WORKFLOW that prevents surprise charges while enabling powerful analysis.
    
    The guide covers:
    - The materialized-first workflow (DEFAULT approach - check existing subsets, create if needed)
    - How to query cost-effectively (date filters, table selection, column selection)
    - Table-specific guidance (Events, Mentions, GKG, CloudVision)
    - Tool reference organized by workflow step
    
    Returns:
        Complete usage guide with recommended workflow and cost-avoidance techniques
    """
    return get_usage_guide_impl()


# ============================================================================
# QUERY TOOLS
# ============================================================================

@mcp.tool(tags=["query"])
def query_events(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100,
    order_by: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Query the GDELT Events table for structured event data.
    
    Use this tool to analyze global events and actor relationships. This is the SMALLEST GDELT
    table - always query this first before EventMentions or GKG. Each row represents a unique
    event with actors, action codes, and contextual information.
    
    Use when you need: global events, who-did-what-to-whom relationships, event locations and dates,
    impact measures (Goldstein scale), or specific event types via CAMEO codes.
    
    ⚠️ COST: ~$0.0002-0.001 per day WITH date filters. WITHOUT date filters can scan 50GB+ → $0.25+
    
    Args:
        where_clause: SQL WHERE clause without WHERE keyword. MUST include "SQLDATE >= YYYYMMDD"
        select_fields: Comma-separated field names (default: "*")
        limit: Maximum rows to return (default: 100, max: 10000)
        order_by: ORDER BY clause without ORDER BY keyword (e.g., "SQLDATE DESC")
    
    Returns:
        List of dictionaries representing query results
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return query_events_impl(credentials, where_clause, select_fields, limit, order_by)


@mcp.tool(tags=["query"])
def query_eventmentions(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query the GDELT EventMentions table for media source information.
    
    Use this tool to find news articles and sources that mention specific events, analyze media
    coverage and sentiment, or track which news sources reported on an event. This table contains
    one row for each time an event appears in media, linking back to Events via GLOBALEVENTID.
    
    Use when you need: article-level mentions of events, media coverage analysis, source URLs,
    confidence scores, or to drill into specific event coverage after querying Events.
    
    ⚠️ COST: ~10x larger than Events. Query Events with date filters first, then filter by
    GLOBALEVENTID here. Without filters can scan 10GB+ → $0.05+
    
    Args:
        where_clause: SQL WHERE clause without WHERE keyword. Filter by GLOBALEVENTID from Events
        select_fields: Comma-separated field names (default: "*")
        limit: Maximum rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing query results
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return query_eventmentions_impl(credentials, where_clause, select_fields, limit)


@mcp.tool(tags=["query"])
def query_gkg(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query the GDELT GKG (Global Knowledge Graph) table for semantic content.
    
    Use this tool for deep semantic analysis of news content beyond just events. Each row represents
    a processed news document with extracted themes, entities, locations, sentiment, and metadata.
    This is 20x LARGER than Events - only use when Events/Mentions can't answer your question.
    
    Use when you need: themes and topics (PROTEST, ECONOMY, etc.), named entities (persons,
    organizations, locations), geographic coordinates from content, emotional tone and sentiment,
    or extracted counts and measures.
    
    🚨 CRITICAL: GKG is the MOST EXPENSIVE table. WITH date filter ~$0.025/day. WITHOUT date
    filter can scan 2.5TB+ → $12.50+. STRONGLY use materialization for analysis.
    
    Args:
        where_clause: SQL WHERE clause without WHERE keyword. MUST include "DATE >= YYYYMMDDhhmmss"
        select_fields: Comma-separated field names (strongly recommend specific fields, not "*")
        limit: Maximum rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing query results
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return query_gkg_impl(credentials, where_clause, select_fields, limit)


@mcp.tool(tags=["query"])
def query_cloudvision(
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query the GDELT CloudVision table for visual analysis of news images.
    
    Use this tool to analyze visual content from news images. Each row represents an image with
    Google Cloud Vision API analysis including detected objects, labels, facial recognition,
    OCR text, logos, and safe search classifications.
    
    Use when you need: visual content analysis (objects, labels, scenes), facial recognition and
    emotions, OCR text from images, logo/brand detection, or safe search classifications.
    
    ⚠️ COST: Can be large depending on coverage. Include timestamp filters. Without filters
    can scan 5GB+ → $0.025+
    
    Args:
        where_clause: SQL WHERE clause without WHERE keyword. Include timestamp >= YYYYMMDDhhmmss
        select_fields: Comma-separated field names (default: "*")
        limit: Maximum rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing query results
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return query_cloudvision_impl(credentials, where_clause, select_fields, limit)


# ============================================================================
# COST OPTIMIZATION TOOLS
# ============================================================================

@mcp.tool(tags=["cost"])
def estimate_query_cost(
    table: str,
    where_clause: Optional[str] = None,
    select_fields: str = "*"
) -> Dict[str, Any]:
    """
    Estimate query cost before executing (dry-run only, no actual query).
    
    Use this tool to check the cost before running a query on large tables. Performs a BigQuery
    dry-run that estimates bytes scanned without executing. Always use this for exploratory
    queries on GKG or EventMentions to verify date filters are effective and avoid accidents.
    
    Use when you need: cost verification before running queries, to check if date filters are
    working, or to get warnings about expensive operations.
    
    Args:
        table: Table name - "events", "eventmentions", "gkg", or "cloudvision"
        where_clause: Optional WHERE clause without WHERE keyword
        select_fields: Field names to select (default: "*")
    
    Returns:
        Dictionary with bytes_processed, gb_processed, and estimated_cost_usd
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}
    return estimate_query_cost_impl(credentials, table, where_clause, select_fields)


@mcp.tool(tags=["cost"])
def create_materialized_subset(
    source_table: str,
    subset_name: str,
    where_clause: str,
    select_fields: str = "*",
    description: Optional[str] = None,
    expiration_hours: int = 48
) -> Dict[str, Any]:
    """
    Create a materialized subset for cost-effective iterative analysis.
    
    RECOMMENDED WORKFLOW: Create subset once with tight date filters, then query it multiple times
    for near-free iteration. This is 50-100x cheaper than querying GDELT directly multiple times.
    Subsets auto-expire after 48 hours to prevent storage costs.
    
    Use when you need: iterative data exploration, multi-step analysis, to share filtered data,
    or to avoid repeated expensive queries.
    
    Args:
        source_table: Source - "events", "eventmentions", "gkg", or "cloudvision"
        subset_name: Subset name (alphanumeric and underscores only, e.g., "ukraine_jan2025")
        where_clause: WHERE clause to filter data. MUST include date filters for cost-effectiveness
        select_fields: Fields to select (default: "*", but consider selecting only needed fields)
        description: Optional description for documentation
        expiration_hours: Hours until auto-deletion (default: 48)
    
    Returns:
        Dictionary with creation status, cost estimate, rows created, and expiration info
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}
    return create_materialized_subset_impl(
        credentials, source_table, subset_name, where_clause, select_fields, description, expiration_hours
    )


@mcp.tool(tags=["cost"])
def list_materialized_subsets() -> List[Dict[str, Any]]:
    """
    List all materialized subset tables with metadata.
    
    Use this tool to see available subsets, check expiration status, monitor storage usage, or
    find subsets that need extension or cleanup. Shows name, size, row count, expiration status,
    and description for each subset.
    
    Returns:
        List of dictionaries with subset metadata
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return list_materialized_subsets_impl(credentials)


@mcp.tool(tags=["cost"])
def query_materialized_subset(
    subset_name: str,
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """
    Query a materialized subset (near-free operation).
    
    This is step 2 in the cost-optimization workflow. After creating a subset once (~$0.001-0.01),
    query it many times (~$0.00001 each). Enables fast iteration, cheap analysis on filtered data,
    and quick prototyping without re-filtering the full GDELT tables.
    
    Use when you need: to iterate on analysis, run multiple analyses on same filtered data, or
    test queries quickly without cost concerns.
    
    Args:
        subset_name: Subset name (from list_materialized_subsets)
        where_clause: Optional additional WHERE clause for further filtering
        select_fields: Fields to select (default: "*")
        limit: Maximum rows to return (default: 1000, max: 10000)
    
    Returns:
        List of dictionaries representing query results
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return query_materialized_subset_impl(credentials, subset_name, where_clause, select_fields, limit)


@mcp.tool(tags=["cost"])
def extend_subset_expiration(
    subset_name: str,
    additional_hours: int = 48
) -> Dict[str, Any]:
    """
    Extend the expiration time of a materialized subset.
    
    Use this tool when analysis takes longer than expected, you want to share the subset with
    team members, or a long-running project needs persistent data. By default, subsets auto-expire
    after 48 hours to prevent storage costs.
    
    Args:
        subset_name: Name of the subset to extend
        additional_hours: Hours to add to expiration (default: 48)
    
    Returns:
        Dictionary with update status and new expiration time
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}
    return extend_subset_expiration_impl(credentials, subset_name, additional_hours)


@mcp.tool(tags=["cost"])
def delete_materialized_subset(subset_name: str) -> Dict[str, Any]:
    """
    Delete a materialized subset table (manual cleanup).
    
    Use this tool when analysis is complete and the subset is no longer needed, you want to free
    storage quota, or need to recreate a subset with different parameters. Subsets auto-expire
    after 48 hours by default, but you can delete earlier.
    
    Args:
        subset_name: Name of the subset to delete
    
    Returns:
        Dictionary with deletion status
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}
    return delete_materialized_subset_impl(credentials, subset_name)


# ============================================================================
# CAMEO TAXONOMY TOOLS
# ============================================================================

@mcp.tool(tags=["cameo"])
def get_cameo_event_codes(
    category: Optional[str] = None,
    search_keyword: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get CAMEO event code taxonomy for understanding event types.
    
    Use this tool to lookup event code definitions before querying the Events table, find the
    right EventRootCode or EventCode for analysis, discover available event types (protests,
    military actions, negotiations, etc.), or search by keyword.
    
    CAMEO codes are hierarchical: root codes (e.g., "19") represent broad categories, specific
    codes (e.g., "193") represent detailed types. Always use this before querying events to
    ensure correct codes.
    
    Args:
        category: Optional two-digit category code to filter by (e.g., "01", "19")
        search_keyword: Optional keyword to search in event descriptions (e.g., "protest")
    
    Returns:
        Dictionary of event codes and descriptions
    """
    return get_cameo_event_codes_impl(category, search_keyword)


@mcp.tool(tags=["cameo"])
def get_cameo_actor_codes(code_type: str = "all") -> Dict[str, Any]:
    """
    Get CAMEO actor code taxonomy for understanding actors in events.
    
    Use this tool to lookup country codes (3-letter ISO like USA, CHN, RUS) for Actor queries,
    find actor type codes (GOV=Government, MIL=Military, COP=Police, etc.), or understand actor
    classification before querying Events.
    
    Actor codes identify WHO is involved in events. Country codes identify national actors, type
    codes classify the kind of actor (government, military, rebel, media, etc.). Use this reference
    before filtering by Actor1CountryCode, Actor2CountryCode, or actor types.
    
    Args:
        code_type: Type of codes - "countries", "types", or "all" (default: "all")
    
    Returns:
        Dictionary of actor codes and descriptions
    """
    return get_cameo_actor_codes_impl(code_type)


# ============================================================================
# Run the server
# ============================================================================

if __name__ == "__main__":
    mcp.run()
