"""GDELT 2.0 MCP Server - Provides access to GDELT BigQuery tables and CAMEO taxonomies."""

import os
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from fastmcp import FastMCP

from utils.auth import get_credentials_from_token, get_credentials_from_env
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
    """
    Query the GDELT Events table for structured event data about interactions between actors worldwide.
    
    Use this tool when you need to analyze:
    - Global events and actions between countries, organizations, or individuals
    - Who did what to whom (Actor1 -> Event -> Actor2 relationships)
    - Event locations, dates, and geographic coordinates
    - Event impact measures (Goldstein scale, number of mentions, sources, articles)
    - Specific event types using CAMEO codes (e.g., military actions, protests, negotiations)
    
    This is the SMALLEST GDELT table - always query this first before EventMentions or GKG.
    Each row represents a unique event with actors, action codes, and contextual information.
    
    ⚠️ COST WARNING: Events table scans ~100-200MB per day without date filters.
       - ✅ WITH date filter (SQLDATE >= YYYYMMDD): ~$0.0002-0.001 per day
       - ❌ WITHOUT date filter: Can scan 50GB+ → $0.25+
       - 💡 TIP: Use create_materialized_subset for repeated analysis (50-100x cheaper)
    
    ✅ REQUIRED: Include SQLDATE >= YYYYMMDD filter in where_clause
    
    Args:
        where_clause: SQL WHERE clause without WHERE keyword (MUST include "SQLDATE >= YYYYMMDD")
        select_fields: Comma-separated fields (default: all)
        limit: Maximum number of rows to return (default: 100, max: 10000)
        order_by: ORDER BY clause without ORDER BY keyword (e.g., "SQLDATE DESC")
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_events(
            where_clause="EventRootCode = '19' AND SQLDATE >= 20250101 AND SQLDATE < 20250108",
            select_fields="SQLDATE, Actor1Name, Actor2Name, EventCode, GoldsteinScale",
            limit=100
        )
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return query_events_impl(credentials, where_clause, select_fields, limit, order_by)


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
    
    ⚠️ COST WARNING: EventMentions table is ~10x larger than Events.
       - ✅ RECOMMENDED: Query Events with date filters first, then use GLOBALEVENTID here
       - ✅ ALTERNATIVE: Include date range via MentionTimeDate or EventTimeDate filters
       - ❌ WITHOUT filters: Can scan 10GB+ → $0.05+
       - 💡 TIP: Use create_materialized_subset for repeated analysis
    
    💡 RECOMMENDED: Filter via GLOBALEVENTID from Events table queries (preferred workflow)
    
    Args:
        where_clause: SQL WHERE clause without WHERE keyword
        select_fields: Comma-separated fields (default: all)
        limit: Maximum number of rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_eventmentions(
            where_clause="GLOBALEVENTID = 1234567890",
            limit=50
        )
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return query_eventmentions_impl(credentials, where_clause, select_fields, limit)


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
    
    This table is 20x LARGER than Events - only use when Events/Mentions can't answer your question.
    The GKG provides deeper semantic analysis of news content beyond just events. Each row represents 
    a processed news document with extracted knowledge.
    
    🚨 CRITICAL COST WARNING: GKG is the LARGEST and MOST EXPENSIVE table!
       - ✅ WITH date filter (DATE >= YYYYMMDDhhmmss): ~$0.025 per day
       - ❌ WITHOUT date filter: Can scan 2.5TB+ → $12.50+
       - 💡 STRONGLY RECOMMENDED: Use create_materialized_subset (100x cheaper for analysis)
    
    ⚠️ REQUIRED: Include DATE >= YYYYMMDDhhmmss filter in where_clause
    ⚠️ RECOMMENDED: Keep date ranges to 1-7 days, select specific fields only
    
    Args:
        where_clause: SQL WHERE clause without WHERE keyword (MUST include "DATE >= YYYYMMDDhhmmss")
        select_fields: Comma-separated fields (strongly recommend specific fields, not "*")
        limit: Maximum number of rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_gkg(
            where_clause="Themes LIKE '%PROTEST%' AND DATE >= 20250101000000 AND DATE < 20250108000000",
            select_fields="DATE, Themes, V2Locations, V2Tone",
            limit=100
        )
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return query_gkg_impl(credentials, where_clause, select_fields, limit)


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
    
    ⚠️ COST WARNING: CloudVision table can be large depending on image coverage.
       - ✅ RECOMMENDED: Include timestamp filters for cost-effective queries
       - ❌ WITHOUT filters: Can scan 5GB+ → $0.025+
       - 💡 TIP: Use specific label/entity filters to reduce scanned data
    
    💡 RECOMMENDED: Include timestamp filters (format: YYYYMMDDhhmmss) in where_clause
    
    Args:
        where_clause: SQL WHERE clause without WHERE keyword
        select_fields: Comma-separated fields (default: all)
        limit: Maximum number of rows to return (default: 100, max: 10000)
    
    Returns:
        List of dictionaries representing the query results
    
    Example:
        query_cloudvision(
            where_clause="labels LIKE '%protest%' AND timestamp >= 20250101000000",
            limit=50
        )
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

@mcp.tool()
def estimate_query_cost(
    table: str,
    where_clause: Optional[str] = None,
    select_fields: str = "*"
) -> Dict[str, Any]:
    """
    Estimate the cost of a query BEFORE executing it (dry-run, no actual query).
    
    Use this tool to:
    - Check the cost of a query before running it
    - Verify that your date filters are effective
    - Avoid expensive accidents
    - Get a warning if you're about to scan >1GB
    
    This performs a BigQuery dry-run that estimates bytes scanned without executing the query.
    Always use this for exploratory queries on large tables (GKG, Mentions).
    
    Args:
        table: Table to query - "events", "eventmentions", "gkg", or "cloudvision"
        where_clause: Optional WHERE clause (without WHERE keyword)
        select_fields: Fields to select (default: all)
    
    Returns:
        Dictionary with bytes_processed, gb_processed, and estimated_cost_usd
    
    Example:
        estimate_query_cost(
            table="gkg",
            where_clause="Themes LIKE '%PROTEST%' AND DATE >= 20250101000000",
            select_fields="Themes, V2Locations"
        )
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}
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
    """
    Create a materialized subset table from GDELT data with auto-expiration.
    
    🎯 RECOMMENDED WORKFLOW for cost-effective analysis:
    1. Create subset once with tight date filters (this tool)
    2. Query subset multiple times for near-free iteration (use query_materialized_subset)
    3. Auto-cleanup after 48 hours (or manual delete)
    
    This is 50-100x cheaper than querying GDELT directly multiple times!
    
    Use this when you need to:
    - Explore data iteratively (try different queries on same dataset)
    - Perform multi-step analysis
    - Share filtered data with team members
    - Avoid repeated expensive queries
    
    ⚠️ IMPORTANT: Your where_clause MUST include date filters for cost-effectiveness:
    - Events: SQLDATE >= YYYYMMDD
    - GKG: DATE >= YYYYMMDDhhmmss
    
    Args:
        source_table: Source table - "events", "eventmentions", "gkg", or "cloudvision"
        subset_name: Name for subset (alphanumeric and underscores only, e.g., "ukraine_jan2025")
        where_clause: WHERE clause to filter data (MUST include date filters)
        select_fields: Fields to select (default: all - but consider selecting only needed fields)
        description: Optional description for documentation
        expiration_hours: Hours until auto-deletion (default: 48, prevents forgotten tables)
    
    Returns:
        Dictionary with creation status, cost estimate, rows created, and expiration info
    
    Example:
        create_materialized_subset(
            source_table="events",
            subset_name="ukraine_conflict_jan2025",
            where_clause="SQLDATE BETWEEN 20250101 AND 20250131 AND (Actor1CountryCode = 'UKR' OR Actor2CountryCode = 'UKR')",
            select_fields="SQLDATE, Actor1Name, Actor2Name, EventCode, GoldsteinScale, ActionGeo_Lat, ActionGeo_Long",
            description="Ukraine-related events for January 2025 analysis"
        )
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}
    return create_materialized_subset_impl(
        credentials, source_table, subset_name, where_clause, select_fields, description, expiration_hours
    )


@mcp.tool()
def list_materialized_subsets() -> List[Dict[str, Any]]:
    """
    List all materialized subset tables in your project.
    
    Shows all subsets you've created with metadata including:
    - Subset name and full table ID
    - Creation and expiration timestamps
    - Hours until expiration (or if already expired)
    - Size in MB and row count
    - Description
    
    Use this to:
    - See what subsets you have available
    - Check expiration status
    - Monitor storage usage
    - Find subsets that need extension or cleanup
    
    Returns:
        List of dictionaries with subset metadata
    
    Example response:
        [
            {
                "subset_name": "ukraine_jan2025",
                "size_mb": 45.2,
                "num_rows": 12543,
                "expires_in_hours": 36.5,
                "is_expired": false,
                "description": "Ukraine events January 2025"
            }
        ]
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return list_materialized_subsets_impl(credentials)


@mcp.tool()
def query_materialized_subset(
    subset_name: str,
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """
    Query a materialized subset table (near-free operation!).
    
    This is the second step in the recommended cost-optimization workflow:
    1. Create subset once → ~$0.001-0.01
    2. Query subset many times → ~$0.00001 each (THIS TOOL)
    
    Benefits:
    - ⚡ Fast: subset is much smaller than full GDELT
    - 💰 Cheap: queries cost ~$0.00001 vs $0.01+ on full tables
    - 🔄 Iterate: try different analyses without re-filtering
    
    Use this for:
    - Iterative data exploration
    - Multiple analyses on same filtered data
    - Quick prototyping and testing
    - Dashboard queries
    
    Args:
        subset_name: Name of the subset to query (from list_materialized_subsets)
        where_clause: Optional additional WHERE clause for further filtering
        select_fields: Fields to select (default: all)
        limit: Maximum rows to return (default: 1000, max: 10000)
    
    Returns:
        List of dictionaries representing query results
    
    Example:
        query_materialized_subset(
            subset_name="ukraine_jan2025",
            where_clause="EventCode LIKE '19%'",  # Filter to military actions
            select_fields="SQLDATE, EventCode, Actor1Name, Actor2Name",
            limit=500
        )
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return [{"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}]
    return query_materialized_subset_impl(credentials, subset_name, where_clause, select_fields, limit)


@mcp.tool()
def extend_subset_expiration(
    subset_name: str,
    additional_hours: int = 48
) -> Dict[str, Any]:
    """
    Extend the expiration time of a materialized subset.
    
    By default, subsets auto-expire after 48 hours to prevent forgotten tables from
    accumulating storage costs. Use this tool to keep a subset longer if needed.
    
    Use this when:
    - Analysis is taking longer than expected
    - You want to share subset with team members
    - Long-running project needs persistent data
    
    Args:
        subset_name: Name of the subset to extend
        additional_hours: Hours to add to expiration (default: 48)
    
    Returns:
        Dictionary with update status and new expiration time
    
    Example:
        extend_subset_expiration(
            subset_name="ukraine_jan2025",
            additional_hours=72  # Extend by 3 more days
        )
    """
    credentials = get_credentials_from_token()
    if not credentials:
        credentials = get_credentials_from_env()
    if not credentials:
        return {"error": "Authentication required", "message": "Please provide a valid Bearer token or set GCP environment variables"}
    return extend_subset_expiration_impl(credentials, subset_name, additional_hours)


@mcp.tool()
def delete_materialized_subset(subset_name: str) -> Dict[str, Any]:
    """
    Delete a materialized subset table (manual cleanup).
    
    Subsets auto-expire after 48 hours by default, but you can manually delete them
    earlier to clean up storage and avoid costs.
    
    Use this when:
    - Analysis is complete and subset no longer needed
    - Want to free up storage quota
    - Need to recreate subset with different parameters
    
    Args:
        subset_name: Name of the subset to delete
    
    Returns:
        Dictionary with deletion status
    
    Example:
        delete_materialized_subset(subset_name="ukraine_jan2025")
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
    return get_cameo_event_codes_impl(category, search_keyword)


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
    return get_cameo_actor_codes_impl(code_type)


# ============================================================================
# Run the server
# ============================================================================

if __name__ == "__main__":
    mcp.run()
