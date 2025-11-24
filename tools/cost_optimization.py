"""Cost optimization tools for GDELT MCP server."""

from typing import Any, Dict, List, Optional
from bigquery_client import GDELTBigQueryClient


def estimate_query_cost_impl(
    credentials: tuple,
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
        credentials: Tuple of (project_id, private_key, client_email)
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
    project_id, private_key, client_email = credentials
    
    try:
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        table_map = {
            "events": client.EVENTS_TABLE,
            "eventmentions": client.EVENTMENTIONS_TABLE,
            "gkg": client.GKG_TABLE,
            "cloudvision": client.CLOUDVISION_TABLE
        }
        
        if table not in table_map:
            return {
                "error": f"Invalid table name. Must be one of: {', '.join(table_map.keys())}"
            }
        
        full_table = table_map[table]
        
        query = f"SELECT {select_fields} FROM `{full_table}`"
        if where_clause:
            query += f" WHERE {where_clause}"
        query += " LIMIT 1000"
        
        cost_info = client.estimate_query_cost(query)
        
        if "gb_processed" in cost_info and cost_info["gb_processed"] > 1.0:
            cost_info["warning"] = "🔴 HIGH COST: This query will scan >1GB. Consider adding date filters or using materialization."
        elif "gb_processed" in cost_info and cost_info["gb_processed"] > 0.1:
            cost_info["info"] = "🟡 MODERATE COST: Consider tightening date filters or selecting fewer fields."
        elif "gb_processed" in cost_info:
            cost_info["info"] = "🟢 LOW COST: This query is well-optimized."
        
        return cost_info
        
    except Exception as e:
        return {
            "error": "Cost estimation failed",
            "message": str(e)
        }


def create_materialized_subset_impl(
    credentials: tuple,
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
        credentials: Tuple of (project_id, private_key, client_email)
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
    project_id, private_key, client_email = credentials
    
    try:
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        table_map = {
            "events": client.EVENTS_TABLE,
            "eventmentions": client.EVENTMENTIONS_TABLE,
            "gkg": client.GKG_TABLE,
            "cloudvision": client.CLOUDVISION_TABLE
        }
        
        if source_table not in table_map:
            return {
                "error": f"Invalid source_table. Must be one of: {', '.join(table_map.keys())}"
            }
        
        full_table = table_map[source_table]
        
        result = client.create_materialized_subset(
            source_table=full_table,
            subset_name=subset_name,
            where_clause=where_clause,
            select_fields=select_fields,
            description=description,
            expiration_hours=expiration_hours
        )
        
        return result
        
    except Exception as e:
        return {
            "error": "Subset creation failed",
            "message": str(e),
            "help": "Ensure WHERE clause includes date filters and subset_name uses only alphanumeric characters and underscores"
        }


def list_materialized_subsets_impl(credentials: tuple) -> List[Dict[str, Any]]:
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
    
    Args:
        credentials: Tuple of (project_id, private_key, client_email)
    
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
    project_id, private_key, client_email = credentials
    
    try:
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        subsets = client.list_materialized_subsets()
        return subsets
        
    except Exception as e:
        return [{
            "error": "Failed to list subsets",
            "message": str(e)
        }]


def query_materialized_subset_impl(
    credentials: tuple,
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
        credentials: Tuple of (project_id, private_key, client_email)
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
    project_id, private_key, client_email = credentials
    
    try:
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        limit = min(limit, 10000)
        
        results = client.query_materialized_subset(
            subset_name=subset_name,
            where_clause=where_clause,
            select_fields=select_fields,
            limit=limit
        )
        
        return results
        
    except Exception as e:
        return [{
            "error": "Query failed",
            "message": str(e),
            "help": "Verify the subset exists using list_materialized_subsets"
        }]


def extend_subset_expiration_impl(
    credentials: tuple,
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
        credentials: Tuple of (project_id, private_key, client_email)
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
    project_id, private_key, client_email = credentials
    
    try:
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        result = client.extend_subset_expiration(
            subset_name=subset_name,
            additional_hours=additional_hours
        )
        
        return result
        
    except Exception as e:
        return {
            "error": "Failed to extend expiration",
            "message": str(e),
            "help": "Verify the subset exists using list_materialized_subsets"
        }


def delete_materialized_subset_impl(credentials: tuple, subset_name: str) -> Dict[str, Any]:
    """
    Delete a materialized subset table (manual cleanup).
    
    Subsets auto-expire after 48 hours by default, but you can manually delete them
    earlier to clean up storage and avoid costs.
    
    Use this when:
    - Analysis is complete and subset no longer needed
    - Want to free up storage quota
    - Need to recreate subset with different parameters
    
    Args:
        credentials: Tuple of (project_id, private_key, client_email)
        subset_name: Name of the subset to delete
    
    Returns:
        Dictionary with deletion status
    
    Example:
        delete_materialized_subset(subset_name="ukraine_jan2025")
    """
    project_id, private_key, client_email = credentials
    
    try:
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        result = client.delete_materialized_subset(subset_name=subset_name)
        return result
        
    except Exception as e:
        return {
            "error": "Failed to delete subset",
            "message": str(e),
            "help": "Verify the subset exists using list_materialized_subsets"
        }
