"""Cost optimization tools for GDELT MCP server."""

from typing import Any, Dict, List, Optional
from bigquery_client import GDELTBigQueryClient


def estimate_query_cost_impl(
    credentials: tuple,
    table: str,
    where_clause: Optional[str] = None,
    select_fields: str = "*"
) -> Dict[str, Any]:
    """Implementation for estimating query cost."""
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
    """Implementation for creating materialized subset."""
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
    """Implementation for listing materialized subsets."""
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
    """Implementation for querying materialized subset."""
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
    """Implementation for extending subset expiration."""
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
    """Implementation for deleting materialized subset."""
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
