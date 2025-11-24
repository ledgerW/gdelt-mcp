"""Query tools for GDELT MCP server."""

from typing import Any, Dict, List, Optional
from bigquery_client import GDELTBigQueryClient


def query_events_impl(
    credentials: tuple,
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100,
    order_by: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Implementation for querying GDELT Events table."""
    project_id, private_key, client_email = credentials
    
    try:
        client = GDELTBigQueryClient(
            project_id=project_id,
            private_key=private_key,
            client_email=client_email
        )
        
        limit = min(limit, 10000)
        
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


def query_eventmentions_impl(
    credentials: tuple,
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Implementation for querying GDELT EventMentions table."""
    project_id, private_key, client_email = credentials
    
    try:
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


def query_gkg_impl(
    credentials: tuple,
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Implementation for querying GDELT GKG table."""
    project_id, private_key, client_email = credentials
    
    try:
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


def query_cloudvision_impl(
    credentials: tuple,
    where_clause: Optional[str] = None,
    select_fields: str = "*",
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Implementation for querying GDELT CloudVision table."""
    project_id, private_key, client_email = credentials
    
    try:
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
