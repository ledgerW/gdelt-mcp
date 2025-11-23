"""BigQuery client for GDELT 2.0 data access."""

import os
from typing import Any, Dict, List, Optional
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class GDELTBigQueryClient:
    """Client for querying GDELT 2.0 tables in BigQuery."""
    
    # GDELT 2.0 table names
    EVENTS_TABLE = "gdelt-bq.gdeltv2.events_partitioned"
    EVENTMENTIONS_TABLE = "gdelt-bq.gdeltv2.eventmentions_partitioned"
    GKG_TABLE = "gdelt-bq.gdeltv2.gkg_partitioned"
    CLOUDVISION_TABLE = "gdelt-bq.gdeltv2.cloudvision_partitioned"
    
    def __init__(self, credentials_path: Optional[str] = None, project_id: Optional[str] = None):
        """
        Initialize BigQuery client.
        
        Args:
            credentials_path: Path to GCP service account JSON file (optional)
            project_id: GCP project ID (optional, defaults to GCP_PROJECT_ID env var)
        """
        # Get project ID
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        
        # Try to load credentials from file first if provided
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        # Otherwise, try to create credentials from environment variables
        elif os.getenv("GCP_PRIVATE_KEY") and os.getenv("GCP_CLIENT_EMAIL"):
            # Build credentials info dict from environment variables
            credentials_info = {
                "type": "service_account",
                "project_id": self.project_id,
                "private_key": os.getenv("GCP_PRIVATE_KEY", "").replace("\\n", "\n"),
                "client_email": os.getenv("GCP_CLIENT_EMAIL"),
                "token_uri": "https://oauth2.googleapis.com/token",
            }
            
            # Create credentials directly from the dict
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        else:
            # Attempt to use default credentials (e.g., from gcloud CLI)
            self.client = bigquery.Client(project=self.project_id)
    
    def query(
        self,
        table: str,
        where_clause: Optional[str] = None,
        select_fields: str = "*",
        limit: int = 1000,
        timeout: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Execute a query on a GDELT table.
        
        Args:
            table: Table name (one of the class constants)
            where_clause: Optional WHERE clause (without the WHERE keyword)
            select_fields: Fields to select (default: all)
            limit: Maximum number of rows to return
            timeout: Query timeout in seconds
            
        Returns:
            List of dictionaries representing rows
        """
        # Build the query
        query = f"SELECT {select_fields} FROM `{table}`"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        query += f" LIMIT {limit}"
        
        try:
            # Execute query
            query_job = self.client.query(query, timeout=timeout)
            
            # Wait for results
            results = query_job.result()
            
            # Convert to list of dictionaries
            rows = []
            for row in results:
                rows.append(dict(row.items()))
            
            return rows
            
        except Exception as e:
            raise RuntimeError(f"BigQuery query failed: {str(e)}")
    
    def get_sample_data(self, table: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get sample data from a table.
        
        Args:
            table: Table name
            limit: Number of sample rows
            
        Returns:
            List of sample rows
        """
        return self.query(table=table, limit=limit)
    
    def estimate_query_cost(self, query: str) -> Dict[str, Any]:
        """
        Estimate the cost of a query without executing it.
        
        Args:
            query: SQL query string
            
        Returns:
            Dictionary with cost estimation info
        """
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            query_job = self.client.query(query, job_config=job_config)
            
            # A dry run query completes immediately
            bytes_processed = query_job.total_bytes_processed
            gb_processed = bytes_processed / (1024 ** 3)
            
            return {
                "bytes_processed": bytes_processed,
                "gb_processed": round(gb_processed, 2),
                "estimated_cost_usd": round(gb_processed * 0.005, 4)  # $5 per TB
            }
        except Exception as e:
            return {"error": str(e)}
