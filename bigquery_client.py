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
    
    def __init__(
        self, 
        credentials_path: Optional[str] = None, 
        project_id: Optional[str] = None,
        private_key: Optional[str] = None,
        client_email: Optional[str] = None
    ):
        """
        Initialize BigQuery client.
        
        Args:
            credentials_path: Path to GCP service account JSON file (optional)
            project_id: GCP project ID (optional, defaults to GCP_PROJECT_ID env var)
            private_key: GCP service account private key (optional, for token-based auth)
            client_email: GCP service account email (optional, for token-based auth)
        """
        # Get project ID
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        
        # Priority: explicit credential params > credentials_path > env vars > default
        if private_key and client_email:
            # Use provided credentials (from Bearer token)
            credentials_info = {
                "type": "service_account",
                "project_id": self.project_id,
                "private_key": private_key.replace("\\n", "\n"),
                "client_email": client_email,
                "token_uri": "https://oauth2.googleapis.com/token",
            }
            
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        elif credentials_path and os.path.exists(credentials_path):
            # Use credentials file
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        elif os.getenv("GCP_PRIVATE_KEY") and os.getenv("GCP_CLIENT_EMAIL"):
            # Use environment variables
            credentials_info = {
                "type": "service_account",
                "project_id": self.project_id,
                "private_key": os.getenv("GCP_PRIVATE_KEY", "").replace("\\n", "\n"),
                "client_email": os.getenv("GCP_CLIENT_EMAIL"),
                "token_uri": "https://oauth2.googleapis.com/token",
            }
            
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        else:
            # Attempt to use default credentials (e.g., from gcloud CLI)
            self.client = bigquery.Client(project=self.project_id)
    
    def _extract_partition_filter(self, where_clause: str, table: str) -> Optional[str]:
        """
        Extract or generate _PARTITIONTIME filter from WHERE clause for partition pruning.
        
        Args:
            where_clause: The WHERE clause to analyze
            table: The table being queried
            
        Returns:
            _PARTITIONTIME filter string or None
        """
        if not where_clause:
            return None
        
        import re
        
        # For Events table: look for SQLDATE filters (format: YYYYMMDD)
        if "events" in table:
            # Match patterns like "SQLDATE >= 20240101" or "SQLDATE = 20240101"
            match = re.search(r'SQLDATE\s*>=?\s*(\d{8})', where_clause, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                # Convert YYYYMMDD to YYYY-MM-DD
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                return f"_PARTITIONTIME >= '{formatted_date}'"
        
        # For GKG table: look for DATE filters (format: YYYYMMDDhhmmss)
        elif "gkg" in table:
            match = re.search(r'DATE\s*>=?\s*(\d{14})', where_clause, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                # Convert YYYYMMDDhhmmss to YYYY-MM-DD
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                return f"_PARTITIONTIME >= '{formatted_date}'"
        
        # Check if _PARTITIONTIME is already in the WHERE clause
        if "_PARTITIONTIME" in where_clause.upper():
            return None  # Already has partition filter
        
        return None
    
    def query(
        self,
        table: str,
        where_clause: Optional[str] = None,
        select_fields: str = "*",
        limit: int = 1000,
        timeout: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Execute a query on a GDELT table with automatic partition pruning.
        
        Automatically adds _PARTITIONTIME filters when date fields are detected
        to leverage BigQuery's partition pruning for faster and cheaper queries.
        
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
        
        # Add WHERE clause with automatic partition pruning
        if where_clause:
            # Try to extract/generate partition filter for optimization
            partition_filter = self._extract_partition_filter(where_clause, table)
            
            if partition_filter:
                # Add partition filter first for optimal pruning, then original where clause
                query += f" WHERE {partition_filter} AND ({where_clause})"
            else:
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
