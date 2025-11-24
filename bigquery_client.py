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
    
    def create_materialized_subset(
        self,
        source_table: str,
        subset_name: str,
        where_clause: str,
        select_fields: str = "*",
        description: Optional[str] = None,
        expiration_hours: int = 48
    ) -> Dict[str, Any]:
        """
        Create a materialized subset table from a GDELT table with auto-expiration.
        
        This is the recommended workflow for cost-effective analysis:
        1. Filter data once with tight date ranges
        2. Store in a subset table (auto-expires in 48 hours)
        3. Query the subset multiple times (near-free)
        
        Args:
            source_table: Source GDELT table name
            subset_name: Name for the new subset table (alphanumeric and underscores only)
            where_clause: WHERE clause to filter data (MUST include date filters)
            select_fields: Fields to select (default: all)
            description: Optional description for the subset
            expiration_hours: Hours until auto-deletion (default: 48)
            
        Returns:
            Dictionary with creation status and metadata
        """
        try:
            # Ensure dataset exists
            dataset_id = f"{self.project_id}.gdelt_subsets"
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            
            try:
                self.client.create_dataset(dataset, exists_ok=True)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise
            
            # Build CREATE TABLE query
            table_id = f"{dataset_id}.{subset_name}"
            create_query = f"""
            CREATE OR REPLACE TABLE `{table_id}` AS
            SELECT {select_fields}
            FROM `{source_table}`
            WHERE {where_clause}
            """
            
            # Estimate cost first
            cost_estimate = self.estimate_query_cost(create_query)
            
            # Execute creation query
            query_job = self.client.query(create_query)
            result = query_job.result()
            
            # Set expiration
            table = self.client.get_table(table_id)
            expiration_timestamp = f"TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR)"
            
            alter_query = f"""
            ALTER TABLE `{table_id}`
            SET OPTIONS(
                expiration_timestamp = {expiration_timestamp}
            )
            """
            self.client.query(alter_query).result()
            
            # Update description if provided
            if description:
                table.description = description
                self.client.update_table(table, ["description"])
            
            return {
                "status": "success",
                "table_id": table_id,
                "subset_name": subset_name,
                "source_table": source_table,
                "rows_created": query_job.num_dml_affected_rows or 0,
                "cost_estimate": cost_estimate,
                "expires_in_hours": expiration_hours,
                "message": f"Subset created successfully. Will auto-delete in {expiration_hours} hours."
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "help": "Ensure your WHERE clause includes date filters for partition pruning"
            }
    
    def list_materialized_subsets(self) -> List[Dict[str, Any]]:
        """
        List all materialized subset tables in the user's project.
        
        Returns:
            List of subset metadata dictionaries
        """
        try:
            dataset_id = f"{self.project_id}.gdelt_subsets"
            
            # Check if dataset exists
            try:
                dataset = self.client.get_dataset(dataset_id)
            except Exception:
                return []
            
            # List tables in dataset
            tables = self.client.list_tables(dataset_id)
            
            subsets = []
            for table_ref in tables:
                table = self.client.get_table(table_ref)
                
                # Calculate expiration info
                expires_in_hours = None
                is_expired = False
                if table.expires:
                    from datetime import datetime, timezone
                    now = datetime.now(timezone.utc)
                    time_diff = table.expires - now
                    expires_in_hours = round(time_diff.total_seconds() / 3600, 1)
                    is_expired = expires_in_hours <= 0
                
                subsets.append({
                    "subset_name": table.table_id,
                    "table_id": f"{dataset_id}.{table.table_id}",
                    "created": table.created.isoformat() if table.created else None,
                    "expires": table.expires.isoformat() if table.expires else None,
                    "expires_in_hours": expires_in_hours,
                    "is_expired": is_expired,
                    "size_mb": round(table.num_bytes / (1024 ** 2), 2) if table.num_bytes else 0,
                    "num_rows": table.num_rows or 0,
                    "description": table.description or ""
                })
            
            return subsets
            
        except Exception as e:
            return [{
                "error": str(e),
                "message": "Failed to list subsets"
            }]
    
    def delete_materialized_subset(self, subset_name: str) -> Dict[str, Any]:
        """
        Delete a materialized subset table.
        
        Args:
            subset_name: Name of the subset to delete
            
        Returns:
            Dictionary with deletion status
        """
        try:
            table_id = f"{self.project_id}.gdelt_subsets.{subset_name}"
            self.client.delete_table(table_id)
            
            return {
                "status": "success",
                "message": f"Subset '{subset_name}' deleted successfully"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "message": f"Failed to delete subset '{subset_name}'"
            }
    
    def extend_subset_expiration(
        self,
        subset_name: str,
        additional_hours: int = 48
    ) -> Dict[str, Any]:
        """
        Extend the expiration time of a materialized subset.
        
        Args:
            subset_name: Name of the subset
            additional_hours: Additional hours to extend (default: 48)
            
        Returns:
            Dictionary with update status
        """
        try:
            table_id = f"{self.project_id}.gdelt_subsets.{subset_name}"
            
            # Get current table
            table = self.client.get_table(table_id)
            
            # Calculate new expiration
            from datetime import datetime, timedelta, timezone
            if table.expires:
                new_expiration = table.expires + timedelta(hours=additional_hours)
            else:
                new_expiration = datetime.now(timezone.utc) + timedelta(hours=additional_hours)
            
            # Update expiration
            alter_query = f"""
            ALTER TABLE `{table_id}`
            SET OPTIONS(
                expiration_timestamp = TIMESTAMP '{new_expiration.strftime('%Y-%m-%d %H:%M:%S UTC')}'
            )
            """
            self.client.query(alter_query).result()
            
            return {
                "status": "success",
                "subset_name": subset_name,
                "new_expiration": new_expiration.isoformat(),
                "message": f"Expiration extended by {additional_hours} hours"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "message": f"Failed to extend expiration for '{subset_name}'"
            }
    
    def query_materialized_subset(
        self,
        subset_name: str,
        where_clause: Optional[str] = None,
        select_fields: str = "*",
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Query a materialized subset table (near-free operation).
        
        Args:
            subset_name: Name of the subset to query
            where_clause: Optional additional WHERE clause
            select_fields: Fields to select (default: all)
            limit: Maximum number of rows to return
            
        Returns:
            List of dictionaries representing rows
        """
        table_id = f"{self.project_id}.gdelt_subsets.{subset_name}"
        
        # Build query
        query = f"SELECT {select_fields} FROM `{table_id}`"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        query += f" LIMIT {limit}"
        
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            rows = []
            for row in results:
                rows.append(dict(row.items()))
            
            return rows
            
        except Exception as e:
            raise RuntimeError(f"Query failed: {str(e)}")
