"""Authentication utilities for GDELT MCP server."""

import os
from typing import Optional, Tuple
from fastmcp.server.dependencies import get_http_headers


def get_credentials_from_token() -> Optional[Tuple[str, str, str]]:
    """
    Extract and validate GCP credentials from Bearer token.
    
    Token format: project_id|private_key|client_email
    
    Returns:
        Tuple of (project_id, private_key, client_email) or None if invalid
    """
    headers = get_http_headers()
    auth_header = headers.get("authorization", "")
    
    if not auth_header.startswith("Bearer "):
        return None
    
    token = auth_header[7:]  # Remove "Bearer " prefix
    parts = token.split("|")
    
    if len(parts) != 3:
        return None
    
    project_id, private_key, client_email = parts
    
    # Basic validation
    if project_id and private_key.startswith("-----BEGIN PRIVATE KEY-----") and "@" in client_email:
        return (project_id, private_key, client_email)
    
    return None


def get_credentials_from_env() -> Optional[Tuple[str, str, str]]:
    """
    Extract and validate GCP credentials from environment variables.
    
    Required environment variables:
    - GCP_PROJECT_ID
    - GCP_PRIVATE_KEY
    - GCP_CLIENT_EMAIL
    
    Returns:
        Tuple of (project_id, private_key, client_email) or None if invalid
    """
    project_id = os.getenv("GCP_PROJECT_ID")
    private_key = os.getenv("GCP_PRIVATE_KEY")
    client_email = os.getenv("GCP_CLIENT_EMAIL")
    
    if not (project_id and private_key and client_email):
        return None
    
    # Basic validation
    if private_key.startswith("-----BEGIN PRIVATE KEY-----") and "@" in client_email:
        return (project_id, private_key, client_email)
    
    return None


def get_credentials() -> Optional[Tuple[str, str, str]]:
    """
    Get GCP credentials with fallback priority.
    
    Priority:
    1. Bearer token (from Authorization header)
    2. Environment variables (GCP_PROJECT_ID, GCP_PRIVATE_KEY, GCP_CLIENT_EMAIL)
    
    Returns:
        Tuple of (project_id, private_key, client_email) or None if no valid credentials
    """
    # Try Bearer token first
    credentials = get_credentials_from_token()
    if credentials:
        return credentials
    
    # Fallback to environment variables
    return get_credentials_from_env()
