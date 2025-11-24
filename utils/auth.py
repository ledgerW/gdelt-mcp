"""Authentication utilities for GDELT MCP server."""

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
    if not project_id or not private_key.startswith("-----BEGIN PRIVATE KEY-----") or "@" not in client_email:
        return None
    
    return (project_id, private_key, client_email)
