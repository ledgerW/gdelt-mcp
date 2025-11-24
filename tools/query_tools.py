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
        credentials: Tuple of (project_id, private_key, client_email)
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
        credentials: Tuple of (project_id, private_key, client_email)
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
        credentials: Tuple of (project_id, private_key, client_email)
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
        credentials: Tuple of (project_id, private_key, client_email)
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
