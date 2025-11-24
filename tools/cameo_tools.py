"""CAMEO taxonomy tools for GDELT MCP server."""

from typing import Any, Dict, Optional
from cameo_lookups import (
    CAMEO_EVENT_CODES,
    CAMEO_COUNTRY_CODES,
    CAMEO_TYPE_CODES,
    search_event_codes,
    get_event_codes_by_category,
)


def get_cameo_event_codes_impl(
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
    if search_keyword:
        codes = search_event_codes(search_keyword)
        return {
            "search_keyword": search_keyword,
            "count": len(codes),
            "codes": codes
        }
    elif category:
        codes = get_event_codes_by_category(category)
        return {
            "category": category,
            "count": len(codes),
            "codes": codes
        }
    else:
        return {
            "count": len(CAMEO_EVENT_CODES),
            "codes": CAMEO_EVENT_CODES
        }


def get_cameo_actor_codes_impl(code_type: str = "all") -> Dict[str, Any]:
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
    if code_type == "countries":
        return {
            "type": "country_codes",
            "count": len(CAMEO_COUNTRY_CODES),
            "codes": CAMEO_COUNTRY_CODES
        }
    elif code_type == "types":
        return {
            "type": "actor_type_codes",
            "count": len(CAMEO_TYPE_CODES),
            "codes": CAMEO_TYPE_CODES
        }
    else:
        return {
            "type": "all",
            "country_codes": CAMEO_COUNTRY_CODES,
            "actor_type_codes": CAMEO_TYPE_CODES,
            "total_count": len(CAMEO_COUNTRY_CODES) + len(CAMEO_TYPE_CODES)
        }
