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
    """Implementation for retrieving CAMEO event codes."""
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
    """Implementation for retrieving CAMEO actor codes."""
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
