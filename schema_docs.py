"""Schema documentation for GDELT 2.0 tables."""

from typing import Dict, List


# Events Table Schema
EVENTS_SCHEMA = {
    "table_name": "gdelt-bq.gdeltv2.events",
    "description": "The GDELT Events table captures distinct events with detailed information about actors, actions, and locations. Updated every 15 minutes.",
    "fields": [
        {"name": "GLOBALEVENTID", "type": "INTEGER", "description": "Globally unique identifier for each event"},
        {"name": "SQLDATE", "type": "INTEGER", "description": "Date the event occurred in YYYYMMDD format"},
        {"name": "MonthYear", "type": "INTEGER", "description": "Month and year in YYYYMM format"},
        {"name": "Year", "type": "INTEGER", "description": "Year the event occurred in YYYY format"},
        {"name": "FractionDate", "type": "FLOAT", "description": "Decimal date for precise temporal analysis"},
        {"name": "Actor1Code", "type": "STRING", "description": "Complete CAMEO code for Actor1 (who initiates the action)"},
        {"name": "Actor1Name", "type": "STRING", "description": "Human-readable name of Actor1"},
        {"name": "Actor1CountryCode", "type": "STRING", "description": "3-character CAMEO country code for Actor1"},
        {"name": "Actor1KnownGroupCode", "type": "STRING", "description": "Known group affiliation for Actor1"},
        {"name": "Actor1EthnicCode", "type": "STRING", "description": "Ethnic group code for Actor1"},
        {"name": "Actor1Religion1Code", "type": "STRING", "description": "Primary religion code for Actor1"},
        {"name": "Actor1Religion2Code", "type": "STRING", "description": "Secondary religion code for Actor1"},
        {"name": "Actor1Type1Code", "type": "STRING", "description": "Primary type code for Actor1 (e.g., GOV, MIL, CVL)"},
        {"name": "Actor1Type2Code", "type": "STRING", "description": "Secondary type code for Actor1"},
        {"name": "Actor1Type3Code", "type": "STRING", "description": "Tertiary type code for Actor1"},
        {"name": "Actor2Code", "type": "STRING", "description": "Complete CAMEO code for Actor2 (who receives the action)"},
        {"name": "Actor2Name", "type": "STRING", "description": "Human-readable name of Actor2"},
        {"name": "Actor2CountryCode", "type": "STRING", "description": "3-character CAMEO country code for Actor2"},
        {"name": "Actor2KnownGroupCode", "type": "STRING", "description": "Known group affiliation for Actor2"},
        {"name": "Actor2EthnicCode", "type": "STRING", "description": "Ethnic group code for Actor2"},
        {"name": "Actor2Religion1Code", "type": "STRING", "description": "Primary religion code for Actor2"},
        {"name": "Actor2Religion2Code", "type": "STRING", "description": "Secondary religion code for Actor2"},
        {"name": "Actor2Type1Code", "type": "STRING", "description": "Primary type code for Actor2"},
        {"name": "Actor2Type2Code", "type": "STRING", "description": "Secondary type code for Actor2"},
        {"name": "Actor2Type3Code", "type": "STRING", "description": "Tertiary type code for Actor2"},
        {"name": "IsRootEvent", "type": "INTEGER", "description": "1 if this is a root-level event, 0 otherwise"},
        {"name": "EventCode", "type": "STRING", "description": "CAMEO event code (e.g., '19' for military force)"},
        {"name": "EventBaseCode", "type": "STRING", "description": "Base-level CAMEO code"},
        {"name": "EventRootCode", "type": "STRING", "description": "Root-level CAMEO code"},
        {"name": "QuadClass", "type": "INTEGER", "description": "Quad class: 1=Verbal Cooperation, 2=Material Cooperation, 3=Verbal Conflict, 4=Material Conflict"},
        {"name": "GoldsteinScale", "type": "FLOAT", "description": "Goldstein scale score (-10 to +10, conflict to cooperation)"},
        {"name": "NumMentions", "type": "INTEGER", "description": "Number of mentions of this event across all documents"},
        {"name": "NumSources", "type": "INTEGER", "description": "Number of distinct sources reporting the event"},
        {"name": "NumArticles", "type": "INTEGER", "description": "Number of source documents containing one or more mentions"},
        {"name": "AvgTone", "type": "FLOAT", "description": "Average tone of all documents (-100 to +100, negative to positive)"},
        {"name": "Actor1Geo_Type", "type": "INTEGER", "description": "Geographic resolution of Actor1 location"},
        {"name": "Actor1Geo_FullName", "type": "STRING", "description": "Full geographic name for Actor1"},
        {"name": "Actor1Geo_CountryCode", "type": "STRING", "description": "2-character FIPS country code for Actor1"},
        {"name": "Actor1Geo_ADM1Code", "type": "STRING", "description": "Administrative division 1 code for Actor1"},
        {"name": "Actor1Geo_ADM2Code", "type": "STRING", "description": "Administrative division 2 code for Actor1"},
        {"name": "Actor1Geo_Lat", "type": "FLOAT", "description": "Latitude of Actor1 location"},
        {"name": "Actor1Geo_Long", "type": "FLOAT", "description": "Longitude of Actor1 location"},
        {"name": "Actor1Geo_FeatureID", "type": "STRING", "description": "GeoNames or GNIS FeatureID for Actor1"},
        {"name": "Actor2Geo_Type", "type": "INTEGER", "description": "Geographic resolution of Actor2 location"},
        {"name": "Actor2Geo_FullName", "type": "STRING", "description": "Full geographic name for Actor2"},
        {"name": "Actor2Geo_CountryCode", "type": "STRING", "description": "2-character FIPS country code for Actor2"},
        {"name": "Actor2Geo_ADM1Code", "type": "STRING", "description": "Administrative division 1 code for Actor2"},
        {"name": "Actor2Geo_ADM2Code", "type": "STRING", "description": "Administrative division 2 code for Actor2"},
        {"name": "Actor2Geo_Lat", "type": "FLOAT", "description": "Latitude of Actor2 location"},
        {"name": "Actor2Geo_Long", "type": "FLOAT", "description": "Longitude of Actor2 location"},
        {"name": "Actor2Geo_FeatureID", "type": "STRING", "description": "GeoNames or GNIS FeatureID for Actor2"},
        {"name": "ActionGeo_Type", "type": "INTEGER", "description": "Geographic resolution of action location"},
        {"name": "ActionGeo_FullName", "type": "STRING", "description": "Full geographic name where action occurred"},
        {"name": "ActionGeo_CountryCode", "type": "STRING", "description": "2-character FIPS country code for action"},
        {"name": "ActionGeo_ADM1Code", "type": "STRING", "description": "Administrative division 1 code for action"},
        {"name": "ActionGeo_ADM2Code", "type": "STRING", "description": "Administrative division 2 code for action"},
        {"name": "ActionGeo_Lat", "type": "FLOAT", "description": "Latitude where action occurred"},
        {"name": "ActionGeo_Long", "type": "FLOAT", "description": "Longitude where action occurred"},
        {"name": "ActionGeo_FeatureID", "type": "STRING", "description": "GeoNames or GNIS FeatureID for action"},
        {"name": "DATEADDED", "type": "INTEGER", "description": "Date/time event added to database (YYYYMMDDHHMMSS in UTC)"},
        {"name": "SOURCEURL", "type": "STRING", "description": "URL or citation of the source document"},
    ],
    "sample_queries": [
        {
            "description": "Get recent events involving the United States",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.events` WHERE Actor1CountryCode = 'USA' OR Actor2CountryCode = 'USA' ORDER BY SQLDATE DESC LIMIT 100"
        },
        {
            "description": "Find military conflict events (EventCode 19)",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.events` WHERE EventRootCode = '19' ORDER BY SQLDATE DESC LIMIT 100"
        },
        {
            "description": "Get events with high Goldstein scale (cooperation)",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.events` WHERE GoldsteinScale > 5 ORDER BY SQLDATE DESC LIMIT 100"
        }
    ]
}

# EventMentions Table Schema
EVENTMENTIONS_SCHEMA = {
    "table_name": "gdelt-bq.gdeltv2.eventmentions",
    "description": "Records every mention of every event, one row per mention. Links to the Events table via GLOBALEVENTID.",
    "fields": [
        {"name": "GLOBALEVENTID", "type": "INTEGER", "description": "Foreign key to Events table"},
        {"name": "EventTimeDate", "type": "INTEGER", "description": "15-minute timestamp when event occurred (YYYYMMDDHHMM00)"},
        {"name": "MentionTimeDate", "type": "INTEGER", "description": "15-minute timestamp when mention was published"},
        {"name": "MentionType", "type": "INTEGER", "description": "Numeric identifier for source type"},
        {"name": "MentionSourceName", "type": "STRING", "description": "Human-friendly source name"},
        {"name": "MentionIdentifier", "type": "STRING", "description": "Unique identifier for the source document (URL or citation)"},
        {"name": "SentenceID", "type": "INTEGER", "description": "Sentence number within document where event mentioned"},
        {"name": "Actor1CharOffset", "type": "INTEGER", "description": "Character offset of Actor1 in sentence"},
        {"name": "Actor2CharOffset", "type": "INTEGER", "description": "Character offset of Actor2 in sentence"},
        {"name": "ActionCharOffset", "type": "INTEGER", "description": "Character offset of action in sentence"},
        {"name": "InRawText", "type": "INTEGER", "description": "1 if found in raw text, 0 if in parsed metadata"},
        {"name": "Confidence", "type": "INTEGER", "description": "Confidence score for this mention (0-100)"},
        {"name": "MentionDocLen", "type": "INTEGER", "description": "Length of source document in characters"},
        {"name": "MentionDocTone", "type": "FLOAT", "description": "Tone of the source document (-100 to +100)"},
        {"name": "MentionDocTranslationInfo", "type": "STRING", "description": "Translation information if article was machine translated"},
        {"name": "Extras", "type": "STRING", "description": "Additional semi-structured metadata in XML format"},
    ],
    "sample_queries": [
        {
            "description": "Get all mentions of a specific event",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.eventmentions` WHERE GLOBALEVENTID = 123456789 ORDER BY MentionTimeDate"
        },
        {
            "description": "Find high-confidence mentions from last 24 hours",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.eventmentions` WHERE Confidence > 80 AND MentionTimeDate >= FORMAT_TIMESTAMP('%Y%m%d%H%M%S', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) LIMIT 100"
        }
    ]
}

# GKG Table Schema
GKG_SCHEMA = {
    "table_name": "gdelt-bq.gdeltv2.gkg",
    "description": "Global Knowledge Graph - extracts detailed metadata from articles including themes, locations, persons, organizations, and more.",
    "fields": [
        {"name": "GKGRECORDID", "type": "STRING", "description": "Unique identifier for this GKG record"},
        {"name": "DATE", "type": "INTEGER", "description": "Date/time in YYYYMMDDHHMMSS format (UTC)"},
        {"name": "SourceCollectionIdentifier", "type": "INTEGER", "description": "Source collection type"},
        {"name": "SourceCommonName", "type": "STRING", "description": "Source website domain"},
        {"name": "DocumentIdentifier", "type": "STRING", "description": "URL or identifier of source document"},
        {"name": "Counts", "type": "STRING", "description": "List of counts (e.g., number killed/wounded) in format: Type#Number#ObjectType#Location"},
        {"name": "V2Counts", "type": "STRING", "description": "Enhanced count fields with additional context"},
        {"name": "Themes", "type": "STRING", "description": "Semicolon-delimited list of themes (e.g., PROTEST, TERROR, WB_123_HEALTH)"},
        {"name": "V2Themes", "type": "STRING", "description": "Enhanced themes with offset positions"},
        {"name": "Locations", "type": "STRING", "description": "Semicolon-delimited list of locations with type, name, country, lat/long, feature ID"},
        {"name": "V2Locations", "type": "STRING", "description": "Enhanced locations with character offsets"},
        {"name": "Persons", "type": "STRING", "description": "Semicolon-delimited list of person names mentioned"},
        {"name": "V2Persons", "type": "STRING", "description": "Enhanced persons with character offsets"},
        {"name": "Organizations", "type": "STRING", "description": "Semicolon-delimited list of organizations mentioned"},
        {"name": "V2Organizations", "type": "STRING", "description": "Enhanced organizations with character offsets"},
        {"name": "V2Tone", "type": "STRING", "description": "Six comma-delimited tone metrics"},
        {"name": "Dates", "type": "STRING", "description": "All dates mentioned in the article"},
        {"name": "GCAM", "type": "STRING", "description": "Global Content Analysis Measures - 24 emotional dimensions"},
        {"name": "SharingImage", "type": "STRING", "description": "URL of primary image for social sharing"},
        {"name": "RelatedImages", "type": "STRING", "description": "Semicolon-delimited list of all images in article"},
        {"name": "SocialImageEmbeds", "type": "STRING", "description": "Social media image embeds"},
        {"name": "SocialVideoEmbeds", "type": "STRING", "description": "Social media video embeds"},
        {"name": "Quotations", "type": "STRING", "description": "All quotations extracted from article"},
        {"name": "AllNames", "type": "STRING", "description": "All person names with character offsets"},
        {"name": "Amounts", "type": "STRING", "description": "All amounts/numbers mentioned with context"},
        {"name": "TranslationInfo", "type": "STRING", "description": "Machine translation information"},
        {"name": "Extras", "type": "STRING", "description": "Additional XML-formatted metadata"},
    ],
    "sample_queries": [
        {
            "description": "Find articles about specific themes (e.g., climate change)",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.gkg` WHERE Themes LIKE '%ENV_CLIMATECHANGE%' ORDER BY DATE DESC LIMIT 100"
        },
        {
            "description": "Get articles mentioning specific person",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.gkg` WHERE Persons LIKE '%Biden%' ORDER BY DATE DESC LIMIT 100"
        },
        {
            "description": "Find articles from specific location",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.gkg` WHERE Locations LIKE '%Ukraine%' ORDER BY DATE DESC LIMIT 100"
        }
    ]
}

# CloudVision Table Schema
CLOUDVISION_SCHEMA = {
    "table_name": "gdelt-bq.gdeltv2.cloudvision",
    "description": "Results from Google Cloud Vision API analyzing images in news articles. Maps visual content to semantic labels.",
    "fields": [
        {"name": "url", "type": "STRING", "description": "URL of the analyzed image"},
        {"name": "timestamp", "type": "INTEGER", "description": "Processing timestamp"},
        {"name": "labels", "type": "STRING", "description": "Comma-separated list of visual labels detected"},
        {"name": "label_scores", "type": "STRING", "description": "Confidence scores for each label (0-1)"},
        {"name": "faces", "type": "INTEGER", "description": "Number of faces detected"},
        {"name": "text", "type": "STRING", "description": "Text detected in image via OCR"},
        {"name": "landmarks", "type": "STRING", "description": "Named landmarks detected"},
        {"name": "logos", "type": "STRING", "description": "Corporate logos detected"},
        {"name": "safe_search", "type": "STRING", "description": "Safe search annotations (adult, violence, etc.)"},
        {"name": "web_entities", "type": "STRING", "description": "Web entities associated with image"},
    ],
    "sample_queries": [
        {
            "description": "Find images with specific labels",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.cloudvision` WHERE labels LIKE '%protest%' LIMIT 100"
        },
        {
            "description": "Get images with faces detected",
            "query": "SELECT * FROM `gdelt-bq.gdeltv2.cloudvision` WHERE faces > 0 ORDER BY timestamp DESC LIMIT 100"
        }
    ]
}


def get_table_schema(table_name: str) -> Dict:
    """Get schema information for a specific table."""
    schemas = {
        "events": EVENTS_SCHEMA,
        "eventmentions": EVENTMENTIONS_SCHEMA,
        "gkg": GKG_SCHEMA,
        "cloudvision": CLOUDVISION_SCHEMA,
    }
    return schemas.get(table_name.lower(), {})


def get_all_schemas() -> Dict[str, Dict]:
    """Get all table schemas."""
    return {
        "events": EVENTS_SCHEMA,
        "eventmentions": EVENTMENTIONS_SCHEMA,
        "gkg": GKG_SCHEMA,
        "cloudvision": CLOUDVISION_SCHEMA,
    }


def get_field_names(table_name: str) -> List[str]:
    """Get list of field names for a table."""
    schema = get_table_schema(table_name)
    return [field["name"] for field in schema.get("fields", [])]


def get_sample_queries(table_name: str) -> List[Dict]:
    """Get sample queries for a table."""
    schema = get_table_schema(table_name)
    return schema.get("sample_queries", [])
