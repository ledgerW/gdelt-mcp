"""Schema resources for GDELT MCP server."""

from schema_docs import get_table_schema


def get_events_schema_resource_impl() -> str:
    """Schema and documentation for the GDELT Events table."""
    schema = get_table_schema("events")
    return f"""# GDELT Events Table Schema

**Table:** {schema['table_name']}

**Description:** {schema['description']}

## Fields

{chr(10).join([f"- **{field['name']}** ({field['type']}): {field['description']}" for field in schema['fields']])}

## Sample Queries

{chr(10).join([f"### {q['description']}{chr(10)}```sql{chr(10)}{q['query']}{chr(10)}```{chr(10)}" for q in schema['sample_queries']])}
"""


def get_eventmentions_schema_resource_impl() -> str:
    """Schema and documentation for the GDELT EventMentions table."""
    schema = get_table_schema("eventmentions")
    return f"""# GDELT EventMentions Table Schema

**Table:** {schema['table_name']}

**Description:** {schema['description']}

## Fields

{chr(10).join([f"- **{field['name']}** ({field['type']}): {field['description']}" for field in schema['fields']])}

## Sample Queries

{chr(10).join([f"### {q['description']}{chr(10)}```sql{chr(10)}{q['query']}{chr(10)}```{chr(10)}" for q in schema['sample_queries']])}
"""


def get_gkg_schema_resource_impl() -> str:
    """Schema and documentation for the GDELT GKG (Global Knowledge Graph) table."""
    schema = get_table_schema("gkg")
    return f"""# GDELT GKG (Global Knowledge Graph) Table Schema

**Table:** {schema['table_name']}

**Description:** {schema['description']}

## Fields

{chr(10).join([f"- **{field['name']}** ({field['type']}): {field['description']}" for field in schema['fields']])}

## Sample Queries

{chr(10).join([f"### {q['description']}{chr(10)}```sql{chr(10)}{q['query']}{chr(10)}```{chr(10)}" for q in schema['sample_queries']])}
"""


def get_cloudvision_schema_resource_impl() -> str:
    """Schema and documentation for the GDELT CloudVision table."""
    schema = get_table_schema("cloudvision")
    return f"""# GDELT CloudVision Table Schema

**Table:** {schema['table_name']}

**Description:** {schema['description']}

## Fields

{chr(10).join([f"- **{field['name']}** ({field['type']}): {field['description']}" for field in schema['fields']])}

## Sample Queries

{chr(10).join([f"### {q['description']}{chr(10)}```sql{chr(10)}{q['query']}{chr(10)}```{chr(10)}" for q in schema['sample_queries']])}
"""


def get_cost_optimization_guide_impl() -> str:
    """REQUIRED READING: Cost-effective querying workflow for GDELT BigQuery data."""
    return """# GDELT BigQuery Cost Optimization Guide

🚨 **READ THIS FIRST** before using GDELT MCP tools to avoid expensive queries! 🚨

GDELT contains hundreds of billions of rows across massive tables. Without proper techniques, a single query can cost $1-5 or more. This guide shows you how to query effectively for pennies or free.

---

## Table Size Hierarchy (Recommendation #1)

**ALWAYS query the smallest table first that can answer your question:**

| Table           | Relative Size | Daily Size | Use When You Need                     |
|-----------------|---------------|------------|---------------------------------------|
| **Events**      | Small         | ~100-200MB | High-level who/what/where/when        |
| **Mentions**    | ~10x larger   | ~1-2GB     | Article-level references to events    |
| **GKG**         | ~20x larger   | ~5-10GB    | Themes, entities, semantic content    |
| **CloudVision** | Large         | ~varies    | Visual analysis of news images        |

**Decision Tree:**
1. Can Events table answer this? → Use `query_events`
2. Need article mentions? → Use `query_eventmentions`  
3. Need themes/entities? → Use `query_gkg`
4. Need image analysis? → Use `query_cloudvision`

---

## Partition Pruning (Recommendation #2)

⚠️ **THIS IS THE MOST IMPORTANT COST SAVER** ⚠️

GDELT tables are partitioned by date. Queries without date filters scan the ENTIRE dataset.

### ❌ BAD (Expensive):
```sql
-- Scans 2.5TB+ → $12.50+
SELECT * FROM gkg WHERE Themes LIKE '%PROTEST%'
```

### ✅ GOOD (Cheap):
```sql
-- Scans ~50MB → $0.0003
SELECT * FROM events 
WHERE SQLDATE >= 20250101 AND EventRootCode = '14'
LIMIT 100
```

### Date Filter Requirements:

**Events Table:** Must include `SQLDATE >= YYYYMMDD`
```
Example: SQLDATE >= 20250101
```

**GKG Table:** Must include `DATE >= YYYYMMDDhhmmss`
```
Example: DATE >= 20250101000000
```

**EventMentions Table:** Use `GLOBALEVENTID` to link from Events

**CloudVision Table:** Use `timestamp >= YYYYMMDDhhmmss` for filtering

**Rule:** If you don't see a date filter in your WHERE clause, STOP and add one!

---

## Column Selection (Recommendation #3)

Selecting `*` (all columns) costs more than selecting specific fields.

### Cost Comparison (1 day of Events):

| Query Type          | Data Scanned | Cost     |
|---------------------|--------------|----------|
| `SELECT *`          | ~150MB       | $0.0008  |
| `SELECT SQLDATE, Actor1Name, Actor2Name, EventCode` | ~40MB | $0.0002 |

**Best Practice:** Only select the columns you actually need.

---

## Materialization Workflow (Recommendation #4)

🎯 **THE RECOMMENDED WORKFLOW FOR ALL ANALYSIS**

Instead of querying GDELT directly multiple times:

### Step 1: Create a Materialized Subset (One-Time Cost)
```python
create_materialized_subset(
    source_table="events",
    subset_name="ukraine_events_jan2025",
    where_clause="SQLDATE BETWEEN 20250101 AND 20250131 AND (Actor1CountryCode = 'UKR' OR Actor2CountryCode = 'UKR')",
    select_fields="SQLDATE, Actor1Name, Actor2Name, EventCode, GoldsteinScale, ActionGeo_Lat, ActionGeo_Long",
    description="Ukraine-related events for January 2025"
)
```
- Filters once: ~$0.001-0.01
- Auto-expires in 48 hours (no forgotten tables)
- Stores in your project as `{project}.gdelt_subsets.ukraine_events_jan2025`

### Step 2: Query the Subset (Near-Free)
```python
# Query 1: Count by event type
query_materialized_subset(
    subset_name="ukraine_events_jan2025",
    where_clause="EventCode LIKE '19%'",  # Military actions
    limit=1000
)

# Query 2: Geographic analysis  
query_materialized_subset(
    subset_name="ukraine_events_jan2025",
    select_fields="ActionGeo_Lat, ActionGeo_Long, EventCode, SQLDATE",
    limit=5000
)

# Query 3: Trend analysis
query_materialized_subset(
    subset_name="ukraine_events_jan2025",
    where_clause="GoldsteinScale < -5",
    limit=500
)
```
Each query: ~$0.00001 (essentially free!)

### Cost Savings:
- Direct queries: 3 queries × $0.01 = **$0.03**
- Materialized workflow: $0.01 + (3 × $0.00001) = **$0.01**
- **Savings: 66%** (and scales with more queries!)

---

## GKG-Specific Guidance (Recommendation #9)

The GKG table is the largest and most expensive. **Never query it directly without date filters.**

### ❌ NEVER DO THIS:
```sql
SELECT * FROM gkg WHERE Themes LIKE '%PROTEST%'
-- Scans 2.5TB+ per year → $12.50+
```

### ✅ ALWAYS DO THIS:
```sql
SELECT Themes, V2Locations 
FROM gkg 
WHERE DATE >= 20250101000000 
  AND DATE < 20250201000000
  AND Themes LIKE '%PROTEST%'
LIMIT 1000
-- Scans ~5GB → $0.025
```

**GKG Best Practices:**
1. Always include `DATE >= YYYYMMDDhhmmss` filter
2. Keep date ranges tight (1-7 days ideal)
3. Select specific fields, not `*`
4. Use materialization for multi-step analysis

---

## Verified Cost Examples

These are real-world examples from production usage:

### Events Table:
- ✅ 1 day, 4 fields, date filter: 40MB → **$0.0002** 
- ✅ 7 days, 10 fields, date filter: 300MB → **$0.0015**
- ❌ 365 days, SELECT *, no filter: 50GB → **$0.25**

### GKG Table:  
- ✅ 1 day, theme filter, date filter: 5GB → **$0.025**
- ❌ 30 days, SELECT *, no date filter: 150GB → **$0.75**
- ❌ 1 year, no date filter: 2.5TB+ → **$12.50+**

### Materialized Subset Queries:
- ✅ Any query on subset: 0.001-0.1MB → **~$0.000001** (free tier)

---

## Recommended Workflow Summary

1. **Read this guide** ✓
2. **Choose the smallest appropriate table** (Events → Mentions → GKG)
3. **Include date filters** (SQLDATE or DATE)
4. **Select specific fields** (not `*`)
5. **Use estimate_query_cost** to check cost before running
6. **Create materialized subset** if doing exploratory analysis
7. **Query the subset** multiple times (near-free)
8. **Clean up** when done (or wait for 48h auto-expiration)

---

## Quick Reference

### Date Filter Formats:
- Events: `SQLDATE >= 20250101`
- GKG: `DATE >= 20250101000000`
- Mentions: Use `GLOBALEVENTID` from Events
- CloudVision: `timestamp >= 20250101000000`

### BigQuery Pricing:
- **$5 per TB** scanned
- First **10 GB per month: FREE**

### Cost Targets:
- 🟢 <100MB ($0.0005): Excellent
- 🟡 100MB-1GB ($0.0005-0.005): Good  
- 🟠 1GB-10GB ($0.005-0.05): Acceptable for specific needs
- 🔴 >10GB ($0.05+): Review if necessary

### Tools Available:
- `estimate_query_cost`: Check cost before running
- `create_materialized_subset`: Filter once, query many times
- `list_materialized_subsets`: See your subsets
- `query_materialized_subset`: Query subsets (near-free)
- `extend_subset_expiration`: Keep subset longer
- `delete_materialized_subset`: Manual cleanup

---

## Questions?

- **"My query is expensive, what do I do?"** → Add date filters, select fewer fields, or use materialization
- **"How do I find events for analysis?"** → Start with Events table, tight date range
- **"I need to iterate on analysis"** → Create materialized subset first
- **"Subset expired, what happened?"** → Auto-deleted after 48h (configurable)

**Remember:** Proper technique makes GDELT essentially free to use. Improper technique makes it very expensive!
"""
