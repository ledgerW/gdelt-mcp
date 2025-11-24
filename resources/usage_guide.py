"""GDELT usage guide resource - the sole source of GDELT workflow best practices."""


def get_usage_guide_impl() -> str:
    """Implementation for GDELT usage guide resource."""
    return """# GDELT BigQuery Usage Guide

🚨 **MASSIVE DATASET WARNING** 🚨

GDELT is petabyte-scale. Poorly-formed queries cost $1-50+. Follow the workflow below to query for pennies or free.

---

## THE RECOMMENDED WORKFLOW

**Default to this for ALL GDELT tasks.** Key insight: users always have follow-up questions. Materialized subsets enable 50-100x cheaper iteration.

### STEP 1: Check Existing Subsets

**Tool:** `list_materialized_subsets`

Before creating anything, check if a suitable subset exists. Reusing = free + fast.

**Decision:** Found suitable subset? → STEP 3. No subset? → STEP 2.

### STEP 2: Create Materialized Subset

**Tool:** `create_materialized_subset`

Create a filtered, reusable copy:
- One-time cost: ~$0.001-0.01
- Each query: ~$0.00001 (essentially free)
- Auto-expires in 48 hours

**Critical Requirements:**

1. **Date Filters** (prevents full table scan):
   - Events: `SQLDATE >= YYYYMMDD` or `SQLDATE BETWEEN ...`
   - GKG: `DATE >= YYYYMMDDhhmmss` or `DATE BETWEEN ...`
   - Without filters: Events 50GB+ ($0.25+), GKG 2.5TB+ ($12.50+)

2. **Specific Fields** (not `*`):
   - Reduces creation cost, storage, query time
   
3. **Smallest Table**:
   - Events (100-200MB/day) → EventMentions (~1-2GB/day) → GKG (~5-10GB/day)
   - Always start with Events unless you specifically need themes/entities

4. **Tight Date Ranges**:
   - 1-7 days: Ideal
   - 1-30 days: Acceptable
   - 90+ days: Consider multiple subsets

### STEP 3: Query Subset

**Tool:** `query_materialized_subset`

Iterate freely on filtered data. Each query scans KBs not GBs, costs ~$0.00001.

**Cost Reality:** 5 direct queries = $0.05. Materialized workflow = $0.01 (80% savings, scales with more queries).

### STEP 4: Manage (Optional)

- `extend_subset_expiration`: Keep longer than 48h
- `delete_materialized_subset`: Manual cleanup
- Auto-expiration prevents forgotten storage costs

---

## DIRECT QUERIES (Advanced)

Use **only** when:
- 100% certain it's one-time (no follow-ups)
- Need real-time data (not snapshot)
- Very narrow query (<100 rows, tight filters)

**Tools:** `query_events`, `query_eventmentions`, `query_gkg`, `query_cloudvision`

**Safety Checklist:**
- ✅ Date filters included?
- ✅ Specific fields (not `*`)?
- ✅ Smallest table chosen?
- ✅ Estimated cost? (`estimate_query_cost`)

**Cost Targets:**
- 🟢 <100MB: Excellent
- 🟡 100MB-1GB: Acceptable
- 🟠 1GB-10GB: Review necessity
- 🔴 >10GB: Use materialized subset

**If estimate shows high cost → create subset instead.**

---

## TABLE REFERENCE

### Events (SMALLEST - Start Here)
- **Contains:** Structured events, actor relationships, CAMEO codes, locations
- **Use For:** Who did what to whom, event timelines
- **Size:** ~100-200MB/day
- **Required:** `SQLDATE >= YYYYMMDD`
- **Cost:** ~$0.0002-0.001/day with filters

### EventMentions (MEDIUM)
- **Contains:** Media mentions, article URLs, sources
- **Use For:** Media coverage analysis (query Events first, then filter by GLOBALEVENTID)
- **Size:** ~1-2GB/day (~10x Events)
- **Best Practice:** Filter by GLOBALEVENTID from Events, not by date

### GKG (LARGEST - Use Materialization!)
- **Contains:** Themes, entities, sentiment, semantic content
- **Use For:** Theme analysis, entity extraction (when Events can't answer)
- **Size:** ~5-10GB/day (~20x Events)
- **Required:** `DATE >= YYYYMMDDhhmmss`
- **Critical:** WITHOUT date filter = $12.50+ per query
- **Recommendation:** ALWAYS create subset for GKG analysis

### CloudVision (VARIES)
- **Contains:** Image analysis, OCR, facial recognition, logos
- **Use For:** Visual content analysis
- **Size:** Varies by coverage
- **Required:** `timestamp >= YYYYMMDDhhmmss`

---

## TOOL REFERENCE BY STEP

**Discovery:** `get_usage_guide`, `get_events_schema`, `get_gkg_schema`, `get_eventmentions_schema`, `get_cloudvision_schema`, `get_cameo_event_codes`, `get_cameo_actor_codes`

**STEP 1:** `list_materialized_subsets`

**STEP 2:** `create_materialized_subset`, `estimate_query_cost` (optional verification)

**STEP 3:** `query_materialized_subset` (primary), `query_events`, `query_eventmentions`, `query_gkg`, `query_cloudvision` (advanced)

**STEP 4:** `extend_subset_expiration`, `delete_materialized_subset`

---

## KEY PRINCIPLES

1. **Always check existing subsets first** (`list_materialized_subsets`)
2. **Default to materialized workflow** (users have follow-ups)
3. **Include date filters** (prevents full scan)
4. **Select specific fields** (not `*`)
5. **Start with Events** (smallest table)
6. **Estimate before direct queries** (`estimate_query_cost`)
7. **Trust auto-expiration** (48h default)

**Cost Reality:**
- ❌ Without workflow: $1-50+ per session
- ✅ With workflow: $0.001-0.025 per session

**The insight:** GDELT is petabyte-scale, but materialized-first workflow makes analysis essentially free. Filter once, query many times, stay in free tier.
"""
