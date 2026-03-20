# file: 39_01_diagnose_table_design.py
# Purpose: Diagnose current Redshift table design problems
# Generates SQL to run on Redshift — identifies optimization opportunities
# Run from: Your machine — outputs SQL to execute on Redshift

import boto3
from datetime import datetime

AWS_REGION = "us-east-2"

def generate_diagnostic_sql():
    """Generate diagnostic SQL to run on Redshift Query Editor"""

    print("=" * 70)
    print("🔍 REDSHIFT TABLE DESIGN DIAGNOSTIC")
    print(f"   Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print(f"""
-- ═══════════════════════════════════════════════════════════════
-- DIAGNOSTIC 1: Table Design Overview
-- Shows: distribution style, sort keys, row counts, bloat
-- ═══════════════════════════════════════════════════════════════

SELECT
    "schema" AS schema_name,
    "table" AS table_name,
    diststyle,
    sortkey1,
    sortkey_num,
    tbl_rows AS live_rows,
    estimated_visible_rows,
    CASE WHEN tbl_rows > 0 
         THEN ROUND((tbl_rows - estimated_visible_rows)::FLOAT / tbl_rows * 100, 1)
         ELSE 0 
    END AS bloat_pct,
    unsorted,
    size AS size_mb,
    pct_used
FROM svv_table_info
WHERE "schema" = 'analytics'
ORDER BY tbl_rows DESC;


-- ═══════════════════════════════════════════════════════════════
-- DIAGNOSTIC 2: Distribution Skew
-- Shows: how evenly rows are distributed across slices
-- Skew > 1.5 = problem (one slice has 50%+ more than average)
-- ═══════════════════════════════════════════════════════════════

SELECT
    t.name AS table_name,
    s.slice,
    s.num_values AS rows_in_slice,
    ROUND(s.num_values::FLOAT / NULLIF(SUM(s.num_values) OVER (PARTITION BY t.name), 0) * 100, 1) AS pct_of_total
FROM svv_diskusage s
JOIN stv_tbl_perm t ON s.tbl = t.id
WHERE t.name IN ('fact_orders', 'dim_customers', 'dim_products')
  AND s.col = 0  -- First column represents row distribution
ORDER BY t.name, s.slice;


-- ═══════════════════════════════════════════════════════════════
-- DIAGNOSTIC 3: Join Performance — Check for Shuffles
-- Run this BEFORE optimization to capture baseline
-- ═══════════════════════════════════════════════════════════════

EXPLAIN
SELECT o.order_id, o.total_amount, c.name, c.tier
FROM analytics.fact_orders o
JOIN analytics.dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_date > CURRENT_DATE - 90;

-- LOOK FOR:
-- DS_DIST_ALL_NONE = network shuffle (BAD with EVEN distribution)
-- DS_DIST_NONE = co-located join (GOOD with matching DISTKEY)
-- DS_BCAST_INNER = broadcasting smaller table (OK for small dims)


-- ═══════════════════════════════════════════════════════════════
-- DIAGNOSTIC 4: Zone Map Effectiveness
-- Shows: how many blocks are skipped for date-filtered queries
-- ═══════════════════════════════════════════════════════════════

EXPLAIN
SELECT COUNT(*), SUM(total_amount)
FROM analytics.fact_orders
WHERE order_date >= '2025-01-01';

-- LOOK FOR:
-- "Seq Scan" with rows= close to total rows = NO zone map benefit
-- "Seq Scan" with rows= much less than total = zone maps working


-- ═══════════════════════════════════════════════════════════════
-- DIAGNOSTIC 5: Table Bloat Check
-- Ghost rows from DELETE+INSERT pattern
-- ═══════════════════════════════════════════════════════════════

SELECT
    "schema",
    "table",
    tbl_rows,
    estimated_visible_rows,
    tbl_rows - estimated_visible_rows AS ghost_rows,
    CASE WHEN tbl_rows > 0
         THEN ROUND((tbl_rows - estimated_visible_rows)::FLOAT / tbl_rows * 100, 1)
         ELSE 0
    END AS ghost_pct,
    unsorted AS unsorted_pct,
    CASE 
        WHEN (tbl_rows - estimated_visible_rows)::FLOAT / NULLIF(tbl_rows, 0) > 0.2
        THEN 'VACUUM NEEDED'
        ELSE 'OK'
    END AS vacuum_recommendation,
    CASE
        WHEN unsorted > 20 THEN 'SORT NEEDED'
        ELSE 'OK'
    END AS sort_recommendation
FROM svv_table_info
WHERE "schema" = 'analytics'
ORDER BY ghost_pct DESC;


-- ═══════════════════════════════════════════════════════════════
-- DIAGNOSTIC 6: Statistics Freshness
-- Shows: when ANALYZE last ran for each table
-- ═══════════════════════════════════════════════════════════════

SELECT
    database,
    "schema",
    "table",
    stats_off AS statistics_staleness_pct
FROM svv_table_info
WHERE "schema" = 'analytics'
ORDER BY stats_off DESC;

-- stats_off > 10 = statistics significantly stale → run ANALYZE


-- ═══════════════════════════════════════════════════════════════
-- DIAGNOSTIC 7: Redshift Advisor Recommendations
-- Redshift's built-in optimization suggestions
-- ═══════════════════════════════════════════════════════════════

SELECT
    event_time,
    event_category,
    solution AS recommendation,
    SUBSTRING(event, 1, 200) AS details
FROM stl_alert_event_log
WHERE event_time > CURRENT_DATE - 7
ORDER BY event_time DESC
LIMIT 20;


-- ═══════════════════════════════════════════════════════════════
-- DIAGNOSTIC 8: Column Compression Encoding
-- Shows current encoding — identifies optimization opportunities
-- ═══════════════════════════════════════════════════════════════

SELECT
    tablename,
    "column",
    type,
    encoding,
    distkey,
    sortkey
FROM pg_table_def
WHERE schemaname = 'analytics'
  AND tablename IN ('fact_orders', 'dim_customers', 'dim_products')
ORDER BY tablename, "column";

-- encoding = 'none' on non-key columns = WASTING SPACE
-- Redshift auto-assigns encoding during COPY but not always optimal


-- ═══════════════════════════════════════════════════════════════
-- DIAGNOSTIC 9: Top Slow Queries (last 7 days)
-- Find which queries need optimization most
-- ═══════════════════════════════════════════════════════════════

SELECT
    q.query,
    q.userid,
    q.elapsed / 1000000.0 AS elapsed_seconds,
    q.aborted,
    SUBSTRING(q.querytxt, 1, 150) AS query_preview,
    q.starttime
FROM stl_query q
WHERE q.starttime > CURRENT_DATE - 7
  AND q.elapsed > 5000000  -- > 5 seconds
  AND q.querytxt NOT LIKE '%stl_%'
  AND q.querytxt NOT LIKE '%svv_%'
  AND q.querytxt NOT LIKE '%pg_%'
ORDER BY q.elapsed DESC
LIMIT 20;
    """)
    print("=" * 70)
    print("📋 Copy the SQL above and run on Redshift Query Editor")
    print("   Results will identify exactly what needs optimization")
    print("=" * 70)


if __name__ == "__main__":
    generate_diagnostic_sql()