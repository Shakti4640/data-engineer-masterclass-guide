# file: 39_02_optimize_tables.py
# Purpose: Generate SQL to optimize Redshift table design
# Uses CTAS deep copy pattern for clean optimization

import boto3
from datetime import datetime

AWS_REGION = "us-east-2"


def generate_optimization_sql():
    """Generate optimization SQL for Redshift"""

    print("=" * 70)
    print("🚀 REDSHIFT TABLE OPTIMIZATION SQL")
    print(f"   Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print(f"""
-- ═══════════════════════════════════════════════════════════════
-- PHASE 1: OPTIMIZED TABLE CREATION (CTAS Deep Copy)
-- Run during maintenance window (3-4 AM, low traffic)
-- ═══════════════════════════════════════════════════════════════

-- ── Step 1A: Create optimized fact_orders ──
-- DISTKEY: customer_id (co-locates with dim_customers for JOINs)
-- SORTKEY: order_date (zone maps skip old data for date filters)
-- ENCODE: AUTO (Redshift chooses optimal encoding per column)

CREATE TABLE analytics.fact_orders_optimized
DISTKEY(customer_id)
COMPOUND SORTKEY(order_date, status)
AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    status,
    order_date,
    _extraction_date,
    _extraction_timestamp,
    _source_system,
    _source_database,
    _source_table
FROM analytics.fact_orders;

-- Verify row count matches
SELECT 'original' AS version, COUNT(*) FROM analytics.fact_orders
UNION ALL
SELECT 'optimized', COUNT(*) FROM analytics.fact_orders_optimized;


-- ── Step 1B: Create optimized dim_customers ──
-- DISTKEY: customer_id (MATCHES fact_orders → co-located JOINs)
-- SORTKEY: signup_date (common filter for cohort analysis)

CREATE TABLE analytics.dim_customers_optimized
DISTKEY(customer_id)
SORTKEY(signup_date)
AS
SELECT * FROM analytics.dim_customers;

-- Verify
SELECT 'original' AS version, COUNT(*) FROM analytics.dim_customers
UNION ALL
SELECT 'optimized', COUNT(*) FROM analytics.dim_customers_optimized;


-- ── Step 1C: Create optimized dim_products ──
-- DISTSTYLE ALL: tiny table (50K rows) → copy to every node
-- Every JOIN with this table is local → zero network transfer
-- SORTKEY: category (common GROUP BY and filter)

CREATE TABLE analytics.dim_products_optimized
DISTSTYLE ALL
SORTKEY(category)
AS
SELECT * FROM analytics.dim_products;

-- Verify
SELECT 'original' AS version, COUNT(*) FROM analytics.dim_products
UNION ALL
SELECT 'optimized', COUNT(*) FROM analytics.dim_products_optimized;


-- ═══════════════════════════════════════════════════════════════
-- PHASE 2: SWAP TABLES (Atomic Rename)
-- Brief moment where table name changes — schedule during low traffic
-- ═══════════════════════════════════════════════════════════════

-- Begin transaction for atomic swap
BEGIN;

-- Rename originals to backup
ALTER TABLE analytics.fact_orders RENAME TO fact_orders_backup;
ALTER TABLE analytics.dim_customers RENAME TO dim_customers_backup;
ALTER TABLE analytics.dim_products RENAME TO dim_products_backup;

-- Rename optimized to production names
ALTER TABLE analytics.fact_orders_optimized RENAME TO fact_orders;
ALTER TABLE analytics.dim_customers_optimized RENAME TO dim_customers;
ALTER TABLE analytics.dim_products_optimized RENAME TO dim_products;

COMMIT;

-- ═══════════════════════════════════════════════════════════════
-- PHASE 3: RECREATE VIEWS (they reference table OIDs)
-- Views break after table rename — must recreate
-- ═══════════════════════════════════════════════════════════════

-- Recreate view from Project 38 (Spectrum mixed query)
CREATE OR REPLACE VIEW analytics.v_orders_with_rfm AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.status,
    o.order_date,
    r.rfm_segment,
    r.monetary AS customer_ltv,
    r.frequency AS customer_orders,
    r.recency_days
FROM analytics.fact_orders o
LEFT JOIN ext_silver.customer_rfm r
    ON o.customer_id = r.customer_id;

-- Recreate gold-backed views from Project 38
CREATE OR REPLACE VIEW analytics.v_revenue_summary AS
SELECT * FROM ext_gold.revenue_by_category_month;

CREATE OR REPLACE VIEW analytics.v_customer_segments AS
SELECT * FROM ext_gold.customer_segments_summary;

CREATE OR REPLACE VIEW analytics.v_product_leaderboard AS
SELECT * FROM ext_gold.product_performance;

-- Historical + Current union view
CREATE OR REPLACE VIEW analytics.v_all_orders_history AS
SELECT order_id, customer_id, product_id, quantity,
       unit_price, total_amount, status, order_date,
       'local' AS data_source
FROM analytics.fact_orders
WHERE order_date >= CURRENT_DATE - 90
UNION ALL
SELECT order_id, customer_id, product_id, quantity,
       unit_price, total_amount, status,
       CAST(order_date AS DATE),
       'spectrum' AS data_source
FROM ext_silver.clean_orders
WHERE CAST(year AS INT) < EXTRACT(YEAR FROM CURRENT_DATE)
   OR (CAST(year AS INT) = EXTRACT(YEAR FROM CURRENT_DATE)
       AND CAST(month AS INT) < EXTRACT(MONTH FROM CURRENT_DATE) - 3);


-- ═══════════════════════════════════════════════════════════════
-- PHASE 4: ANALYZE (Refresh Statistics)
-- Must run after table swap for optimizer to work correctly
-- ═══════════════════════════════════════════════════════════════

ANALYZE analytics.fact_orders;
ANALYZE analytics.dim_customers;
ANALYZE analytics.dim_products;


-- ═══════════════════════════════════════════════════════════════
-- PHASE 5: VERIFY OPTIMIZATION
-- ═══════════════════════════════════════════════════════════════

-- Verify distribution and sort keys
SELECT
    "schema",
    "table",
    diststyle,
    sortkey1,
    sortkey_num,
    tbl_rows,
    unsorted,
    size AS size_mb
FROM svv_table_info
WHERE "schema" = 'analytics'
ORDER BY tbl_rows DESC;

-- Expected:
-- fact_orders:    DISTKEY(customer_id), SORTKEY(order_date, status), unsorted=0
-- dim_customers:  DISTKEY(customer_id), SORTKEY(signup_date), unsorted=0
-- dim_products:   DISTSTYLE ALL, SORTKEY(category), unsorted=0

-- Verify JOIN is co-located (no shuffle)
EXPLAIN
SELECT o.order_id, o.total_amount, c.name, c.tier
FROM analytics.fact_orders o
JOIN analytics.dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_date > CURRENT_DATE - 90;

-- Expected: DS_DIST_NONE (co-located join — no redistribution)
-- If you see DS_DIST_ALL_NONE → DISTKEY not matching → investigate

-- Verify zone map effectiveness
EXPLAIN
SELECT COUNT(*), SUM(total_amount)
FROM analytics.fact_orders
WHERE order_date >= '2025-01-01';

-- Expected: rows= much smaller than total rows (zone map pruning)

-- Verify products broadcast
EXPLAIN
SELECT o.order_id, p.name, p.category
FROM analytics.fact_orders o
JOIN analytics.dim_products p ON o.product_id = p.product_id;

-- Expected: DS_DIST_ALL_NONE (products ALL-distributed → no shuffle)


-- ═══════════════════════════════════════════════════════════════
-- PHASE 6: CLEANUP (After verification — wait 24h)
-- Drop backup tables only after confirming everything works
-- ═══════════════════════════════════════════════════════════════

-- RUN AFTER 24 HOURS OF SUCCESSFUL OPERATION:
-- DROP TABLE IF EXISTS analytics.fact_orders_backup;
-- DROP TABLE IF EXISTS analytics.dim_customers_backup;
-- DROP TABLE IF EXISTS analytics.dim_products_backup;

-- NOTE: Keep backups for at least 24h in case rollback needed
-- ROLLBACK: rename backup tables back to original names
    """)
    print("=" * 70)


if __name__ == "__main__":
    generate_optimization_sql()