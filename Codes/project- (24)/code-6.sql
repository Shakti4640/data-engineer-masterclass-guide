-- file: 06_analyst_query_patterns.sql
-- Ready-to-use query patterns for analysts
-- Organized by complexity level

-- ═══════════════════════════════════════════════════════════
-- PATTERN 1: EXPLORATION (What does the data look like?)
-- ═══════════════════════════════════════════════════════════

-- Preview data (always start here)
SELECT * FROM quickcart_datalake.raw_orders LIMIT 10;

-- Check table size and row count
SELECT COUNT(*) as total_rows FROM quickcart_datalake.raw_orders;

-- Check distinct values in a column
SELECT DISTINCT status FROM quickcart_datalake.raw_orders;

-- Check data range
SELECT 
    MIN(order_date) as earliest,
    MAX(order_date) as latest,
    COUNT(DISTINCT order_date) as distinct_dates
FROM quickcart_datalake.raw_orders;

-- ═══════════════════════════════════════════════════════════
-- PATTERN 2: AGGREGATION (Summarize the data)
-- ═══════════════════════════════════════════════════════════

-- Group by with common metrics
SELECT 
    status,
    COUNT(*) as orders,
    ROUND(SUM(total_amount), 2) as revenue,
    ROUND(AVG(total_amount), 2) as avg_order,
    ROUND(MIN(total_amount), 2) as min_order,
    ROUND(MAX(total_amount), 2) as max_order
FROM quickcart_datalake.raw_orders
GROUP BY status
ORDER BY revenue DESC;

-- Time-series aggregation
SELECT 
    order_date,
    COUNT(*) as daily_orders,
    ROUND(SUM(total_amount), 2) as daily_revenue
FROM quickcart_datalake.raw_orders
GROUP BY order_date
ORDER BY order_date;

-- ═══════════════════════════════════════════════════════════
-- PATTERN 3: FILTERING (Find specific records)
-- ═══════════════════════════════════════════════════════════

-- Date range filter
-- ⚠️  On unpartitioned CSV: still scans ALL data
-- ✅  After partitioning (Project 26): scans only matching dates
SELECT *
FROM quickcart_datalake.raw_orders
WHERE order_date BETWEEN DATE '2025-01-15' AND DATE '2025-01-20'
LIMIT 100;

-- High-value orders
SELECT *
FROM quickcart_datalake.raw_orders
WHERE total_amount > 500
  AND status = 'completed'
ORDER BY total_amount DESC
LIMIT 50;

-- Multi-condition filter
SELECT *
FROM quickcart_datalake.raw_orders
WHERE status IN ('completed', 'shipped')
  AND quantity >= 5
  AND order_date >= DATE '2025-01-01'
LIMIT 100;

-- ═══════════════════════════════════════════════════════════
-- PATTERN 4: JOINS (Combine tables)
-- ═══════════════════════════════════════════════════════════

-- Orders with customer details
SELECT 
    o.order_id,
    o.order_date,
    o.total_amount,
    o.status,
    c.name as customer_name,
    c.tier as customer_tier,
    c.city
FROM quickcart_datalake.raw_orders o
JOIN quickcart_datalake.raw_customers c
    ON o.customer_id = c.customer_id
WHERE o.status = 'completed'
LIMIT 100;

-- Orders with product details
SELECT 
    o.order_id,
    p.name as product_name,
    p.category,
    o.quantity,
    o.unit_price,
    o.total_amount
FROM quickcart_datalake.raw_orders o
JOIN quickcart_datalake.raw_products p
    ON o.product_id = p.product_id
ORDER BY o.total_amount DESC
LIMIT 50;

-- Three-table join: orders + customers + products
SELECT 
    o.order_date,
    c.name as customer,
    c.tier,
    p.name as product,
    p.category,
    o.quantity,
    o.total_amount,
    o.status
FROM quickcart_datalake.raw_orders o
JOIN quickcart_datalake.raw_customers c ON o.customer_id = c.customer_id
JOIN quickcart_datalake.raw_products p ON o.product_id = p.product_id
WHERE o.order_date = DATE '2025-01-15'
ORDER BY o.total_amount DESC
LIMIT 50;

-- ═══════════════════════════════════════════════════════════
-- PATTERN 5: WINDOW FUNCTIONS (Advanced analytics)
-- ═══════════════════════════════════════════════════════════

-- Running total revenue per day
SELECT 
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (ORDER BY order_date) as cumulative_revenue
FROM (
    SELECT 
        order_date,
        ROUND(SUM(total_amount), 2) as daily_revenue
    FROM quickcart_datalake.raw_orders
    GROUP BY order_date
)
ORDER BY order_date;

-- Rank customers by spend
SELECT 
    customer_id,
    total_spent,
    ROW_NUMBER() OVER (ORDER BY total_spent DESC) as rank,
    ROUND(100.0 * total_spent / SUM(total_spent) OVER(), 4) as pct_of_total
FROM (
    SELECT 
        customer_id,
        ROUND(SUM(total_amount), 2) as total_spent
    FROM quickcart_datalake.raw_orders
    WHERE status = 'completed'
    GROUP BY customer_id
)
ORDER BY rank
LIMIT 20;

-- Day-over-day revenue change
SELECT 
    order_date,
    daily_revenue,
    LAG(daily_revenue) OVER (ORDER BY order_date) as prev_day_revenue,
    ROUND(
        100.0 * (daily_revenue - LAG(daily_revenue) OVER (ORDER BY order_date))
        / NULLIF(LAG(daily_revenue) OVER (ORDER BY order_date), 0),
        2
    ) as pct_change
FROM (
    SELECT 
        order_date,
        ROUND(SUM(total_amount), 2) as daily_revenue
    FROM quickcart_datalake.raw_orders
    GROUP BY order_date
)
ORDER BY order_date;

-- ═══════════════════════════════════════════════════════════
-- PATTERN 6: COST-CONSCIOUS QUERYING
-- ═══════════════════════════════════════════════════════════

-- ❌ EXPENSIVE: scans all columns (CSV — no column pruning)
SELECT * FROM quickcart_datalake.raw_orders;
-- Data scanned: ~1.5 GB = $0.0075

-- ✅ CHEAPER: select only needed columns (still scans all for CSV)
SELECT order_id, total_amount, status FROM quickcart_datalake.raw_orders;
-- Data scanned: ~1.5 GB = $0.0075 (CSV can't prune columns!)

-- ✅✅ CHEAPEST: use Parquet table + select columns
SELECT status, total_amount FROM quickcart_datalake.curated_orders_snapshot;
-- Data scanned: ~50 MB = $0.00025 (Parquet reads only 2 columns)

-- EXPLAIN to check before running expensive query
EXPLAIN
SELECT status, COUNT(*), SUM(total_amount)
FROM quickcart_datalake.raw_orders
GROUP BY status;

-- ═══════════════════════════════════════════════════════════
-- PATTERN 7: ATHENA v3 SPECIFIC FEATURES
-- ═══════════════════════════════════════════════════════════

-- Safe casting (returns NULL instead of error)
SELECT 
    order_id,
    try_cast(total_amount AS INTEGER) as amount_int
FROM quickcart_datalake.raw_orders
LIMIT 10;

-- TABLESAMPLE for quick estimates on large tables
SELECT 
    status,
    COUNT(*) * 100 as estimated_count   -- Multiply by inverse of sample rate
FROM quickcart_datalake.raw_orders
TABLESAMPLE BERNOULLI (1)              -- 1% sample
GROUP BY status;

-- Prepared statements (parameterized queries)
PREPARE revenue_by_date FROM
SELECT 
    order_date,
    COUNT(*) as orders,
    ROUND(SUM(total_amount), 2) as revenue
FROM quickcart_datalake.raw_orders
WHERE order_date = ?
GROUP BY order_date;

EXECUTE revenue_by_date USING DATE '2025-01-15';