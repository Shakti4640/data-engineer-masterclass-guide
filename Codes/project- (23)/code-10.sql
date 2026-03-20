-- file: 09_test_athena_queries.sql
-- Validate crawler-created tables by running real queries
-- These queries ONLY work because crawlers created the catalog tables

-- ═══════════════════════════════════════════════════════════
-- 1. VERIFY RAW ORDERS TABLE
-- ═══════════════════════════════════════════════════════════

-- Basic row count
SELECT COUNT(*) as total_orders
FROM quickcart_datalake.raw_orders;

-- Sample data
SELECT * FROM quickcart_datalake.raw_orders LIMIT 5;

-- Verify data types are correct (after type fix in Step 6)
SELECT 
    order_id,
    quantity,                        -- Should be INT
    unit_price,                      -- Should be DOUBLE
    total_amount,                    -- Should be DOUBLE
    order_date,                      -- Should be DATE (fixed from STRING)
    YEAR(order_date) as order_year   -- This only works if order_date is DATE type
FROM quickcart_datalake.raw_orders
LIMIT 5;

-- ═══════════════════════════════════════════════════════════
-- 2. VERIFY RAW CUSTOMERS TABLE
-- ═══════════════════════════════════════════════════════════

SELECT COUNT(*) as total_customers
FROM quickcart_datalake.raw_customers;

SELECT tier, COUNT(*) as customer_count
FROM quickcart_datalake.raw_customers
GROUP BY tier
ORDER BY customer_count DESC;

-- ═══════════════════════════════════════════════════════════
-- 3. VERIFY RAW PRODUCTS TABLE
-- ═══════════════════════════════════════════════════════════

SELECT COUNT(*) as total_products
FROM quickcart_datalake.raw_products;

SELECT category, COUNT(*) as product_count, ROUND(AVG(price), 2) as avg_price
FROM quickcart_datalake.raw_products
GROUP BY category
ORDER BY product_count DESC;

-- ═══════════════════════════════════════════════════════════
-- 4. VERIFY PARQUET TABLE (from Redshift UNLOAD — Project 22)
-- ═══════════════════════════════════════════════════════════

SELECT COUNT(*) as total_rows
FROM quickcart_datalake.curated_orders_snapshot;

-- Parquet types should be perfect (embedded schema)
SELECT 
    order_id,
    quantity,         -- int (exact from Parquet)
    total_amount,     -- double (exact from Parquet)
    order_date,       -- date (exact from Parquet)
    day_of_week,      -- int (computed in UNLOAD query)
    is_completed      -- int (computed in UNLOAD query)
FROM quickcart_datalake.curated_orders_snapshot
LIMIT 5;

-- ═══════════════════════════════════════════════════════════
-- 5. CROSS-TABLE JOIN (proves all tables work together)
-- ═══════════════════════════════════════════════════════════

-- Join orders with customers to find top-spending customers
SELECT 
    c.customer_id,
    c.name,
    c.tier,
    c.city,
    COUNT(o.order_id) as order_count,
    ROUND(SUM(o.total_amount), 2) as total_spent,
    ROUND(AVG(o.total_amount), 2) as avg_order
FROM quickcart_datalake.raw_orders o
JOIN quickcart_datalake.raw_customers c
    ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.tier, c.city
ORDER BY total_spent DESC
LIMIT 10;

-- Join orders with products to find best-selling categories
SELECT 
    p.category,
    COUNT(o.order_id) as orders,
    SUM(o.quantity) as units_sold,
    ROUND(SUM(o.total_amount), 2) as revenue
FROM quickcart_datalake.raw_orders o
JOIN quickcart_datalake.raw_products p
    ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;

-- ═══════════════════════════════════════════════════════════
-- 6. COMPARE CSV vs PARQUET PERFORMANCE
-- ═══════════════════════════════════════════════════════════

-- CSV table scan (scans ALL data — expensive)
SELECT status, COUNT(*) as cnt, ROUND(SUM(total_amount), 2) as revenue
FROM quickcart_datalake.raw_orders
GROUP BY status;
-- Check "Data scanned" in Athena → will be ~1.5 GB (full CSV scan)

-- Parquet table scan (columnar — reads only needed columns)
SELECT status, COUNT(*) as cnt, ROUND(SUM(total_amount), 2) as revenue
FROM quickcart_datalake.curated_orders_snapshot
GROUP BY status;
-- Check "Data scanned" in Athena → will be ~50 MB (columnar read)
-- Same query, 30x less data scanned = 30x cheaper!