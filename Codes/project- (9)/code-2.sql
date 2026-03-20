-- file: migrations/V002_add_indexes.sql
-- Purpose: Add indexes based on actual query patterns from Project 8
-- Run as: mysql -u root -p quickcart_db < V002_add_indexes.sql

USE quickcart_db;

-- ===================================================================
-- BEFORE adding indexes: analyze current query performance
-- These queries represent REAL access patterns from the application
-- ===================================================================

-- QUERY PATTERN 1: Customer support looks up order + customer
-- SELECT ... FROM orders o JOIN customers c WHERE o.order_id = ?
-- → Already fast: uses PRIMARY KEY on both tables
-- → No new index needed

-- QUERY PATTERN 2: Marketing — orders by date range + status
-- SELECT ... FROM orders WHERE order_date BETWEEN ? AND ? AND status = ?
-- → Project 8 has idx_date_status(order_date, status) — GOOD
-- → But let's verify and add covering index

-- QUERY PATTERN 3: Dashboard — daily revenue by status
-- SELECT order_date, status, COUNT(*), SUM(total_amount) FROM orders GROUP BY ...
-- → Needs covering index including total_amount
-- → Avoids bookmark lookup for aggregation

-- ===================================================================
-- COVERING INDEX: orders dashboard query
-- Includes total_amount so SUM() can be computed from index alone
-- ===================================================================
ALTER TABLE orders
    ADD KEY idx_date_status_amount (order_date, status, total_amount);

-- The old idx_date_status is now REDUNDANT because:
-- idx_date_status_amount(order_date, status, total_amount)
-- serves all queries that idx_date_status(order_date, status) served
-- PLUS it covers aggregation queries
-- However, the wider index uses more memory — trade-off is acceptable here

-- Drop redundant index (from Project 8)
-- NOTE: verify with EXPLAIN before dropping in production
ALTER TABLE orders
    DROP KEY idx_date_status;


-- ===================================================================
-- INDEX: orders by customer + date (customer order history page)
-- Query: SELECT * FROM orders WHERE customer_id = ? ORDER BY order_date DESC
-- ===================================================================
-- Project 8 has idx_customer_id(customer_id) — but it doesn't help with ORDER BY
-- Adding date to the index eliminates filesort

ALTER TABLE orders
    DROP KEY idx_customer_id,
    ADD KEY idx_customer_date (customer_id, order_date DESC);

-- This composite index serves:
-- WHERE customer_id = ?                          (leftmost prefix)
-- WHERE customer_id = ? ORDER BY order_date DESC (index provides order)
-- WHERE customer_id = ? AND order_date > ?       (range on second column)


-- ===================================================================
-- INDEX: order_items for revenue-by-product reports
-- Query: SELECT p.category, SUM(oi.line_total) FROM order_items oi JOIN products p
-- ===================================================================
-- Already has idx_product_id — but joining requires order_id too

ALTER TABLE order_items
    ADD KEY idx_product_order (product_id, order_id, quantity, unit_price);

-- Covering index for product-level aggregation
-- Avoids hitting main table for quantity and unit_price


-- ===================================================================
-- VERIFY ALL INDEXES
-- ===================================================================
SELECT 
    TABLE_NAME,
    INDEX_NAME,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns,
    INDEX_TYPE,
    NON_UNIQUE,
    CARDINALITY
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = 'quickcart_db'
GROUP BY TABLE_NAME, INDEX_NAME, INDEX_TYPE, NON_UNIQUE, CARDINALITY
ORDER BY TABLE_NAME, INDEX_NAME;