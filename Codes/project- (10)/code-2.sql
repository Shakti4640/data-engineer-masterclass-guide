-- file: 02_create_schema.sql
-- Purpose: Create Redshift-optimized analytics schema
-- Run via: psql -h <endpoint> -p 5439 -U admin_user -d quickcart_dw -f 02_create_schema.sql
-- 
-- KEY DIFFERENCE from MariaDB (Project 9):
-- → No FOREIGN KEY enforcement (Redshift doesn't enforce, uses for optimizer hints)
-- → Distribution keys chosen for JOIN optimization
-- → Sort keys chosen for WHERE clause optimization
-- → Column encodings for compression
-- → No AUTO_INCREMENT — use explicit IDs

-- ===================================================================
-- SCHEMA: analytics
-- Separate schema for business tables (vs pg_catalog, information_schema)
-- ===================================================================
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics;

-- ===================================================================
-- DIMENSION: dim_customers
-- DISTSTYLE ALL: small table (<1M rows), copied to every node
-- Every JOIN with this table is local — no network shuffle
-- ===================================================================
CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_id     VARCHAR(20)     NOT NULL,
    name            VARCHAR(100)    NOT NULL,
    email           VARCHAR(255)    NOT NULL,
    city            VARCHAR(100),
    state           VARCHAR(50),
    tier            VARCHAR(20)     DEFAULT 'bronze',
    signup_date     DATE            NOT NULL,
    created_at      TIMESTAMP       DEFAULT GETDATE(),
    updated_at      TIMESTAMP       DEFAULT GETDATE(),
    
    -- PRIMARY KEY: not enforced but used by query optimizer
    -- Optimizer knows customer_id is unique → better join estimates
    PRIMARY KEY (customer_id)
)
DISTSTYLE ALL                       -- Copy to all nodes (small dimension)
SORTKEY (customer_id)               -- Fast lookup by customer_id
;

-- ===================================================================
-- DIMENSION: dim_products
-- DISTSTYLE ALL: small table (<100K rows)
-- ===================================================================
CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_id      VARCHAR(20)     NOT NULL,
    name            VARCHAR(200)    NOT NULL,
    category        VARCHAR(100)    NOT NULL,
    price           DECIMAL(10,2)   NOT NULL,
    stock_quantity  INTEGER         DEFAULT 0,
    is_active       BOOLEAN         DEFAULT TRUE,
    created_at      TIMESTAMP       DEFAULT GETDATE(),
    updated_at      TIMESTAMP       DEFAULT GETDATE(),
    
    PRIMARY KEY (product_id)
)
DISTSTYLE ALL
SORTKEY (category, product_id)      -- Fast GROUP BY category
;

-- ===================================================================
-- DIMENSION: dim_dates (Date dimension — common in warehousing)
-- WHY: pre-computed date attributes avoid repeated DATE functions
-- Instead of: EXTRACT(month FROM order_date) in every query
-- Just JOIN: dim_dates.month_name, dim_dates.quarter, dim_dates.is_weekend
-- ===================================================================
CREATE TABLE IF NOT EXISTS analytics.dim_dates (
    date_key        DATE            NOT NULL,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL,
    month           SMALLINT        NOT NULL,
    month_name      VARCHAR(20)     NOT NULL,
    week            SMALLINT        NOT NULL,
    day_of_month    SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL,
    day_name        VARCHAR(20)     NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,
    is_holiday      BOOLEAN         DEFAULT FALSE,
    fiscal_year     SMALLINT        NOT NULL,
    fiscal_quarter  SMALLINT        NOT NULL,
    
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date_key)
;

-- ===================================================================
-- FACT: fact_orders (Order header — one row per order)
-- DISTKEY(customer_id): co-locate with dim_customers for fast JOINs
-- SORTKEY(order_date): most queries filter by date range
-- ===================================================================
CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id        VARCHAR(30)     NOT NULL    ENCODE ZSTD,
    customer_id     VARCHAR(20)     NOT NULL    ENCODE ZSTD,
    total_amount    DECIMAL(12,2)   NOT NULL    ENCODE AZ64,
    status          VARCHAR(20)     NOT NULL    ENCODE ZSTD,
    order_date      DATE            NOT NULL    ENCODE AZ64,
    item_count      SMALLINT        DEFAULT 0   ENCODE AZ64,
    created_at      TIMESTAMP       DEFAULT GETDATE()  ENCODE AZ64,
    updated_at      TIMESTAMP       DEFAULT GETDATE()  ENCODE AZ64,
    
    PRIMARY KEY (order_id)
)
DISTKEY (customer_id)               -- JOINs with dim_customers: no shuffle
COMPOUND SORTKEY (order_date, status)  -- Date range + status filter
;
-- NOTE: DISTKEY(customer_id) means:
-- All orders for CUST-000001 are on the SAME node slice
-- JOIN fact_orders ON dim_customers WHERE customer_id = ? → local join
-- But dim_customers is DISTSTYLE ALL → already on every node anyway
-- So this DISTKEY mainly helps when joining two DISTKEY(customer_id) tables

-- ===================================================================
-- FACT: fact_order_items (Order detail — one row per product per order)
-- DISTKEY(customer_id): NOT product_id, because most JOINs are 
--   fact_order_items → fact_orders ON order_id
--   If we used DISTKEY(order_id), items for same order = same slice
--   But order_id has HIGH cardinality → good distribution
-- ===================================================================
CREATE TABLE IF NOT EXISTS analytics.fact_order_items (
    item_id         BIGINT          NOT NULL    ENCODE AZ64,
    order_id        VARCHAR(30)     NOT NULL    ENCODE ZSTD,
    product_id      VARCHAR(20)     NOT NULL    ENCODE ZSTD,
    customer_id     VARCHAR(20)     NOT NULL    ENCODE ZSTD,
    quantity        SMALLINT        NOT NULL    ENCODE AZ64,
    unit_price      DECIMAL(10,2)   NOT NULL    ENCODE AZ64,
    line_total      DECIMAL(12,2)   NOT NULL    ENCODE AZ64,
    order_date      DATE            NOT NULL    ENCODE AZ64,
    
    PRIMARY KEY (item_id)
)
DISTKEY (customer_id)
COMPOUND SORTKEY (order_date, product_id)
;
-- WHY customer_id as DISTKEY for order_items too:
-- → fact_orders DISTKEY(customer_id) + fact_order_items DISTKEY(customer_id)
-- → JOIN ON customer_id: co-located, no redistribution
-- → We denormalize customer_id and order_date into order_items
--   to avoid joining back to fact_orders for common filters

-- ===================================================================
-- STAGING TABLE: used for incremental loads (Project 21, 65)
-- Temporary holding area before MERGE into fact tables
-- ===================================================================
CREATE TABLE IF NOT EXISTS analytics.stg_orders (
    order_id        VARCHAR(30),
    customer_id     VARCHAR(20),
    total_amount    DECIMAL(12,2),
    status          VARCHAR(20),
    order_date      DATE,
    item_count      SMALLINT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
)
DISTSTYLE EVEN                      -- Staging: no optimization needed
;

-- ===================================================================
-- VERIFY
-- ===================================================================
SELECT 
    schemaname,
    tablename,
    diststyle,
    sortkey1,
    tbl_rows
FROM svv_table_info
WHERE schemaname = 'analytics'
ORDER BY tablename;

-- ===================================================================
-- POPULATE dim_dates (2024-01-01 to 2026-12-31)
-- Generate 3 years of date dimension — never changes
-- ===================================================================
INSERT INTO analytics.dim_dates
WITH date_series AS (
    SELECT 
        (DATE '2024-01-01' + row_number OVER ()  - 1)::DATE AS date_key
    FROM stv_blocklist
    LIMIT 1096         -- 3 years = 365*3 + 1 leap day
)
SELECT 
    date_key,
    EXTRACT(YEAR FROM date_key)::SMALLINT AS year,
    EXTRACT(QUARTER FROM date_key)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM date_key)::SMALLINT AS month,
    TO_CHAR(date_key, 'Month') AS month_name,
    EXTRACT(WEEK FROM date_key)::SMALLINT AS week,
    EXTRACT(DAY FROM date_key)::SMALLINT AS day_of_month,
    EXTRACT(DOW FROM date_key)::SMALLINT AS day_of_week,
    TO_CHAR(date_key, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,                 -- Populate holidays separately
    CASE 
        WHEN EXTRACT(MONTH FROM date_key) >= 4 
        THEN EXTRACT(YEAR FROM date_key)::SMALLINT
        ELSE (EXTRACT(YEAR FROM date_key) - 1)::SMALLINT
    END AS fiscal_year,                  -- April fiscal year start
    CASE 
        WHEN EXTRACT(MONTH FROM date_key) >= 4 
        THEN ((EXTRACT(MONTH FROM date_key) - 4) / 3 + 1)::SMALLINT
        ELSE ((EXTRACT(MONTH FROM date_key) + 8) / 3 + 1)::SMALLINT
    END AS fiscal_quarter
FROM date_series
WHERE date_key <= '2026-12-31';

-- Verify date dimension
SELECT MIN(date_key) AS first_date, MAX(date_key) AS last_date, COUNT(*) AS total_days
FROM analytics.dim_dates;