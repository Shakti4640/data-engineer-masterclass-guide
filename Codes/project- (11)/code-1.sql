-- file: migrations/R001_scd2_customers.sql
-- Purpose: Create SCD Type 2 customer dimension
-- Depends: Project 10 schema (basic dim_customers)
-- Run via: psql -h <endpoint> -p 5439 -U admin_user -d quickcart_dw

SET search_path TO analytics;

-- ===================================================================
-- DROP basic dimension from Project 10 (replacing with SCD2)
-- ===================================================================
-- Keep the old table for reference until migration verified
ALTER TABLE analytics.dim_customers RENAME TO dim_customers_v1_backup;

-- ===================================================================
-- SCD TYPE 2 CUSTOMER DIMENSION
-- 
-- KEY DESIGN DECISIONS:
-- → surrogate_key: unique per VERSION (not per customer)
-- → customer_id: natural business key (from MariaDB)
-- → effective_from/to: validity window for this version
-- → is_current: fast filter for "latest version only"
-- → DISTSTYLE ALL: small table, every node gets full copy
-- → SORTKEY(customer_id, effective_from): fast point-in-time lookup
-- ===================================================================
CREATE TABLE analytics.dim_customers_scd2 (
    -- Surrogate key: unique identifier for this VERSION of the customer
    -- Generated during ETL as: ROW_NUMBER() or hash-based
    surrogate_key       BIGINT          NOT NULL    ENCODE AZ64,
    
    -- Natural business key: identifies the CUSTOMER across versions
    customer_id         VARCHAR(20)     NOT NULL    ENCODE ZSTD,
    
    -- Attributes that may change (tracked by SCD2)
    name                VARCHAR(100)    NOT NULL    ENCODE ZSTD,
    email               VARCHAR(255)    NOT NULL    ENCODE ZSTD,
    city                VARCHAR(100)                ENCODE ZSTD,
    state               VARCHAR(50)                 ENCODE ZSTD,
    tier                VARCHAR(20)     DEFAULT 'bronze' ENCODE BYTEDICT,
    
    -- Attributes that rarely change (still included for completeness)
    signup_date         DATE            NOT NULL    ENCODE AZ64,
    
    -- SCD2 metadata columns
    effective_from      DATE            NOT NULL    ENCODE AZ64,
    effective_to        DATE            NOT NULL    ENCODE AZ64,
    -- effective_to = '9999-12-31' means this version is CURRENT
    
    is_current          BOOLEAN         NOT NULL    ENCODE RAW,
    -- Redundant with effective_to but makes queries much simpler:
    -- WHERE is_current = TRUE vs WHERE effective_to = '9999-12-31'
    
    -- Audit columns (for ETL debugging)
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()   ENCODE AZ64,
    etl_batch_id        VARCHAR(50)                         ENCODE ZSTD,
    source_system       VARCHAR(50)     DEFAULT 'mariadb'   ENCODE BYTEDICT,
    
    PRIMARY KEY (surrogate_key)
)
DISTSTYLE ALL
COMPOUND SORTKEY (customer_id, effective_from)
;

-- ===================================================================
-- VIEW: dim_customers_current
-- Convenience view — shows only latest version of each customer
-- Analysts use THIS instead of dealing with SCD2 complexity
-- ===================================================================
CREATE OR REPLACE VIEW analytics.dim_customers_current AS
SELECT 
    surrogate_key,
    customer_id,
    name,
    email,
    city,
    state,
    tier,
    signup_date,
    effective_from,
    etl_loaded_at
FROM analytics.dim_customers_scd2
WHERE is_current = TRUE;

-- ===================================================================
-- MIGRATE data from old dim_customers (Project 10) to SCD2
-- All existing customers get ONE version (current)
-- ===================================================================
INSERT INTO analytics.dim_customers_scd2 (
    surrogate_key, customer_id, name, email, city, state, tier,
    signup_date, effective_from, effective_to, is_current,
    etl_batch_id, source_system
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY customer_id) AS surrogate_key,
    customer_id,
    name,
    email,
    city,
    state,
    tier,
    signup_date,
    signup_date AS effective_from,           -- Version started at signup
    '9999-12-31'::DATE AS effective_to,      -- Currently active
    TRUE AS is_current,
    'MIGRATION-001' AS etl_batch_id,
    'mariadb' AS source_system
FROM analytics.dim_customers_v1_backup;

-- Verify migration
SELECT 
    COUNT(*) AS total_versions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions,
    MIN(effective_from) AS earliest_version,
    MAX(effective_from) AS latest_version
FROM analytics.dim_customers_scd2;

-- Verify view
SELECT COUNT(*) AS current_customers FROM analytics.dim_customers_current;