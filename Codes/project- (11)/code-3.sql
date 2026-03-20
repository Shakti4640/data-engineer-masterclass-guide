-- file: migrations/R003_staging_wide_agg.sql
-- Purpose: Create staging tables, denormalized wide fact, aggregates, audit

-- ===================================================================
-- STAGING SCHEMA
-- ===================================================================
CREATE SCHEMA IF NOT EXISTS staging;

-- Staging tables mirror fact/dim structure but with NO constraints
-- DISTSTYLE EVEN — no optimization needed for temporary data
-- NO SORTKEY — data will be replaced each load

CREATE TABLE staging.stg_customers (
    customer_id     VARCHAR(20),
    name            VARCHAR(100),
    email           VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    tier            VARCHAR(20),
    signup_date     DATE,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
) DISTSTYLE EVEN;

CREATE TABLE staging.stg_products (
    product_id      VARCHAR(20),
    name            VARCHAR(200),
    category        VARCHAR(100),
    price           DECIMAL(10,2),
    stock_quantity  INTEGER,
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
) DISTSTYLE EVEN;

CREATE TABLE staging.stg_orders (
    order_id        VARCHAR(30),
    customer_id     VARCHAR(20),
    total_amount    DECIMAL(12,2),
    status          VARCHAR(20),
    order_date      DATE,
    item_count      SMALLINT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
) DISTSTYLE EVEN;

CREATE TABLE staging.stg_order_items (
    item_id         BIGINT,
    order_id        VARCHAR(30),
    product_id      VARCHAR(20),
    quantity        SMALLINT,
    unit_price      DECIMAL(10,2),
    line_total      DECIMAL(12,2)
) DISTSTYLE EVEN;

-- ===================================================================
-- DENORMALIZED WIDE FACT TABLE
-- Pre-joins: fact_orders + dim_customers_current + dim_products
-- 90% of dashboard queries need these 3 tables joined
-- This eliminates runtime JOINs for common queries
-- ===================================================================
CREATE TABLE analytics.fact_orders_wide (
    -- Order facts
    order_id            VARCHAR(30)     NOT NULL    ENCODE ZSTD,
    order_date          DATE            NOT NULL    ENCODE AZ64,
    total_amount        DECIMAL(12,2)   NOT NULL    ENCODE AZ64,
    item_count          SMALLINT                    ENCODE AZ64,
    net_amount          DECIMAL(12,2)               ENCODE AZ64,
    status              VARCHAR(20)     NOT NULL    ENCODE BYTEDICT,
    
    -- Customer attributes (denormalized from dim_customers)
    customer_id         VARCHAR(20)     NOT NULL    ENCODE ZSTD,
    customer_name       VARCHAR(100)                ENCODE ZSTD,
    customer_city       VARCHAR(100)                ENCODE ZSTD,
    customer_state      VARCHAR(50)                 ENCODE ZSTD,
    customer_tier       VARCHAR(20)                 ENCODE BYTEDICT,
    customer_signup     DATE                        ENCODE AZ64,
    
    -- Date attributes (denormalized from dim_dates)
    order_year          SMALLINT                    ENCODE AZ64,
    order_quarter       SMALLINT                    ENCODE AZ64,
    order_month         SMALLINT                    ENCODE AZ64,
    order_month_name    VARCHAR(20)                 ENCODE BYTEDICT,
    order_day_name      VARCHAR(20)                 ENCODE BYTEDICT,
    is_weekend          BOOLEAN                     ENCODE RAW,
    
    -- Audit
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()   ENCODE AZ64,
    etl_batch_id        VARCHAR(50)                         ENCODE ZSTD,
    
    PRIMARY KEY (order_id)
)
DISTKEY (customer_id)
COMPOUND SORTKEY (order_date, customer_tier, status)
;

-- ===================================================================
-- AGGREGATE SCHEMA
-- ===================================================================
CREATE SCHEMA IF NOT EXISTS agg;

-- Daily revenue summary
CREATE TABLE agg.agg_daily_revenue (
    date_key            DATE            NOT NULL    ENCODE AZ64,
    status              VARCHAR(20)     NOT NULL    ENCODE BYTEDICT,
    order_count         INTEGER         NOT NULL    ENCODE AZ64,
    total_revenue       DECIMAL(14,2)   NOT NULL    ENCODE AZ64,
    avg_order_value     DECIMAL(10,2)   NOT NULL    ENCODE AZ64,
    total_items         INTEGER         NOT NULL    ENCODE AZ64,
    unique_customers    INTEGER         NOT NULL    ENCODE AZ64,
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()   ENCODE AZ64,
    
    PRIMARY KEY (date_key, status)
)
DISTSTYLE ALL
SORTKEY (date_key)
;

-- Monthly category summary
CREATE TABLE agg.agg_monthly_category (
    year_month          VARCHAR(7)      NOT NULL    ENCODE ZSTD,
    category            VARCHAR(100)    NOT NULL    ENCODE ZSTD,
    order_count         INTEGER         NOT NULL    ENCODE AZ64,
    units_sold          INTEGER         NOT NULL    ENCODE AZ64,
    revenue             DECIMAL(14,2)   NOT NULL    ENCODE AZ64,
    unique_customers    INTEGER         NOT NULL    ENCODE AZ64,
    unique_products     INTEGER         NOT NULL    ENCODE AZ64,
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()   ENCODE AZ64,
    
    PRIMARY KEY (year_month, category)
)
DISTSTYLE ALL
SORTKEY (year_month)
;

-- ===================================================================
-- AUDIT SCHEMA
-- ===================================================================
CREATE SCHEMA IF NOT EXISTS audit;

-- ETL job execution log
CREATE TABLE audit.etl_job_log (
    job_id              BIGINT IDENTITY(1,1)    NOT NULL,
    job_name            VARCHAR(200)    NOT NULL    ENCODE ZSTD,
    batch_id            VARCHAR(50)     NOT NULL    ENCODE ZSTD,
    status              VARCHAR(20)     NOT NULL    ENCODE BYTEDICT,
    started_at          TIMESTAMP       NOT NULL    ENCODE AZ64,
    completed_at        TIMESTAMP                   ENCODE AZ64,
    rows_processed      BIGINT          DEFAULT 0   ENCODE AZ64,
    rows_inserted       BIGINT          DEFAULT 0   ENCODE AZ64,
    rows_updated        BIGINT          DEFAULT 0   ENCODE AZ64,
    rows_deleted        BIGINT          DEFAULT 0   ENCODE AZ64,
    error_message       VARCHAR(2000)               ENCODE ZSTD,
    source_system       VARCHAR(50)                 ENCODE BYTEDICT,
    target_table        VARCHAR(200)                ENCODE ZSTD,
    
    PRIMARY KEY (job_id)
)
DISTSTYLE ALL
SORTKEY (started_at)
;

-- Row count verification
CREATE TABLE audit.etl_row_counts (
    check_id            BIGINT IDENTITY(1,1)    NOT NULL,
    batch_id            VARCHAR(50)     NOT NULL    ENCODE ZSTD,
    table_name          VARCHAR(200)    NOT NULL    ENCODE ZSTD,
    source_count        BIGINT                      ENCODE AZ64,
    target_count        BIGINT                      ENCODE AZ64,
    match_status        VARCHAR(20)                 ENCODE BYTEDICT,
    checked_at          TIMESTAMP       DEFAULT GETDATE()   ENCODE AZ64,
    
    PRIMARY KEY (check_id)
)
DISTSTYLE ALL
SORTKEY (checked_at)
;