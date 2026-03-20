-- file: migrations/R002_redesign_facts.sql
-- Purpose: Redesign fact tables for production COPY loading
-- Adds: surrogate_key reference, audit columns, proper encoding

SET search_path TO analytics;

-- ===================================================================
-- Backup existing fact tables
-- ===================================================================
ALTER TABLE analytics.fact_orders RENAME TO fact_orders_v1_backup;
ALTER TABLE analytics.fact_order_items RENAME TO fact_order_items_v1_backup;

-- ===================================================================
-- FACT: fact_orders (REDESIGNED)
-- 
-- CHANGES from Project 10:
-- → Added customer_sk (surrogate key for SCD2 point-in-time join)
-- → Added date_key (FK to dim_dates for date dimension joins)
-- → Added audit columns (etl_loaded_at, etl_batch_id)
-- → Explicit ENCODE per column (verified with ANALYZE COMPRESSION)
-- → Column order matches expected CSV layout (for COPY positional)
-- ===================================================================
CREATE TABLE analytics.fact_orders (
    -- Business keys
    order_id            VARCHAR(30)     NOT NULL    ENCODE ZSTD,
    customer_id         VARCHAR(20)     NOT NULL    ENCODE ZSTD,
    
    -- Dimension surrogate keys (for point-in-time accuracy)
    customer_sk         BIGINT                      ENCODE AZ64,
    -- customer_sk references dim_customers_scd2.surrogate_key
    -- Populated during ETL: look up SK where customer was current at order_date
    
    -- Measures (the numbers analysts aggregate)
    total_amount        DECIMAL(12,2)   NOT NULL    ENCODE AZ64,
    item_count          SMALLINT        DEFAULT 0   ENCODE AZ64,
    discount_amount     DECIMAL(10,2)   DEFAULT 0   ENCODE AZ64,
    shipping_amount     DECIMAL(10,2)   DEFAULT 0   ENCODE AZ64,
    net_amount          DECIMAL(12,2)               ENCODE AZ64,
    -- net_amount = total_amount - discount_amount + shipping_amount
    -- Computed during ETL, not GENERATED (Redshift doesn't support GENERATED)
    
    -- Degenerate dimensions (dimension attributes stored in fact)
    status              VARCHAR(20)     NOT NULL    ENCODE BYTEDICT,
    -- Status is a degenerate dimension: low cardinality, stored directly
    -- Not worth creating dim_order_status for 5 values
    
    -- Date keys
    order_date          DATE            NOT NULL    ENCODE AZ64,
    -- Also serves as FK to dim_dates.date_key
    
    -- Timestamps (for CDC tracking — connects to Project 42)
    source_created_at   TIMESTAMP                   ENCODE AZ64,
    source_updated_at   TIMESTAMP                   ENCODE AZ64,
    -- These come from MariaDB's created_at/updated_at columns
    
    -- Audit columns
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()  ENCODE AZ64,
    etl_batch_id        VARCHAR(50)                        ENCODE ZSTD,
    source_system       VARCHAR(50)     DEFAULT 'mariadb'  ENCODE BYTEDICT,
    
    PRIMARY KEY (order_id)
)
DISTKEY (customer_id)
COMPOUND SORTKEY (order_date, status)
;

-- ===================================================================
-- FACT: fact_order_items (REDESIGNED)
-- ===================================================================
CREATE TABLE analytics.fact_order_items (
    -- Surrogate key for this table
    item_id             BIGINT          NOT NULL    ENCODE AZ64,
    
    -- Foreign keys
    order_id            VARCHAR(30)     NOT NULL    ENCODE ZSTD,
    product_id          VARCHAR(20)     NOT NULL    ENCODE ZSTD,
    customer_id         VARCHAR(20)     NOT NULL    ENCODE ZSTD,
    
    -- Dimension surrogate keys
    customer_sk         BIGINT                      ENCODE AZ64,
    
    -- Measures
    quantity            SMALLINT        NOT NULL    ENCODE AZ64,
    unit_price          DECIMAL(10,2)   NOT NULL    ENCODE AZ64,
    line_total          DECIMAL(12,2)   NOT NULL    ENCODE AZ64,
    
    -- Denormalized date (avoids joining back to fact_orders)
    order_date          DATE            NOT NULL    ENCODE AZ64,
    
    -- Audit
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()  ENCODE AZ64,
    etl_batch_id        VARCHAR(50)                        ENCODE ZSTD,
    
    PRIMARY KEY (item_id)
)
DISTKEY (customer_id)
COMPOUND SORTKEY (order_date, product_id)
;

-- ===================================================================
-- MIGRATE data from v1 backup
-- ===================================================================
INSERT INTO analytics.fact_orders (
    order_id, customer_id, customer_sk, total_amount, item_count,
    discount_amount, shipping_amount, net_amount, status, order_date,
    source_created_at, source_updated_at, etl_batch_id
)
SELECT 
    f.order_id,
    f.customer_id,
    d.surrogate_key AS customer_sk,
    f.total_amount,
    f.item_count,
    0 AS discount_amount,
    0 AS shipping_amount,
    f.total_amount AS net_amount,
    f.status,
    f.order_date,
    f.created_at AS source_created_at,
    f.updated_at AS source_updated_at,
    'MIGRATION-002' AS etl_batch_id
FROM analytics.fact_orders_v1_backup f
LEFT JOIN analytics.dim_customers_scd2 d
    ON f.customer_id = d.customer_id
   AND f.order_date >= d.effective_from
   AND f.order_date < d.effective_to;

INSERT INTO analytics.fact_order_items (
    item_id, order_id, product_id, customer_id, customer_sk,
    quantity, unit_price, line_total, order_date, etl_batch_id
)
SELECT 
    fi.item_id,
    fi.order_id,
    fi.product_id,
    fi.customer_id,
    d.surrogate_key AS customer_sk,
    fi.quantity,
    fi.unit_price,
    fi.line_total,
    fi.order_date,
    'MIGRATION-002' AS etl_batch_id
FROM analytics.fact_order_items_v1_backup fi
LEFT JOIN analytics.dim_customers_scd2 d
    ON fi.customer_id = d.customer_id
   AND fi.order_date >= d.effective_from
   AND fi.order_date < d.effective_to;

-- Verify
SELECT 'fact_orders' AS tbl, COUNT(*) AS rows FROM analytics.fact_orders
UNION ALL
SELECT 'fact_order_items', COUNT(*) FROM analytics.fact_order_items
UNION ALL
SELECT 'with_customer_sk', COUNT(*) FROM analytics.fact_orders WHERE customer_sk IS NOT NULL;