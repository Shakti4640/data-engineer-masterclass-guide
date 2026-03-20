-- file: 01_create_redshift_table.sql
-- Run this in Redshift query editor or via psycopg2

-- Drop if exists (for clean re-runs)
DROP TABLE IF EXISTS public.orders;

-- Create orders table optimized for COPY loading
-- Distribution and sort keys explained in Project 39
-- For now: EVEN distribution (simplest, good for initial loads)

CREATE TABLE public.orders (
    order_id        VARCHAR(20)     NOT NULL,    -- ORD-20250101-000001
    customer_id     VARCHAR(15)     NOT NULL,    -- CUST-000001
    product_id      VARCHAR(12)     NOT NULL,    -- PROD-00001
    quantity        INTEGER         NOT NULL,
    unit_price      DECIMAL(10,2)   NOT NULL,
    total_amount    DECIMAL(12,2)   NOT NULL,
    status          VARCHAR(20)     NOT NULL,    -- completed/pending/shipped
    order_date      DATE            NOT NULL
)
DISTSTYLE EVEN                      -- Rows spread evenly across all slices
SORTKEY (order_date);               -- Optimizes date-range queries

-- Verify table created
SELECT "table", encoded, diststyle, sortkey1
FROM svv_table_info
WHERE "table" = 'orders';