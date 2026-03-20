-- file: migrations/V001_extend_schema.sql
-- Purpose: Add new tables to support production features
-- Depends: Project 8 schema (customers, products, orders, order_items)
-- Run as: mysql -u root -p quickcart_db < V001_extend_schema.sql

USE quickcart_db;

-- ===================================================================
-- TABLE: customer_addresses (1:many with customers)
-- WHY: Customer can have billing + shipping + old addresses
-- Storing in customer table = 1NF violation
-- ===================================================================
CREATE TABLE IF NOT EXISTS customer_addresses (
    address_id      BIGINT UNSIGNED     NOT NULL AUTO_INCREMENT,
    customer_id     VARCHAR(20)         NOT NULL,
    address_type    ENUM('billing', 'shipping', 'both') 
                                        NOT NULL DEFAULT 'both',
    street_line1    VARCHAR(255)        NOT NULL,
    street_line2    VARCHAR(255)        DEFAULT NULL,
    city            VARCHAR(100)        NOT NULL,
    state           VARCHAR(50)         NOT NULL,
    zip_code        VARCHAR(20)         NOT NULL,
    country         VARCHAR(50)         NOT NULL DEFAULT 'US',
    is_default      BOOLEAN             NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP           DEFAULT CURRENT_TIMESTAMP 
                                        ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (address_id),

    CONSTRAINT fk_address_customer
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        ON DELETE CASCADE           -- Delete customer → delete addresses
        ON UPDATE CASCADE,

    -- Fast lookup: all addresses for a customer
    KEY idx_customer_id (customer_id),

    -- Fast lookup: default address for a customer
    KEY idx_customer_default (customer_id, is_default)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Customer addresses — multiple per customer';


-- ===================================================================
-- TABLE: product_reviews (1:many with products AND customers)
-- WHY: Tracks customer feedback, used for analytics and recommendations
-- ===================================================================
CREATE TABLE IF NOT EXISTS product_reviews (
    review_id       BIGINT UNSIGNED     NOT NULL AUTO_INCREMENT,
    product_id      VARCHAR(20)         NOT NULL,
    customer_id     VARCHAR(20)         NOT NULL,
    rating          TINYINT UNSIGNED    NOT NULL,
    review_title    VARCHAR(200)        DEFAULT NULL,
    review_body     TEXT                DEFAULT NULL,
    is_verified     BOOLEAN             NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP           DEFAULT CURRENT_TIMESTAMP 
                                        ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (review_id),

    CONSTRAINT fk_review_product
        FOREIGN KEY (product_id) REFERENCES products(product_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_review_customer
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        ON DELETE CASCADE,

    -- One review per customer per product
    UNIQUE KEY uk_product_customer (product_id, customer_id),

    -- Rating must be 1-5
    CONSTRAINT chk_rating CHECK (rating BETWEEN 1 AND 5),

    -- Reviews for a product, newest first
    KEY idx_product_created (product_id, created_at DESC),

    -- Reviews by a customer
    KEY idx_customer_id (customer_id),

    -- Filter by rating
    KEY idx_rating (rating)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Product reviews by customers — one per customer per product';


-- ===================================================================
-- TABLE: order_status_history (audit trail — append only)
-- WHY: Track every status change for customer support & analytics
-- PATTERN: never UPDATE this table — only INSERT
-- ===================================================================
CREATE TABLE IF NOT EXISTS order_status_history (
    history_id      BIGINT UNSIGNED     NOT NULL AUTO_INCREMENT,
    order_id        VARCHAR(30)         NOT NULL,
    old_status      ENUM('pending', 'shipped', 'completed',
                         'cancelled', 'refunded')   DEFAULT NULL,
    new_status      ENUM('pending', 'shipped', 'completed',
                         'cancelled', 'refunded')   NOT NULL,
    changed_by      VARCHAR(100)        NOT NULL DEFAULT 'system',
    change_reason   VARCHAR(500)        DEFAULT NULL,
    changed_at      TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (history_id),

    CONSTRAINT fk_history_order
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
        ON DELETE CASCADE,

    -- Timeline for an order: all status changes in sequence
    KEY idx_order_changed (order_id, changed_at),

    -- Analytics: how many orders changed to X status today
    KEY idx_status_date (new_status, changed_at),

    -- Audit: who changed what
    KEY idx_changed_by (changed_by, changed_at)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Append-only audit log of order status changes';


-- ===================================================================
-- TABLE: _etl_watermarks (CDC tracking — used in Projects 31, 42)
-- WHY: Track last extraction time per table for incremental loads
-- Pattern: ETL reads updated_at > watermark, then updates watermark
-- ===================================================================
CREATE TABLE IF NOT EXISTS _etl_watermarks (
    table_name      VARCHAR(100)        NOT NULL,
    pipeline_name   VARCHAR(100)        NOT NULL,
    last_extracted  TIMESTAMP           NOT NULL DEFAULT '2000-01-01 00:00:00',
    row_count       BIGINT UNSIGNED     DEFAULT 0,
    status          ENUM('idle', 'running', 'completed', 'failed')
                                        DEFAULT 'idle',
    updated_at      TIMESTAMP           DEFAULT CURRENT_TIMESTAMP 
                                        ON UPDATE CURRENT_TIMESTAMP,

    -- One watermark per table per pipeline
    PRIMARY KEY (table_name, pipeline_name)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='ETL watermark tracking — enables incremental extraction';


-- ===================================================================
-- INITIALIZE WATERMARKS for existing tables
-- ===================================================================
INSERT IGNORE INTO _etl_watermarks (table_name, pipeline_name, last_extracted)
VALUES
    ('customers',       'glue_s3_export',    '2000-01-01 00:00:00'),
    ('products',        'glue_s3_export',    '2000-01-01 00:00:00'),
    ('orders',          'glue_s3_export',    '2000-01-01 00:00:00'),
    ('order_items',     'glue_s3_export',    '2000-01-01 00:00:00'),
    ('product_reviews', 'glue_s3_export',    '2000-01-01 00:00:00');


-- ===================================================================
-- VERIFY ALL TABLES
-- ===================================================================
SELECT 
    TABLE_NAME,
    ENGINE,
    TABLE_ROWS,
    ROUND(DATA_LENGTH / 1024, 1) AS data_kb,
    ROUND(INDEX_LENGTH / 1024, 1) AS index_kb,
    TABLE_COMMENT
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'quickcart_db'
ORDER BY TABLE_NAME;