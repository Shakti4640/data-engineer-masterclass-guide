-- file: 04_create_schema.sql
-- Purpose: Create QuickCart e-commerce schema
-- Run as: mysql -u root -p quickcart_db < 04_create_schema.sql

-- ===================================================================
-- WHY THIS SCHEMA DESIGN:
-- → customers, products = Dimension tables (slowly changing)
-- → orders = Fact table header (one row per order)
-- → order_items = Fact table detail (one row per product in order)
-- → This is a classic STAR SCHEMA — reused in Redshift (Project 11)
-- ===================================================================

USE quickcart_db;

-- ===================================================================
-- CUSTOMERS TABLE (Dimension)
-- ===================================================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(20)     NOT NULL,
    name            VARCHAR(100)    NOT NULL,
    email           VARCHAR(255)    NOT NULL,
    city            VARCHAR(100),
    state           VARCHAR(50),
    tier            ENUM('bronze', 'silver', 'gold', 'platinum') 
                                    DEFAULT 'bronze',
    signup_date     DATE            NOT NULL,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP 
                                    ON UPDATE CURRENT_TIMESTAMP,
    
    -- PRIMARY KEY: clustered index in InnoDB
    -- All data physically sorted by customer_id on disk
    PRIMARY KEY (customer_id),
    
    -- UNIQUE constraint on email — prevents duplicate signups
    UNIQUE KEY uk_email (email),
    
    -- INDEX on city — for queries like "all customers in New York"
    KEY idx_city (city),
    
    -- INDEX on tier — for queries like "all gold customers"
    KEY idx_tier (tier),
    
    -- INDEX on signup_date — for date range queries
    KEY idx_signup_date (signup_date)
    
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Customer dimension table — source of truth for customer data';


-- ===================================================================
-- PRODUCTS TABLE (Dimension)
-- ===================================================================
CREATE TABLE IF NOT EXISTS products (
    product_id      VARCHAR(20)     NOT NULL,
    name            VARCHAR(200)    NOT NULL,
    category        VARCHAR(100)    NOT NULL,
    price           DECIMAL(10, 2)  NOT NULL,
    stock_quantity  INT             DEFAULT 0,
    is_active       BOOLEAN         DEFAULT TRUE,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP 
                                    ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (product_id),
    KEY idx_category (category),
    KEY idx_is_active (is_active),
    
    -- COMPOSITE INDEX: category + active — for "active electronics"
    KEY idx_category_active (category, is_active)
    
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Product dimension table — catalog of all products';


-- ===================================================================
-- ORDERS TABLE (Fact - Header)
-- ===================================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR(30)     NOT NULL,
    customer_id     VARCHAR(20)     NOT NULL,
    total_amount    DECIMAL(12, 2)  NOT NULL,
    status          ENUM('pending', 'shipped', 'completed', 
                         'cancelled', 'refunded') 
                                    DEFAULT 'pending',
    order_date      DATE            NOT NULL,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP 
                                    ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (order_id),
    
    -- FOREIGN KEY: ensures every order has a valid customer
    CONSTRAINT fk_orders_customer 
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        ON DELETE RESTRICT      -- Can't delete customer with orders
        ON UPDATE CASCADE,      -- Customer ID rename propagates
    
    -- INDEX on customer_id: fast lookup "all orders for customer X"
    KEY idx_customer_id (customer_id),
    
    -- INDEX on order_date: fast date range queries
    KEY idx_order_date (order_date),
    
    -- INDEX on status: fast "all pending orders"
    KEY idx_status (status),
    
    -- COMPOSITE INDEX: date + status — for "completed orders last month"
    KEY idx_date_status (order_date, status)
    
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Order fact table — one row per order placed';


-- ===================================================================
-- ORDER_ITEMS TABLE (Fact - Detail)
-- ===================================================================
CREATE TABLE IF NOT EXISTS order_items (
    item_id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id        VARCHAR(30)     NOT NULL,
    product_id      VARCHAR(20)     NOT NULL,
    quantity        INT             NOT NULL DEFAULT 1,
    unit_price      DECIMAL(10, 2)  NOT NULL,
    line_total      DECIMAL(12, 2)  GENERATED ALWAYS AS 
                                    (quantity * unit_price) STORED,
    
    PRIMARY KEY (item_id),
    
    CONSTRAINT fk_items_order 
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
        ON DELETE CASCADE,      -- Delete order → delete its items
    
    CONSTRAINT fk_items_product 
        FOREIGN KEY (product_id) REFERENCES products(product_id)
        ON DELETE RESTRICT,     -- Can't delete product with order history
    
    KEY idx_order_id (order_id),
    KEY idx_product_id (product_id),
    
    -- UNIQUE: prevent duplicate product in same order
    UNIQUE KEY uk_order_product (order_id, product_id)
    
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Order line items — one row per product per order';


-- ===================================================================
-- VERIFY SCHEMA
-- ===================================================================
SELECT 
    TABLE_NAME,
    ENGINE,
    TABLE_ROWS,
    ROUND(DATA_LENGTH / 1024 / 1024, 2) AS data_mb,
    ROUND(INDEX_LENGTH / 1024 / 1024, 2) AS index_mb,
    TABLE_COMMENT
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = 'quickcart_db'
ORDER BY TABLE_NAME;