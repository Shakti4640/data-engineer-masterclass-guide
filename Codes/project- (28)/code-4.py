-- file: 04_mariadb_setup.sql
-- Creates staging table and processing tracking table
-- Run in MariaDB before starting the worker

-- ═══════════════════════════════════════════
-- DATABASE
-- ═══════════════════════════════════════════
CREATE DATABASE IF NOT EXISTS quickcart;
USE quickcart;

-- ═══════════════════════════════════════════
-- STAGING TABLE (truncated on each file load)
-- ═══════════════════════════════════════════
CREATE TABLE IF NOT EXISTS staging_shipfast_updates (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    tracking_id         VARCHAR(50) NOT NULL,
    order_id            VARCHAR(20) NOT NULL,
    shipping_status     VARCHAR(30) NOT NULL,
    shipped_date        DATE NULL,
    carrier             VARCHAR(50),
    estimated_delivery  DATE NULL,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups during merge
CREATE INDEX idx_staging_order ON staging_shipfast_updates(order_id);
CREATE INDEX idx_staging_tracking ON staging_shipfast_updates(tracking_id);

-- ═══════════════════════════════════════════
-- PROCESSING TRACKER (idempotency)
-- ═══════════════════════════════════════════
CREATE TABLE IF NOT EXISTS processed_files (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    file_key            VARCHAR(500) NOT NULL UNIQUE,
    rows_loaded         INT NOT NULL DEFAULT 0,
    processing_seconds  DECIMAL(10,2),
    processed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_file_key (file_key)
);

-- ═══════════════════════════════════════════
-- VERIFY TABLES
-- ═══════════════════════════════════════════
SHOW TABLES;
DESCRIBE staging_shipfast_updates;
DESCRIBE processed_files;