# file: 19_01_create_mariadb_schema.py
# Purpose: Create MariaDB database, tables, and metadata tracking table
# Dependency: MariaDB running on EC2 (Project 8-9)
# NOTE: Run this ONCE to set up the schema

import pymysql
import os

# ── DATABASE CONFIGURATION ──
# In production: use Secrets Manager (Project 70) or env vars
# Here: environment variables as minimum acceptable approach
DB_CONFIG = {
    "host": os.environ.get("MARIADB_HOST", "localhost"),
    "port": int(os.environ.get("MARIADB_PORT", 3306)),
    "user": os.environ.get("MARIADB_USER", "quickcart_etl"),
    "password": os.environ.get("MARIADB_PASSWORD", "etl_password_123"),
    "charset": "utf8mb4",
    "autocommit": True  # For DDL statements
}

DATABASE_NAME = "quickcart"


def get_connection(database=None):
    """Create MariaDB connection with optional database selection"""
    config = DB_CONFIG.copy()
    if database:
        config["database"] = database
    return pymysql.connect(**config)


def create_database_and_tables():
    """Create QuickCart database with all required tables"""

    # ── STEP 1: CREATE DATABASE ──
    print("=" * 70)
    print("STEP 1: Creating database")
    print("=" * 70)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} "
                   f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    print(f"   ✅ Database '{DATABASE_NAME}' ready")

    cursor.execute(f"USE {DATABASE_NAME}")

    # ── STEP 2: CREATE ORDERS TABLE ──
    print("\n" + "=" * 70)
    print("STEP 2: Creating orders table")
    print("=" * 70)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id        VARCHAR(30) NOT NULL,
            customer_id     VARCHAR(20) NOT NULL,
            product_id      VARCHAR(20) NOT NULL,
            quantity         INT NOT NULL DEFAULT 0,
            unit_price      DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
            total_amount    DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
            status          VARCHAR(20) NOT NULL DEFAULT 'unknown',
            order_date      DATE NOT NULL,
            
            -- Primary key for uniqueness
            PRIMARY KEY (order_id),
            
            -- Indexes for common query patterns
            INDEX idx_order_date (order_date),
            INDEX idx_customer_id (customer_id),
            INDEX idx_status (status),
            INDEX idx_product_id (product_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    print("   ✅ Table 'orders' created")

    # ── STEP 3: CREATE CUSTOMERS TABLE ──
    print("\n" + "=" * 70)
    print("STEP 3: Creating customers table")
    print("=" * 70)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id     VARCHAR(20) NOT NULL,
            name            VARCHAR(100) NOT NULL,
            email           VARCHAR(200),
            city            VARCHAR(100),
            state           VARCHAR(50),
            tier            VARCHAR(20) DEFAULT 'bronze',
            signup_date     DATE,
            
            PRIMARY KEY (customer_id),
            INDEX idx_tier (tier),
            INDEX idx_city (city)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    print("   ✅ Table 'customers' created")

    # ── STEP 4: CREATE PRODUCTS TABLE ──
    print("\n" + "=" * 70)
    print("STEP 4: Creating products table")
    print("=" * 70)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id       VARCHAR(20) NOT NULL,
            name             VARCHAR(200) NOT NULL,
            category         VARCHAR(100),
            price            DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
            stock_quantity   INT DEFAULT 0,
            is_active        BOOLEAN DEFAULT TRUE,
            
            PRIMARY KEY (product_id),
            INDEX idx_category (category)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    print("   ✅ Table 'products' created")

    # ── STEP 5: CREATE LOAD METADATA TABLE ──
    # Tracks every file load for: idempotency check, audit, debugging
    print("\n" + "=" * 70)
    print("STEP 5: Creating load_metadata tracking table")
    print("=" * 70)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS load_metadata (
            load_id          INT AUTO_INCREMENT PRIMARY KEY,
            source_file      VARCHAR(500) NOT NULL,
            target_table     VARCHAR(100) NOT NULL,
            s3_bucket        VARCHAR(200),
            s3_key           VARCHAR(500),
            file_size_bytes  BIGINT DEFAULT 0,
            rows_loaded      INT DEFAULT 0,
            rows_rejected    INT DEFAULT 0,
            load_status      ENUM('started', 'completed', 'failed', 'skipped')
                             NOT NULL DEFAULT 'started',
            error_message    TEXT,
            started_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at     DATETIME,
            
            -- Unique constraint: same file loaded to same table only once
            UNIQUE KEY uk_file_table (source_file, target_table),
            
            INDEX idx_status (load_status),
            INDEX idx_started (started_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    print("   ✅ Table 'load_metadata' created")

    # ── STEP 6: VERIFY ──
    print("\n" + "=" * 70)
    print("VERIFICATION: Tables in database")
    print("=" * 70)

    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
        count = cursor.fetchone()[0]
        print(f"   📋 {table[0]}: {count} rows")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    create_database_and_tables()
    print("\n🎯 MariaDB schema ready for data loading")
    print("   → Run 19_02_load_csv_to_mariadb.py next")