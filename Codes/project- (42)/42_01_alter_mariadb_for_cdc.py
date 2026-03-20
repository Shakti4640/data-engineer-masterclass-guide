# file: 42_01_alter_mariadb_for_cdc.py
# Purpose: Add updated_at and created_at columns to MariaDB tables
# Prerequisite: MariaDB tables exist (Project 9)

import pymysql
from datetime import datetime

# MariaDB connection (Project 8/9 — EC2 hosted)
MARIADB_CONFIG = {
    "host": "10.0.1.50",
    "port": 3306,
    "user": "quickcart_app",
    "password": "secure_password_here",
    "database": "quickcart"
}

# Tables to enable CDC on
CDC_TABLES = {
    "orders": "order_id",
    "customers": "customer_id",
    "products": "product_id"
}


def alter_tables_for_cdc():
    """
    Add CDC columns to existing tables:
    
    created_at: set once on INSERT, never changes
    → DEFAULT CURRENT_TIMESTAMP
    
    updated_at: refreshed on every UPDATE automatically
    → DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    
    WHY separate columns:
    → created_at tells you WHEN the row was born
    → updated_at tells you WHEN the row was last touched
    → If created_at == updated_at → row was never updated after creation
    → If updated_at > created_at → row was modified
    
    Also creates INDEX on updated_at for fast CDC queries.
    """
    conn = pymysql.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()

    for table_name, pk_column in CDC_TABLES.items():
        print(f"\n{'='*50}")
        print(f"🔧 Altering table: {table_name}")
        print(f"{'='*50}")

        # Check if columns already exist
        cursor.execute(f"DESCRIBE {table_name}")
        existing_columns = {row[0] for row in cursor.fetchall()}

        # --- ADD created_at ---
        if "created_at" not in existing_columns:
            sql = f"""
                ALTER TABLE {table_name}
                ADD COLUMN created_at TIMESTAMP 
                NOT NULL 
                DEFAULT CURRENT_TIMESTAMP
                COMMENT 'Row creation timestamp — set once on INSERT'
            """
            cursor.execute(sql)
            print(f"   ✅ Added: created_at")
        else:
            print(f"   ℹ️  created_at already exists")

        # --- ADD updated_at ---
        if "updated_at" not in existing_columns:
            sql = f"""
                ALTER TABLE {table_name}
                ADD COLUMN updated_at TIMESTAMP 
                NOT NULL 
                DEFAULT CURRENT_TIMESTAMP 
                ON UPDATE CURRENT_TIMESTAMP
                COMMENT 'Last modification timestamp — auto-refreshes on UPDATE'
            """
            cursor.execute(sql)
            print(f"   ✅ Added: updated_at (ON UPDATE CURRENT_TIMESTAMP)")
        else:
            print(f"   ℹ️  updated_at already exists")

        # --- CREATE INDEX on updated_at ---
        # Check if index exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = '{MARIADB_CONFIG["database"]}' 
            AND TABLE_NAME = '{table_name}' 
            AND INDEX_NAME = 'idx_{table_name}_updated_at'
        """)
        index_exists = cursor.fetchone()[0] > 0

        if not index_exists:
            sql = f"""
                CREATE INDEX idx_{table_name}_updated_at 
                ON {table_name}(updated_at)
            """
            cursor.execute(sql)
            print(f"   ✅ Created index: idx_{table_name}_updated_at")
        else:
            print(f"   ℹ️  Index already exists")

        # --- VERIFY ---
        cursor.execute(f"DESCRIBE {table_name}")
        print(f"\n   📋 Current schema:")
        for row in cursor.fetchall():
            col_name, col_type, null, key, default, extra = row
            print(f"      {col_name:<20} {col_type:<20} {extra}")

        # --- SHOW ROW COUNT ---
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"\n   📊 Total rows: {count:,}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n✅ All tables CDC-ready")


def verify_on_update_works():
    """Test that updated_at auto-refreshes on UPDATE"""
    conn = pymysql.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()

    print(f"\n🧪 Testing ON UPDATE CURRENT_TIMESTAMP behavior...")

    # Get a sample row
    cursor.execute("SELECT order_id, status, updated_at FROM orders LIMIT 1")
    row = cursor.fetchone()
    if not row:
        print("   ⚠️  No rows in orders table — skip verification")
        return

    order_id, current_status, old_timestamp = row
    print(f"   Before: order_id={order_id}, status={current_status}, updated_at={old_timestamp}")

    # Update a non-timestamp column
    new_status = "test_cdc_verify"
    cursor.execute(
        "UPDATE orders SET status = %s WHERE order_id = %s",
        (new_status, order_id)
    )
    conn.commit()

    # Read back
    cursor.execute(
        "SELECT status, updated_at FROM orders WHERE order_id = %s",
        (order_id,)
    )
    new_row = cursor.fetchone()
    new_timestamp = new_row[1]
    print(f"   After:  order_id={order_id}, status={new_status}, updated_at={new_timestamp}")

    if new_timestamp > old_timestamp:
        print(f"   ✅ ON UPDATE CURRENT_TIMESTAMP is working!")
    else:
        print(f"   ❌ updated_at did NOT change — check column definition")

    # Restore original status
    cursor.execute(
        "UPDATE orders SET status = %s WHERE order_id = %s",
        (current_status, order_id)
    )
    conn.commit()
    print(f"   ✅ Original status restored")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    alter_tables_for_cdc()
    verify_on_update_works()