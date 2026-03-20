# file: 65_01_prepare_mariadb_for_cdc.py
# Run on: Account A's EC2 (MariaDB host)
# Purpose: Add CDC columns and indexes to source tables

import pymysql

MARIADB_HOST = "localhost"
MARIADB_USER = "root"
MARIADB_PASS = "RootP@ss2025!"
DATABASE = "quickcart"


def add_cdc_columns():
    """
    Add audit columns required for timestamp-based CDC
    
    COLUMNS:
    → created_at: when row was first inserted
    → updated_at: when row was last modified (auto-updates on UPDATE)
    → is_deleted: soft delete flag (application sets to TRUE instead of DELETE)
    
    IDEMPOTENT: checks if columns exist before adding
    """
    conn = pymysql.connect(
        host=MARIADB_HOST, user=MARIADB_USER,
        password=MARIADB_PASS, database=DATABASE
    )

    tables = ["orders", "customers", "products"]

    try:
        with conn.cursor() as cur:
            for table in tables:
                print(f"\n📋 Preparing: {table}")

                # Check existing columns
                cur.execute(f"DESCRIBE {table}")
                existing_cols = {row[0] for row in cur.fetchall()}

                # Add created_at if missing
                if "created_at" not in existing_cols:
                    cur.execute(f"""
                        ALTER TABLE {table}
                        ADD COLUMN created_at TIMESTAMP
                        DEFAULT CURRENT_TIMESTAMP
                    """)
                    # Backfill: set created_at = earliest reasonable date for existing rows
                    cur.execute(f"""
                        UPDATE {table} SET created_at = '2024-01-01 00:00:00'
                        WHERE created_at IS NULL
                    """)
                    print(f"   ✅ Added: created_at")
                else:
                    print(f"   ℹ️  created_at already exists")

                # Add updated_at if missing
                if "updated_at" not in existing_cols:
                    cur.execute(f"""
                        ALTER TABLE {table}
                        ADD COLUMN updated_at TIMESTAMP
                        DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP
                    """)
                    cur.execute(f"""
                        UPDATE {table} SET updated_at = CURRENT_TIMESTAMP
                        WHERE updated_at IS NULL
                    """)
                    print(f"   ✅ Added: updated_at (auto-updates on modification)")
                else:
                    print(f"   ℹ️  updated_at already exists")

                # Add is_deleted if missing
                if "is_deleted" not in existing_cols:
                    cur.execute(f"""
                        ALTER TABLE {table}
                        ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE
                    """)
                    print(f"   ✅ Added: is_deleted (soft delete flag)")
                else:
                    print(f"   ℹ️  is_deleted already exists")

                # Create index on updated_at (CRITICAL for CDC performance)
                cur.execute(f"SHOW INDEX FROM {table} WHERE Column_name = 'updated_at'")
                if not cur.fetchall():
                    cur.execute(f"""
                        CREATE INDEX idx_{table}_updated_at ON {table}(updated_at)
                    """)
                    print(f"   ✅ Index created: idx_{table}_updated_at")
                else:
                    print(f"   ℹ️  Index on updated_at already exists")

                # Verify
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                cur.execute(f"SELECT MIN(updated_at), MAX(updated_at) FROM {table}")
                min_ts, max_ts = cur.fetchone()
                print(f"   📊 Rows: {count:,} | updated_at range: {min_ts} → {max_ts}")

        conn.commit()
        print(f"\n✅ All tables prepared for CDC")

    finally:
        conn.close()


if __name__ == "__main__":
    add_cdc_columns()