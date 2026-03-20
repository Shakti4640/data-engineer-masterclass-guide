# file: 19_05_cleanup.py
# Purpose: Clean up MariaDB tables and loaded data

import pymysql
import os

DB_CONFIG = {
    "host": os.environ.get("MARIADB_HOST", "localhost"),
    "port": int(os.environ.get("MARIADB_PORT", 3306)),
    "user": os.environ.get("MARIADB_USER", "quickcart_etl"),
    "password": os.environ.get("MARIADB_PASSWORD", "etl_password_123"),
    "database": "quickcart",
    "charset": "utf8mb4",
    "autocommit": True
}


def cleanup_data_only():
    """Delete all data but keep tables (for re-loading)"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("=" * 70)
    print("CLEANUP: Deleting all data (keeping tables)")
    print("=" * 70)

    tables = ["orders", "customers", "products", "load_metadata"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        cursor.execute(f"DELETE FROM {table}")
        print(f"   🗑️  {table}: deleted {count:,} rows")

    cursor.close()
    conn.close()
    print("   ✅ All data deleted — tables intact")


def cleanup_full():
    """Drop all tables and database"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("=" * 70)
    print("CLEANUP: Dropping all tables and database")
    print("=" * 70)

    tables = ["orders", "customers", "products", "load_metadata"]
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        print(f"   🗑️  Dropped table: {table}")

    cursor.execute("DROP DATABASE IF EXISTS quickcart")
    print("   🗑️  Dropped database: quickcart")

    cursor.close()
    conn.close()
    print("   ✅ Full cleanup complete")


def cleanup_temp_files():
    """Remove any leftover temp files from downloads"""
    download_dir = "/tmp/s3_to_mariadb"
    if os.path.exists(download_dir):
        for f in os.listdir(download_dir):
            filepath = os.path.join(download_dir, f)
            os.remove(filepath)
            print(f"   🗑️  Deleted temp file: {filepath}")
        os.rmdir(download_dir)
        print(f"   ✅ Removed temp directory: {download_dir}")
    else:
        print(f"   ℹ️  No temp directory found")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--full":
        cleanup_full()
    else:
        cleanup_data_only()

    cleanup_temp_files()

    print("\n🎯 Cleanup complete")
    print("   → Data only: python 19_05_cleanup.py")
    print("   → Full drop:  python 19_05_cleanup.py --full")