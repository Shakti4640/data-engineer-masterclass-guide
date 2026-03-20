# file: 61_05_prepare_mariadb.py
# Run on: Account A's EC2 instance (where MariaDB runs)
# Purpose: Create read-only user for Glue and verify remote access

import subprocess
import pymysql

MARIADB_HOST = "localhost"
MARIADB_ROOT_USER = "root"
MARIADB_ROOT_PASS = "RootP@ss2025!"  # In production: use Secrets Manager

# Glue reader credentials
GLUE_USER = "glue_reader"
GLUE_PASS = "GlueR3ad0nly#2025"
GLUE_HOST_PATTERN = "10.2.%"  # Allow all IPs from Account B's Glue subnet


def verify_bind_address():
    """
    MariaDB must listen on 0.0.0.0, not just 127.0.0.1
    
    CHECK: grep bind-address /etc/my.cnf.d/server.cnf
    MUST BE: bind-address = 0.0.0.0
    
    If it's 127.0.0.1:
    → sudo sed -i 's/bind-address.*=.*127.0.0.1/bind-address = 0.0.0.0/' /etc/my.cnf.d/server.cnf
    → sudo systemctl restart mariadb
    """
    print("🔍 Checking MariaDB bind-address...")
    result = subprocess.run(
        ["mysql", "-u", MARIADB_ROOT_USER, f"-p{MARIADB_ROOT_PASS}",
         "-e", "SHOW VARIABLES LIKE 'bind_address';"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if "127.0.0.1" in result.stdout:
        print("❌ MariaDB is bound to localhost only!")
        print("   Fix: Edit /etc/my.cnf.d/server.cnf")
        print("   Set: bind-address = 0.0.0.0")
        print("   Then: sudo systemctl restart mariadb")
        return False
    print("✅ MariaDB is accepting remote connections")
    return True


def create_glue_reader_user():
    """
    Create a dedicated read-only user for Glue
    
    SECURITY PRINCIPLES:
    → Specific host pattern (10.2.%) — not '%' (any host)
    → SELECT only — no INSERT, UPDATE, DELETE
    → Only on specific database — not all databases
    → Separate user per consumer — not shared root
    """
    conn = pymysql.connect(
        host=MARIADB_HOST,
        user=MARIADB_ROOT_USER,
        password=MARIADB_ROOT_PASS,
        charset="utf8mb4"
    )

    try:
        with conn.cursor() as cursor:
            # Drop user if exists (idempotent)
            cursor.execute(
                f"DROP USER IF EXISTS '{GLUE_USER}'@'{GLUE_HOST_PATTERN}'"
            )

            # Create user with specific host pattern
            cursor.execute(
                f"CREATE USER '{GLUE_USER}'@'{GLUE_HOST_PATTERN}' "
                f"IDENTIFIED BY '{GLUE_PASS}'"
            )
            print(f"✅ User created: {GLUE_USER}@{GLUE_HOST_PATTERN}")

            # Grant SELECT only on quickcart database
            cursor.execute(
                f"GRANT SELECT ON quickcart.* "
                f"TO '{GLUE_USER}'@'{GLUE_HOST_PATTERN}'"
            )
            print(f"✅ Granted SELECT on quickcart.* to {GLUE_USER}")

            # Flush privileges to apply immediately
            cursor.execute("FLUSH PRIVILEGES")
            print("✅ Privileges flushed")

            # Verify grants
            cursor.execute(
                f"SHOW GRANTS FOR '{GLUE_USER}'@'{GLUE_HOST_PATTERN}'"
            )
            grants = cursor.fetchall()
            print("\n📋 Grants for glue_reader:")
            for grant in grants:
                print(f"   {grant[0]}")

        conn.commit()

    finally:
        conn.close()


def verify_tables_exist():
    """
    Verify the tables that Glue will extract exist and have data
    """
    conn = pymysql.connect(
        host=MARIADB_HOST,
        user=MARIADB_ROOT_USER,
        password=MARIADB_ROOT_PASS,
        database="quickcart",
        charset="utf8mb4"
    )

    try:
        with conn.cursor() as cursor:
            tables = ["orders", "customers", "products"]
            print("\n📊 Table verification:")
            print(f"{'Table':<20} {'Row Count':>12}")
            print("-" * 35)

            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table:<20} {count:>10,}")

    finally:
        conn.close()


if __name__ == "__main__":
    verify_bind_address()
    create_glue_reader_user()
    verify_tables_exist()