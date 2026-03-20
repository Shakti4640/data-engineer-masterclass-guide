# file: 61_07_test_connectivity.py
# Run from: An EC2 instance in Account B's GLUE SUBNET (10.2.6.0/24)
# Purpose: Verify network path works BEFORE configuring Glue
# This is the MOST IMPORTANT debugging step

import socket
import pymysql
import sys

MARIADB_HOST = "10.1.3.50"   # Account A's MariaDB private IP
MARIADB_PORT = 3306
GLUE_USER = "glue_reader"
GLUE_PASS = "GlueR3ad0nly#2025"
DATABASE = "quickcart"


def test_tcp_connectivity():
    """
    Layer 4 test: Can we establish TCP connection?
    
    If this FAILS → network issue:
    → VPC Peering not active
    → Route table missing
    → Security group blocking
    → NACL blocking (check both inbound and outbound NACLs)
    """
    print(f"🔌 Testing TCP connection to {MARIADB_HOST}:{MARIADB_PORT}...")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)

    try:
        result = sock.connect_ex((MARIADB_HOST, MARIADB_PORT))
        if result == 0:
            print(f"   ✅ TCP connection SUCCEEDED — port {MARIADB_PORT} is reachable")
            return True
        else:
            print(f"   ❌ TCP connection FAILED — error code: {result}")
            print(f"   → Check: VPC Peering status")
            print(f"   → Check: Route tables in BOTH accounts")
            print(f"   → Check: Security groups in BOTH accounts")
            print(f"   → Check: NACLs (often forgotten)")
            return False
    except socket.timeout:
        print(f"   ❌ TCP connection TIMED OUT after 10s")
        print(f"   → Packets are being dropped (not rejected)")
        print(f"   → Most likely: missing route or SG rule")
        return False
    finally:
        sock.close()


def test_mariadb_authentication():
    """
    Layer 7 test: Can we authenticate to MariaDB?
    
    If TCP works but this FAILS → database config issue:
    → User doesn't exist for this host pattern
    → Wrong password
    → User not granted access to database
    """
    print(f"\n🔑 Testing MariaDB authentication...")

    try:
        conn = pymysql.connect(
            host=MARIADB_HOST,
            port=MARIADB_PORT,
            user=GLUE_USER,
            password=GLUE_PASS,
            database=DATABASE,
            connect_timeout=10
        )
        print(f"   ✅ Authentication SUCCEEDED as {GLUE_USER}")

        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            print(f"   MariaDB version: {version}")

            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"   Accessible tables: {[t[0] for t in tables]}")

            # Test actual data read
            cursor.execute("SELECT COUNT(*) FROM orders LIMIT 1")
            count = cursor.fetchone()[0]
            print(f"   Orders row count: {count:,}")

        conn.close()
        return True

    except pymysql.err.OperationalError as e:
        error_code = e.args[0]
        if error_code == 1045:
            print(f"   ❌ Access denied — wrong user/password/host")
            print(f"   → Verify: SHOW GRANTS FOR '{GLUE_USER}'@'10.2.%'")
        elif error_code == 2003:
            print(f"   ❌ Can't connect — host unreachable")
        elif error_code == 1049:
            print(f"   ❌ Unknown database '{DATABASE}'")
        else:
            print(f"   ❌ Error {error_code}: {e}")
        return False


def test_full_query():
    """
    Simulate what Glue will do: SELECT from each table
    """
    print(f"\n📊 Testing full table reads (simulating Glue)...")

    try:
        conn = pymysql.connect(
            host=MARIADB_HOST,
            port=MARIADB_PORT,
            user=GLUE_USER,
            password=GLUE_PASS,
            database=DATABASE,
            connect_timeout=10
        )

        tables_to_test = ["orders", "customers", "products"]

        with conn.cursor() as cursor:
            print(f"\n{'Table':<20} {'Rows':>12} {'Sample Read':>15}")
            print("-" * 50)

            for table in tables_to_test:
                # Count total rows
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]

                # Read sample (first 5 rows) — same as Glue initial fetch
                cursor.execute(f"SELECT * FROM {table} LIMIT 5")
                sample = cursor.fetchall()
                col_count = len(cursor.description)

                print(f"  {table:<20} {count:>10,}   {col_count} columns ✅")

            # Test a JOIN query (Glue might push down joins)
            print(f"\n🔗 Testing cross-table JOIN...")
            cursor.execute("""
                SELECT o.order_id, c.name, o.total_amount
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                LIMIT 3
            """)
            join_results = cursor.fetchall()
            print(f"   JOIN returned {len(join_results)} sample rows ✅")

        conn.close()
        print(f"\n✅ ALL TESTS PASSED — Glue will work")
        return True

    except Exception as e:
        print(f"   ❌ Query test failed: {e}")
        return False


def run_all_tests():
    """
    Master test runner — run all 3 layers in sequence
    If Layer N fails, no point testing Layer N+1
    """
    print("=" * 60)
    print("🧪 CROSS-ACCOUNT CONNECTIVITY TEST SUITE")
    print(f"   Source: MariaDB @ {MARIADB_HOST}:{MARIADB_PORT}")
    print(f"   From: Account B Glue Subnet (10.2.6.0/24)")
    print("=" * 60)

    # Layer 4: TCP
    tcp_ok = test_tcp_connectivity()
    if not tcp_ok:
        print("\n🛑 STOP: Fix network connectivity before proceeding")
        print("   Checklist:")
        print("   [ ] VPC Peering status = active")
        print("   [ ] Account A RT: 10.2.0.0/16 → pcx-xxx")
        print("   [ ] Account B RT: 10.1.0.0/16 → pcx-xxx")
        print("   [ ] Account A SG: inbound 10.2.6.0/24 on 3306")
        print("   [ ] Account A NACL: inbound 10.2.6.0/24 on 3306")
        print("   [ ] Account B NACL: outbound 10.1.3.50/32 on 3306")
        print("   [ ] MariaDB bind-address = 0.0.0.0")
        sys.exit(1)

    # Layer 7: Authentication
    auth_ok = test_mariadb_authentication()
    if not auth_ok:
        print("\n🛑 STOP: Fix MariaDB authentication before proceeding")
        print("   Checklist:")
        print(f"   [ ] User exists: SELECT user,host FROM mysql.user WHERE user='{GLUE_USER}'")
        print(f"   [ ] Host pattern: must match '10.2.%' not 'localhost'")
        print(f"   [ ] Grants: SHOW GRANTS FOR '{GLUE_USER}'@'10.2.%'")
        print(f"   [ ] Database exists: SHOW DATABASES LIKE '{DATABASE}'")
        sys.exit(1)

    # Layer Application: Full query test
    query_ok = test_full_query()
    if not query_ok:
        print("\n🛑 STOP: Fix table access before proceeding")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("✅ ALL 3 LAYERS PASSED — PROCEED TO GLUE JOB CONFIGURATION")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()