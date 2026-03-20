# file: 61_13_validate_redshift.py
# Run from: Account B (222222222222)
# Purpose: Validate data landed correctly in Redshift

import boto3
import psycopg2
from datetime import datetime

# Redshift connection details
REDSHIFT_HOST = "quickcart-analytics-cluster.cdefghijk.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "analytics"
REDSHIFT_USER = "etl_writer"
REDSHIFT_PASS = "R3dsh1ftWr1t3r#2025"
TARGET_SCHEMA = "quickcart_ops"


def validate_load():
    """
    Post-load validation checks:
    1. Row counts — did all data arrive?
    2. Freshness — is etl_loaded_at recent?
    3. Nulls — are critical columns populated?
    4. Duplicates — did we accidentally double-load?
    5. Value ranges — are amounts reasonable?
    """
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASS
    )
    conn.autocommit = True

    try:
        cur = conn.cursor()

        print("=" * 70)
        print("🔍 POST-LOAD VALIDATION")
        print(f"   Schema: {TARGET_SCHEMA}")
        print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        tables = ["orders", "dim_customers", "dim_products"]

        for table in tables:
            full_table = f"{TARGET_SCHEMA}.{table}"
            print(f"\n📋 Validating: {full_table}")
            print("-" * 50)

            # 1. Row count
            cur.execute(f"SELECT COUNT(*) FROM {full_table}")
            row_count = cur.fetchone()[0]
            status = "✅" if row_count > 0 else "❌"
            print(f"   {status} Row count: {row_count:,}")

            # 2. Freshness — check etl_loaded_at
            cur.execute(f"""
                SELECT MAX(etl_loaded_at), MIN(etl_loaded_at)
                FROM {full_table}
            """)
            max_ts, min_ts = cur.fetchone()
            print(f"   📅 Load window: {min_ts} → {max_ts}")

            # 3. Null check on primary key
            pk_col = "order_id" if table == "orders" else \
                     "customer_id" if table == "dim_customers" else "product_id"
            cur.execute(f"""
                SELECT COUNT(*) FROM {full_table}
                WHERE {pk_col} IS NULL
            """)
            null_count = cur.fetchone()[0]
            status = "✅" if null_count == 0 else "❌"
            print(f"   {status} NULL {pk_col}: {null_count}")

            # 4. Duplicate check on primary key
            cur.execute(f"""
                SELECT {pk_col}, COUNT(*) as cnt
                FROM {full_table}
                GROUP BY {pk_col}
                HAVING COUNT(*) > 1
                LIMIT 5
            """)
            dupes = cur.fetchall()
            status = "✅" if len(dupes) == 0 else "❌"
            print(f"   {status} Duplicate {pk_col}s: {len(dupes)}")
            if dupes:
                for d in dupes:
                    print(f"      ⚠️  {d[0]}: {d[1]} occurrences")

            # 5. Value range check (orders only)
            if table == "orders":
                cur.execute(f"""
                    SELECT 
                        MIN(total_amount), MAX(total_amount), AVG(total_amount),
                        COUNT(CASE WHEN total_amount < 0 THEN 1 END) as negative_count
                    FROM {full_table}
                """)
                min_amt, max_amt, avg_amt, neg_count = cur.fetchone()
                print(f"   💰 Amount range: ${min_amt:.2f} → ${max_amt:.2f} (avg: ${avg_amt:.2f})")
                status = "✅" if neg_count == 0 else "❌"
                print(f"   {status} Negative amounts: {neg_count}")

            # 6. ETL source consistency
            cur.execute(f"""
                SELECT etl_source, COUNT(*) 
                FROM {full_table}
                GROUP BY etl_source
            """)
            sources = cur.fetchall()
            print(f"   🏷️  ETL sources: {dict(sources)}")

        # --- CROSS-TABLE REFERENTIAL CHECK ---
        print(f"\n{'='*50}")
        print("🔗 Cross-Table Referential Integrity")
        print(f"{'='*50}")

        cur.execute(f"""
            SELECT COUNT(DISTINCT o.customer_id) as matched,
                   COUNT(DISTINCT CASE WHEN c.customer_id IS NULL THEN o.customer_id END) as orphaned
            FROM {TARGET_SCHEMA}.orders o
            LEFT JOIN {TARGET_SCHEMA}.dim_customers c
                ON o.customer_id = c.customer_id
        """)
        matched, orphaned = cur.fetchone()
        status = "✅" if orphaned == 0 else "⚠️"
        print(f"   {status} Customer references: {matched:,} matched, {orphaned:,} orphaned")

        cur.execute(f"""
            SELECT COUNT(DISTINCT o.product_id) as matched,
                   COUNT(DISTINCT CASE WHEN p.product_id IS NULL THEN o.product_id END) as orphaned
            FROM {TARGET_SCHEMA}.orders o
            LEFT JOIN {TARGET_SCHEMA}.dim_products p
                ON o.product_id = p.product_id
        """)
        matched, orphaned = cur.fetchone()
        status = "✅" if orphaned == 0 else "⚠️"
        print(f"   {status} Product references: {matched:,} matched, {orphaned:,} orphaned")

        print(f"\n{'='*70}")
        print("✅ VALIDATION COMPLETE")
        print(f"{'='*70}")

    finally:
        conn.close()


if __name__ == "__main__":
    validate_load()