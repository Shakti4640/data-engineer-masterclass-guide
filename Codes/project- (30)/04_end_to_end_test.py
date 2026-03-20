# file: 04_end_to_end_test.py
# Tests the complete chain: upload file to S3 → verify in Redshift

import boto3
import psycopg2
import csv
import gzip
import os
import time
import random
from datetime import datetime

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "YourSecurePassword123!"


def generate_test_export():
    """Generate test CSV that mimics MariaDB export"""
    today = datetime.now().strftime("%Y-%m-%d")
    local_path = "/tmp/test_products.csv.gz"

    categories = ["Electronics", "Clothing", "Home", "Books", "Sports"]

    with gzip.open(local_path, "wt", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "name", "category", "price", "stock_quantity", "is_active"])
        for i in range(1, 201):
            writer.writerow([
                f"PROD-{i:05d}",
                f"Test Product {i}",
                random.choice(categories),
                round(random.uniform(4.99, 999.99), 2),
                random.randint(0, 5000),
                random.choice(["true", "false"])
            ])

    print(f"✅ Generated test export: {local_path} (200 rows)")
    return local_path, today


def upload_to_s3(local_path, export_date):
    """Upload to S3 mariadb_exports path (triggers the pipeline)"""
    s3_client = boto3.client("s3", region_name=REGION)
    s3_key = f"mariadb_exports/products/export_date={export_date}/products.csv.gz"

    s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
    print(f"✅ Uploaded to: s3://{BUCKET_NAME}/{s3_key}")
    print(f"   → This triggers S3 event → SQS → worker → COPY")
    return s3_key


def wait_for_redshift_load(expected_rows, timeout=180):
    """Poll Redshift until products table has expected rows"""
    print(f"\n⏳ Waiting for Redshift COPY to complete...")

    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = psycopg2.connect(
                host=REDSHIFT_HOST, port=REDSHIFT_PORT,
                dbname=REDSHIFT_DB, user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM public.products;")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            elapsed = round(time.time() - start, 1)

            if count == expected_rows:
                print(f"\n✅ REDSHIFT LOADED!")
                print(f"   Rows: {count}")
                print(f"   Wait time: {elapsed}s")
                return True

            print(f"   ⏳ {elapsed}s — products has {count} rows (waiting for {expected_rows})")
            time.sleep(10)

        except psycopg2.Error as e:
            print(f"   ⚠️  Redshift error: {e}")
            time.sleep(10)

    print(f"\n❌ TIMEOUT — Redshift not updated within {timeout}s")
    return False


def verify_redshift_data():
    """Verify data in Redshift products table"""
    conn = psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB, user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    cursor = conn.cursor()

    print(f"\n📊 REDSHIFT VERIFICATION:")

    cursor.execute("SELECT COUNT(*) FROM public.products;")
    print(f"   Total rows: {cursor.fetchone()[0]}")

    cursor.execute("""
        SELECT category, COUNT(*) as cnt, ROUND(AVG(price::decimal), 2) as avg_price
        FROM public.products GROUP BY category ORDER BY cnt DESC;
    """)
    print(f"\n   Category Distribution:")
    for row in cursor.fetchall():
        print(f"      {row[0]:<15} {row[1]:>5} products, avg ${row[2]}")

    cursor.execute("SELECT * FROM public.products LIMIT 3;")
    print(f"\n   Sample Rows:")
    for row in cursor.fetchall():
        print(f"      {row}")

    cursor.close()
    conn.close()


def run_test():
    """Complete end-to-end test"""
    print("=" * 60)
    print("🧪 END-TO-END TEST: S3 → SQS → EC2 → Redshift COPY")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Generate and upload
    local_path, export_date = generate_test_export()
    s3_key = upload_to_s3(local_path, export_date)
    os.remove(local_path)

    # Wait for Redshift
    loaded = wait_for_redshift_load(expected_rows=200, timeout=180)

    if loaded:
        verify_redshift_data()
        print(f"\n{'=' * 60}")
        print(f"🎉 END-TO-END TEST PASSED!")
        print(f"   File uploaded to S3 → auto-loaded into Redshift")
        print(f"{'=' * 60}")
    else:
        print(f"\n{'=' * 60}")
        print(f"❌ END-TO-END TEST FAILED")
        print(f"   Possible causes:")
        print(f"   1. Worker not running (python 03_redshift_copy_worker.py)")
        print(f"   2. SQS queue not receiving S3 events")
        print(f"   3. Redshift COPY failed (check worker logs)")
        print(f"   4. S3 notification not configured for mariadb_exports/ prefix")
        print(f"{'=' * 60}")

    return loaded


if __name__ == "__main__":
    run_test()