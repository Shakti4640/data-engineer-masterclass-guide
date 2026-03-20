# file: 05_end_to_end_test.py
# Complete integration test: upload file → verify in MariaDB
# Tests the ENTIRE pipeline from S3 to database

import boto3
import csv
import os
import time
import random
import pymysql
from datetime import datetime, timedelta

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
TEST_PREFIX = "incoming/shipfast/"

MARIADB_HOST = "localhost"
MARIADB_PORT = 3306
MARIADB_USER = "quickcart_etl"
MARIADB_PASSWORD = "[REDACTED:PASSWORD]"
MARIADB_DATABASE = "quickcart"


def generate_test_csv(num_rows=100):
    """Generate realistic ShipFast shipping update CSV"""
    filepath = "/tmp/test_shipfast_update.csv"
    statuses = ["shipped", "in_transit", "delivered", "delayed", "returned"]
    carriers = ["FedEx", "UPS", "USPS", "DHL", "OnTrac"]

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "tracking_id", "order_id", "shipping_status",
            "shipped_date", "carrier", "estimated_delivery"
        ])
        for i in range(1, num_rows + 1):
            ship_date = datetime.now() - timedelta(days=random.randint(0, 7))
            est_date = ship_date + timedelta(days=random.randint(2, 10))
            writer.writerow([
                f"TRK-{datetime.now().strftime('%Y%m%d')}-{i:06d}",
                f"ORD-{random.randint(20250101, 20250130)}-{random.randint(1, 999999):06d}",
                random.choice(statuses),
                ship_date.strftime("%Y-%m-%d"),
                random.choice(carriers),
                est_date.strftime("%Y-%m-%d")
            ])

    print(f"✅ Test CSV generated: {filepath} ({num_rows} rows)")
    return filepath


def upload_test_file(local_path):
    """Upload test file to S3 incoming prefix"""
    s3_client = boto3.client("s3", region_name=REGION)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{TEST_PREFIX}shipping_updates_{timestamp}.csv"

    s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
    print(f"✅ Uploaded to: s3://{BUCKET_NAME}/{s3_key}")
    return s3_key


def wait_for_processing(s3_key, timeout_seconds=120):
    """Poll MariaDB until file appears in processed_files table"""
    print(f"\n⏳ Waiting for pipeline to process file...")
    print(f"   Checking every 5 seconds (timeout: {timeout_seconds}s)")

    start = time.time()

    while time.time() - start < timeout_seconds:
        try:
            conn = pymysql.connect(
                host=MARIADB_HOST, port=MARIADB_PORT,
                user=MARIADB_USER, password=MARIADB_PASSWORD,
                database=MARIADB_DATABASE
            )
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT rows_loaded, processing_seconds, processed_at "
                    "FROM processed_files WHERE file_key = %s",
                    (s3_key,)
                )
                result = cursor.fetchone()

                if result:
                    rows, proc_time, proc_at = result
                    elapsed = round(time.time() - start, 1)
                    print(f"\n✅ FILE PROCESSED!")
                    print(f"   Rows loaded:      {rows}")
                    print(f"   Processing time:  {proc_time}s")
                    print(f"   Processed at:     {proc_at}")
                    print(f"   End-to-end wait:  {elapsed}s")
                    conn.close()
                    return True

                # Also check staging table
                cursor.execute("SELECT COUNT(*) FROM staging_shipfast_updates")
                staging_count = cursor.fetchone()[0]

            conn.close()
            elapsed = round(time.time() - start, 1)
            print(f"   ⏳ {elapsed}s elapsed — staging has {staging_count} rows...")
            time.sleep(5)

        except pymysql.Error as e:
            print(f"   ⚠️  MariaDB error: {e}")
            time.sleep(5)

    print(f"\n❌ TIMEOUT — file not processed within {timeout_seconds}s")
    print(f"   Check:")
    print(f"   1. Is the worker running? (python 03_enhanced_worker.py)")
    print(f"   2. Is SQS queue receiving messages?")
    print(f"   3. Is S3 event notification configured?")
    print(f"   4. Is SNS → SQS subscription active?")
    return False


def verify_staging_data():
    """Verify data in staging table looks correct"""
    conn = pymysql.connect(
        host=MARIADB_HOST, port=MARIADB_PORT,
        user=MARIADB_USER, password=MARIADB_PASSWORD,
        database=MARIADB_DATABASE
    )

    with conn.cursor() as cursor:
        # Row count
        cursor.execute("SELECT COUNT(*) FROM staging_shipfast_updates")
        total = cursor.fetchone()[0]

        # Status distribution
        cursor.execute("""
            SELECT shipping_status, COUNT(*) as cnt
            FROM staging_shipfast_updates
            GROUP BY shipping_status
            ORDER BY cnt DESC
        """)
        statuses = cursor.fetchall()

        # Carrier distribution
        cursor.execute("""
            SELECT carrier, COUNT(*) as cnt
            FROM staging_shipfast_updates
            GROUP BY carrier
            ORDER BY cnt DESC
        """)
        carriers = cursor.fetchall()

        # Sample rows
        cursor.execute("""
            SELECT tracking_id, order_id, shipping_status, shipped_date, carrier
            FROM staging_shipfast_updates
            LIMIT 5
        """)
        samples = cursor.fetchall()

    conn.close()

    print(f"\n📊 STAGING TABLE VERIFICATION")
    print(f"{'=' * 60}")

    print(f"\n   Total rows: {total}")

    print(f"\n   📦 Status Distribution:")
    for status, cnt in statuses:
        pct = round(100 * cnt / max(total, 1), 1)
        print(f"      {status:<15} {cnt:>5} ({pct}%)")

    print(f"\n   🚚 Carrier Distribution:")
    for carrier, cnt in carriers:
        pct = round(100 * cnt / max(total, 1), 1)
        print(f"      {carrier:<15} {cnt:>5} ({pct}%)")

    print(f"\n   📋 Sample Rows:")
    print(f"      {'Tracking':<28} {'Order':<25} {'Status':<12} {'Date':<12} {'Carrier'}")
    print(f"      {'─'*28} {'─'*25} {'─'*12} {'─'*12} {'─'*10}")
    for row in samples:
        print(f"      {row[0]:<28} {row[1]:<25} {row[2]:<12} {str(row[3]):<12} {row[4]}")

    return total > 0


def check_pipeline_health():
    """Check all pipeline components are healthy"""
    print(f"\n{'=' * 60}")
    print(f"🏥 PIPELINE HEALTH CHECK")
    print(f"{'=' * 60}")

    issues = []

    # Check S3 notification
    s3_client = boto3.client("s3", region_name=REGION)
    try:
        notif = s3_client.get_bucket_notification_configuration(Bucket=BUCKET_NAME)
        topic_configs = notif.get("TopicConfigurations", [])
        shipfast_config = [t for t in topic_configs if t.get("Id") == "ShipFastFileArrival"]
        if shipfast_config:
            print(f"   ✅ S3 Event Notification: configured")
        else:
            print(f"   ❌ S3 Event Notification: NOT configured for ShipFast")
            issues.append("S3 notification missing")
    except Exception as e:
        print(f"   ❌ S3 check failed: {e}")
        issues.append(f"S3: {e}")

    # Check SNS topic
    sns_client = boto3.client("sns", region_name=REGION)
    try:
        topics = sns_client.list_topics()["Topics"]
        matching = [t for t in topics if "incoming-file-events" in t["TopicArn"]]
        if matching:
            # Check subscriptions
            subs = sns_client.list_subscriptions_by_topic(TopicArn=matching[0]["TopicArn"])
            sub_count = len(subs.get("Subscriptions", []))
            print(f"   ✅ SNS Topic: exists ({sub_count} subscriptions)")
        else:
            print(f"   ❌ SNS Topic: NOT found")
            issues.append("SNS topic missing")
    except Exception as e:
        print(f"   ❌ SNS check failed: {e}")
        issues.append(f"SNS: {e}")

    # Check SQS queue
    sqs_client = boto3.client("sqs", region_name=REGION)
    try:
        queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
        )["Attributes"]
        visible = attrs.get("ApproximateNumberOfMessages", "0")
        in_flight = attrs.get("ApproximateNumberOfMessagesNotVisible", "0")
        print(f"   ✅ SQS Queue: exists (visible: {visible}, in-flight: {in_flight})")
    except Exception as e:
        print(f"   ❌ SQS check failed: {e}")
        issues.append(f"SQS: {e}")

    # Check MariaDB
    try:
        conn = pymysql.connect(
            host=MARIADB_HOST, port=MARIADB_PORT,
            user=MARIADB_USER, password=MARIADB_PASSWORD,
            database=MARIADB_DATABASE
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.execute("SHOW TABLES LIKE 'staging_shipfast_updates'")
            if cursor.fetchone():
                print(f"   ✅ MariaDB: connected, staging table exists")
            else:
                print(f"   ❌ MariaDB: connected, but staging table MISSING")
                issues.append("Staging table missing — run 04_mariadb_setup.sql")
        conn.close()
    except pymysql.Error as e:
        print(f"   ❌ MariaDB: connection failed — {e}")
        issues.append(f"MariaDB: {e}")

    # Summary
    if issues:
        print(f"\n   🚨 {len(issues)} issue(s) found:")
        for issue in issues:
            print(f"      ⚠️  {issue}")
        return False
    else:
        print(f"\n   🎉 All components healthy!")
        return True


def run_end_to_end_test():
    """Complete integration test"""
    print("=" * 60)
    print("🧪 END-TO-END PIPELINE TEST")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Step 1: Health check
    healthy = check_pipeline_health()
    if not healthy:
        print(f"\n❌ Fix health issues before running test")
        return False

    # Step 2: Generate test data
    print(f"\n{'─' * 60}")
    local_path = generate_test_csv(num_rows=100)

    # Step 3: Upload to S3 (triggers the pipeline)
    s3_key = upload_test_file(local_path)
    os.remove(local_path)

    # Step 4: Wait for processing
    processed = wait_for_processing(s3_key, timeout_seconds=120)

    if not processed:
        return False

    # Step 5: Verify data
    verified = verify_staging_data()

    # Final result
    print(f"\n{'=' * 60}")
    if processed and verified:
        print(f"🎉 END-TO-END TEST PASSED!")
        print(f"   File dropped in S3 → processed → loaded into MariaDB")
        print(f"   Pipeline is working correctly")
    else:
        print(f"❌ END-TO-END TEST FAILED")
        print(f"   Processed: {processed}")
        print(f"   Verified: {verified}")
    print(f"{'=' * 60}")

    return processed and verified


if __name__ == "__main__":
    run_end_to_end_test()