# file: 17_03_test_notification.py
# Purpose: Upload a test file and verify SNS email alert arrives
# Dependency: Notification configured in 17_02

import boto3
import csv
import os
import time
from datetime import datetime

REGION = "us-east-2"
SOURCE_BUCKET = "quickcart-raw-data-prod"
TOPIC_NAME = "quickcart-s3-file-alerts"


def create_test_csv():
    """Create a small test CSV file"""
    filepath = "/tmp/test_notification_file.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id",
                         "quantity", "unit_price", "total_amount",
                         "status", "order_date"])
        writer.writerow(["ORD-TEST-000001", "CUST-000001", "PROD-00001",
                         "1", "99.99", "99.99", "completed", "2025-01-15"])
    return filepath


def upload_test_file_matching_filter():
    """
    Upload file that MATCHES the notification filter
    → Prefix: daily_exports/ ✅
    → Suffix: .csv ✅
    → Should trigger SNS notification
    """
    s3_client = boto3.client("s3", region_name=REGION)
    local_path = create_test_csv()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # This key MATCHES our filter: prefix=daily_exports/ AND suffix=.csv
    matching_key = f"daily_exports/orders/test_notification_{timestamp}.csv"

    print("=" * 70)
    print("TEST 1: Upload file MATCHING filter (should trigger alert)")
    print("=" * 70)
    print(f"   File: {matching_key}")
    print(f"   Prefix match: daily_exports/ ✅")
    print(f"   Suffix match: .csv ✅")
    print(f"   Expected: SNS notification SENT → email received")

    upload_start = time.time()

    s3_client.upload_file(
        Filename=local_path,
        Bucket=SOURCE_BUCKET,
        Key=matching_key,
        ExtraArgs={
            "ContentType": "text/csv",
            "Metadata": {
                "test": "true",
                "purpose": "notification-verification"
            }
        }
    )

    upload_time = time.time() - upload_start
    print(f"   ✅ Uploaded in {upload_time:.2f}s")
    print(f"   ⏳ Check your email inbox in 5-30 seconds...")
    print(f"   → Subject will be: 'Amazon S3 Notification'")
    print(f"   → Body contains JSON with bucket name + object key")

    return matching_key


def upload_test_file_not_matching_filter():
    """
    Upload file that does NOT match the notification filter
    → Prefix: wrong_prefix/ ❌
    → Should NOT trigger SNS notification
    """
    s3_client = boto3.client("s3", region_name=REGION)
    local_path = create_test_csv()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # This key does NOT match: prefix is "test_area/" not "daily_exports/"
    non_matching_key = f"test_area/non_matching_{timestamp}.csv"

    print("\n" + "=" * 70)
    print("TEST 2: Upload file NOT matching filter (should NOT trigger)")
    print("=" * 70)
    print(f"   File: {non_matching_key}")
    print(f"   Prefix match: daily_exports/ ❌ (key starts with test_area/)")
    print(f"   Expected: NO SNS notification — no email")

    s3_client.upload_file(
        Filename=local_path,
        Bucket=SOURCE_BUCKET,
        Key=non_matching_key
    )
    print(f"   ✅ Uploaded — no notification expected")
    print(f"   → If you receive an email for this file: filter is misconfigured")

    return non_matching_key


def upload_test_file_wrong_suffix():
    """
    Upload file matching prefix but NOT suffix
    → Prefix: daily_exports/ ✅
    → Suffix: .json ❌ (filter expects .csv)
    → Should NOT trigger notification
    """
    s3_client = boto3.client("s3", region_name=REGION)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    non_matching_key = f"daily_exports/orders/test_{timestamp}.json"

    print("\n" + "=" * 70)
    print("TEST 3: Upload file matching prefix but NOT suffix")
    print("=" * 70)
    print(f"   File: {non_matching_key}")
    print(f"   Prefix match: daily_exports/ ✅")
    print(f"   Suffix match: .csv ❌ (file is .json)")
    print(f"   Expected: NO SNS notification — suffix doesn't match")

    # Create a small JSON file
    s3_client.put_object(
        Bucket=SOURCE_BUCKET,
        Key=non_matching_key,
        Body='{"test": true}',
        ContentType="application/json"
    )
    print(f"   ✅ Uploaded — no notification expected")

    return non_matching_key


def check_sns_metrics():
    """Check CloudWatch metrics to verify SNS received the event"""
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    print("\n" + "=" * 70)
    print("VERIFICATION: Checking SNS CloudWatch metrics")
    print("=" * 70)
    print("   (Metrics may take 1-2 minutes to appear)")

    sts = boto3.client("sts", region_name=REGION)
    account_id = sts.get_caller_identity()["Account"]
    topic_arn = f"arn:aws:sns:{REGION}:{account_id}:{TOPIC_NAME}"

    response = cloudwatch.get_metric_statistics(
        Namespace="AWS/SNS",
        MetricName="NumberOfMessagesPublished",
        Dimensions=[
            {"Name": "TopicName", "Value": TOPIC_NAME}
        ],
        StartTime=datetime.utcnow().replace(
            hour=0, minute=0, second=0
        ),
        EndTime=datetime.utcnow(),
        Period=3600,
        Statistics=["Sum"]
    )

    datapoints = response.get("Datapoints", [])
    if datapoints:
        total = sum(dp["Sum"] for dp in datapoints)
        print(f"   Messages published today: {int(total)}")
    else:
        print("   No metrics yet — wait 1-2 minutes and re-check")
        print("   Or: check email inbox directly")


def cleanup_test_files(keys_to_delete):
    """Remove test files from S3"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("\n" + "=" * 70)
    print("CLEANUP: Removing test files")
    print("=" * 70)

    for key in keys_to_delete:
        try:
            s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=key)
            print(f"   🗑️  Deleted: {key}")
        except Exception as e:
            print(f"   ⚠️  Could not delete {key}: {e}")


if __name__ == "__main__":
    print("🧪 S3 EVENT NOTIFICATION — TEST SUITE")
    print("=" * 70)
    print(f"   Bucket:  {SOURCE_BUCKET}")
    print(f"   Topic:   {TOPIC_NAME}")
    print(f"   Time:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")

    test_keys = []

    # Test 1: Should trigger notification
    key1 = upload_test_file_matching_filter()
    test_keys.append(key1)

    # Wait for S3 event to propagate
    print("\n   ⏳ Waiting 5 seconds for event propagation...")
    time.sleep(5)

    # Test 2: Should NOT trigger notification (wrong prefix)
    key2 = upload_test_file_not_matching_filter()
    test_keys.append(key2)

    # Test 3: Should NOT trigger notification (wrong suffix)
    key3 = upload_test_file_wrong_suffix()
    test_keys.append(key3)

    # Check metrics
    check_sns_metrics()

    # Summary
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY")
    print("=" * 70)
    print("   Test 1 (matching prefix+suffix):   Should receive email ✅")
    print("   Test 2 (wrong prefix):             Should NOT receive email ❌")
    print("   Test 3 (wrong suffix):             Should NOT receive email ❌")
    print()
    print("   → If you received email for Test 1 only → CORRECT ✅")
    print("   → If you received email for Test 2 or 3 → filter misconfigured ⚠️")
    print("   → If no email for Test 1 → check subscription confirmation")

    # Cleanup
    print()
    user_input = input("   Delete test files? (y/n): ").strip().lower()
    if user_input == "y":
        cleanup_test_files(test_keys)
    else:
        print("   Keeping test files")