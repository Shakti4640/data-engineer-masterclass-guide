# file: 18_03_test_and_consume.py
# Purpose: Upload test file → verify SQS receives event → consume message
# This is the CORE SQS consumption pattern used in Projects 27, 28, 30

import boto3
import json
import csv
import os
import time
from datetime import datetime
from urllib.parse import unquote_plus

REGION = "us-east-2"
SOURCE_BUCKET = "quickcart-raw-data-prod"
QUEUE_NAME = "quickcart-file-processing-queue"


def get_queue_url():
    sqs_client = boto3.client("sqs", region_name=REGION)
    return sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]


def check_queue_depth(sqs_client, queue_url):
    """Check how many messages are in the queue"""
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"
        ]
    )["Attributes"]

    available = int(attrs["ApproximateNumberOfMessages"])
    in_flight = int(attrs["ApproximateNumberOfMessagesNotVisible"])
    delayed = int(attrs["ApproximateNumberOfMessagesDelayed"])

    print(f"   Queue depth: {available} available | "
          f"{in_flight} in-flight | {delayed} delayed")
    return available


def upload_test_file():
    """Upload a test CSV that matches the notification filter"""
    s3_client = boto3.client("s3", region_name=REGION)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create test CSV
    filepath = "/tmp/sqs_test_orders.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id",
                         "quantity", "unit_price", "total_amount",
                         "status", "order_date"])
        for i in range(5):
            writer.writerow([
                f"ORD-TEST-{i:04d}", f"CUST-{i:04d}", f"PROD-{i:04d}",
                i + 1, 29.99, round((i + 1) * 29.99, 2),
                "completed", "2025-01-15"
            ])

    s3_key = f"daily_exports/orders/sqs_test_{timestamp}.csv"

    print("=" * 70)
    print("STEP 1: Uploading test file to trigger S3 → SQS event")
    print("=" * 70)
    print(f"   File:     {s3_key}")
    print(f"   Bucket:   {SOURCE_BUCKET}")
    print(f"   Prefix:   daily_exports/ ✅ (matches filter)")
    print(f"   Suffix:   .csv ✅ (matches filter)")

    s3_client.upload_file(
        Filename=filepath,
        Bucket=SOURCE_BUCKET,
        Key=s3_key,
        ExtraArgs={"ContentType": "text/csv"}
    )
    print(f"   ✅ Uploaded: s3://{SOURCE_BUCKET}/{s3_key}")

    os.remove(filepath)
    return s3_key


def parse_s3_event_from_sqs_message(message_body):
    """
    Parse S3 event from SQS message body
    
    DIRECT PATH (S3 → SQS):
    → Message body IS the S3 event JSON
    → Single json.loads() needed
    
    CONTRAST WITH FAN-OUT PATH (S3 → SNS → SQS):
    → Message body is SNS ENVELOPE containing S3 event as string
    → DOUBLE json.loads() needed (covered in Project 28)
    """
    event = json.loads(message_body)

    # ── CHECK FOR TEST EVENT ──
    # S3 sends this when notification is first configured
    if "Event" in event and event.get("Event") == "s3:TestEvent":
        print("   ⚠️  Received S3 TEST EVENT (not a real file event)")
        print("   → This is normal on first configuration")
        print("   → Skipping...")
        return None

    # ── CHECK FOR SNS-WRAPPED EVENT ──
    # If message came through SNS (S3 → SNS → SQS), unwrap first
    if "Type" in event and event.get("Type") == "Notification":
        print("   ℹ️  Detected SNS envelope — unwrapping...")
        inner_message = json.loads(event["Message"])
        event = inner_message

    # ── PARSE S3 EVENT RECORDS ──
    if "Records" not in event:
        print(f"   ⚠️  Unexpected message format (no 'Records' key)")
        print(f"   → Body preview: {message_body[:200]}")
        return None

    results = []
    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        raw_key = record["s3"]["object"]["key"]
        decoded_key = unquote_plus(raw_key)  # URL decode (Project 17 lesson)
        object_size = record["s3"]["object"].get("size", 0)
        event_name = record["eventName"]
        event_time = record["eventTime"]

        results.append({
            "bucket": bucket_name,
            "key": decoded_key,
            "size": object_size,
            "event_name": event_name,
            "event_time": event_time,
            "s3_uri": f"s3://{bucket_name}/{decoded_key}"
        })

    return results


def consume_messages(max_messages=10, delete_after=True):
    """
    Core SQS consumption loop
    
    THIS IS THE PATTERN REUSED IN:
    → Project 27: EC2 worker continuous polling
    → Project 28: Full pipeline S3 → SNS → SQS → EC2
    → Project 30: Automated S3 → Redshift load
    → Project 52: Auto-scaling workers
    """
    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = get_queue_url()

    print("\n" + "=" * 70)
    print("STEP 2: Waiting for queue depth to update (5 seconds)")
    print("=" * 70)
    time.sleep(5)
    check_queue_depth(sqs_client, queue_url)

    print("\n" + "=" * 70)
    print("STEP 3: Consuming messages from SQS (long polling)")
    print("=" * 70)

    messages_processed = 0
    empty_receives = 0
    max_empty = 2  # Stop after 2 empty long-poll cycles

    while messages_processed < max_messages and empty_receives < max_empty:
        print(f"\n   📥 Polling for messages (wait up to 10s)...")

        response = sqs_client.receive_message(
            QueueUrl=queue_url,

            # --- MAX MESSAGES PER RECEIVE ---
            # Can return 1-10 messages per call
            # SQS may return fewer than requested (by design)
            MaxNumberOfMessages=10,

            # --- LONG POLLING ---
            # Wait up to 10 seconds for messages
            # Returns immediately if messages available
            # Returns empty after 10 seconds if no messages
            WaitTimeSeconds=10,

            # --- MESSAGE ATTRIBUTES ---
            # Request additional metadata about the message
            AttributeNames=["All"],

            # --- VISIBILITY TIMEOUT OVERRIDE ---
            # Override queue-level timeout for this specific receive
            # Useful for: quick inspection (short timeout) vs processing (long)
            # Omitted here: uses queue default (600 seconds from 18_01)
        )

        messages = response.get("Messages", [])

        if not messages:
            empty_receives += 1
            print(f"   (empty — {empty_receives}/{max_empty} before stopping)")
            continue

        empty_receives = 0  # Reset on successful receive

        for msg in messages:
            messages_processed += 1
            message_id = msg["MessageId"]
            receipt_handle = msg["ReceiptHandle"]
            body = msg["Body"]
            receive_count = msg["Attributes"].get("ApproximateReceiveCount", "?")
            sent_timestamp = msg["Attributes"].get("SentTimestamp", "?")

            print(f"\n   ── Message #{messages_processed} ──")
            print(f"   MessageId:     {message_id}")
            print(f"   ReceiveCount:  {receive_count}")
            if sent_timestamp != "?":
                sent_time = datetime.fromtimestamp(int(sent_timestamp) / 1000)
                print(f"   Sent At:       {sent_time.strftime('%Y-%m-%d %H:%M:%S')}")

            # ── PARSE S3 EVENT ──
            parsed_events = parse_s3_event_from_sqs_message(body)

            if parsed_events is None:
                # Test event or unparseable — delete and skip
                if delete_after:
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    print("   🗑️  Deleted (test/unparseable event)")
                continue

            for event_info in parsed_events:
                print(f"   Event:         {event_info['event_name']}")
                print(f"   Bucket:        {event_info['bucket']}")
                print(f"   Key:           {event_info['key']}")
                print(f"   Size:          {event_info['size']:,} bytes")
                print(f"   Event Time:    {event_info['event_time']}")
                print(f"   S3 URI:        {event_info['s3_uri']}")

            # ── SIMULATE PROCESSING ──
            # In real pipeline: download file, validate, load to DB
            # Here: just acknowledge we received it
            print(f"   Processing:    ✅ (simulated)")

            # ── DELETE MESSAGE (acknowledge success) ──
            if delete_after:
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
                print(f"   Deleted:       ✅ (removed from queue)")
            else:
                print(f"   Deleted:       ❌ (kept for re-inspection)")
                print(f"   → Will reappear after visibility timeout")

    # ── FINAL QUEUE STATE ──
    print("\n" + "=" * 70)
    print("STEP 4: Final queue state")
    print("=" * 70)
    check_queue_depth(sqs_client, queue_url)

    return messages_processed


def cleanup_test_file(s3_key):
    """Remove test file from S3"""
    s3_client = boto3.client("s3", region_name=REGION)
    try:
        s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=s3_key)
        print(f"\n   🗑️  Cleaned up test file: {s3_key}")
    except Exception as e:
        print(f"\n   ⚠️  Could not clean up: {e}")


if __name__ == "__main__":
    print("🧪 S3 → SQS EVENT NOTIFICATION — TEST + CONSUME")
    print("=" * 70)

    # Upload test file (triggers S3 → SQS event)
    test_key = upload_test_file()

    # Consume messages from SQS
    count = consume_messages(max_messages=5, delete_after=True)

    print("\n" + "=" * 70)
    print(f"📊 SUMMARY: Processed {count} message(s)")
    print("=" * 70)

    # Cleanup
    cleanup_test_file(test_key)