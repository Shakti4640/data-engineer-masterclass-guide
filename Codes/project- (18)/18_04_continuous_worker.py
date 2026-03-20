# file: 18_04_continuous_worker.py
# Purpose: Continuously poll SQS and process files as they arrive
# THIS IS THE PRODUCTION PATTERN used in Projects 27, 28, 30
# Run this on EC2 as a long-running service

import boto3
import json
import signal
import sys
import os
import time
from datetime import datetime
from urllib.parse import unquote_plus

REGION = "us-east-2"
QUEUE_NAME = "quickcart-file-processing-queue"
SOURCE_BUCKET = "quickcart-raw-data-prod"
DOWNLOAD_DIR = "/tmp/sqs_processing"

# ── GRACEFUL SHUTDOWN ──
# Handle Ctrl+C to stop worker cleanly
shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    print("\n\n   🛑 Shutdown requested — finishing current message...")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_queue_url():
    sqs_client = boto3.client("sqs", region_name=REGION)
    return sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]


def parse_s3_event(message_body):
    """
    Parse S3 event from SQS message body
    Handles: direct S3→SQS, S3→SNS→SQS, test events
    Same logic as 18_03 — extracted into reusable function
    """
    event = json.loads(message_body)

    # Skip S3 test events
    if event.get("Event") == "s3:TestEvent":
        return None

    # Handle SNS-wrapped events (S3 → SNS → SQS path)
    if event.get("Type") == "Notification":
        event = json.loads(event["Message"])

    if "Records" not in event:
        return None

    record = event["Records"][0]
    return {
        "bucket": record["s3"]["bucket"]["name"],
        "key": unquote_plus(record["s3"]["object"]["key"]),
        "size": record["s3"]["object"].get("size", 0),
        "event_name": record["eventName"],
        "event_time": record["eventTime"]
    }


def process_file(s3_client, bucket, key, size):
    """
    Process a single file from S3
    
    IN REAL PIPELINES THIS WOULD:
    → Download file from S3
    → Validate structure (column count, data types)
    → Transform data (clean, normalize)
    → Load into MariaDB (Project 19) or Redshift (Project 21)
    → Log processing result
    
    HERE: We simulate processing by downloading + counting rows
    Projects 27, 28, 30 replace this with actual loading logic
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = os.path.basename(key)
    local_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        # ── DOWNLOAD FROM S3 ──
        print(f"      📥 Downloading: s3://{bucket}/{key}")
        s3_client.download_file(bucket, key, local_path)
        file_size = os.path.getsize(local_path)
        print(f"      📦 Downloaded: {file_size:,} bytes")

        # ── VALIDATE FILE ──
        # Basic validation: file not empty, has header row
        with open(local_path, "r") as f:
            lines = f.readlines()

        if len(lines) < 2:
            print(f"      ⚠️  File has {len(lines)} lines (expected header + data)")
            return False

        header = lines[0].strip()
        data_rows = len(lines) - 1
        column_count = len(header.split(","))

        print(f"      ✅ Validated: {data_rows} data rows, {column_count} columns")
        print(f"      📋 Header: {header[:80]}{'...' if len(header) > 80 else ''}")

        # ── SIMULATE PROCESSING ──
        # In Projects 19/27/28: actual database load happens here
        processing_time = min(data_rows * 0.001, 5.0)  # Simulate work
        time.sleep(processing_time)
        print(f"      ✅ Processed in {processing_time:.1f}s (simulated)")

        # ── CLEANUP LOCAL FILE ──
        os.remove(local_path)

        return True

    except Exception as e:
        print(f"      ❌ Processing failed: {e}")
        # Clean up partial download
        if os.path.exists(local_path):
            os.remove(local_path)
        return False


def extend_visibility_timeout(sqs_client, queue_url, receipt_handle, seconds):
    """
    Extend visibility timeout for a message being processed
    
    WHY THIS MATTERS:
    → If processing takes longer than expected
    → Message would reappear and get processed AGAIN
    → Extending timeout prevents duplicate processing
    → Call this periodically during long-running processing
    
    PATTERN: "Heartbeat"
    → Start processing
    → Every N seconds: extend timeout by 2*N seconds
    → When done: delete message
    → If crash: timeout eventually expires → message reappears → retry
    """
    try:
        sqs_client.change_message_visibility(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=seconds
        )
        return True
    except Exception as e:
        print(f"      ⚠️  Could not extend visibility: {e}")
        return False


def run_worker():
    """
    Main worker loop — runs until shutdown requested
    
    PRODUCTION DEPLOYMENT:
    → Run as systemd service on EC2
    → Or as ECS task / Fargate container
    → Or as Kubernetes pod
    → Auto-restart on crash via process manager
    
    SCALING (Project 52):
    → Multiple instances of this worker poll same queue
    → SQS ensures each message goes to only one worker
    → Auto-scale based on queue depth
    """
    sqs_client = boto3.client("sqs", region_name=REGION)
    s3_client = boto3.client("s3", region_name=REGION)
    queue_url = get_queue_url()

    print("=" * 70)
    print("🏭 SQS WORKER STARTED")
    print("=" * 70)
    print(f"   Queue:     {QUEUE_NAME}")
    print(f"   Polling:   Long poll (20 seconds)")
    print(f"   Mode:      Continuous until Ctrl+C")
    print(f"   Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   PID:       {os.getpid()}")
    print("-" * 70)

    stats = {
        "messages_received": 0,
        "messages_processed": 0,
        "messages_failed": 0,
        "messages_skipped": 0,
        "start_time": time.time()
    }

    poll_count = 0

    while not shutdown_requested:
        poll_count += 1

        try:
            # ── RECEIVE MESSAGES ──
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,  # Process one at a time for reliability
                WaitTimeSeconds=20,     # Long polling — wait up to 20 seconds
                AttributeNames=["All"],
                MessageAttributeNames=["All"]
            )

            messages = response.get("Messages", [])

            if not messages:
                # Queue empty — log periodically (not every poll)
                if poll_count % 10 == 0:
                    elapsed = time.time() - stats["start_time"]
                    print(f"   [{datetime.now().strftime('%H:%M:%S')}] "
                          f"Polling... (uptime: {elapsed/60:.0f}min | "
                          f"processed: {stats['messages_processed']} | "
                          f"failed: {stats['messages_failed']})")
                continue

            for msg in messages:
                if shutdown_requested:
                    print("   🛑 Shutdown — not processing new messages")
                    break

                stats["messages_received"] += 1
                message_id = msg["MessageId"]
                receipt_handle = msg["ReceiptHandle"]
                receive_count = int(
                    msg["Attributes"].get("ApproximateReceiveCount", "1")
                )

                print(f"\n   ┌─ MESSAGE RECEIVED ──────────────────────")
                print(f"   │ MessageId:    {message_id[:20]}...")
                print(f"   │ ReceiveCount: {receive_count}"
                      f"{'  ⚠️ RETRY!' if receive_count > 1 else ''}")

                # ── PARSE S3 EVENT ──
                event_info = parse_s3_event(msg["Body"])

                if event_info is None:
                    print(f"   │ Type:         Test/Non-S3 event — skipping")
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    stats["messages_skipped"] += 1
                    print(f"   └─ SKIPPED ─────────────────────────────")
                    continue

                print(f"   │ Event:        {event_info['event_name']}")
                print(f"   │ File:         {event_info['key']}")
                print(f"   │ Size:         {event_info['size']:,} bytes")
                print(f"   │ Event Time:   {event_info['event_time']}")
                print(f"   │")

                # ── PROCESS FILE ──
                success = process_file(
                    s3_client=s3_client,
                    bucket=event_info["bucket"],
                    key=event_info["key"],
                    size=event_info["size"]
                )

                if success:
                    # ── DELETE MESSAGE (acknowledge success) ──
                    # CRITICAL: only delete AFTER successful processing
                    # If we crash before this line → message reappears → retry
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    stats["messages_processed"] += 1
                    print(f"   │ Result:       ✅ SUCCESS — deleted from queue")
                else:
                    # ── DON'T DELETE — let message retry ──
                    # Message will reappear after visibility timeout
                    # After maxReceiveCount retries → goes to DLQ (Project 50)
                    stats["messages_failed"] += 1
                    print(f"   │ Result:       ❌ FAILED — will retry")
                    print(f"   │               Message stays in queue")
                    print(f"   │               Retry #{receive_count} of max")

                print(f"   └───────────────────────────────────────────")

        except Exception as e:
            print(f"\n   ⚠️  Worker error: {e}")
            print(f"   → Sleeping 5 seconds before retrying...")
            time.sleep(5)

    # ── SHUTDOWN SUMMARY ──
    elapsed = time.time() - stats["start_time"]
    print("\n" + "=" * 70)
    print("🏭 WORKER SHUTDOWN SUMMARY")
    print("=" * 70)
    print(f"   Uptime:             {elapsed/60:.1f} minutes")
    print(f"   Messages received:  {stats['messages_received']}")
    print(f"   Messages processed: {stats['messages_processed']}")
    print(f"   Messages failed:    {stats['messages_failed']}")
    print(f"   Messages skipped:   {stats['messages_skipped']}")
    print(f"   Stopped at:         {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)


if __name__ == "__main__":
    run_worker()