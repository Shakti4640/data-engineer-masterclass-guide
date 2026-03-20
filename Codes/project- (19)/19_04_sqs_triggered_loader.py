# file: 19_04_sqs_triggered_loader.py
# Purpose: Combine SQS consumer (Project 18) with CSV loader (this project)
# THIS IS THE FULL AUTOMATED PIPELINE: S3 → SQS → EC2 → MariaDB
# Run as a service on EC2 — files load automatically when they land in S3

import boto3
import json
import signal
import os
import time
from datetime import datetime
from urllib.parse import unquote_plus

# Import the loader from this project
from load_csv_to_mariadb_module import load_csv_to_mariadb

REGION = "us-east-2"
QUEUE_NAME = "quickcart-file-processing-queue"

# Graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    print("\n\n   🛑 Shutdown requested — finishing current load...")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def parse_s3_event(message_body):
    """Parse S3 event from SQS message (same as Project 18)"""
    event = json.loads(message_body)

    if event.get("Event") == "s3:TestEvent":
        return None

    if event.get("Type") == "Notification":
        event = json.loads(event["Message"])

    if "Records" not in event:
        return None

    record = event["Records"][0]
    return {
        "bucket": record["s3"]["bucket"]["name"],
        "key": unquote_plus(record["s3"]["object"]["key"]),
        "size": record["s3"]["object"].get("size", 0),
        "event_time": record["eventTime"]
    }


def run_auto_loader():
    """
    Continuously poll SQS → parse S3 event → load CSV into MariaDB
    
    ARCHITECTURE:
    S3 (file lands) → SQS (event queued) → THIS SCRIPT → MariaDB (data loaded)
    
    This replaces:
    → Manual download (Project 1 download)
    → Manual load (this project's manual run)
    → With: fully automated event-driven pipeline
    """
    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]

    print("=" * 70)
    print("🏭 AUTO-LOADER: SQS → S3 Download → MariaDB Load")
    print("=" * 70)
    print(f"   Queue:     {QUEUE_NAME}")
    print(f"   Mode:      Continuous (Ctrl+C to stop)")
    print(f"   Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 70)

    stats = {
        "files_loaded": 0,
        "files_failed": 0,
        "files_skipped": 0,
        "total_rows": 0,
        "start_time": time.time()
    }

    poll_count = 0

    while not shutdown_requested:
        poll_count += 1

        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                AttributeNames=["All"]
            )

            messages = response.get("Messages", [])

            if not messages:
                if poll_count % 15 == 0:  # Log every ~5 minutes
                    elapsed = time.time() - stats["start_time"]
                    print(f"\n   [{datetime.now().strftime('%H:%M:%S')}] "
                          f"Waiting for files... "
                          f"(uptime: {elapsed/60:.0f}min | "
                          f"loaded: {stats['files_loaded']} | "
                          f"rows: {stats['total_rows']:,})")
                continue

            for msg in messages:
                if shutdown_requested:
                    break

                receipt_handle = msg["ReceiptHandle"]
                receive_count = int(
                    msg["Attributes"].get("ApproximateReceiveCount", "1")
                )

                # ── PARSE S3 EVENT ──
                event_info = parse_s3_event(msg["Body"])

                if event_info is None:
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    stats["files_skipped"] += 1
                    continue

                print(f"\n   {'='*60}")
                print(f"   📨 NEW FILE EVENT (attempt #{receive_count})")
                print(f"   File: {event_info['key']}")
                print(f"   Size: {event_info['size']:,} bytes")
                print(f"   {'='*60}")

                # ── LOAD INTO MARIADB ──
                # This calls the core loader from 19_02
                result = load_csv_to_mariadb(
                    bucket=event_info["bucket"],
                    s3_key=event_info["key"],
                    force_reload=(receive_count > 1)  # Force on retry
                )

                if result["success"]:
                    # ── SUCCESS: Delete message from queue ──
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    stats["files_loaded"] += 1
                    stats["total_rows"] += result.get("rows_loaded", 0)
                    print(f"   ✅ Load complete — message deleted from queue")
                else:
                    # ── FAILURE: Don't delete — will retry ──
                    stats["files_failed"] += 1
                    print(f"   ❌ Load failed — message will retry")
                    print(f"   → Error: {result.get('error', 'unknown')[:100]}")
                    print(f"   → Attempt {receive_count} — "
                          f"will reappear after visibility timeout")

        except Exception as e:
            print(f"\n   ⚠️  Worker error: {e}")
            time.sleep(5)

    # ── SHUTDOWN SUMMARY ──
    elapsed = time.time() - stats["start_time"]
    print("\n" + "=" * 70)
    print("🏭 AUTO-LOADER SHUTDOWN")
    print("=" * 70)
    print(f"   Uptime:        {elapsed/60:.1f} minutes")
    print(f"   Files loaded:  {stats['files_loaded']}")
    print(f"   Files failed:  {stats['files_failed']}")
    print(f"   Files skipped: {stats['files_skipped']}")
    print(f"   Total rows:    {stats['total_rows']:,}")
    print("=" * 70)


if __name__ == "__main__":
    run_auto_loader()