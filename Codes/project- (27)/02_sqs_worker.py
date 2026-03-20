# file: 02_sqs_worker.py
# EC2 Worker that continuously polls SQS for file processing tasks
# This is the MAIN worker script — runs as a daemon on EC2

import boto3
import json
import os
import signal
import sys
import time
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# ═══════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════
REGION = "us-east-2"
QUEUE_NAME = "quickcart-file-processing-queue"
DOWNLOAD_DIR = "/tmp/sqs-worker-downloads"
MAX_MESSAGES_PER_POLL = 10    # Max batch size
LONG_POLL_WAIT = 20           # Seconds (maximum)
SHUTDOWN_GRACEFUL = False     # Flag for graceful shutdown

# ═══════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/var/log/sqs-worker.log", mode="a")
    ]
)
logger = logging.getLogger("sqs-worker")


# ═══════════════════════════════════════════════════
# GRACEFUL SHUTDOWN HANDLER
# ═══════════════════════════════════════════════════
def signal_handler(signum, frame):
    """
    Handle SIGTERM/SIGINT for graceful shutdown
    
    WHY THIS MATTERS:
    → systemctl stop → sends SIGTERM
    → EC2 termination → sends SIGTERM
    → Ctrl+C → sends SIGINT
    → Without handler: worker killed mid-processing
    → With handler: finish current message, then exit
    """
    global SHUTDOWN_GRACEFUL
    logger.info(f"⚠️  Received signal {signum} — shutting down gracefully...")
    SHUTDOWN_GRACEFUL = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# ═══════════════════════════════════════════════════
# S3 EVENT PARSER
# ═══════════════════════════════════════════════════
def parse_s3_event(message_body):
    """
    Parse S3 event from SQS message body
    
    TWO POSSIBLE FORMATS:
    
    Format 1: S3 → SQS directly
    Body = {"Records": [{"s3": {"bucket": {...}, "object": {...}}}]}
    
    Format 2: S3 → SNS → SQS (fan-out pattern from Project 14)
    Body = {"Message": "{\"Records\": [{\"s3\": {...}}]}"}
    (S3 event wrapped inside SNS notification)
    
    This function handles BOTH formats
    """
    try:
        body = json.loads(message_body)
    except json.JSONDecodeError:
        logger.error(f"Cannot parse message body as JSON: {message_body[:200]}")
        return None

    # Check if it's an SNS wrapper
    if "Message" in body and "Type" in body:
        # SNS notification — unwrap
        try:
            inner = json.loads(body["Message"])
            body = inner
        except (json.JSONDecodeError, KeyError):
            logger.error(f"Cannot parse SNS Message payload")
            return None

    # Check for S3 test event (sent when configuring notifications)
    if body.get("Event") == "s3:TestEvent":
        logger.info("📋 S3 test event received — skipping")
        return "TEST_EVENT"

    # Extract S3 records
    records = body.get("Records", [])
    if not records:
        logger.warning(f"No Records in message body")
        return None

    events = []
    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", "")
        key = s3_info.get("object", {}).get("key", "")
        size = s3_info.get("object", {}).get("size", 0)
        event_name = record.get("eventName", "")
        event_time = record.get("eventTime", "")

        # URL-decode the key (S3 encodes special characters)
        import urllib.parse
        key = urllib.parse.unquote_plus(key)

        if bucket and key:
            events.append({
                "bucket": bucket,
                "key": key,
                "size": size,
                "event": event_name,
                "time": event_time
            })

    return events if events else None


# ═══════════════════════════════════════════════════
# FILE PROCESSOR
# ═══════════════════════════════════════════════════
def process_file(s3_event):
    """
    Process a single S3 file event
    
    THIS IS WHERE YOUR BUSINESS LOGIC GOES
    Replace with: Redshift COPY, MariaDB load, Glue trigger, etc.
    
    For this project: download file, validate, log success
    Project 28 builds on this with full end-to-end processing
    Project 30 adds Redshift COPY as the processing step
    """
    bucket = s3_event["bucket"]
    key = s3_event["key"]
    size = s3_event["size"]

    logger.info(f"📥 Processing: s3://{bucket}/{key} ({size} bytes)")

    s3_client = boto3.client("s3", region_name=REGION)

    # --- STEP 1: VALIDATE FILE EXISTS ---
    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
        actual_size = head["ContentLength"]
        logger.info(f"   File confirmed: {actual_size} bytes")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.warning(f"   ⚠️  File not found (may have been deleted): {key}")
            return True  # Return True to delete message (file gone, nothing to do)
        raise

    # --- STEP 2: DOWNLOAD FILE ---
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = os.path.basename(key)
    local_path = os.path.join(DOWNLOAD_DIR, filename)

    s3_client.download_file(bucket, key, local_path)
    logger.info(f"   Downloaded to: {local_path}")

    # --- STEP 3: VALIDATE FILE ---
    local_size = os.path.getsize(local_path)
    if local_size != actual_size:
        logger.error(f"   ❌ Size mismatch: S3={actual_size}, local={local_size}")
        return False  # Return False to NOT delete message (retry)

    # --- STEP 4: PROCESS FILE (business logic placeholder) ---
    # Count rows as a simple validation
    with open(local_path, "r") as f:
        line_count = sum(1 for _ in f) - 1  # Subtract header
    logger.info(f"   Rows in file: {line_count:,}")

    # TODO: Replace with actual processing:
    # → Load into MariaDB (Project 19 pattern)
    # → Trigger Glue job (Project 25 pattern)
    # → Redshift COPY (Project 21 pattern)
    # → Transform and write to S3 (Project 25 pattern)

    # Simulate processing time
    time.sleep(2)

    # --- STEP 5: CLEANUP ---
    os.remove(local_path)
    logger.info(f"   ✅ Processing complete for: {key}")

    return True  # Success — message should be deleted


# ═══════════════════════════════════════════════════
# MAIN WORKER LOOP
# ═══════════════════════════════════════════════════
def run_worker():
    """
    Main worker loop — continuously polls SQS
    
    LOOP MECHANICS:
    1. Call receive_message with long polling (20s wait)
    2. If messages received → process each one
    3. If processing succeeds → delete message
    4. If processing fails → don't delete (message retries)
    5. If no messages → long poll waited 20s → loop again
    6. If shutdown signal → finish current batch → exit
    """
    sqs_client = boto3.client("sqs", region_name=REGION)

    # Get queue URL
    try:
        queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    except ClientError as e:
        logger.error(f"❌ Cannot find queue '{QUEUE_NAME}': {e}")
        logger.error(f"   Run 01_create_sqs_queue.py first")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("🏭 SQS WORKER STARTED")
    logger.info(f"   Queue: {QUEUE_NAME}")
    logger.info(f"   URL: {queue_url}")
    logger.info(f"   Long poll: {LONG_POLL_WAIT}s")
    logger.info(f"   Batch size: {MAX_MESSAGES_PER_POLL}")
    logger.info(f"   PID: {os.getpid()}")
    logger.info("=" * 60)

    # Metrics
    total_processed = 0
    total_errors = 0
    start_time = time.time()

    while not SHUTDOWN_GRACEFUL:
        try:
            # --- RECEIVE MESSAGES (LONG POLL) ---
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=MAX_MESSAGES_PER_POLL,
                WaitTimeSeconds=LONG_POLL_WAIT,
                AttributeNames=["All"],
                MessageAttributeNames=["All"]
            )

            messages = response.get("Messages", [])

            if not messages:
                # No messages — long poll timed out (20s)
                # This is NORMAL — just loop again
                continue

            logger.info(f"\n📬 Received {len(messages)} message(s)")

            # --- PROCESS EACH MESSAGE ---
            for message in messages:
                if SHUTDOWN_GRACEFUL:
                    logger.info("⚠️  Shutdown requested — stopping after current message")
                    break

                message_id = message["MessageId"]
                receipt_handle = message["ReceiptHandle"]
                receive_count = message.get("Attributes", {}).get(
                    "ApproximateReceiveCount", "1"
                )

                logger.info(f"\n📨 Message: {message_id} (attempt #{receive_count})")

                try:
                    # Parse S3 event
                    events = parse_s3_event(message["Body"])

                    if events == "TEST_EVENT":
                        # S3 test notification — delete and skip
                        sqs_client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle
                        )
                        continue

                    if events is None:
                        logger.error(f"   ❌ Cannot parse message — sending to DLQ")
                        # Don't delete — let it retry and eventually go to DLQ
                        total_errors += 1
                        continue

                    # Process each S3 event in the message
                    all_succeeded = True
                    for event in events:
                        success = process_file(event)
                        if not success:
                            all_succeeded = False
                            total_errors += 1

                    # --- DELETE MESSAGE ONLY ON SUCCESS ---
                    if all_succeeded:
                        sqs_client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle
                        )
                        total_processed += 1
                        logger.info(f"   🗑️  Message deleted (processed successfully)")
                    else:
                        logger.warning(f"   ⚠️  Processing failed — message will retry")
                        total_errors += 1

                except Exception as e:
                    logger.error(f"   ❌ Unexpected error: {e}")
                    total_errors += 1
                    # Don't delete — let visibility timeout expire → retry

        except KeyboardInterrupt:
            logger.info("⚠️  KeyboardInterrupt — shutting down")
            break

        except Exception as e:
            logger.error(f"❌ Worker loop error: {e}")
            time.sleep(5)  # Back off before retrying

    # --- SHUTDOWN SUMMARY ---
    uptime = round(time.time() - start_time)
    logger.info(f"\n{'=' * 60}")
    logger.info(f"🛑 WORKER SHUTDOWN")
    logger.info(f"   Uptime:    {uptime}s ({uptime//3600}h {(uptime%3600)//60}m)")
    logger.info(f"   Processed: {total_processed}")
    logger.info(f"   Errors:    {total_errors}")
    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    run_worker()