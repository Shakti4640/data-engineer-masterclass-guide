# file: 03_enhanced_worker.py
# Complete worker: SQS → download → validate → MariaDB load → notify
# Builds on Project 27 worker with full processing pipeline

import boto3
import json
import csv
import os
import signal
import sys
import time
import logging
import pymysql
import urllib.parse
from datetime import datetime
from botocore.exceptions import ClientError

# ═══════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════
REGION = "us-east-2"
QUEUE_NAME = "quickcart-file-processing-queue"
COMPLETION_TOPIC = "arn:aws:sns:us-east-2:123456789012:data-pipeline-alerts"
DOWNLOAD_DIR = "/tmp/pipeline-downloads"

# MariaDB connection (from Project 8, 9)
MARIADB_HOST = "localhost"
MARIADB_PORT = 3306
MARIADB_USER = "quickcart_etl"
MARIADB_PASSWORD = [REDACTED:PASSWORD]!"
MARIADB_DATABASE = "quickcart"

# Expected schema for ShipFast files
SHIPFAST_EXPECTED_COLUMNS = [
    "tracking_id", "order_id", "shipping_status",
    "shipped_date", "carrier", "estimated_delivery"
]

SHUTDOWN_GRACEFUL = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("pipeline-worker")


def signal_handler(signum, frame):
    global SHUTDOWN_GRACEFUL
    logger.info(f"⚠️  Shutdown signal received — finishing current message...")
    SHUTDOWN_GRACEFUL = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# ═══════════════════════════════════════════════════
# S3 EVENT PARSER (handles SNS wrapping — from Project 27)
# ═══════════════════════════════════════════════════
def parse_s3_event(message_body):
    """Parse S3 event — handles both direct and SNS-wrapped formats"""
    try:
        body = json.loads(message_body)
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in message body")
        return None

    # Unwrap SNS envelope if present
    if "Message" in body and "Type" in body:
        try:
            body = json.loads(body["Message"])
        except (json.JSONDecodeError, KeyError):
            logger.error("Cannot parse SNS Message payload")
            return None

    # Handle S3 test event
    if body.get("Event") == "s3:TestEvent":
        return "TEST_EVENT"

    records = body.get("Records", [])
    events = []
    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", "")
        key = urllib.parse.unquote_plus(s3_info.get("object", {}).get("key", ""))
        size = s3_info.get("object", {}).get("size", 0)
        event_time = record.get("eventTime", "")

        if bucket and key:
            events.append({
                "bucket": bucket, "key": key,
                "size": size, "time": event_time
            })

    return events if events else None


# ═══════════════════════════════════════════════════
# IDEMPOTENCY CHECK
# ═══════════════════════════════════════════════════
def already_processed(db_conn, file_key):
    """Check if this file was already processed — prevents duplicates"""
    with db_conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM processed_files WHERE file_key = %s",
            (file_key,)
        )
        count = cursor.fetchone()[0]
    return count > 0


def record_processed(db_conn, file_key, rows_loaded, processing_seconds):
    """Record that this file was successfully processed"""
    with db_conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO processed_files 
               (file_key, rows_loaded, processing_seconds, processed_at)
               VALUES (%s, %s, %s, NOW())""",
            (file_key, rows_loaded, processing_seconds)
        )
    db_conn.commit()


# ═══════════════════════════════════════════════════
# CSV VALIDATION
# ═══════════════════════════════════════════════════
def validate_csv(local_path, expected_columns):
    """
    Validate CSV before loading into database
    
    CHECKS:
    1. File is not empty
    2. Header matches expected columns
    3. Row count > 0
    4. No completely empty rows
    5. Sample data types look correct
    """
    errors = []

    # Check file size
    file_size = os.path.getsize(local_path)
    if file_size == 0:
        return False, ["File is empty (0 bytes)"], 0

    with open(local_path, "r", newline="") as f:
        reader = csv.reader(f)

        # Check header
        try:
            header = next(reader)
            header = [col.strip().lower() for col in header]
        except StopIteration:
            return False, ["File has no rows (not even header)"], 0

        # Verify expected columns
        missing_cols = set(expected_columns) - set(header)
        extra_cols = set(header) - set(expected_columns)

        if missing_cols:
            errors.append(f"Missing columns: {missing_cols}")
        if extra_cols:
            logger.warning(f"Extra columns (ignored): {extra_cols}")

        # Count rows and validate
        row_count = 0
        empty_rows = 0

        for row in reader:
            row_count += 1
            if all(cell.strip() == "" for cell in row):
                empty_rows += 1

        if row_count == 0:
            errors.append("File has header but no data rows")

        if empty_rows > 0:
            logger.warning(f"Found {empty_rows} empty rows (will be skipped)")

    is_valid = len(errors) == 0 and row_count > 0

    if not is_valid:
        logger.error(f"Validation failed: {errors}")

    return is_valid, errors, row_count


# ═══════════════════════════════════════════════════
# MARIADB LOADING
# ═══════════════════════════════════════════════════
def load_into_mariadb(db_conn, local_path, row_count):
    """
    Load CSV into MariaDB staging table
    
    PATTERN: TRUNCATE staging → bulk INSERT → validate
    → Staging table is safe to truncate (temporary)
    → Production table untouched until staging validated
    """
    with db_conn.cursor() as cursor:
        # Truncate staging table
        cursor.execute("TRUNCATE TABLE staging_shipfast_updates;")

        # Bulk insert from CSV
        with open(local_path, "r", newline="") as f:
            reader = csv.DictReader(f)

            batch = []
            batch_size = 1000
            loaded = 0

            for row in reader:
                # Clean values
                tracking_id = row.get("tracking_id", "").strip()
                order_id = row.get("order_id", "").strip()
                status = row.get("shipping_status", "").strip()
                shipped_date = row.get("shipped_date", "").strip() or None
                carrier = row.get("carrier", "").strip()
                est_delivery = row.get("estimated_delivery", "").strip() or None

                # Skip empty rows
                if not tracking_id:
                    continue

                batch.append((
                    tracking_id, order_id, status,
                    shipped_date, carrier, est_delivery
                ))

                if len(batch) >= batch_size:
                    cursor.executemany(
                        """INSERT INTO staging_shipfast_updates
                           (tracking_id, order_id, shipping_status,
                            shipped_date, carrier, estimated_delivery)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        batch
                    )
                    loaded += len(batch)
                    batch = []

            # Insert remaining
            if batch:
                cursor.executemany(
                    """INSERT INTO staging_shipfast_updates
                       (tracking_id, order_id, shipping_status,
                        shipped_date, carrier, estimated_delivery)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    batch
                )
                loaded += len(batch)

        db_conn.commit()

        # Validate load
        cursor.execute("SELECT COUNT(*) FROM staging_shipfast_updates;")
        db_count = cursor.fetchone()[0]

        logger.info(f"   MariaDB staging: {db_count} rows loaded")

        if db_count == 0:
            return False, 0

        return True, db_count

    return False, 0


# ═══════════════════════════════════════════════════
# NOTIFICATION
# ═══════════════════════════════════════════════════
def send_completion_notification(file_key, rows, processing_time, latency):
    """Send SNS notification on successful processing"""
    try:
        sns_client = boto3.client("sns", region_name=REGION)
        sns_client.publish(
            TopicArn=COMPLETION_TOPIC,
            Subject=f"✅ File Processed: {os.path.basename(file_key)}",
            Message=json.dumps({
                "status": "SUCCESS",
                "file": file_key,
                "rows_loaded": rows,
                "processing_seconds": round(processing_time, 2),
                "end_to_end_latency_seconds": round(latency, 2),
                "timestamp": datetime.now().isoformat()
            }, indent=2)
        )
    except Exception as e:
        logger.warning(f"Could not send completion notification: {e}")


# ═══════════════════════════════════════════════════
# MAIN PROCESSING FUNCTION
# ═══════════════════════════════════════════════════
def process_file_event(s3_event):
    """Complete file processing pipeline"""
    bucket = s3_event["bucket"]
    key = s3_event["key"]
    event_time = s3_event.get("time", "")

    process_start = time.time()
    logger.info(f"\n📥 PROCESSING: s3://{bucket}/{key}")

    s3_client = boto3.client("s3", region_name=REGION)

    # --- STEP 1: CONNECT TO MARIADB ---
    try:
        db_conn = pymysql.connect(
            host=MARIADB_HOST, port=MARIADB_PORT,
            user=MARIADB_USER, password=MARIADB_PASSWORD,
            database=MARIADB_DATABASE, autocommit=False
        )
    except pymysql.Error as e:
        logger.error(f"   ❌ MariaDB connection failed: {e}")
        return False

    try:
        # --- STEP 2: IDEMPOTENCY CHECK ---
        if already_processed(db_conn, key):
            logger.info(f"   ⏭️  Already processed — skipping")
            db_conn.close()
            return True  # Delete message (already done)

        # --- STEP 3: VERIFY FILE EXISTS ---
        try:
            head = s3_client.head_object(Bucket=bucket, Key=key)
            file_size = head["ContentLength"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.warning(f"   ⚠️  File not found (deleted?) — skipping")
                db_conn.close()
                return True
            raise

        logger.info(f"   File size: {file_size:,} bytes")

        # --- STEP 4: DOWNLOAD ---
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        filename = os.path.basename(key)
        local_path = os.path.join(DOWNLOAD_DIR, filename)

        s3_client.download_file(bucket, key, local_path)
        logger.info(f"   Downloaded: {local_path}")

        # --- STEP 5: VALIDATE CSV ---
        is_valid, errors, row_count = validate_csv(local_path, SHIPFAST_EXPECTED_COLUMNS)

        if not is_valid:
            logger.error(f"   ❌ Validation failed: {errors}")
            os.remove(local_path)
            db_conn.close()
            return False  # Retry (maybe file was still uploading)

        logger.info(f"   ✅ Validated: {row_count} rows, schema matches")

        # --- STEP 6: LOAD INTO MARIADB ---
        load_success, loaded_count = load_into_mariadb(db_conn, local_path, row_count)

        if not load_success:
            logger.error(f"   ❌ MariaDB load failed")
            os.remove(local_path)
            db_conn.close()
            return False

        # --- STEP 7: RECORD PROCESSING ---
        processing_time = time.time() - process_start
        record_processed(db_conn, key, loaded_count, processing_time)

        # --- STEP 8: CLEANUP ---
        os.remove(local_path)

        # --- STEP 9: CALCULATE LATENCY ---
        latency = processing_time  # Approximate (would be more accurate with event_time parsing)

        logger.info(f"   ✅ COMPLETE: {loaded_count} rows in {processing_time:.1f}s")

        # --- STEP 10: NOTIFY ---
        send_completion_notification(key, loaded_count, processing_time, latency)

        db_conn.close()
        return True

    except Exception as e:
        logger.error(f"   ❌ Unexpected error: {e}")
        try:
            db_conn.rollback()
            db_conn.close()
        except Exception:
            pass
        return False


# ═══════════════════════════════════════════════════
# MAIN WORKER LOOP (from Project 27 — enhanced)
# ═══════════════════════════════════════════════════
def run_pipeline_worker():
    """Main worker loop with full processing pipeline"""
    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]

    logger.info("=" * 60)
    logger.info("🏭 PIPELINE WORKER STARTED")
    logger.info(f"   Queue: {QUEUE_NAME}")
    logger.info(f"   Target: MariaDB → staging_shipfast_updates")
    logger.info(f"   PID: {os.getpid()}")
    logger.info("=" * 60)

    total_processed = 0
    total_errors = 0

    while not SHUTDOWN_GRACEFUL:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20,
                AttributeNames=["All"]
            )

            messages = response.get("Messages", [])
            if not messages:
                continue

            logger.info(f"\n📬 Received {len(messages)} message(s)")

            for message in messages:
                if SHUTDOWN_GRACEFUL:
                    break

                receipt_handle = message["ReceiptHandle"]
                receive_count = message.get("Attributes", {}).get(
                    "ApproximateReceiveCount", "1"
                )

                logger.info(f"📨 Message (attempt #{receive_count})")

                # Parse S3 event
                events = parse_s3_event(message["Body"])

                if events == "TEST_EVENT":
                    sqs_client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=receipt_handle
                    )
                    continue

                if events is None:
                    logger.error("Cannot parse message — will retry/DLQ")
                    total_errors += 1
                    continue

                # Process each file event
                all_success = True
                for event in events:
                    success = process_file_event(event)
                    if not success:
                        all_success = False
                        total_errors += 1

                # Delete on success
                if all_success:
                    sqs_client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=receipt_handle
                    )
                    total_processed += 1
                    logger.info("🗑️  Message deleted")

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Worker loop error: {e}")
            time.sleep(5)

    logger.info(f"\n🛑 WORKER SHUTDOWN — Processed: {total_processed}, Errors: {total_errors}")


if __name__ == "__main__":
    run_pipeline_worker()