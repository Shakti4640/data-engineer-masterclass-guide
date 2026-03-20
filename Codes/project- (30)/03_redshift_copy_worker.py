# file: 03_redshift_copy_worker.py
# EC2 Worker: polls SQS → receives S3 events → runs Redshift COPY
# Combines: Project 27 (SQS worker) + Project 21 (COPY) + Project 28 (parsing)

import boto3
import json
import os
import sys
import signal
import time
import logging
import urllib.parse
import psycopg2
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

from table_config import (
    get_table_config, extract_export_date,
    build_copy_sql, TABLE_CONFIGS, REGION
)

# ═══════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════
QUEUE_NAME = "quickcart-redshift-load-queue"
BUCKET_NAME = "quickcart-raw-data-prod"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:123456789012:data-pipeline-alerts"

REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "YourSecurePassword123!"

SHUTDOWN_GRACEFUL = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/var/log/redshift-copy-worker.log", mode="a")
    ]
)
logger = logging.getLogger("redshift-copy-worker")


def signal_handler(signum, frame):
    global SHUTDOWN_GRACEFUL
    logger.info(f"⚠️  Shutdown signal — finishing current COPY...")
    SHUTDOWN_GRACEFUL = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# ═══════════════════════════════════════════════════
# S3 EVENT PARSER (from Project 27 — handles SNS wrapping)
# ═══════════════════════════════════════════════════
def parse_s3_event(message_body):
    """Parse S3 event — handles direct and SNS-wrapped formats"""
    try:
        body = json.loads(message_body)
    except json.JSONDecodeError:
        return None

    if "Message" in body and "Type" in body:
        try:
            body = json.loads(body["Message"])
        except (json.JSONDecodeError, KeyError):
            return None

    if body.get("Event") == "s3:TestEvent":
        return "TEST_EVENT"

    records = body.get("Records", [])
    events = []
    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", "")
        key = urllib.parse.unquote_plus(s3_info.get("object", {}).get("key", ""))
        size = s3_info.get("object", {}).get("size", 0)
        if bucket and key:
            events.append({"bucket": bucket, "key": key, "size": size})

    return events if events else None


# ═══════════════════════════════════════════════════
# REDSHIFT COPY EXECUTOR
# ═══════════════════════════════════════════════════
def execute_redshift_copy(s3_event):
    """
    Execute Redshift COPY for a single S3 file event
    
    FLOW:
    1. Parse S3 key → determine table
    2. Validate export date
    3. Connect to Redshift
    4. BEGIN transaction
    5. TRUNCATE target table
    6. COPY from S3
    7. Validate row count
    8. COMMIT (or ROLLBACK on failure)
    9. Return metrics
    """
    bucket = s3_event["bucket"]
    key = s3_event["key"]

    logger.info(f"\n{'═' * 60}")
    logger.info(f"📥 PROCESSING: s3://{bucket}/{key}")
    start_time = time.time()

    # --- STEP 1: DETERMINE TABLE ---
    table_name, config = get_table_config(key)

    if config is None:
        logger.warning(f"   ⚠️  Unknown table '{table_name}' — skipping")
        return {"table": table_name, "status": "SKIPPED", "reason": "unknown table"}

    schema = config["redshift_schema"]
    table = config["redshift_table"]
    full_table = f"{schema}.{table}"

    logger.info(f"   Target: {full_table}")

    # --- STEP 2: VALIDATE EXPORT DATE ---
    export_date = extract_export_date(key)
    if export_date:
        logger.info(f"   Export date: {export_date}")
    else:
        logger.warning(f"   ⚠️  Cannot extract export_date from key")

    # --- STEP 3: VERIFY FILE EXISTS ---
    s3_client = boto3.client("s3", region_name=REGION)
    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
        file_size = head["ContentLength"]
        logger.info(f"   File size: {round(file_size/(1024*1024), 2)} MB")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.warning(f"   ⚠️  File not found — skipping")
            return {"table": table_name, "status": "SKIPPED", "reason": "file not found"}
        raise

    # --- STEP 4: BUILD COPY SQL ---
    # S3 path: use the directory (not the file) for COPY
    # COPY reads ALL files in the directory
    s3_dir = f"s3://{bucket}/{'/'.join(key.split('/')[:-1])}/"
    copy_sql = build_copy_sql(s3_dir, config)

    logger.info(f"   COPY source: {s3_dir}")

    # --- STEP 5: CONNECT TO REDSHIFT ---
    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST, port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB, user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
        conn.autocommit = False  # Manual transaction control
        cursor = conn.cursor()
    except psycopg2.Error as e:
        logger.error(f"   ❌ Redshift connection failed: {e}")
        return {"table": table_name, "status": "FAILED", "error": f"Connection: {e}"}

    try:
        # --- STEP 6: GET PRE-COPY ROW COUNT ---
        cursor.execute(f"SELECT COUNT(*) FROM {full_table};")
        rows_before = cursor.fetchone()[0]
        logger.info(f"   Rows before: {rows_before:,}")

        # --- STEP 7: TRUNCATE ---
        logger.info(f"   TRUNCATE {full_table}...")
        cursor.execute(f"TRUNCATE TABLE {full_table};")

        # --- STEP 8: COPY ---
        logger.info(f"   COPY starting...")
        copy_start = time.time()
        cursor.execute(copy_sql)
        copy_duration = round(time.time() - copy_start, 2)

        # --- STEP 9: GET POST-COPY ROW COUNT ---
        cursor.execute(f"SELECT COUNT(*) FROM {full_table};")
        rows_after = cursor.fetchone()[0]
        logger.info(f"   Rows loaded: {rows_after:,} ({copy_duration}s)")

        # --- STEP 10: VALIDATE ---
        validation = config.get("validation", {})
        min_rows = validation.get("min_rows", 0)
        max_rows = validation.get("max_rows", float("inf"))

        if rows_after < min_rows:
            logger.error(f"   ❌ Too few rows: {rows_after} < {min_rows} minimum")
            conn.rollback()
            return {
                "table": table_name, "status": "FAILED",
                "error": f"Row count {rows_after} below minimum {min_rows}"
            }

        if rows_after > max_rows:
            logger.warning(f"   ⚠️  Unusually high row count: {rows_after} > {max_rows}")

        # --- STEP 11: CHECK LOAD ERRORS ---
        cursor.execute("""
            SELECT COUNT(*) FROM stl_load_errors 
            WHERE query = pg_last_copy_id();
        """)
        error_count = cursor.fetchone()[0]

        if error_count > 0:
            logger.error(f"   ❌ {error_count} load errors detected")
            cursor.execute("""
                SELECT TRIM(filename), line_number, TRIM(colname), TRIM(err_reason)
                FROM stl_load_errors WHERE query = pg_last_copy_id() LIMIT 5;
            """)
            for err in cursor.fetchall():
                logger.error(f"      File: {err[0]}, Line: {err[1]}, Col: {err[2]}, Reason: {err[3]}")
            conn.rollback()
            return {"table": table_name, "status": "FAILED", "error": f"{error_count} load errors"}

        # --- STEP 12: COMMIT ---
        conn.commit()
        logger.info(f"   ✅ COMMITTED — {full_table} now has {rows_after:,} rows")

        total_duration = round(time.time() - start_time, 2)

        return {
            "table": table_name,
            "status": "SUCCESS",
            "rows_before": rows_before,
            "rows_after": rows_after,
            "copy_duration": copy_duration,
            "total_duration": total_duration,
            "export_date": export_date,
            "s3_key": key
        }

    except psycopg2.Error as e:
        logger.error(f"   ❌ Redshift error: {e}")
        try:
            conn.rollback()
            logger.info(f"   ↩️  ROLLBACK — {full_table} still has old data")
        except Exception:
            pass
        return {"table": table_name, "status": "FAILED", "error": str(e)}

    finally:
        cursor.close()
        conn.close()


# ═══════════════════════════════════════════════════
# NOTIFICATION
# ═══════════════════════════════════════════════════
def send_load_notification(result):
    """Send SNS notification after each table load"""
    try:
        sns_client = boto3.client("sns", region_name=REGION)

        table = result.get("table", "unknown")
        status = result.get("status", "UNKNOWN")

        if status == "SUCCESS":
            subject = f"✅ Redshift COPY: {table} loaded ({result['rows_after']:,} rows)"
            message = json.dumps({
                "status": "SUCCESS",
                "table": table,
                "rows_loaded": result["rows_after"],
                "copy_seconds": result["copy_duration"],
                "total_seconds": result["total_duration"],
                "export_date": result.get("export_date"),
                "s3_key": result.get("s3_key"),
                "timestamp": datetime.now().isoformat()
            }, indent=2)
        else:
            subject = f"❌ Redshift COPY FAILED: {table}"
            message = json.dumps({
                "status": "FAILED",
                "table": table,
                "error": result.get("error", "unknown"),
                "timestamp": datetime.now().isoformat()
            }, indent=2)

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=message
        )
    except Exception as e:
        logger.warning(f"   Could not send notification: {e}")


# ═══════════════════════════════════════════════════
# MAIN WORKER LOOP
# ═══════════════════════════════════════════════════
def run_copy_worker():
    """Main worker loop — polls SQS, executes COPY"""
    sqs_client = boto3.client("sqs", region_name=REGION)

    try:
        queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    except ClientError:
        logger.error(f"❌ Queue not found: {QUEUE_NAME}")
        logger.error(f"   Run 01_create_redshift_load_queue.py first")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("🏭 REDSHIFT COPY WORKER STARTED")
    logger.info(f"   Queue: {QUEUE_NAME}")
    logger.info(f"   Cluster: {REDSHIFT_HOST}")
    logger.info(f"   Tables: {', '.join(TABLE_CONFIGS.keys())}")
    logger.info(f"   PID: {os.getpid()}")
    logger.info("=" * 60)

    stats = {"processed": 0, "failed": 0, "skipped": 0}

    while not SHUTDOWN_GRACEFUL:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,      # Process ONE at a time (COPY is heavy)
                WaitTimeSeconds=20,
                AttributeNames=["All"]
            )

            messages = response.get("Messages", [])
            if not messages:
                continue

            message = messages[0]
            receipt_handle = message["ReceiptHandle"]
            receive_count = message.get("Attributes", {}).get("ApproximateReceiveCount", "1")

            logger.info(f"\n📬 Message received (attempt #{receive_count})")

            # Parse S3 event
            events = parse_s3_event(message["Body"])

            if events == "TEST_EVENT":
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                logger.info("   S3 test event — deleted")
                continue

            if events is None:
                logger.error("   Cannot parse message — will retry/DLQ")
                stats["failed"] += 1
                continue

            # Process each S3 event (usually just one per message)
            all_success = True
            for event in events:
                # Skip non-CSV files (manifests, metadata)
                if not event["key"].endswith(".csv.gz"):
                    logger.info(f"   Skipping non-CSV: {event['key']}")
                    continue

                result = execute_redshift_copy(event)
                send_load_notification(result)

                if result["status"] == "SUCCESS":
                    stats["processed"] += 1
                elif result["status"] == "SKIPPED":
                    stats["skipped"] += 1
                else:
                    stats["failed"] += 1
                    all_success = False

            # Delete message on success
            if all_success:
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                logger.info("🗑️  Message deleted")
            else:
                logger.warning("⚠️  Message NOT deleted — will retry")

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Worker loop error: {e}")
            time.sleep(10)

    # Shutdown summary
    logger.info(f"\n{'=' * 60}")
    logger.info(f"🛑 WORKER SHUTDOWN")
    logger.info(f"   Processed: {stats['processed']}")
    logger.info(f"   Failed:    {stats['failed']}")
    logger.info(f"   Skipped:   {stats['skipped']}")
    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    run_copy_worker()