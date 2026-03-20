# file: 01_mariadb_nightly_export.py
# Complete nightly MariaDB → S3 export with:
# - Server-side cursor for large tables
# - GZIP compression
# - Date-stamped S3 paths (Hive partitioned)
# - Export manifest
# - SNS notifications
# - Retry logic
# - Comprehensive logging

import boto3
import pymysql
import pymysql.cursors
import csv
import gzip
import os
import sys
import time
import json
import logging
import tempfile
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# ═══════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════
REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
S3_PREFIX = "mariadb_exports"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:123456789012:data-pipeline-alerts"

MARIADB_HOST = "localhost"
MARIADB_PORT = 3306
MARIADB_USER = "quickcart_etl"
MARIADB_PASSWORD = "SecurePassword123!"
MARIADB_DATABASE = "quickcart"

# Tables to export with their configurations
EXPORT_TABLES = [
    {
        "table": "orders",
        "query": "SELECT * FROM orders",
        "use_ss_cursor": True,     # Server-side cursor for large tables
        "batch_size": 10000
    },
    {
        "table": "customers",
        "query": "SELECT * FROM customers",
        "use_ss_cursor": False,    # Small table — standard cursor fine
        "batch_size": 5000
    },
    {
        "table": "products",
        "query": "SELECT * FROM products",
        "use_ss_cursor": False,
        "batch_size": 5000
    },
    {
        "table": "staging_shipfast_updates",
        "query": "SELECT * FROM staging_shipfast_updates",
        "use_ss_cursor": True,
        "batch_size": 10000
    }
]

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30
EXPORT_TIMEOUT_MINUTES = 30

# Logging
LOG_DIR = "/var/log/mariadb-export"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, log_filename))
    ]
)
logger = logging.getLogger("mariadb-export")


# ═══════════════════════════════════════════════════
# DATABASE CONNECTION
# ═══════════════════════════════════════════════════
def get_db_connection(use_ss_cursor=False):
    """
    Create MariaDB connection with optional server-side cursor
    
    SSCursor (SSDictCursor):
    → Fetches rows from server on demand (not all at once)
    → Essential for tables > 1M rows
    → Keeps connection open longer — use during low-traffic
    """
    cursor_class = pymysql.cursors.SSDictCursor if use_ss_cursor else pymysql.cursors.DictCursor

    return pymysql.connect(
        host=MARIADB_HOST,
        port=MARIADB_PORT,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        database=MARIADB_DATABASE,
        cursorclass=cursor_class,
        connect_timeout=30,
        read_timeout=300
    )


# ═══════════════════════════════════════════════════
# SINGLE TABLE EXPORT
# ═══════════════════════════════════════════════════
def export_table(table_config, export_date):
    """
    Export single table from MariaDB to GZIP CSV in S3
    
    FLOW:
    1. Connect to MariaDB
    2. Execute SELECT query
    3. Stream rows to local GZIP CSV file
    4. Upload GZIP CSV to S3 (date-stamped path)
    5. Verify upload
    6. Cleanup local file
    7. Return metrics
    """
    table_name = table_config["table"]
    query = table_config["query"]
    use_ss = table_config["use_ss_cursor"]
    batch_size = table_config["batch_size"]
    date_str = export_date.strftime("%Y-%m-%d")

    logger.info(f"\n{'─' * 50}")
    logger.info(f"📤 EXPORTING: {table_name}")
    logger.info(f"   Query: {query[:80]}...")
    logger.info(f"   Cursor: {'server-side' if use_ss else 'standard'}")

    start_time = time.time()

    # --- STEP 1: CONNECT ---
    conn = get_db_connection(use_ss_cursor=use_ss)
    cursor = conn.cursor()

    try:
        # --- STEP 2: EXECUTE QUERY ---
        logger.info(f"   Executing query...")
        cursor.execute(query)

        # Get column names from cursor description
        if use_ss:
            # SSCursor: fetch first batch to get columns
            first_batch = cursor.fetchmany(batch_size)
            if not first_batch:
                logger.warning(f"   ⚠️  Table is empty — skipping")
                return {"table": table_name, "status": "EMPTY", "rows": 0,
                        "size_bytes": 0, "duration_seconds": 0}
            columns = list(first_batch[0].keys())
        else:
            # Standard cursor: all rows already in memory
            all_rows = cursor.fetchall()
            if not all_rows:
                logger.warning(f"   ⚠️  Table is empty — skipping")
                return {"table": table_name, "status": "EMPTY", "rows": 0,
                        "size_bytes": 0, "duration_seconds": 0}
            columns = list(all_rows[0].keys())

        # --- STEP 3: WRITE TO LOCAL GZIP CSV ---
        local_dir = tempfile.mkdtemp(prefix="mariadb_export_")
        local_filename = f"{table_name}.csv.gz"
        local_path = os.path.join(local_dir, local_filename)

        logger.info(f"   Writing to: {local_path}")

        row_count = 0
        with gzip.open(local_path, "wt", newline="", compresslevel=6) as gz_file:
            writer = csv.DictWriter(gz_file, fieldnames=columns)
            writer.writeheader()

            if use_ss:
                # Server-side cursor: process first batch + continue fetching
                for row in first_batch:
                    # Convert non-serializable types
                    clean_row = {k: str(v) if v is not None else "" for k, v in row.items()}
                    writer.writerow(clean_row)
                    row_count += 1

                # Fetch remaining batches
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    for row in batch:
                        clean_row = {k: str(v) if v is not None else "" for k, v in row.items()}
                        writer.writerow(clean_row)
                        row_count += 1

                    if row_count % 100000 == 0:
                        logger.info(f"   ... {row_count:,} rows written")
            else:
                # Standard cursor: all rows in memory
                for row in all_rows:
                    clean_row = {k: str(v) if v is not None else "" for k, v in row.items()}
                    writer.writerow(clean_row)
                    row_count += 1

        file_size = os.path.getsize(local_path)
        logger.info(f"   Rows: {row_count:,}")
        logger.info(f"   Compressed size: {round(file_size / (1024*1024), 2)} MB")

        # --- STEP 4: UPLOAD TO S3 ---
        s3_key = f"{S3_PREFIX}/{table_name}/export_date={date_str}/{local_filename}"
        logger.info(f"   Uploading to: s3://{BUCKET_NAME}/{s3_key}")

        s3_client = boto3.client("s3", region_name=REGION)
        s3_client.upload_file(
            Filename=local_path,
            Bucket=BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={
                "ContentType": "application/gzip",
                "ServerSideEncryption": "AES256",
                "Metadata": {
                    "source-table": table_name,
                    "export-date": date_str,
                    "row-count": str(row_count),
                    "exported-at": datetime.now().isoformat()
                }
            }
        )

        # --- STEP 5: VERIFY UPLOAD ---
        head = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        remote_size = head["ContentLength"]

        if remote_size != file_size:
            raise Exception(f"Size mismatch: local={file_size}, S3={remote_size}")

        logger.info(f"   ✅ Upload verified ({remote_size:,} bytes)")

        # --- STEP 6: CLEANUP ---
        os.remove(local_path)
        os.rmdir(local_dir)

        duration = round(time.time() - start_time, 2)
        logger.info(f"   Duration: {duration}s")

        return {
            "table": table_name,
            "status": "SUCCESS",
            "rows": row_count,
            "size_bytes": file_size,
            "s3_key": s3_key,
            "duration_seconds": duration
        }

    except Exception as e:
        duration = round(time.time() - start_time, 2)
        logger.error(f"   ❌ Export failed: {e}")
        return {
            "table": table_name,
            "status": "FAILED",
            "error": str(e),
            "rows": 0,
            "size_bytes": 0,
            "duration_seconds": duration
        }
    finally:
        cursor.close()
        conn.close()


def export_table_with_retry(table_config, export_date):
    """Export with retry logic"""
    table_name = table_config["table"]

    for attempt in range(1, MAX_RETRIES + 1):
        result = export_table(table_config, export_date)

        if result["status"] in ("SUCCESS", "EMPTY"):
            return result

        if attempt < MAX_RETRIES:
            logger.warning(f"   ⚠️  Attempt {attempt}/{MAX_RETRIES} failed — retrying in {RETRY_DELAY_SECONDS}s")
            time.sleep(RETRY_DELAY_SECONDS)
        else:
            logger.error(f"   ❌ All {MAX_RETRIES} attempts failed for {table_name}")

    return result  # Return last failed result


# ═══════════════════════════════════════════════════
# EXPORT MANIFEST
# ═══════════════════════════════════════════════════
def write_manifest(export_date, results, total_duration):
    """Write export manifest JSON to S3 for auditing"""
    date_str = export_date.strftime("%Y-%m-%d")

    manifest = {
        "export_date": date_str,
        "export_timestamp": datetime.now().isoformat(),
        "total_duration_seconds": round(total_duration, 2),
        "tables": results,
        "summary": {
            "total_tables": len(results),
            "succeeded": sum(1 for r in results if r["status"] == "SUCCESS"),
            "failed": sum(1 for r in results if r["status"] == "FAILED"),
            "empty": sum(1 for r in results if r["status"] == "EMPTY"),
            "total_rows": sum(r.get("rows", 0) for r in results),
            "total_size_bytes": sum(r.get("size_bytes", 0) for r in results)
        }
    }

    manifest_key = f"{S3_PREFIX}/_manifests/export_{date_str}.json"

    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2, default=str),
        ContentType="application/json",
        ServerSideEncryption="AES256"
    )

    logger.info(f"\n📋 Manifest written: s3://{BUCKET_NAME}/{manifest_key}")
    return manifest


# ═══════════════════════════════════════════════════
# SNS NOTIFICATION
# ═══════════════════════════════════════════════════
def send_notification(manifest):
    """Send completion notification via SNS (Project 12 pattern)"""
    summary = manifest["summary"]
    date_str = manifest["export_date"]

    succeeded = summary["succeeded"]
    failed = summary["failed"]
    total = summary["total_tables"]
    total_rows = summary["total_rows"]
    total_mb = round(summary["total_size_bytes"] / (1024 * 1024), 2)
    duration = manifest["total_duration_seconds"]

    if failed == 0:
        subject = f"✅ MariaDB Export SUCCESS — {date_str}"
        status_emoji = "✅"
    elif succeeded > 0:
        subject = f"⚠️ MariaDB Export PARTIAL — {date_str} ({failed} failed)"
        status_emoji = "⚠️"
    else:
        subject = f"❌ MariaDB Export FAILED — {date_str}"
        status_emoji = "❌"

    # Build table details
    table_lines = []
    for result in manifest["tables"]:
        if result["status"] == "SUCCESS":
            table_lines.append(
                f"  ✅ {result['table']}: {result['rows']:,} rows, "
                f"{round(result['size_bytes']/(1024*1024), 1)} MB, "
                f"{result['duration_seconds']}s"
            )
        elif result["status"] == "EMPTY":
            table_lines.append(f"  ⏭️ {result['table']}: empty (skipped)")
        else:
            table_lines.append(
                f"  ❌ {result['table']}: FAILED — {result.get('error', 'unknown')}"
            )

    message = "\n".join([
        f"{status_emoji} MARIADB NIGHTLY EXPORT — {date_str}",
        "",
        f"Tables: {succeeded}/{total} succeeded",
        f"Rows:   {total_rows:,}",
        f"Size:   {total_mb} MB (compressed)",
        f"Time:   {round(duration)}s",
        "",
        "DETAILS:",
        *table_lines,
        "",
        f"Manifest: s3://{BUCKET_NAME}/{S3_PREFIX}/_manifests/export_{date_str}.json",
        f"Timestamp: {datetime.now().isoformat()}"
    ])

    try:
        sns_client = boto3.client("sns", region_name=REGION)
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],  # SNS subject max 100 chars
            Message=message
        )
        logger.info(f"📧 Notification sent: {subject}")
    except Exception as e:
        logger.error(f"⚠️  Could not send notification: {e}")


# ═══════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════
def run_nightly_export(export_date=None):
    """
    Main entry point for nightly export
    
    Args:
        export_date: date to export (default: yesterday)
    """
    if export_date is None:
        export_date = datetime.now() - timedelta(days=1)
    
    if isinstance(export_date, str):
        export_date = datetime.strptime(export_date, "%Y-%m-%d")

    date_str = export_date.strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info(f"🌙 MARIADB NIGHTLY EXPORT")
    logger.info(f"   Export date: {date_str}")
    logger.info(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"   Tables: {len(EXPORT_TABLES)}")
    logger.info(f"   Target: s3://{BUCKET_NAME}/{S3_PREFIX}/")
    logger.info("=" * 60)

    pipeline_start = time.time()
    results = []

    # Export each table
    for table_config in EXPORT_TABLES:
        result = export_table_with_retry(table_config, export_date)
        results.append(result)

    total_duration = time.time() - pipeline_start

    # Write manifest
    manifest = write_manifest(export_date, results, total_duration)

    # Print summary
    summary = manifest["summary"]
    logger.info(f"\n{'=' * 60}")
    logger.info(f"📊 EXPORT SUMMARY")
    logger.info(f"{'=' * 60}")
    logger.info(f"   Succeeded: {summary['succeeded']}/{summary['total_tables']}")
    logger.info(f"   Failed:    {summary['failed']}")
    logger.info(f"   Empty:     {summary['empty']}")
    logger.info(f"   Rows:      {summary['total_rows']:,}")
    logger.info(f"   Size:      {round(summary['total_size_bytes']/(1024*1024), 2)} MB")
    logger.info(f"   Duration:  {round(total_duration)}s")
    logger.info(f"{'=' * 60}")

    # Send notification
    send_notification(manifest)

    # Exit code
    if summary["failed"] > 0:
        logger.error(f"❌ Export completed with {summary['failed']} failure(s)")
        return 1
    else:
        logger.info(f"✅ Export completed successfully")
        return 0


if __name__ == "__main__":
    # Allow date override via command line
    if len(sys.argv) > 1:
        export_date = sys.argv[1]  # Format: YYYY-MM-DD
    else:
        export_date = None  # Default: yesterday

    exit_code = run_nightly_export(export_date)
    sys.exit(exit_code)