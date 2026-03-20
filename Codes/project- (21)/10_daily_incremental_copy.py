# file: 10_daily_incremental_copy.py
# Loads ONLY today's new file — does NOT reload historical data
# This is the pattern you run every day after initial full load
# Builds on Project 28 (S3 → SQS → EC2 Poller → Redshift COPY)

import boto3
import psycopg2
import json
import time
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"
S3_PREFIX = "daily_exports/orders"
MANIFEST_PREFIX = "manifests/daily"

REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWOR[REDACTED:PASSWORD]d123!"
REDSHIFT_IAM_ROLE = "arn:aws:iam::123456789012:role/RedshiftS3ReadRole"


def get_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )


def check_file_exists(s3_client, bucket, key):
    """Verify file exists in S3 before attempting COPY"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False


def create_daily_manifest(s3_client, target_date):
    """
    Create manifest for SINGLE day's file
    
    WHY manifest for single file?
    → Consistency: same code pattern for full load and daily load
    → Safety: mandatory=true catches missing files
    → Audit trail: manifest in S3 records what was loaded when
    """
    date_str = target_date.strftime("%Y%m%d")
    
    # Determine file extension (check .gz first, then .csv)
    gz_key = f"{S3_PREFIX}/orders_{date_str}.csv.gz"
    csv_key = f"{S3_PREFIX}/orders_{date_str}.csv"

    if check_file_exists(s3_client, BUCKET_NAME, gz_key):
        data_key = gz_key
        is_gzip = True
    elif check_file_exists(s3_client, BUCKET_NAME, csv_key):
        data_key = csv_key
        is_gzip = False
    else:
        print(f"❌ No data file found for {date_str}")
        print(f"   Checked: s3://{BUCKET_NAME}/{gz_key}")
        print(f"   Checked: s3://{BUCKET_NAME}/{csv_key}")
        return None, False

    manifest = {
        "entries": [
            {
                "url": f"s3://{BUCKET_NAME}/{data_key}",
                "mandatory": True
            }
        ]
    }

    manifest_key = f"{MANIFEST_PREFIX}/orders_load_{date_str}_manifest.json"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
        ServerSideEncryption="AES256",
        Metadata={
            "load-date": date_str,
            "load-type": "daily-incremental",
            "generated-at": datetime.now().isoformat()
        }
    )

    manifest_url = f"s3://{BUCKET_NAME}/{manifest_key}"
    print(f"✅ Daily manifest created: {manifest_url}")
    print(f"   Data file: s3://{BUCKET_NAME}/{data_key}")

    return manifest_url, is_gzip


def check_already_loaded(cursor, target_date):
    """
    Prevent duplicate loading by checking if date already exists
    
    SIMPLE IDEMPOTENCY CHECK:
    → Query target table for rows with this date
    → If rows exist → skip loading (already done)
    → If no rows → proceed with load
    
    NOTE: For production CDC merge patterns → see Project 65
    """
    cursor.execute(
        "SELECT COUNT(*) FROM public.orders WHERE order_date = %s;",
        (target_date.strftime("%Y-%m-%d"),)
    )
    count = cursor.fetchone()[0]
    return count > 0


def execute_daily_copy(target_date=None):
    """
    Load single day's orders into Redshift
    
    FLOW:
    1. Determine target date (default: yesterday)
    2. Check if already loaded (idempotency)
    3. Verify source file exists in S3
    4. Create manifest for that single file
    5. Execute COPY
    6. Validate loaded data
    """
    if target_date is None:
        target_date = datetime.now() - timedelta(days=1)

    date_str = target_date.strftime("%Y-%m-%d")
    print("=" * 60)
    print(f"🚀 DAILY INCREMENTAL LOAD — {date_str}")
    print("=" * 60)

    s3_client = boto3.client("s3", region_name=REGION)
    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        # --- STEP 1: IDEMPOTENCY CHECK ---
        if check_already_loaded(cursor, target_date):
            print(f"\n⏭️  Data for {date_str} already exists in Redshift")
            print(f"   Skipping to prevent duplicates")
            print(f"   To force reload: DELETE FROM orders WHERE order_date = '{date_str}'")
            return

        # --- STEP 2: CREATE MANIFEST ---
        manifest_url, is_gzip = create_daily_manifest(s3_client, target_date)
        if manifest_url is None:
            print(f"\n❌ Cannot proceed — no source file for {date_str}")
            return

        # --- STEP 3: PRE-LOAD COUNT ---
        cursor.execute("SELECT COUNT(*) FROM public.orders;")
        rows_before = cursor.fetchone()[0]
        print(f"\n📊 Rows before load: {rows_before:,}")

        # --- STEP 4: EXECUTE COPY ---
        gzip_clause = "GZIP" if is_gzip else ""
        
        copy_sql = f"""
            COPY public.orders
            FROM '{manifest_url}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            MANIFEST
            CSV
            IGNOREHEADER 1
            {gzip_clause}
            REGION '{REGION}'
            DATEFORMAT 'auto'
            TIMEFORMAT 'auto'
            MAXERROR 0
            COMPUPDATE OFF
            STATUPDATE ON;
        """

        print(f"\n⏳ Loading {date_str} data...")
        start_time = time.time()
        cursor.execute(copy_sql)
        elapsed = round(time.time() - start_time, 2)

        # --- STEP 5: POST-LOAD VALIDATION ---
        cursor.execute("SELECT COUNT(*) FROM public.orders;")
        rows_after = cursor.fetchone()[0]
        rows_loaded = rows_after - rows_before

        print(f"\n✅ Daily load completed!")
        print(f"   Date loaded:   {date_str}")
        print(f"   Rows added:    {rows_loaded:,}")
        print(f"   Total rows:    {rows_after:,}")
        print(f"   Duration:      {elapsed} seconds")

        # --- STEP 6: DATE-SPECIFIC VALIDATION ---
        cursor.execute("""
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT customer_id) as customers,
                ROUND(SUM(total_amount), 2) as revenue,
                ROUND(AVG(total_amount), 2) as avg_order
            FROM public.orders
            WHERE order_date = %s;
        """, (date_str,))

        stats = cursor.fetchone()
        print(f"\n📊 Validation for {date_str}:")
        print(f"   Rows:          {stats[0]:,}")
        print(f"   Customers:     {stats[1]:,}")
        print(f"   Revenue:       ${stats[2]:,.2f}")
        print(f"   Avg order:     ${stats[3]:,.2f}")

        # --- STEP 7: CHECK ERRORS ---
        cursor.execute("""
            SELECT COUNT(*) FROM stl_load_errors 
            WHERE query = pg_last_copy_id();
        """)
        error_count = cursor.fetchone()[0]
        if error_count > 0:
            print(f"\n⚠️  {error_count} load errors detected!")
            print(f"   Run: SELECT * FROM stl_load_errors WHERE query = pg_last_copy_id();")
        else:
            print(f"\n✅ Zero load errors")

    except psycopg2.Error as e:
        print(f"\n❌ Load failed: {e}")

        # --- DETAILED ERROR DIAGNOSIS ---
        try:
            cursor.execute("""
                SELECT TRIM(filename), line_number, 
                       TRIM(colname), TRIM(err_reason)
                FROM stl_load_errors
                ORDER BY starttime DESC LIMIT 10;
            """)
            errors = cursor.fetchall()
            if errors:
                print(f"\n🔍 Error details:")
                for err in errors:
                    print(f"   File: {err[0]}, Line: {err[1]}")
                    print(f"   Column: {err[2]}, Reason: {err[3]}")
        except Exception:
            pass
        raise

    finally:
        cursor.close()
        conn.close()


def backfill_date_range(start_date, end_date):
    """
    Load multiple days in sequence
    
    USE CASE: initial catchup or filling gaps after outage
    
    WHY sequential (not parallel)?
    → Each COPY consumes cluster resources
    → Parallel COPYs compete for I/O
    → Sequential with idempotency check = safe to re-run
    """
    print(f"\n📅 BACKFILL: {start_date} → {end_date}")
    print("=" * 60)

    current = start_date
    loaded = 0
    skipped = 0
    failed = 0

    while current <= end_date:
        try:
            execute_daily_copy(current)
            loaded += 1
        except Exception as e:
            print(f"⚠️  Failed for {current}: {e}")
            failed += 1
        
        current += timedelta(days=1)
        print()

    print("=" * 60)
    print(f"📊 BACKFILL SUMMARY")
    print(f"   Loaded: {loaded} days")
    print(f"   Failed: {failed} days")
    print("=" * 60)


if __name__ == "__main__":
    # Option 1: Load yesterday (default daily run)
    execute_daily_copy()

    # Option 2: Load specific date
    # execute_daily_copy(datetime(2025, 1, 15))

    # Option 3: Backfill a range
    # backfill_date_range(
    #     datetime(2025, 1, 1),
    #     datetime(2025, 1, 30)
    # )
