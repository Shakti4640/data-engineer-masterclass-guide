# file: 05_execute_copy.py
# Connects to Redshift and runs COPY command
# Full production-grade loading with validation

import boto3
import psycopg2
import json
import time
from datetime import datetime

# --- REDSHIFT CONNECTION ---
REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "YourSecurePassword123!"  # Use Secrets Manager in prod (Project 70)

# --- S3 / IAM ---
BUCKET_NAME = "quickcart-raw-data-prod"
MANIFEST_KEY = "manifests/orders_full_load_manifest.json"
REDSHIFT_IAM_ROLE = "arn:aws:iam::123456789012:role/RedshiftS3ReadRole"
REGION = "us-east-2"


def get_connection():
    """Establish Redshift connection via psycopg2"""
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )


def get_row_count(cursor, table_name):
    """Get current row count of target table"""
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    return cursor.fetchone()[0]


def execute_copy_with_manifest():
    """
    Execute COPY command using manifest file
    
    COPY SYNTAX BREAKDOWN:
    ──────────────────────
    COPY {table}
    FROM '{manifest_s3_url}'         ← S3 path to manifest JSON
    IAM_ROLE '{role_arn}'            ← Redshift uses this role to access S3
    MANIFEST                         ← Tells COPY the FROM path is a manifest
    CSV                              ← Input format is CSV
    IGNOREHEADER 1                   ← Skip first row (column headers)
    GZIP                             ← Files are GZIP compressed
    REGION '{region}'                ← S3 bucket region (if different from cluster)
    DATEFORMAT 'auto'                ← Auto-detect date formats
    MAXERROR 0                       ← Fail on first error (strictest)
    COMPUPDATE OFF                   ← Skip encoding analysis (faster reload)
    STATUPDATE ON                    ← Update statistics after load
    TIMEFORMAT 'auto';               ← Auto-detect time formats
    """

    conn = get_connection()
    conn.autocommit = True  # COPY must run outside explicit transaction block
    cursor = conn.cursor()

    manifest_url = f"s3://{BUCKET_NAME}/{MANIFEST_KEY}"

    try:
        # --- PRE-LOAD: Check current state ---
        rows_before = get_row_count(cursor, "public.orders")
        print(f"📊 Rows before COPY: {rows_before:,}")

        # --- OPTIONAL: Truncate for full reload ---
        if rows_before > 0:
            print(f"🗑️  Truncating existing data (full reload mode)...")
            cursor.execute("TRUNCATE TABLE public.orders;")
            print(f"   ✅ Table truncated")

        # --- EXECUTE COPY ---
        copy_sql = f"""
            COPY public.orders
            FROM '{manifest_url}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            MANIFEST
            CSV
            IGNOREHEADER 1
            GZIP
            REGION '{REGION}'
            DATEFORMAT 'auto'
            TIMEFORMAT 'auto'
            MAXERROR 0
            COMPUPDATE OFF
            STATUPDATE ON;
        """

        print(f"\n🚀 Executing COPY command...")
        print(f"   Source: {manifest_url}")
        print(f"   Target: public.orders")

        start_time = time.time()
        cursor.execute(copy_sql)
        elapsed = round(time.time() - start_time, 2)

        # --- POST-LOAD: Validate ---
        rows_after = get_row_count(cursor, "public.orders")
        rows_loaded = rows_after - 0  # After truncate, before was 0

        print(f"\n✅ COPY completed successfully!")
        print(f"   Rows loaded: {rows_loaded:,}")
        print(f"   Duration: {elapsed} seconds")
        print(f"   Throughput: {round(rows_loaded / max(elapsed, 0.01)):,} rows/sec")

        # --- CHECK FOR LOAD ERRORS ---
        check_load_errors(cursor)

        # --- CHECK FILE-LEVEL DETAILS ---
        check_load_files(cursor)

        # --- SAMPLE DATA VALIDATION ---
        validate_sample_data(cursor)

    except psycopg2.Error as e:
        print(f"\n❌ COPY failed: {e}")
        print(f"\n🔍 Checking error details...")
        check_load_errors(cursor)
        raise

    finally:
        cursor.close()
        conn.close()


def check_load_errors(cursor):
    """Query STL_LOAD_ERRORS for any row-level failures"""
    print(f"\n🔍 Checking STL_LOAD_ERRORS...")

    cursor.execute("""
        SELECT 
            filename,
            line_number,
            colname,
            type,
            raw_field_value,
            err_reason
        FROM stl_load_errors
        WHERE query = pg_last_copy_id()
        ORDER BY starttime DESC
        LIMIT 20;
    """)

    errors = cursor.fetchall()
    if not errors:
        print(f"   ✅ No load errors found")
    else:
        print(f"   ⚠️  {len(errors)} error(s) found:")
        for err in errors:
            print(f"   File: {err[0]}")
            print(f"   Line: {err[1]}, Column: {err[2]}, Type: {err[3]}")
            print(f"   Raw Value: {err[4]}")
            print(f"   Reason: {err[5]}")
            print()


def check_load_files(cursor):
    """Query STL_LOAD_COMMITS to see per-file load details"""
    print(f"\n📁 Files loaded (STL_LOAD_COMMITS):")

    cursor.execute("""
        SELECT 
            TRIM(filename) as filename,
            lines_scanned,
            ROUND(bytes_scanned / 1024.0 / 1024.0, 2) as mb_scanned
        FROM stl_load_commits
        WHERE query = pg_last_copy_id()
        ORDER BY filename;
    """)

    files = cursor.fetchall()
    total_lines = 0
    total_mb = 0

    for f in files:
        print(f"   {f[0]} → {f[1]:,} rows, {f[2]} MB")
        total_lines += f[1]
        total_mb += f[2]

    print(f"   ────────────────────────────────────")
    print(f"   Total: {total_lines:,} rows, {total_mb} MB across {len(files)} files")


def validate_sample_data(cursor):
    """Quick sanity check on loaded data"""
    print(f"\n📋 Sample data (first 5 rows):")

    cursor.execute("""
        SELECT order_id, customer_id, total_amount, status, order_date
        FROM public.orders
        ORDER BY order_id
        LIMIT 5;
    """)

    rows = cursor.fetchall()
    print(f"   {'ORDER_ID':<25} {'CUSTOMER_ID':<15} {'AMOUNT':>10} {'STATUS':<12} {'DATE'}")
    print(f"   {'─'*25} {'─'*15} {'─'*10} {'─'*12} {'─'*10}")
    for row in rows:
        print(f"   {row[0]:<25} {row[1]:<15} {row[2]:>10.2f} {row[3]:<12} {row[4]}")

    # Aggregate validation
    cursor.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            MIN(order_date) as earliest_date,
            MAX(order_date) as latest_date,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_order_value
        FROM public.orders;
    """)

    stats = cursor.fetchone()
    print(f"\n📊 Aggregate validation:")
    print(f"   Total rows:        {stats[0]:,}")
    print(f"   Unique customers:  {stats[1]:,}")
    print(f"   Unique products:   {stats[2]:,}")
    print(f"   Date range:        {stats[3]} → {stats[4]}")
    print(f"   Total revenue:     ${stats[5]:,.2f}")
    print(f"   Avg order value:   ${stats[6]:,.2f}")


if __name__ == "__main__":
    execute_copy_with_manifest()