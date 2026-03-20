# file: 02_unload_finance_report.py
# Finance team needs: monthly revenue summary as single CSV in S3
# They open it in Excel — must be ONE file with headers

import psycopg2
import boto3
import time
from datetime import datetime

# --- CONFIGURATION ---
REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "YourSecurePassword123!"
REDSHIFT_IAM_ROLE = "arn:aws:iam::123456789012:role/RedshiftS3ReadRole"

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def get_connection():
    conn = psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB, user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    conn.autocommit = True
    return conn


def unload_finance_monthly_report(year, month):
    """
    UNLOAD aggregated monthly revenue to single CSV
    
    KEY DECISIONS:
    → PARALLEL OFF: Finance needs ONE file for Excel
    → HEADER: column names in first row
    → CSV: universal format
    → ALLOWOVERWRITE: re-running for same month replaces old file
    → No GZIP: Excel can't open .gz files natively
    """
    conn = get_connection()
    cursor = conn.cursor()

    month_str = f"{year}-{month:02d}"
    s3_prefix = f"s3://{BUCKET_NAME}/exports/finance/monthly_revenue/{month_str}/"

    # --- THE QUERY ---
    # Aggregated summary — small result set (30 rows max for daily)
    inner_query = f"""
        SELECT 
            order_date,
            COUNT(*) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(quantity) as total_units_sold,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_order_value,
            ROUND(MIN(total_amount), 2) as min_order_value,
            ROUND(MAX(total_amount), 2) as max_order_value,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_orders,
            SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) as cancelled_orders,
            SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) as refunded_orders
        FROM public.orders
        WHERE order_date >= '{year}-{month:02d}-01'
          AND order_date < '{year}-{month:02d}-01'::date + INTERVAL '1 month'
        GROUP BY order_date
        ORDER BY order_date
    """

    # --- UNLOAD COMMAND ---
    unload_sql = f"""
        UNLOAD ('{inner_query.replace(chr(39), chr(39)+chr(39))}')
        TO '{s3_prefix}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        CSV
        HEADER
        PARALLEL OFF
        ALLOWOVERWRITE
        REGION '{REGION}';
    """

    print(f"📊 FINANCE REPORT — {month_str}")
    print(f"   Destination: {s3_prefix}")
    print(f"   Format: CSV with headers, single file")

    start_time = time.time()
    cursor.execute(unload_sql)
    elapsed = round(time.time() - start_time, 2)

    # --- VERIFY OUTPUT ---
    verify_unload_output(cursor, s3_prefix)

    print(f"\n✅ Finance report exported in {elapsed}s")
    print(f"   Finance team can download from: {s3_prefix}000")

    cursor.close()
    conn.close()


def verify_unload_output(cursor, s3_prefix):
    """Check UNLOAD log for output details"""
    cursor.execute("""
        SELECT 
            TRIM(path) as file_path,
            line_count,
            ROUND(transfer_size / 1024.0, 2) as size_kb
        FROM stl_unload_log
        WHERE query = pg_last_query_id()
        ORDER BY path;
    """)

    files = cursor.fetchall()
    print(f"\n📁 Output files:")
    for f in files:
        print(f"   {f[0]} → {f[1]} rows, {f[2]} KB")


if __name__ == "__main__":
    unload_finance_monthly_report(2025, 1)