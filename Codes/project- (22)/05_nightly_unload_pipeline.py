# file: 05_nightly_unload_pipeline.py
# Runs every night: exports yesterday's orders to S3 for all consumers
# Combines all three use cases into one orchestrated pipeline

import psycopg2
import boto3
import time
import json
from datetime import datetime, timedelta

# Same connection config
REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD [REDACTED:PASSWORD]23!"
REDSHIFT_IAM_ROLE = "arn:aws:iam::123456789012:role/RedshiftS3ReadRole"
BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"

# SNS topic for notifications (from Project 12)
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:123456789012:data-pipeline-alerts"


def get_connection():
    conn = psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB, user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    conn.autocommit = True
    return conn


def send_notification(subject, message):
    """Send SNS notification — pattern from Project 12"""
    try:
        sns_client = boto3.client("sns", region_name=REGION)
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
    except Exception as e:
        print(f"⚠️  SNS notification failed: {e}")


def escape_quotes(sql):
    """Escape single quotes for UNLOAD inner query"""
    return sql.replace("'", "''")


def unload_with_tracking(cursor, description, unload_sql):
    """Execute UNLOAD with timing and logging"""
    print(f"\n{'─' * 60}")
    print(f"📤 {description}")

    start_time = time.time()
    cursor.execute(unload_sql)
    elapsed = round(time.time() - start_time, 2)

    # Get output details
    cursor.execute("""
        SELECT 
            COUNT(*) as file_count,
            SUM(line_count) as total_rows,
            ROUND(SUM(transfer_size) / 1024.0 / 1024.0, 2) as total_mb
        FROM stl_unload_log
        WHERE query = pg_last_query_id();
    """)
    stats = cursor.fetchone()

    result = {
        "description": description,
        "files": stats[0],
        "rows": stats[1],
        "size_mb": stats[2],
        "duration_sec": elapsed
    }

    print(f"   ✅ {stats[1]:,} rows → {stats[0]} files → {stats[2]} MB in {elapsed}s")
    return result


def run_nightly_export():
    """
    Complete nightly export pipeline
    
    FLOW:
    1. Determine yesterday's date
    2. Export finance daily summary (CSV, single file)
    3. Export partner daily orders (CSV, GZIP, partitioned)
    4. Export data science incremental (Parquet)
    5. Send success/failure notification
    """
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    date_compact = yesterday.strftime("%Y%m%d")
    month_str = yesterday.strftime("%Y-%m")

    print("=" * 60)
    print(f"🌙 NIGHTLY EXPORT PIPELINE — {date_str}")
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    results = []
    errors = []

    # ─────────────────────────────────────────────────
    # EXPORT 1: Finance Daily Summary
    # ─────────────────────────────────────────────────
    try:
        finance_query = f"""
            SELECT 
                order_date,
                COUNT(*) as total_orders,
                COUNT(DISTINCT customer_id) as unique_customers,
                ROUND(SUM(total_amount), 2) as revenue,
                ROUND(AVG(total_amount), 2) as avg_order
            FROM public.orders
            WHERE order_date = ''{date_str}''
            GROUP BY order_date
        """

        finance_prefix = f"s3://{BUCKET_NAME}/exports/finance/daily_summary/{date_compact}/"

        finance_sql = f"""
            UNLOAD ('{finance_query}')
            TO '{finance_prefix}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            CSV
            HEADER
            PARALLEL OFF
            ALLOWOVERWRITE
            REGION '{REGION}';
        """

        result = unload_with_tracking(cursor, f"Finance Daily Summary ({date_str})", finance_sql)
        results.append(result)

    except Exception as e:
        print(f"   ❌ Finance export failed: {e}")
        errors.append(f"Finance: {e}")

    # ─────────────────────────────────────────────────
    # EXPORT 2: Partner Daily Orders
    # ─────────────────────────────────────────────────
    try:
        partner_query = f"""
            SELECT 
                order_id,
                customer_id,
                product_id,
                quantity,
                total_amount,
                status,
                order_date
            FROM public.orders
            WHERE order_date = ''{date_str}''
              AND status IN (''completed'', ''shipped'', ''pending'')
        """

        partner_prefix = f"s3://{BUCKET_NAME}/exports/partner_shipfast/daily_orders/{date_compact}/"

        partner_sql = f"""
            UNLOAD ('{partner_query}')
            TO '{partner_prefix}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            CSV
            HEADER
            GZIP
            ALLOWOVERWRITE
            REGION '{REGION}';
        """

        result = unload_with_tracking(cursor, f"Partner Daily Orders ({date_str})", partner_sql)
        results.append(result)

    except Exception as e:
        print(f"   ❌ Partner export failed: {e}")
        errors.append(f"Partner: {e}")

    # ─────────────────────────────────────────────────
    # EXPORT 3: Data Science Daily Parquet
    # ─────────────────────────────────────────────────
    try:
        ds_query = f"""
            SELECT 
                order_id,
                customer_id,
                product_id,
                quantity,
                unit_price,
                total_amount,
                status,
                order_date,
                EXTRACT(DOW FROM order_date) as day_of_week,
                CASE WHEN status = ''completed'' THEN 1 ELSE 0 END as is_completed,
                CASE WHEN total_amount > 500 THEN 1 ELSE 0 END as is_high_value
            FROM public.orders
            WHERE order_date = ''{date_str}''
        """

        ds_prefix = f"s3://{BUCKET_NAME}/exports/data_science/daily_orders/{date_compact}/"

        ds_sql = f"""
            UNLOAD ('{ds_query}')
            TO '{ds_prefix}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            FORMAT AS PARQUET
            ALLOWOVERWRITE
            REGION '{REGION}';
        """

        result = unload_with_tracking(cursor, f"Data Science Parquet ({date_str})", ds_sql)
        results.append(result)

    except Exception as e:
        print(f"   ❌ Data Science export failed: {e}")
        errors.append(f"DataScience: {e}")

    # ─────────────────────────────────────────────────
    # SUMMARY + NOTIFICATION
    # ─────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📊 NIGHTLY EXPORT SUMMARY")
    print("=" * 60)

    total_rows = 0
    total_mb = 0
    total_time = 0

    for r in results:
        print(f"   ✅ {r['description']}")
        print(f"      {r['rows']:,} rows → {r['files']} files → {r['size_mb']} MB ({r['duration_sec']}s)")
        total_rows += r['rows'] or 0
        total_mb += r['size_mb'] or 0
        total_time += r['duration_sec']

    if errors:
        for e in errors:
            print(f"   ❌ FAILED: {e}")

    print(f"\n   Total: {total_rows:,} rows, {total_mb} MB, {total_time}s")
    print("=" * 60)

    # --- SEND NOTIFICATION ---
    if errors:
        send_notification(
            subject=f"❌ NIGHTLY EXPORT PARTIAL FAILURE — {date_str}",
            message=json.dumps({
                "date": date_str,
                "succeeded": len(results),
                "failed": len(errors),
                "errors": errors
            }, indent=2)
        )
    else:
        send_notification(
            subject=f"✅ NIGHTLY EXPORT SUCCESS — {date_str}",
            message=json.dumps({
                "date": date_str,
                "exports": len(results),
                "total_rows": total_rows,
                "total_mb": total_mb,
                "total_seconds": total_time,
                "details": [
                    {
                        "export": r["description"],
                        "rows": r["rows"],
                        "files": r["files"],
                        "size_mb": r["size_mb"],
                        "seconds": r["duration_sec"]
                    }
                    for r in results
                ]
            }, indent=2)
        )

    cursor.close()
    conn.close()

    return 0 if not errors else 1


if __name__ == "__main__":
    exit_code = run_nightly_export()
    exit(exit_code)