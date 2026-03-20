# file: 20_01_export_mariadb_to_s3.py
# Purpose: Export MariaDB tables to CSV files in S3
# Dependency: MariaDB with data (Project 19), S3 bucket (Project 1)

import boto3
import pymysql
import pymysql.cursors
import csv
import os
import gzip
import hashlib
import time
from datetime import datetime, date
from decimal import Decimal

# ── CONFIGURATION ──
REGION = "us-east-2"
EXPORT_BUCKET = "quickcart-raw-data-prod"
EXPORT_PREFIX = "mariadb_exports"
TEMP_DIR = "/tmp/mariadb_exports"
FETCH_BATCH_SIZE = 5000  # Rows fetched from DB per batch
COMPRESS = False  # Set True for gzip compression

DB_CONFIG = {
    "host": os.environ.get("MARIADB_HOST", "localhost"),
    "port": int(os.environ.get("MARIADB_PORT", 3306)),
    "user": os.environ.get("MARIADB_USER", "quickcart_etl"),
    "password": os.environ.get("MARIADB_PASSWORD", "etl_password_123"),
    "database": "quickcart",
    "charset": "utf8mb4",
    "autocommit": True
}


# ── EXPORT CONFIGURATIONS ──
# Each export defines: query, columns, S3 path pattern, description
EXPORT_CONFIGS = {
    "orders_full": {
        "description": "Full daily orders export",
        "query": """
            SELECT order_id, customer_id, product_id,
                   quantity, unit_price, total_amount,
                   status, order_date
            FROM orders
            WHERE order_date = %s
            ORDER BY order_id
        """,
        "params_fn": lambda d: (d,),  # date parameter
        "columns": [
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "status", "order_date"
        ],
        "s3_key_fn": lambda d: (
            f"{EXPORT_PREFIX}/full/orders/"
            f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
            f"orders.csv"
        ),
        "count_query": """
            SELECT COUNT(*) FROM orders WHERE order_date = %s
        """,
    },
    "customers_full": {
        "description": "Full customers export (all customers)",
        "query": """
            SELECT customer_id, name, email,
                   city, state, tier, signup_date
            FROM customers
            ORDER BY customer_id
        """,
        "params_fn": lambda d: (),  # no parameters
        "columns": [
            "customer_id", "name", "email",
            "city", "state", "tier", "signup_date"
        ],
        "s3_key_fn": lambda d: (
            f"{EXPORT_PREFIX}/full/customers/"
            f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
            f"customers.csv"
        ),
        "count_query": "SELECT COUNT(*) FROM customers",
    },
    "products_full": {
        "description": "Full products export (all active)",
        "query": """
            SELECT product_id, name, category,
                   price, stock_quantity, is_active
            FROM products
            ORDER BY product_id
        """,
        "params_fn": lambda d: (),
        "columns": [
            "product_id", "name", "category",
            "price", "stock_quantity", "is_active"
        ],
        "s3_key_fn": lambda d: (
            f"{EXPORT_PREFIX}/full/products/"
            f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
            f"products.csv"
        ),
        "count_query": "SELECT COUNT(*) FROM products",
    },
    "completed_orders": {
        "description": "Filtered: only completed orders (for Finance)",
        "query": """
            SELECT order_id, customer_id, product_id,
                   quantity, unit_price, total_amount,
                   order_date
            FROM orders
            WHERE status = 'completed' AND order_date = %s
            ORDER BY order_id
        """,
        "params_fn": lambda d: (d,),
        "columns": [
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "order_date"
        ],
        "s3_key_fn": lambda d: (
            f"{EXPORT_PREFIX}/filtered/completed_orders/"
            f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
            f"completed_orders.csv"
        ),
        "count_query": """
            SELECT COUNT(*) FROM orders
            WHERE status = 'completed' AND order_date = %s
        """,
    },
    "daily_revenue": {
        "description": "Aggregated: daily revenue by category (for Analytics)",
        "query": """
            SELECT
                o.order_date,
                p.category,
                COUNT(*) as order_count,
                SUM(o.quantity) as total_units,
                ROUND(SUM(o.total_amount), 2) as total_revenue,
                ROUND(AVG(o.total_amount), 2) as avg_order_value
            FROM orders o
            INNER JOIN products p ON o.product_id = p.product_id
            WHERE o.order_date = %s
            GROUP BY o.order_date, p.category
            ORDER BY total_revenue DESC
        """,
        "params_fn": lambda d: (d,),
        "columns": [
            "order_date", "category", "order_count",
            "total_units", "total_revenue", "avg_order_value"
        ],
        "s3_key_fn": lambda d: (
            f"{EXPORT_PREFIX}/aggregated/daily_revenue/"
            f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
            f"daily_revenue.csv"
        ),
        "count_query": None,  # Aggregated — no simple count comparison
    },
}


def format_value(value):
    """
    Convert Python/MariaDB types to clean CSV-safe strings
    
    Handles all types from PHASE 2 of internal mechanics:
    → Decimal → string with proper precision
    → date/datetime → ISO format string
    → None → empty string
    → bool (0/1) → "true"/"false"
    → bytes → skip (binary data)
    """
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, bytes):
        return ""  # Skip binary data
    if isinstance(value, int):
        # Handle boolean stored as TINYINT(1)
        # We check column context in the caller, but fallback here
        return str(value)
    if isinstance(value, float):
        # Avoid floating point artifacts: 29.990000000001
        if value == int(value):
            return str(int(value))
        return f"{value:.2f}"
    return str(value)


def calculate_file_md5(filepath):
    """Calculate MD5 hash of file for integrity verification"""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def export_query_to_csv(cursor, query, params, columns, filepath, compress=False):
    """
    Execute query and write results to CSV file
    
    Uses server-side cursor (SSCursor) for memory efficiency:
    → Fetches FETCH_BATCH_SIZE rows at a time
    → Constant memory regardless of result set size
    → Safe for 10M+ row exports
    
    Returns: (row_count, file_size_bytes)
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # ── EXECUTE QUERY ──
    cursor.execute(query, params)

    # ── OPEN FILE (compressed or plain) ──
    actual_path = filepath + ".gz" if compress else filepath

    if compress:
        file_handle = gzip.open(actual_path, "wt", encoding="utf-8", newline="")
    else:
        file_handle = open(actual_path, "w", encoding="utf-8", newline="")

    row_count = 0

    try:
        writer = csv.writer(
            file_handle,
            quoting=csv.QUOTE_NONNUMERIC,  # Quote strings, not numbers
            lineterminator="\n"
        )

        # ── WRITE HEADER ──
        writer.writerow(columns)

        # ── FETCH AND WRITE IN BATCHES ──
        while True:
            rows = cursor.fetchmany(FETCH_BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                formatted_row = [format_value(val) for val in row]
                writer.writerow(formatted_row)
                row_count += 1

            # Progress for large exports
            if row_count % 50000 == 0 and row_count > 0:
                print(f"      ⏳ Written {row_count:,} rows...")

    finally:
        file_handle.close()

    file_size = os.path.getsize(actual_path)
    return row_count, file_size, actual_path


def upload_to_s3(local_path, bucket, s3_key, metadata=None):
    """
    Upload exported CSV to S3 with metadata
    Reuses pattern from Project 1 (upload with verification)
    """
    s3_client = boto3.client("s3", region_name=REGION)

    content_type = "text/csv"
    if local_path.endswith(".gz"):
        content_type = "application/gzip"

    extra_args = {
        "ContentType": content_type,
        "ServerSideEncryption": "AES256",
        "Metadata": metadata or {}
    }

    s3_client.upload_file(
        Filename=local_path,
        Bucket=bucket,
        Key=s3_key,
        ExtraArgs=extra_args
    )

    # Verify upload (Project 1 pattern)
    response = s3_client.head_object(Bucket=bucket, Key=s3_key)
    remote_size = response["ContentLength"]
    local_size = os.path.getsize(local_path)

    if remote_size != local_size:
        raise RuntimeError(
            f"Upload size mismatch: local={local_size}, remote={remote_size}"
        )

    return remote_size


def run_single_export(export_name, export_date):
    """
    Run a single export configuration
    
    Parameters:
    → export_name: key in EXPORT_CONFIGS
    → export_date: datetime.date object
    
    Returns: dict with export results
    """
    config = EXPORT_CONFIGS[export_name]
    start_time = time.time()

    print(f"\n   {'─' * 60}")
    print(f"   📤 EXPORT: {export_name}")
    print(f"   Description: {config['description']}")
    print(f"   Date: {export_date}")

    conn = None
    local_path = None

    try:
        # ── CONNECT WITH SERVER-SIDE CURSOR ──
        conn = pymysql.connect(
            **DB_CONFIG,
            cursorclass=pymysql.cursors.SSCursor  # Server-side cursor
        )
        cursor = conn.cursor()

        # ── GET EXPECTED ROW COUNT (if available) ──
        expected_count = None
        if config.get("count_query"):
            count_params = config["params_fn"](export_date)
            # Use regular cursor for count (SSCursor can't reuse easily)
            count_conn = pymysql.connect(**DB_CONFIG)
            count_cursor = count_conn.cursor()
            count_cursor.execute(config["count_query"], count_params)
            expected_count = count_cursor.fetchone()[0]
            count_cursor.close()
            count_conn.close()
            print(f"   Expected rows: {expected_count:,}")

            if expected_count == 0:
                print(f"   ⏭️  SKIPPED — no data for this date")
                return {
                    "success": True, "rows_exported": 0,
                    "skipped": True, "reason": "no data"
                }

        # ── BUILD PATHS ──
        s3_key = config["s3_key_fn"](export_date)
        if COMPRESS:
            s3_key += ".gz"

        local_filename = os.path.basename(s3_key)
        local_dir = os.path.join(TEMP_DIR, export_name)
        local_filepath = os.path.join(local_dir, local_filename.replace(".gz", ""))

        print(f"   S3 destination: s3://{EXPORT_BUCKET}/{s3_key}")

        # ── EXPORT TO LOCAL CSV ──
        print(f"   Writing CSV...")
        query_params = config["params_fn"](export_date)
        row_count, file_size, actual_path = export_query_to_csv(
            cursor=cursor,
            query=config["query"],
            params=query_params,
            columns=config["columns"],
            filepath=local_filepath,
            compress=COMPRESS
        )
        local_path = actual_path

        size_mb = round(file_size / (1024 * 1024), 2)
        print(f"   ✅ Written: {row_count:,} rows, {size_mb} MB")

        # ── VERIFY ROW COUNT ──
        if expected_count is not None:
            if row_count == expected_count:
                print(f"   ✅ Row count match: {row_count:,} = {expected_count:,}")
            else:
                print(f"   ⚠️  Row count mismatch: exported={row_count:,}, "
                      f"expected={expected_count:,}")
                print(f"   → Possible cause: concurrent writes during export")

        if row_count == 0:
            print(f"   ⏭️  No rows exported — skipping upload")
            return {
                "success": True, "rows_exported": 0,
                "skipped": True, "reason": "query returned 0 rows"
            }

        # ── CALCULATE FILE HASH ──
        file_md5 = calculate_file_md5(actual_path)

        # ── UPLOAD TO S3 ──
        print(f"   Uploading to S3...")
        s3_metadata = {
            "source-database": "quickcart",
            "source-table": export_name,
            "export-date": str(export_date),
            "row-count": str(row_count),
            "file-md5": file_md5,
            "exported-by": "mariadb-export-script",
            "export-timestamp": datetime.now().isoformat()
        }

        remote_size = upload_to_s3(
            local_path=actual_path,
            bucket=EXPORT_BUCKET,
            s3_key=s3_key,
            metadata=s3_metadata
        )

        duration = time.time() - start_time
        print(f"   ✅ Uploaded: s3://{EXPORT_BUCKET}/{s3_key}")
        print(f"   ⏱️  Duration: {duration:.1f}s | "
              f"Speed: {row_count / max(duration, 0.1):,.0f} rows/sec")

        return {
            "success": True,
            "rows_exported": row_count,
            "file_size": file_size,
            "s3_key": s3_key,
            "duration": duration,
            "skipped": False
        }

    except Exception as e:
        print(f"   ❌ Export failed: {e}")
        return {"success": False, "error": str(e)}

    finally:
        if conn:
            conn.close()
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            print(f"   🗑️  Temp file cleaned up")


def run_all_exports(export_date=None):
    """
    Run all configured exports for a given date
    
    Parameters:
    → export_date: datetime.date. If None, uses today.
    """
    if export_date is None:
        export_date = date.today()

    if isinstance(export_date, str):
        export_date = datetime.strptime(export_date, "%Y-%m-%d").date()

    print("=" * 70)
    print(f"🚀 MARIADB → S3 EXPORT — All Exports for {export_date}")
    print(f"   Time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Bucket: {EXPORT_BUCKET}")
    print(f"   Prefix: {EXPORT_PREFIX}")
    print("=" * 70)

    results = {}
    total_rows = 0
    total_size = 0
    all_success = True

    for export_name in EXPORT_CONFIGS:
        result = run_single_export(export_name, export_date)
        results[export_name] = result

        if result["success"]:
            total_rows += result.get("rows_exported", 0)
            total_size += result.get("file_size", 0)
        else:
            all_success = False

    # ── SUMMARY ──
    print("\n" + "=" * 70)
    print("📊 EXPORT SUMMARY")
    print("=" * 70)

    for name, result in results.items():
        status = "✅" if result["success"] else "❌"
        rows = result.get("rows_exported", 0)
        skipped = result.get("skipped", False)
        duration = result.get("duration", 0)

        if skipped:
            reason = result.get("reason", "unknown")
            print(f"   {status} {name:25s}: SKIPPED ({reason})")
        elif result["success"]:
            size_kb = result.get("file_size", 0) / 1024
            print(f"   {status} {name:25s}: {rows:>8,} rows, "
                  f"{size_kb:>8.0f} KB, {duration:.1f}s")
        else:
            print(f"   {status} {name:25s}: FAILED — "
                  f"{result.get('error', 'unknown')[:40]}")

    total_mb = total_size / (1024 * 1024)
    print(f"\n   TOTAL: {total_rows:,} rows, {total_mb:.2f} MB")
    print(f"   STATUS: {'✅ ALL SUCCEEDED' if all_success else '❌ SOME FAILED'}")

    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Export specific date: python 20_01_... 2025-01-15
        date_arg = sys.argv[1]
        run_all_exports(export_date=date_arg)
    else:
        run_all_exports()