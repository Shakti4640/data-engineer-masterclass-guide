# file: 19_02_load_csv_to_mariadb.py
# Purpose: Download CSV from S3 → validate → bulk load into MariaDB
# This is the CORE DATA LOADING PATTERN reused in Projects 27, 28, 30

import boto3
import pymysql
import csv
import os
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation

# ── CONFIGURATION ──
REGION = "us-east-2"
SOURCE_BUCKET = "quickcart-raw-data-prod"
DOWNLOAD_DIR = "/tmp/s3_to_mariadb"
BATCH_SIZE = 1000  # Rows per INSERT statement

DB_CONFIG = {
    "host": os.environ.get("MARIADB_HOST", "localhost"),
    "port": int(os.environ.get("MARIADB_PORT", 3306)),
    "user": os.environ.get("MARIADB_USER", "quickcart_etl"),
    "password": os.environ.get("MARIADB_PASSWORD", "etl_password_123"),
    "database": "quickcart",
    "charset": "utf8mb4",
    "autocommit": False  # CRITICAL: we manage transactions explicitly
}

# ── TABLE CONFIGURATIONS ──
# Maps entity type to: table name, column list, expected CSV header, transform function
TABLE_CONFIG = {
    "orders": {
        "table": "orders",
        "columns": [
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "status", "order_date"
        ],
        "expected_header": [
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "status", "order_date"
        ],
        "date_column": "order_date",  # Used for idempotent DELETE
    },
    "customers": {
        "table": "customers",
        "columns": [
            "customer_id", "name", "email",
            "city", "state", "tier", "signup_date"
        ],
        "expected_header": [
            "customer_id", "name", "email",
            "city", "state", "tier", "signup_date"
        ],
        "date_column": None,  # Customers: full replace (small table)
    },
    "products": {
        "table": "products",
        "columns": [
            "product_id", "name", "category",
            "price", "stock_quantity", "is_active"
        ],
        "expected_header": [
            "product_id", "name", "category",
            "price", "stock_quantity", "is_active"
        ],
        "date_column": None,  # Products: full replace (small table)
    }
}


def detect_entity_type(s3_key):
    """
    Determine entity type from S3 key
    Key pattern: daily_exports/{entity}/{filename}
    Example: daily_exports/orders/orders_20250115.csv → "orders"
    """
    parts = s3_key.split("/")
    if len(parts) >= 2:
        entity = parts[-2]  # Second-to-last part
        if entity in TABLE_CONFIG:
            return entity
    # Try from filename
    filename = os.path.basename(s3_key)
    for entity in TABLE_CONFIG:
        if filename.startswith(entity):
            return entity
    return None


def extract_date_from_filename(filename):
    """
    Extract date from filename pattern: entity_YYYYMMDD.csv
    Example: orders_20250115.csv → '2025-01-15'
    """
    basename = os.path.splitext(os.path.basename(filename))[0]
    parts = basename.split("_")
    for part in parts:
        if len(part) == 8 and part.isdigit():
            try:
                date_obj = datetime.strptime(part, "%Y%m%d")
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def download_from_s3(bucket, key):
    """Download file from S3 to local temp directory"""
    s3_client = boto3.client("s3", region_name=REGION)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    filename = os.path.basename(key)
    local_path = os.path.join(DOWNLOAD_DIR, filename)

    print(f"   📥 Downloading: s3://{bucket}/{key}")
    s3_client.download_file(bucket, key, local_path)

    file_size = os.path.getsize(local_path)
    print(f"   ✅ Downloaded: {file_size:,} bytes → {local_path}")
    return local_path, file_size


def validate_csv(local_path, entity_type):
    """
    Validate CSV structure before loading
    Returns: (is_valid, row_count, error_message)
    """
    config = TABLE_CONFIG[entity_type]
    expected_header = config["expected_header"]

    try:
        with open(local_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)

            # ── CHECK HEADER ──
            header = next(reader, None)
            if header is None:
                return False, 0, "File is empty (no header row)"

            # Normalize: strip whitespace, lowercase
            header_clean = [col.strip().lower() for col in header]
            expected_clean = [col.strip().lower() for col in expected_header]

            if header_clean != expected_clean:
                return False, 0, (
                    f"Header mismatch.\n"
                    f"         Expected: {expected_clean}\n"
                    f"         Got:      {header_clean}"
                )

            # ── COUNT DATA ROWS ──
            row_count = sum(1 for _ in reader)

            if row_count == 0:
                return False, 0, "File has header but no data rows"

        return True, row_count, None

    except UnicodeDecodeError as e:
        return False, 0, f"Encoding error: {e}"
    except Exception as e:
        return False, 0, f"Validation error: {e}"


def clean_row(row, entity_type):
    """
    Clean and transform a CSV row before INSERT
    
    Handles:
    → Empty strings → None (NULL in MariaDB)
    → Boolean strings → 1/0
    → Numeric validation
    → Whitespace stripping
    """
    cleaned = []
    config = TABLE_CONFIG[entity_type]
    columns = config["columns"]

    for i, value in enumerate(row):
        col_name = columns[i] if i < len(columns) else f"col_{i}"
        value = value.strip()

        # Empty string → NULL
        if value == "" or value.lower() == "null" or value.lower() == "n/a":
            cleaned.append(None)
            continue

        # Boolean conversion (for products.is_active)
        if col_name == "is_active":
            cleaned.append(1 if value.lower() in ("true", "1", "yes") else 0)
            continue

        # Numeric validation for known numeric columns
        if col_name in ("quantity", "stock_quantity"):
            try:
                cleaned.append(int(value))
            except ValueError:
                cleaned.append(0)  # Default for invalid numbers
            continue

        if col_name in ("unit_price", "total_amount", "price"):
            try:
                cleaned.append(float(value))
            except ValueError:
                cleaned.append(0.0)
            continue

        # Default: keep as string
        cleaned.append(value)

    return tuple(cleaned)


def record_load_start(cursor, filename, table_name, bucket, key, file_size):
    """Insert metadata record at start of load"""
    try:
        cursor.execute("""
            INSERT INTO load_metadata
                (source_file, target_table, s3_bucket, s3_key,
                 file_size_bytes, load_status, started_at)
            VALUES (%s, %s, %s, %s, %s, 'started', NOW())
            ON DUPLICATE KEY UPDATE
                load_status = 'started',
                started_at = NOW(),
                error_message = NULL,
                rows_loaded = 0,
                rows_rejected = 0
        """, (filename, table_name, bucket, key, file_size))
        return True
    except Exception as e:
        print(f"   ⚠️  Could not record load start: {e}")
        return False


def record_load_result(cursor, filename, table_name, status,
                       rows_loaded=0, rows_rejected=0, error_msg=None):
    """Update metadata record with load result"""
    try:
        cursor.execute("""
            UPDATE load_metadata
            SET load_status = %s,
                rows_loaded = %s,
                rows_rejected = %s,
                error_message = %s,
                completed_at = NOW()
            WHERE source_file = %s AND target_table = %s
        """, (status, rows_loaded, rows_rejected, error_msg,
              filename, table_name))
    except Exception as e:
        print(f"   ⚠️  Could not record load result: {e}")


def check_already_loaded(cursor, filename, table_name):
    """Check if file was already successfully loaded (idempotency)"""
    cursor.execute("""
        SELECT load_status, rows_loaded, completed_at
        FROM load_metadata
        WHERE source_file = %s AND target_table = %s
        AND load_status = 'completed'
    """, (filename, table_name))

    result = cursor.fetchone()
    if result:
        return True, result
    return False, None


def load_csv_to_mariadb(bucket, s3_key, force_reload=False):
    """
    MAIN LOADING FUNCTION
    
    Downloads CSV from S3, validates, and bulk-loads into MariaDB
    
    Parameters:
    → bucket: S3 bucket name
    → s3_key: full S3 object key
    → force_reload: if True, reload even if already loaded
    
    Returns:
    → dict with: success, rows_loaded, rows_rejected, duration
    """
    start_time = time.time()
    filename = os.path.basename(s3_key)
    local_path = None

    print("\n" + "=" * 70)
    print(f"📦 LOADING: {filename}")
    print(f"   Source:  s3://{bucket}/{s3_key}")
    print(f"   Time:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # ── DETECT ENTITY TYPE ──
    entity_type = detect_entity_type(s3_key)
    if entity_type is None:
        print(f"   ❌ Cannot determine entity type from key: {s3_key}")
        return {"success": False, "error": "Unknown entity type"}

    config = TABLE_CONFIG[entity_type]
    table_name = config["table"]
    print(f"   Entity:  {entity_type} → table: {table_name}")

    # ── EXTRACT DATE FROM FILENAME ──
    file_date = extract_date_from_filename(filename)
    print(f"   Date:    {file_date or 'N/A'}")

    conn = None
    try:
        # ── DOWNLOAD FROM S3 ──
        local_path, file_size = download_from_s3(bucket, s3_key)

        # ── CONNECT TO MARIADB ──
        print(f"\n   🔌 Connecting to MariaDB...")
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print(f"   ✅ Connected to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

        # ── CHECK IF ALREADY LOADED (idempotency) ──
        already_loaded, prev_result = check_already_loaded(
            cursor, filename, table_name
        )
        if already_loaded and not force_reload:
            print(f"\n   ⏭️  SKIPPED — already loaded successfully")
            print(f"   → Previous load: {prev_result[2]} ({prev_result[1]} rows)")
            print(f"   → Use force_reload=True to re-process")
            record_load_result(cursor, filename, table_name, "skipped")
            conn.commit()
            return {"success": True, "rows_loaded": 0, "skipped": True}

        # ── VALIDATE CSV ──
        print(f"\n   🔍 Validating CSV structure...")
        is_valid, csv_row_count, error_msg = validate_csv(local_path, entity_type)

        if not is_valid:
            print(f"   ❌ Validation failed: {error_msg}")
            record_load_start(cursor, filename, table_name, bucket, s3_key, file_size)
            record_load_result(cursor, filename, table_name, "failed",
                               error_msg=error_msg)
            conn.commit()
            return {"success": False, "error": error_msg}

        print(f"   ✅ Valid: {csv_row_count} data rows, "
              f"{len(config['columns'])} columns")

        # ── RECORD LOAD START ──
        record_load_start(cursor, filename, table_name, bucket, s3_key, file_size)
        conn.commit()  # Commit metadata separately

        # ══════════════════════════════════════════════════
        # BEGIN DATA LOADING TRANSACTION
        # ══════════════════════════════════════════════════
        print(f"\n   🔄 Beginning load transaction...")

        # ── DELETE EXISTING DATA (idempotency) ──
        if config["date_column"] and file_date:
            # For orders: delete only rows matching this file's date
            delete_sql = (f"DELETE FROM {table_name} "
                          f"WHERE {config['date_column']} = %s")
            cursor.execute(delete_sql, (file_date,))
            deleted_count = cursor.rowcount
            print(f"   🗑️  Deleted {deleted_count} existing rows for date {file_date}")
        else:
            # For customers/products: full table replace (small tables)
            cursor.execute(f"DELETE FROM {table_name}")
            deleted_count = cursor.rowcount
            print(f"   🗑️  Deleted {deleted_count} existing rows (full replace)")

        # ── BATCH INSERT ──
        columns_str = ", ".join(config["columns"])
        placeholders = ", ".join(["%s"] * len(config["columns"]))
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        rows_loaded = 0
        rows_rejected = 0
        batch = []

        with open(local_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header

            for row_num, row in enumerate(reader, start=2):  # 2 = first data row
                try:
                    # Validate column count
                    if len(row) != len(config["columns"]):
                        print(f"   ⚠️  Row {row_num}: expected {len(config['columns'])} "
                              f"columns, got {len(row)} — skipping")
                        rows_rejected += 1
                        continue

                    # Clean and transform
                    cleaned_row = clean_row(row, entity_type)
                    batch.append(cleaned_row)

                    # Execute batch when full
                    if len(batch) >= BATCH_SIZE:
                        cursor.executemany(insert_sql, batch)
                        rows_loaded += len(batch)
                        batch = []

                        # Progress reporting every 10K rows
                        if rows_loaded % 10000 == 0:
                            pct = (rows_loaded / csv_row_count) * 100
                            print(f"   ⏳ Loaded {rows_loaded:,} / {csv_row_count:,} "
                                  f"({pct:.0f}%)")

                except Exception as e:
                    print(f"   ⚠️  Row {row_num}: {e} — skipping")
                    rows_rejected += 1
                    continue

            # Insert remaining batch
            if batch:
                cursor.executemany(insert_sql, batch)
                rows_loaded += len(batch)

        # ── COMMIT TRANSACTION ──
        conn.commit()
        print(f"\n   ✅ COMMITTED: {rows_loaded:,} rows loaded")

        # ══════════════════════════════════════════════════
        # END DATA LOADING TRANSACTION
        # ══════════════════════════════════════════════════

        # ── VERIFY ROW COUNT ──
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        if config["date_column"] and file_date:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} "
                           f"WHERE {config['date_column']} = %s", (file_date,))
        total_in_table = cursor.fetchone()[0]

        print(f"\n   📊 VERIFICATION:")
        print(f"   → CSV data rows:     {csv_row_count:,}")
        print(f"   → Rows loaded:       {rows_loaded:,}")
        print(f"   → Rows rejected:     {rows_rejected:,}")
        print(f"   → Rows in table:     {total_in_table:,}")

        count_match = (rows_loaded == csv_row_count - rows_rejected)
        print(f"   → Count match:       {'✅ YES' if count_match else '❌ NO'}")

        # ── RECORD LOAD RESULT ──
        duration = time.time() - start_time
        record_load_result(
            cursor, filename, table_name, "completed",
            rows_loaded=rows_loaded, rows_rejected=rows_rejected
        )
        conn.commit()

        print(f"\n   ⏱️  Duration: {duration:.1f} seconds")
        print(f"   📈 Speed: {rows_loaded / max(duration, 0.1):,.0f} rows/second")

        return {
            "success": True,
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected,
            "duration": duration,
            "skipped": False
        }

    except pymysql.err.OperationalError as e:
        error_code = e.args[0]
        print(f"\n   ❌ MariaDB error ({error_code}): {e}")
        if conn:
            conn.rollback()
            print(f"   🔙 Transaction ROLLED BACK — no data changed")
            try:
                cursor = conn.cursor()
                record_load_result(cursor, filename, table_name, "failed",
                                   error_msg=str(e))
                conn.commit()
            except Exception:
                pass
        return {"success": False, "error": str(e)}

    except Exception as e:
        print(f"\n   ❌ Unexpected error: {e}")
        if conn:
            conn.rollback()
            print(f"   🔙 Transaction ROLLED BACK")
        return {"success": False, "error": str(e)}

    finally:
        # ── CLEANUP ──
        if conn:
            conn.close()
            print(f"   🔌 Connection closed")
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            print(f"   🗑️  Temp file deleted: {local_path}")


def load_all_daily_files(date_str=None):
    """
    Load all 3 entity files for a given date
    
    Parameters:
    → date_str: 'YYYYMMDD' format. If None, uses today.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    print("=" * 70)
    print(f"🚀 DAILY LOAD — All entities for {date_str}")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    entities = ["orders", "customers", "products"]
    results = {}

    for entity in entities:
        s3_key = f"daily_exports/{entity}/{entity}_{date_str}.csv"
        result = load_csv_to_mariadb(
            bucket=SOURCE_BUCKET,
            s3_key=s3_key
        )
        results[entity] = result

    # ── SUMMARY ──
    print("\n" + "=" * 70)
    print("📊 DAILY LOAD SUMMARY")
    print("=" * 70)

    total_loaded = 0
    total_rejected = 0
    all_success = True

    for entity, result in results.items():
        status = "✅" if result["success"] else "❌"
        loaded = result.get("rows_loaded", 0)
        rejected = result.get("rows_rejected", 0)
        skipped = result.get("skipped", False)
        duration = result.get("duration", 0)

        if skipped:
            print(f"   {status} {entity:12s}: SKIPPED (already loaded)")
        elif result["success"]:
            print(f"   {status} {entity:12s}: {loaded:>8,} loaded, "
                  f"{rejected:>4} rejected, {duration:.1f}s")
        else:
            print(f"   {status} {entity:12s}: FAILED — {result.get('error', 'unknown')[:50]}")
            all_success = False

        total_loaded += loaded
        total_rejected += rejected

    print(f"\n   TOTAL: {total_loaded:,} rows loaded, {total_rejected} rejected")
    print(f"   STATUS: {'✅ ALL SUCCEEDED' if all_success else '❌ SOME FAILED'}")

    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Load specific date: python 19_02_load_csv_to_mariadb.py 20250115
        date_arg = sys.argv[1]
        load_all_daily_files(date_str=date_arg)
    else:
        # Load today's files
        load_all_daily_files()