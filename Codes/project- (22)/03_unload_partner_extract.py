# file: 03_unload_partner_extract.py
# Partner "ShipFast" needs daily order data for logistics
# Partitioned by date so they can pull just the days they need

import psycopg2
import time

# Same connection config as Step 2 — not repeating (see Project 21 pattern)
REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "YourSecurePassword1[REDACTED:PASSWORD]3!"
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


def unload_partner_daily_orders():
    """
    UNLOAD full order data partitioned by order_date
    
    KEY DECISIONS:
    → PARTITION BY (order_date): creates Hive-style partitions
    → PARALLEL ON (default): maximum speed for 10M rows
    → GZIP: reduces S3 storage and partner download time
    → CSV with HEADER: partner's logistics system reads CSV
    → CLEANPATH: removes old export before writing new
      (partner always sees fresh data, no stale files)
    
    OUTPUT STRUCTURE:
    s3://bucket/exports/partner_shipfast/daily_orders/
     ├── order_date=2025-01-01/
     │    ├── 0000_part_00.csv.gz
     │    ├── 0001_part_00.csv.gz
     │    ├── 0002_part_00.csv.gz
     │    └── 0003_part_00.csv.gz
     ├── order_date=2025-01-02/
     │    └── ...
     └── order_date=2025-01-30/
          └── ...
    """
    conn = get_connection()
    cursor = conn.cursor()

    s3_prefix = f"s3://{BUCKET_NAME}/exports/partner_shipfast/daily_orders/"

    # --- THE QUERY ---
    # Full row-level data — partner needs every order for shipping
    inner_query = """
        SELECT 
            order_id,
            customer_id,
            product_id,
            quantity,
            unit_price,
            total_amount,
            status,
            order_date
        FROM public.orders
        WHERE status IN ('completed', 'shipped', 'pending')
    """

    # --- UNLOAD WITH PARTITION BY ---
    unload_sql = f"""
        UNLOAD ('{inner_query.replace(chr(39), chr(39)+chr(39))}')
        TO '{s3_prefix}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        CSV
        HEADER
        GZIP
        CLEANPATH
        PARTITION BY (order_date)
        REGION '{REGION}';
    """

    print(f"📦 PARTNER EXPORT — ShipFast Daily Orders")
    print(f"   Destination: {s3_prefix}")
    print(f"   Format: CSV + GZIP, partitioned by order_date")

    start_time = time.time()
    cursor.execute(unload_sql)
    elapsed = round(time.time() - start_time, 2)

    # --- VERIFY ---
    cursor.execute("""
        SELECT 
            TRIM(path) as file_path,
            line_count,
            ROUND(transfer_size / 1024.0 / 1024.0, 2) as size_mb
        FROM stl_unload_log
        WHERE query = pg_last_query_id()
        ORDER BY path
        LIMIT 20;
    """)

    files = cursor.fetchall()
    total_rows = 0
    total_mb = 0
    print(f"\n📁 Output files (showing first 20):")
    for f in files:
        print(f"   {f[0]} → {f[1]:,} rows, {f[2]} MB")
        total_rows += f[1]
        total_mb += f[2]

    print(f"\n✅ Partner export completed in {elapsed}s")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Total size: {total_mb} MB (compressed)")
    print(f"\n💡 Partner can read specific dates:")
    print(f"   s3://{BUCKET_NAME}/exports/partner_shipfast/daily_orders/order_date=2025-01-15/")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    unload_partner_daily_orders()