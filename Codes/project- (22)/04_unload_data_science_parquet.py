# file: 04_unload_data_science_parquet.py
# Data science team needs Parquet snapshot for SageMaker training
# Parquet is columnar, self-describing, highly compressed

import psycopg2
import time

# Same connection config — pointing to Project 21 pattern
REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD [REDACTED:PASSWORD]23!"
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


def unload_parquet_snapshot():
    """
    UNLOAD full orders table as Parquet for ML training
    
    KEY DECISIONS:
    → FORMAT AS PARQUET: columnar, self-describing schema
    → No HEADER needed: Parquet embeds schema in file metadata
    → No GZIP needed: Parquet uses internal Snappy compression
    → PARALLEL ON: maximize throughput for 10M rows
    → CLEANPATH: replace previous snapshot
    → No PARTITION BY: data science wants full dataset in one directory
      (they split train/test in their own code)
    
    PARQUET vs CSV SIZE COMPARISON:
    → 10M orders as CSV:     ~1,500 MB
    → 10M orders as GZIP CSV: ~300 MB
    → 10M orders as Parquet:   ~200 MB (with Snappy compression)
    → Parquet wins on size AND read performance
    """
    conn = get_connection()
    cursor = conn.cursor()

    s3_prefix = f"s3://{BUCKET_NAME}/exports/data_science/orders_snapshot/"

    # --- FEATURE-ENGINEERED QUERY ---
    # Add computed columns useful for ML
    inner_query = """
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
            EXTRACT(MONTH FROM order_date) as month_num,
            CASE WHEN status = 'completed' THEN 1 ELSE 0 END as is_completed,
            CASE WHEN total_amount > 500 THEN 1 ELSE 0 END as is_high_value
        FROM public.orders
    """

    # --- UNLOAD AS PARQUET ---
    unload_sql = f"""
        UNLOAD ('{inner_query.replace(chr(39), chr(39)+chr(39))}')
        TO '{s3_prefix}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS PARQUET
        CLEANPATH
        REGION '{REGION}';
    """

    print(f"🧪 DATA SCIENCE EXPORT — Orders Parquet Snapshot")
    print(f"   Destination: {s3_prefix}")
    print(f"   Format: Parquet (Snappy compression)")

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
        ORDER BY path;
    """)

    files = cursor.fetchall()
    total_rows = 0
    total_mb = 0
    print(f"\n📁 Parquet output files:")
    for f in files:
        print(f"   {f[0]} → {f[1]:,} rows, {f[2]} MB")
        total_rows += f[1]
        total_mb += f[2]

    print(f"\n✅ Parquet snapshot exported in {elapsed}s")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Total size: {total_mb} MB")
    print(f"\n💡 Data science team can read with:")
    print(f"   import pyarrow.parquet as pq")
    print(f"   table = pq.read_table('s3://{BUCKET_NAME}/exports/data_science/orders_snapshot/')")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    unload_parquet_snapshot()