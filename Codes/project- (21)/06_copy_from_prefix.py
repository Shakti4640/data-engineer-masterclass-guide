# file: 06_copy_from_prefix.py
# Simpler approach: load ALL files under a prefix
# Use when you trust that only valid data files exist at the prefix

def execute_copy_from_prefix():
    """
    COPY from S3 prefix — loads ALL matching files
    
    WHEN TO USE THIS vs MANIFEST:
    → Prefix: quick ad-hoc loads, dev/test environments
    → Manifest: production loads, when you need exact control
    """
    conn = get_connection()  # Same function from Step 5
    conn.autocommit = True
    cursor = conn.cursor()

    # Notice: path ends with prefix, NOT manifest file
    s3_path = f"s3://{BUCKET_NAME}/daily_exports/orders/"

    copy_sql = f"""
        COPY public.orders
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        CSV
        IGNOREHEADER 1
        GZIP
        REGION '{REGION}'
        DATEFORMAT 'auto'
        MAXERROR 0
        COMPUPDATE OFF
        STATUPDATE ON;
    """
    # Notice: no MANIFEST keyword here
    # COPY will LIST all files under the prefix and load them all

    cursor.execute("TRUNCATE TABLE public.orders;")
    cursor.execute(copy_sql)

    rows = get_row_count(cursor, "public.orders")
    print(f"✅ Loaded {rows:,} rows from prefix")

    cursor.close()
    conn.close()