# file: 11_copy_benchmark.py
# Compare COPY performance across different configurations
# Helps you find the optimal settings for YOUR cluster and data

import psycopg2
import time
from datetime import datetime

REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWOR[REDACTED:PASSWORD]d123!"
REDSHIFT_IAM_ROLE = "arn:aws:iam::123456789012:role/RedshiftS3ReadRole"
BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def benchmark_copy(description, copy_sql, conn):
    """Run a COPY variant and measure performance"""
    cursor = conn.cursor()

    # Clean slate
    cursor.execute("TRUNCATE TABLE public.orders;")

    # Execute and time
    start = time.time()
    cursor.execute(copy_sql)
    elapsed = round(time.time() - start, 2)

    # Get row count
    cursor.execute("SELECT COUNT(*) FROM public.orders;")
    rows = cursor.fetchone()[0]

    throughput = round(rows / max(elapsed, 0.01))

    print(f"\n{'─' * 60}")
    print(f"📊 {description}")
    print(f"   Rows:       {rows:,}")
    print(f"   Duration:   {elapsed}s")
    print(f"   Throughput:  {throughput:,} rows/sec")

    cursor.close()
    return {"description": description, "rows": rows, 
            "seconds": elapsed, "rows_per_sec": throughput}


def run_benchmarks():
    """Compare different COPY configurations"""
    conn = psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB, user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    conn.autocommit = True

    results = []
    base_path = f"s3://{BUCKET_NAME}/daily_exports/orders/"

    # --- BENCHMARK 1: Uncompressed CSV from prefix ---
    results.append(benchmark_copy(
        "Uncompressed CSV — Prefix scan",
        f"""
        COPY public.orders FROM '{base_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        CSV IGNOREHEADER 1 REGION '{REGION}'
        MAXERROR 0 COMPUPDATE OFF STATUPDATE OFF;
        """
    , conn))

    # --- BENCHMARK 2: GZIP CSV from prefix ---
    gz_path = f"s3://{BUCKET_NAME}/daily_exports/orders_gz/"
    results.append(benchmark_copy(
        "GZIP CSV — Prefix scan",
        f"""
        COPY public.orders FROM '{gz_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        CSV IGNOREHEADER 1 GZIP REGION '{REGION}'
        MAXERROR 0 COMPUPDATE OFF STATUPDATE OFF;
        """
    , conn))

    # --- BENCHMARK 3: GZIP CSV from manifest ---
    manifest_path = f"s3://{BUCKET_NAME}/manifests/orders_full_load_manifest.json"
    results.append(benchmark_copy(
        "GZIP CSV — Manifest",
        f"""
        COPY public.orders FROM '{manifest_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        MANIFEST CSV IGNOREHEADER 1 GZIP REGION '{REGION}'
        MAXERROR 0 COMPUPDATE OFF STATUPDATE OFF;
        """
    , conn))

    # --- BENCHMARK 4: With COMPUPDATE ON (first load) ---
    results.append(benchmark_copy(
        "GZIP CSV — Manifest — COMPUPDATE ON",
        f"""
        COPY public.orders FROM '{manifest_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        MANIFEST CSV IGNOREHEADER 1 GZIP REGION '{REGION}'
        MAXERROR 0 COMPUPDATE ON STATUPDATE ON;
        """
    , conn))

    # --- SUMMARY TABLE ---
    print(f"\n{'═' * 70}")
    print(f"📋 BENCHMARK SUMMARY")
    print(f"{'═' * 70}")
    print(f"{'Config':<45} {'Time':>8} {'Rows/sec':>12}")
    print(f"{'─' * 45} {'─' * 8} {'─' * 12}")
    for r in results:
        print(f"{r['description']:<45} {r['seconds']:>7}s {r['rows_per_sec']:>11,}")
    print(f"{'═' * 70}")

    conn.close()
    return results


if __name__ == "__main__":
    run_benchmarks()