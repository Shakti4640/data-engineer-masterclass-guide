# file: 48_03_benchmark_comparison.py
# Purpose: Compare S3 Select vs Athena vs Download for the same query

import boto3
import time
import csv
import io
import os
from s3_select_utility import s3_select_csv, s3_select_parquet, print_results

REGION = "us-east-2"
BUCKET = "quickcart-datalake-prod"

# Bronze CSV
BRONZE_KEY = "bronze/mariadb/full_dump/orders/year=2025/month=01/day=15/orders_20250115.csv"

# Silver Parquet (same data, different format)
SILVER_KEY = "silver/orders/year=2025/month=01/day=15/part-00000.snappy.parquet"


def benchmark_s3_select_csv():
    """Benchmark: S3 Select on CSV"""
    print(f"\n📋 Method 1: S3 Select on Bronze CSV")

    result = s3_select_csv(
        bucket=BUCKET,
        key=BRONZE_KEY,
        sql_expression="""
            SELECT order_id, total_amount
            FROM s3object
            WHERE CAST(total_amount AS DECIMAL) = 0
        """
    )

    stats = result.get("stats", {})
    return {
        "method": "S3 Select (CSV)",
        "duration": result["duration"],
        "bytes_scanned": stats.get("bytes_scanned", 0),
        "bytes_returned": stats.get("bytes_returned", 0),
        "rows": result["row_count"],
        "cost": (stats.get("bytes_scanned", 0) / (1024 ** 3) * 0.002 +
                 stats.get("bytes_returned", 0) / (1024 ** 3) * 0.0007)
    }


def benchmark_s3_select_parquet():
    """Benchmark: S3 Select on Parquet"""
    print(f"\n📋 Method 2: S3 Select on Silver Parquet")

    result = s3_select_parquet(
        bucket=BUCKET,
        key=SILVER_KEY,
        sql_expression="""
            SELECT order_id, total_amount
            FROM s3object
            WHERE total_amount = 0
        """
    )

    stats = result.get("stats", {})
    return {
        "method": "S3 Select (Parquet)",
        "duration": result["duration"],
        "bytes_scanned": stats.get("bytes_scanned", 0),
        "bytes_returned": stats.get("bytes_returned", 0),
        "rows": result["row_count"],
        "cost": (stats.get("bytes_scanned", 0) / (1024 ** 3) * 0.002 +
                 stats.get("bytes_returned", 0) / (1024 ** 3) * 0.0007)
    }


def benchmark_download_and_grep():
    """Benchmark: Download entire file + local processing"""
    print(f"\n📋 Method 3: Download + Local Processing")

    s3_client = boto3.client("s3", region_name=REGION)

    start = time.time()

    # Download to memory (simulate download)
    response = s3_client.get_object(Bucket=BUCKET, Key=BRONZE_KEY)
    body = response["Body"].read()
    download_time = time.time() - start

    # Parse and filter
    reader = csv.DictReader(io.StringIO(body.decode("utf-8")))
    matches = []
    for row in reader:
        try:
            if float(row.get("total_amount", 1)) == 0:
                matches.append(row)
        except (ValueError, TypeError):
            pass

    total_time = time.time() - start

    return {
        "method": "Download + grep",
        "duration": total_time,
        "bytes_scanned": len(body),
        "bytes_returned": len(body),
        "rows": len(matches),
        "cost": len(body) / (1024 ** 3) * 0.09  # S3 GET data transfer
    }


def benchmark_athena():
    """Benchmark: Athena query"""
    print(f"\n📋 Method 4: Athena Query on Silver Parquet")

    athena_client = boto3.client("athena", region_name=REGION)

    sql = """
        SELECT order_id, total_amount
        FROM quickcart_silver.orders
        WHERE total_amount = 0
        AND year = '2025' AND month = '01' AND day = '15'
    """

    start = time.time()

    response = athena_client.start_query_execution(
        QueryString=sql,
        WorkGroup="primary",
        ResultConfiguration={
            "OutputLocation": f"s3://{BUCKET}/_system/athena-results/benchmark/"
        }
    )
    execution_id = response["QueryExecutionId"]

    # Wait
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    total_time = time.time() - start

    if state == "SUCCEEDED":
        stats = status["QueryExecution"].get("Statistics", {})
        data_scanned = stats.get("DataScannedInBytes", 0)

        results = athena_client.get_query_results(
            QueryExecutionId=execution_id, MaxResults=100
        )
        row_count = len(results["ResultSet"]["Rows"]) - 1  # Minus header

        return {
            "method": "Athena (Parquet)",
            "duration": total_time,
            "bytes_scanned": data_scanned,
            "bytes_returned": 0,
            "rows": row_count,
            "cost": data_scanned / (1024 ** 4) * 5  # $5/TB
        }
    else:
        return {
            "method": "Athena (Parquet)",
            "duration": total_time,
            "bytes_scanned": 0,
            "bytes_returned": 0,
            "rows": 0,
            "cost": 0,
            "error": state
        }


def run_benchmark():
    """Run all benchmarks and compare"""
    print("╔" + "═" * 68 + "╗")
    print("║" + "  ⚡ S3 SELECT vs ATHENA vs DOWNLOAD — BENCHMARK".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    results = []

    results.append(benchmark_s3_select_csv())
    results.append(benchmark_s3_select_parquet())

    try:
        results.append(benchmark_download_and_grep())
    except Exception as e:
        print(f"   ⚠️  Download benchmark failed: {e}")

    try:
        results.append(benchmark_athena())
    except Exception as e:
        print(f"   ⚠️  Athena benchmark failed: {e}")

    # Summary table
    print(f"\n{'='*80}")
    print(f"📊 BENCHMARK RESULTS")
    print(f"{'='*80}")
    print(f"{'Method':<25} {'Duration':<12} {'Scanned':<15} {'Returned':<15} {'Cost':<12} {'Rows'}")
    print(f"{'─'*80}")

    for r in results:
        scanned_mb = r["bytes_scanned"] / (1024 * 1024)
        returned_kb = r["bytes_returned"] / 1024

        print(
            f"{r['method']:<25} "
            f"{r['duration']:.2f}s{'':<6} "
            f"{scanned_mb:.1f} MB{'':<7} "
            f"{returned_kb:.1f} KB{'':<8} "
            f"${r['cost']:.6f}{'':<4} "
            f"{r['rows']}"
        )

    print(f"{'─'*80}")

    # Winner analysis
    if results:
        fastest = min(results, key=lambda x: x["duration"])
        cheapest = min(results, key=lambda x: x["cost"])
        least_data = min(results, key=lambda x: x["bytes_returned"])

        print(f"\n🏆 WINNERS:")
        print(f"   Fastest:    {fastest['method']} ({fastest['duration']:.2f}s)")
        print(f"   Cheapest:   {cheapest['method']} (${cheapest['cost']:.6f})")
        print(f"   Least data: {least_data['method']} ({least_data['bytes_returned']/1024:.1f} KB returned)")

    print(f"\n💡 RECOMMENDATION:")
    print(f"   → Single-file inspection: S3 Select (fastest, cheapest, no setup)")
    print(f"   → Multi-file queries: Athena (parallel scan, full SQL)")
    print(f"   → Full file processing: Download (when you need entire file)")


if __name__ == "__main__":
    run_benchmark()