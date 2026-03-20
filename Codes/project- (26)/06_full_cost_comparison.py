# file: 06_full_cost_comparison.py
# The ultimate comparison: CSV vs Parquet vs Partitioned Parquet
# Proves the complete optimization chain from Project 1 → 25 → 26

import boto3
import time

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"


class AthenaExecutor:
    def __init__(self, workgroup="data_engineering"):
        self.athena = boto3.client("athena", region_name=REGION)
        self.workgroup = workgroup

    def run(self, sql, description=""):
        response = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE_NAME},
            WorkGroup=self.workgroup
        )
        query_id = response["QueryExecutionId"]

        while True:
            status = self.athena.get_query_execution(QueryExecutionId=query_id)
            state = status["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                stats = status["QueryExecution"].get("Statistics", {})
                bytes_scanned = stats.get("DataScannedInBytes", 0)
                exec_time = stats.get("TotalExecutionTimeInMillis", 0)
                mb_scanned = round(bytes_scanned / (1024 * 1024), 3)
                cost = round((bytes_scanned / (1024 ** 4)) * 5.0, 8)
                return {
                    "mb": mb_scanned,
                    "cost": cost,
                    "time_ms": exec_time
                }
            elif state in ["FAILED", "CANCELLED"]:
                error = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                print(f"   ❌ {state}: {error}")
                return {"mb": 0, "cost": 0, "time_ms": 0}
            time.sleep(2)


def run_full_comparison():
    """Compare all three formats across multiple query types"""
    executor = AthenaExecutor()

    # Table mappings
    tables = {
        "CSV (raw)":                "raw_orders",
        "Parquet (silver)":         "silver_orders",
        "Parquet+Partition (proj)": "silver_orders_projected"
    }

    # Query templates
    queries = [
        {
            "name": "Full table COUNT(*)",
            "template": "SELECT COUNT(*) FROM {table};"
        },
        {
            "name": "Single date filter",
            "template": "SELECT COUNT(*) FROM {table} WHERE order_date = '2025-01-15';"
        },
        {
            "name": "7-day range filter",
            "template": "SELECT COUNT(*) FROM {table} WHERE order_date BETWEEN '2025-01-10' AND '2025-01-16';"
        },
        {
            "name": "Aggregation (all dates)",
            "template": "SELECT status, COUNT(*), ROUND(SUM(total_amount),2) FROM {table} GROUP BY status;"
        },
        {
            "name": "Aggregation (single date)",
            "template": """SELECT status, COUNT(*), ROUND(SUM(total_amount),2) 
                          FROM {table} WHERE order_date = '2025-01-15' GROUP BY status;"""
        },
        {
            "name": "2-column select (single date)",
            "template": """SELECT status, total_amount FROM {table} 
                          WHERE order_date = '2025-01-15' LIMIT 1000;"""
        }
    ]

    print("=" * 90)
    print("💰 COMPLETE COST COMPARISON: CSV vs PARQUET vs PARTITIONED PARQUET")
    print("=" * 90)

    # Results storage
    all_results = []

    for query_info in queries:
        query_name = query_info["name"]
        print(f"\n{'─' * 90}")
        print(f"📊 Query: {query_name}")
        print(f"{'─' * 90}")
        
        print(f"   {'Format':<30} {'Data Scanned':>12} {'Cost':>12} {'Time':>10}")
        print(f"   {'─'*30} {'─'*12} {'─'*12} {'─'*10}")

        row_results = {"query": query_name}

        for format_name, table_name in tables.items():
            sql = query_info["template"].format(table=table_name)
            
            # Handle type difference: raw CSV has string dates
            if table_name == "raw_orders" and "order_date" in sql:
                # CSV table: order_date is string type
                # Need string comparison for CSV
                pass  # Works with string comparison too

            result = executor.run(sql)
            
            print(f"   {format_name:<30} {result['mb']:>10.3f} MB ${result['cost']:>10.8f} {result['time_ms']:>8}ms")
            
            row_results[format_name] = result

        all_results.append(row_results)

    # ── SUMMARY TABLE ──
    print(f"\n{'=' * 90}")
    print(f"📋 SUMMARY: DATA SCANNED PER QUERY (MB)")
    print(f"{'=' * 90}")
    
    print(f"\n   {'Query':<35} {'CSV':>10} {'Parquet':>10} {'Part.PQ':>10} {'Savings':>10}")
    print(f"   {'─'*35} {'─'*10} {'─'*10} {'─'*10} {'─'*10}")

    for result in all_results:
        csv_mb = result.get("CSV (raw)", {}).get("mb", 0)
        pq_mb = result.get("Parquet (silver)", {}).get("mb", 0)
        ppq_mb = result.get("Parquet+Partition (proj)", {}).get("mb", 0)

        if csv_mb > 0:
            savings = f"{round((1 - ppq_mb / csv_mb) * 100, 1)}%"
        else:
            savings = "N/A"

        print(f"   {result['query']:<35} {csv_mb:>10.1f} {pq_mb:>10.1f} {ppq_mb:>10.1f} {savings:>10}")

    # ── MONTHLY COST PROJECTION ──
    print(f"\n{'=' * 90}")
    print(f"💰 MONTHLY COST PROJECTION (9,000 queries/month)")
    print(f"{'=' * 90}")

    # Assume query mix: 60% single-date, 20% range, 20% full-scan
    single_date_idx = 1  # "Single date filter"
    range_idx = 2        # "7-day range filter"
    full_scan_idx = 0    # "Full table COUNT(*)"

    for format_name in tables.keys():
        single = all_results[single_date_idx].get(format_name, {}).get("cost", 0)
        range_q = all_results[range_idx].get(format_name, {}).get("cost", 0)
        full = all_results[full_scan_idx].get(format_name, {}).get("cost", 0)

        monthly = (5400 * single) + (1800 * range_q) + (1800 * full)
        
        print(f"   {format_name:<30} ${monthly:>10.4f}/month")

    print(f"\n{'=' * 90}")


if __name__ == "__main__":
    run_full_comparison()