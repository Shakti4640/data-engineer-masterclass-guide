# file: 36_03_analyst_query_examples.py
# Purpose: Show analysts how to use gold tables
# Also: demonstrate cost difference between silver and gold queries

import boto3
import time
from datetime import datetime

AWS_REGION = "us-east-2"
S3_BUCKET = "quickcart-datalake-prod"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

athena = boto3.client("athena", region_name=AWS_REGION)


def run_and_compare(name, silver_query, gold_query):
    """
    Run same logical query against silver and gold tables
    Compare: scan volume, cost, and execution time
    """
    print(f"\n{'─' * 70}")
    print(f"📊 COMPARISON: {name}")
    print(f"{'─' * 70}")

    results = {}

    for label, query, database in [
        ("SILVER (raw scan)", silver_query, "silver_quickcart"),
        ("GOLD (materialized)", gold_query, "gold_quickcart")
    ]:
        start = time.time()

        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
        )
        qid = response["QueryExecutionId"]

        while True:
            status = athena.get_query_execution(QueryExecutionId=qid)
            state = status["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)

        elapsed = time.time() - start

        if state == "SUCCEEDED":
            stats = status["QueryExecution"].get("Statistics", {})
            scanned = stats.get("DataScannedInBytes", 0)
            engine_ms = stats.get("EngineExecutionTimeInMillis", 0)
            cost = scanned / (1024**4) * 5  # $5/TB

            results[label] = {
                "scanned_mb": round(scanned / (1024 * 1024), 2),
                "engine_ms": engine_ms,
                "wall_clock_s": round(elapsed, 2),
                "cost": cost
            }

            print(f"   {label}:")
            print(f"      Scanned: {results[label]['scanned_mb']} MB")
            print(f"      Time: {engine_ms}ms (engine) / {elapsed:.1f}s (wall clock)")
            print(f"      Cost: ${cost:.8f}")
        else:
            error = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            print(f"   {label}: FAILED — {error[:100]}")
            results[label] = {"scanned_mb": 0, "cost": 0}

    # Print comparison
    if "SILVER (raw scan)" in results and "GOLD (materialized)" in results:
        silver = results["SILVER (raw scan)"]
        gold = results["GOLD (materialized)"]

        if silver["scanned_mb"] > 0:
            scan_reduction = round(
                (1 - gold["scanned_mb"] / silver["scanned_mb"]) * 100, 1
            )
            cost_reduction = round(
                (1 - gold["cost"] / max(silver["cost"], 0.000001)) * 100, 1
            )
            speed_improvement = round(
                silver["engine_ms"] / max(gold["engine_ms"], 1), 1
            )

            print(f"\n   📈 IMPROVEMENT:")
            print(f"      Scan reduction: {scan_reduction}%")
            print(f"      Cost reduction: {cost_reduction}%")
            print(f"      Speed improvement: {speed_improvement}x faster")


def run_comparisons():
    """Run all analyst query comparisons"""

    print("=" * 70)
    print("💰 SILVER vs GOLD QUERY COST COMPARISON")
    print("=" * 70)

    # ── Comparison 1: Revenue by Category ──
    run_and_compare(
        "Revenue by Product Category (2025)",

        # SILVER: scans full order_details (~15 GB)
        """
        SELECT product_category,
               COUNT(DISTINCT order_id) AS orders,
               ROUND(SUM(revenue), 2) AS total_revenue,
               ROUND(AVG(revenue), 2) AS avg_order
        FROM order_details
        WHERE status = 'completed'
          AND order_year = 2025
        GROUP BY product_category
        ORDER BY total_revenue DESC
        """,

        # GOLD: scans tiny materialized table (~3 KB)
        """
        SELECT product_category,
               SUM(order_count) AS orders,
               SUM(total_revenue) AS total_revenue,
               ROUND(AVG(avg_order_value), 2) AS avg_order
        FROM revenue_by_category_month
        WHERE year = '2025'
        GROUP BY product_category
        ORDER BY total_revenue DESC
        """
    )

    # ── Comparison 2: Customer Segments ──
    run_and_compare(
        "Customer Segment Distribution",

        # SILVER: scans customer_rfm (~200 MB)
        """
        SELECT rfm_segment, customer_tier,
               COUNT(*) AS customers,
               ROUND(AVG(monetary), 2) AS avg_ltv,
               ROUND(AVG(frequency), 1) AS avg_freq
        FROM customer_rfm
        GROUP BY rfm_segment, customer_tier
        ORDER BY avg_ltv DESC
        """,

        # GOLD: scans tiny materialized table (~1 KB)
        """
        SELECT rfm_segment, customer_tier,
               customer_count AS customers,
               avg_lifetime_value AS avg_ltv,
               avg_frequency AS avg_freq
        FROM customer_segments_summary
        ORDER BY avg_lifetime_value DESC
        """
    )

    # ── Comparison 3: Top Products ──
    run_and_compare(
        "Top 20 Products by Revenue",

        # SILVER: scans full order_details (~15 GB)
        """
        SELECT product_name, product_category,
               COUNT(DISTINCT order_id) AS orders,
               ROUND(SUM(revenue), 2) AS total_revenue
        FROM order_details
        WHERE status = 'completed'
        GROUP BY product_name, product_category
        ORDER BY total_revenue DESC
        LIMIT 20
        """,

        # GOLD: scans product_performance (~50 KB)
        """
        SELECT product_name, product_category,
               times_ordered AS orders,
               total_revenue
        FROM product_performance
        ORDER BY total_revenue DESC
        LIMIT 20
        """
    )

    # ── Comparison 4: Daily Conversion Trend ──
    run_and_compare(
        "Conversion Rate Trend (Last 30 Days)",

        # SILVER: scans full order_details (~15 GB)
        """
        SELECT CAST(order_date AS VARCHAR) AS day,
               COUNT(DISTINCT order_id) AS total,
               COUNT(DISTINCT CASE WHEN status = 'completed' THEN order_id END) AS completed,
               ROUND(
                   CAST(COUNT(DISTINCT CASE WHEN status = 'completed' THEN order_id END) AS DOUBLE)
                   / NULLIF(COUNT(DISTINCT order_id), 0) * 100
               , 1) AS completion_rate
        FROM order_details
        GROUP BY order_date
        ORDER BY day DESC
        LIMIT 30
        """,

        # GOLD: scans daily_conversion_metrics (~5 KB)
        """
        SELECT snapshot_date AS day,
               total_orders AS total,
               completed_orders AS completed,
               completion_rate
        FROM daily_conversion_metrics
        ORDER BY snapshot_date DESC
        LIMIT 30
        """
    )

    # ── MONTHLY COST PROJECTION ──
    print(f"\n{'=' * 70}")
    print("💰 MONTHLY COST PROJECTION")
    print("=" * 70)
    print(f"""
   Assumptions:
   → 3 analysts × 15 queries/day = 45 queries/day
   → 30 days/month

   WITHOUT GOLD LAYER:
   → 45 queries × $0.075 average = $3.375/day
   → Monthly: $101.25

   WITH GOLD LAYER:
   → 1 CTAS refresh: $0.25/day (Athena scan + Python Shell)
   → 45 queries on gold: $0.002/day (minimal scan)
   → Monthly: $7.56

   SAVINGS: $93.69/month (92.5%)

   As team grows to 15 analysts (225 queries/day):
   → Without gold: $506/month
   → With gold: $8/month
   → SAVINGS: $498/month (98.4%)
    """)
    print("=" * 70)


if __name__ == "__main__":
    run_comparisons()
    print("\n🏁 DONE")