# file: 46_04_run_federated_queries.py
# Purpose: Demonstrate federated queries — the analyst experience
# Shows: simple query, cross-source JOIN, performance comparison

import boto3
import time
from datetime import datetime

REGION = "us-east-2"
CATALOG_NAME = "mariadb_source"
WORKGROUP = "federated-adhoc"
OUTPUT_LOCATION = "s3://quickcart-datalake-prod/_system/athena-results/federated/"


def run_athena_query(sql, description=""):
    """Execute an Athena query and return results"""
    athena_client = boto3.client("athena", region_name=REGION)

    print(f"\n{'─'*70}")
    if description:
        print(f"📋 {description}")
    print(f"{'─'*70}")
    print(f"   SQL: {sql[:200]}{'...' if len(sql) > 200 else ''}")

    start = time.time()

    response = athena_client.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION}
    )
    execution_id = response["QueryExecutionId"]

    # Wait for completion
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    duration = time.time() - start
    exec_info = status["QueryExecution"]

    if state == "SUCCEEDED":
        # Get statistics
        stats = exec_info.get("Statistics", {})
        data_scanned = stats.get("DataScannedInBytes", 0)
        engine_time = stats.get("EngineExecutionTimeInMillis", 0)

        print(f"   ✅ Status: SUCCEEDED")
        print(f"   ⏱  Total time: {duration:.1f}s")
        print(f"   ⏱  Engine time: {engine_time}ms")
        print(f"   📊 Data scanned: {data_scanned / (1024*1024):.2f} MB")
        print(f"   💰 Est. cost: ${data_scanned / (1024**4) * 5:.6f}")

        # Get results
        results = athena_client.get_query_results(
            QueryExecutionId=execution_id,
            MaxResults=20
        )

        rows = results["ResultSet"]["Rows"]
        if rows:
            # Header
            headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]
            print(f"\n   Results ({len(rows)-1} rows shown):")
            print(f"   {' | '.join(h[:15].ljust(15) for h in headers[:6])}")
            print(f"   {'─'*60}")

            for row in rows[1:11]:  # Show first 10 data rows
                values = [col.get("VarCharValue", "NULL") for col in row["Data"]]
                print(f"   {' | '.join(v[:15].ljust(15) for v in values[:6])}")

            if len(rows) > 11:
                print(f"   ... ({len(rows)-11} more rows)")

        return {
            "status": "SUCCEEDED",
            "duration": duration,
            "data_scanned_mb": data_scanned / (1024*1024),
            "row_count": len(rows) - 1
        }

    else:
        reason = exec_info["Status"].get("StateChangeReason", "Unknown")
        print(f"   ❌ Status: {state}")
        print(f"   Reason: {reason}")
        return {"status": state, "error": reason}


def demo_federated_queries():
    """Demonstrate various federated query patterns"""

    print("╔" + "═" * 68 + "╗")
    print("║" + "  ATHENA FEDERATED QUERY DEMONSTRATION".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    today = datetime.now().strftime("%Y-%m-%d")

    # ━━━ QUERY 1: Simple federated query with predicate pushdown ━━━
    run_athena_query(
        f"""
        SELECT order_id, customer_id, total_amount, status, order_date
        FROM "{CATALOG_NAME}".quickcart.orders
        WHERE order_date = DATE '{today}'
        LIMIT 20
        """,
        "Query 1: Today's orders (live from MariaDB — predicate pushdown)"
    )

    # ━━━ QUERY 2: Aggregation on live data ━━━
    run_athena_query(
        f"""
        SELECT status, COUNT(*) as order_count, 
               ROUND(SUM(total_amount), 2) as total_revenue
        FROM "{CATALOG_NAME}".quickcart.orders
        WHERE order_date = DATE '{today}'
        GROUP BY status
        ORDER BY total_revenue DESC
        """,
        "Query 2: Today's revenue by status (real-time aggregation)"
    )

    # ━━━ QUERY 3: Cross-source JOIN (MariaDB + S3 Gold zone) ━━━
    run_athena_query(
        f"""
        SELECT 
            o.order_id,
            o.total_amount,
            o.status,
            c.segment,
            c.lifetime_value,
            c.churn_risk
        FROM "{CATALOG_NAME}".quickcart.orders o
        JOIN "quickcart_gold".customer_360 c
            ON o.customer_id = c.customer_id
        WHERE o.order_date = DATE '{today}'
        AND c.segment IN ('gold', 'platinum')
        ORDER BY o.total_amount DESC
        LIMIT 20
        """,
        "Query 3: Cross-source JOIN — today's orders from high-value customers"
    )

    # ━━━ QUERY 4: Fraud investigation (real-time use case) ━━━
    run_athena_query(
        f"""
        SELECT 
            customer_id,
            COUNT(*) as order_count,
            SUM(total_amount) as total_spent,
            MIN(order_date) as first_order,
            MAX(order_date) as last_order
        FROM "{CATALOG_NAME}".quickcart.orders
        WHERE order_date >= DATE '{today}' - INTERVAL '7' DAY
        GROUP BY customer_id
        HAVING COUNT(*) > 10
        ORDER BY order_count DESC
        LIMIT 20
        """,
        "Query 4: Fraud investigation — customers with >10 orders in 7 days"
    )

    # ━━━ QUERY 5: Schema exploration (ad-hoc use case) ━━━
    run_athena_query(
        f"""
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable
        FROM "{CATALOG_NAME}".information_schema.columns
        WHERE table_schema = 'quickcart'
        ORDER BY table_name, ordinal_position
        """,
        "Query 5: Schema exploration — discover MariaDB table structures"
    )

    # ━━━ QUERY 6: Compare live vs Gold zone (data freshness validation) ━━━
    run_athena_query(
        f"""
        WITH live_count AS (
            SELECT COUNT(*) as live_rows
            FROM "{CATALOG_NAME}".quickcart.orders
        ),
        gold_count AS (
            SELECT COUNT(*) as gold_rows
            FROM "quickcart_gold".customer_360
        )
        SELECT 
            live_count.live_rows as mariadb_live_orders,
            gold_count.gold_rows as gold_customers,
            live_count.live_rows - gold_count.gold_rows as difference
        FROM live_count, gold_count
        """,
        "Query 6: Compare live MariaDB count vs Gold zone count"
    )

    print(f"\n{'='*70}")
    print(f"✅ FEDERATED QUERY DEMO COMPLETE")
    print(f"{'='*70}")


def performance_comparison():
    """
    Compare query performance: Federated (live) vs S3 Gold zone
    Same logical query, different data sources
    """
    print(f"\n{'='*70}")
    print(f"⚡ PERFORMANCE COMPARISON: Federated vs S3 Gold")
    print(f"{'='*70}")

    # Federated query (live MariaDB)
    result_fed = run_athena_query(
        f"""
        SELECT status, COUNT(*) as cnt, SUM(total_amount) as revenue
        FROM "{CATALOG_NAME}".quickcart.orders
        WHERE order_date >= DATE '2025-01-01'
        AND order_date < DATE '2025-02-01'
        GROUP BY status
        ORDER BY revenue DESC
        """,
        "FEDERATED: January orders aggregation (live MariaDB)"
    )

    # S3 Gold query (pre-computed)
    result_s3 = run_athena_query(
        """
        SELECT segment, COUNT(*) as cnt, 
               SUM(lifetime_value) as total_ltv
        FROM "quickcart_gold".customer_360
        WHERE year = '2025' AND month = '01'
        GROUP BY segment
        ORDER BY total_ltv DESC
        """,
        "S3 GOLD: January customer aggregation (pre-computed Parquet)"
    )

    # Summary
    print(f"\n📊 PERFORMANCE COMPARISON SUMMARY:")
    print(f"{'─'*60}")
    print(f"{'Metric':<25} {'Federated':<20} {'S3 Gold':<20}")
    print(f"{'─'*60}")

    if result_fed["status"] == "SUCCEEDED" and result_s3["status"] == "SUCCEEDED":
        print(f"{'Duration':<25} {result_fed['duration']:.1f}s{'':<14} {result_s3['duration']:.1f}s")
        print(f"{'Data Scanned':<25} {result_fed['data_scanned_mb']:.2f} MB{'':<12} {result_s3['data_scanned_mb']:.2f} MB")

        fed_cost = result_fed['data_scanned_mb'] / (1024*1024) * 5
        s3_cost = result_s3['data_scanned_mb'] / (1024*1024) * 5
        print(f"{'Est. Cost':<25} ${fed_cost:.6f}{'':<13} ${s3_cost:.6f}")
        print(f"{'Data Freshness':<25} {'Real-time':<20} {'Last night':<20}")
        print(f"{'DB Load':<25} {'Yes (MariaDB)':<20} {'None (S3)':<20}")
    print(f"{'─'*60}")

    print(f"\n💡 TAKEAWAY:")
    print(f"   → Federated: slower but REAL-TIME data")
    print(f"   → S3 Gold: faster but data is hours/days old")
    print(f"   → Use federated for: ad-hoc, investigation, exploration")
    print(f"   → Use S3 Gold for: dashboards, reports, scheduled queries")


if __name__ == "__main__":
    demo_federated_queries()
    performance_comparison()