# file: 16_03_run_queries.py
# Purpose: Answer the Marketing Manager's questions using Athena SQL
# Dependency: Tables created in 16_02

import boto3
import time

REGION = "us-east-2"
ATHENA_OUTPUT = "s3://quickcart-athena-results/"
DATABASE = "quickcart_raw"


def run_athena_query(athena_client, query, database=None):
    """Same helper as 16_02 — reused here"""
    params = {
        "QueryString": query,
        "ResultConfiguration": {"OutputLocation": ATHENA_OUTPUT}
    }
    if database:
        params["QueryExecutionContext"] = {"Database": database}

    response = athena_client.start_query_execution(**params)
    query_id = response["QueryExecutionId"]

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            stats = status["QueryExecution"].get("Statistics", {})
            scanned = stats.get("DataScannedInBytes", 0)
            exec_ms = stats.get("EngineExecutionTimeInMillis", 0)
            cost = max((scanned / 1_099_511_627_776) * 5.0, 0.00000476)
            print(f"   📊 Scanned: {scanned:,} bytes | "
                  f"Time: {exec_ms}ms | "
                  f"Cost: ${cost:.6f}")
            return query_id
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            print(f"   ❌ {state}: {reason}")
            return None
        time.sleep(1)


def get_and_print_results(athena_client, query_id, max_rows=20):
    """Fetch and display query results"""
    response = athena_client.get_query_results(
        QueryExecutionId=query_id,
        MaxResults=max_rows + 1
    )
    rows = response["ResultSet"]["Rows"]
    if not rows:
        print("   (no results)")
        return

    headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]
    widths = [len(h) for h in headers]

    data_rows = []
    for row in rows[1:]:
        vals = [col.get("VarCharValue", "NULL") for col in row["Data"]]
        for i, v in enumerate(vals):
            if i < len(widths):
                widths[i] = max(widths[i], len(str(v)))
        data_rows.append(vals)

    # Print
    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sep_line = "-+-".join("-" * w for w in widths)
    print(f"\n   {header_line}")
    print(f"   {sep_line}")
    for row in data_rows:
        row_line = " | ".join(
            str(v).ljust(widths[i]) for i, v in enumerate(row)
        )
        print(f"   {row_line}")
    print()


def run_all_queries():
    athena_client = boto3.client("athena", region_name=REGION)

    # ================================================================
    # QUERY 1: Sanity Check — Preview orders data
    # ================================================================
    # WHY FIRST: Always preview data before running aggregations
    # Catches: column misalignment, header-as-data, NULL issues
    print("\n" + "=" * 70)
    print("QUERY 1: Preview orders data (sanity check)")
    print("=" * 70)
    print('   SQL: SELECT * FROM orders LIMIT 5')

    qid = run_athena_query(
        athena_client,
        "SELECT * FROM orders LIMIT 5",
        DATABASE
    )
    if qid:
        get_and_print_results(athena_client, qid)

    # ================================================================
    # QUERY 2: Total row count
    # ================================================================
    # WHY: Verify expected row count matches uploaded data
    # 180 days × 1000 rows = ~180,000 expected (with test data)
    print("=" * 70)
    print("QUERY 2: Total order count")
    print("=" * 70)
    print('   SQL: SELECT COUNT(*) as total_orders FROM orders')

    qid = run_athena_query(
        athena_client,
        "SELECT COUNT(*) as total_orders FROM orders",
        DATABASE
    )
    if qid:
        get_and_print_results(athena_client, qid)

    # ================================================================
    # QUERY 3: Marketing Manager's Question #1
    # "How many completed orders last month?"
    # ================================================================
    print("=" * 70)
    print("QUERY 3: Completed orders count by status")
    print("=" * 70)
    print('   SQL: SELECT status, COUNT(*) ... GROUP BY status ORDER BY cnt DESC')

    qid = run_athena_query(
        athena_client,
        """
        SELECT 
            status,
            COUNT(*) as order_count,
            ROUND(SUM(CAST(total_amount AS DOUBLE)), 2) as total_revenue
        FROM orders
        GROUP BY status
        ORDER BY order_count DESC
        """,
        DATABASE
    )
    if qid:
        get_and_print_results(athena_client, qid)

    # ================================================================
    # QUERY 4: Revenue by product category (JOIN)
    # ================================================================
    # THIS IS THE POWER QUERY: Join across S3 files from different prefixes
    # orders/ files JOIN with products/ files — all on S3, no loading
    print("=" * 70)
    print("QUERY 4: Revenue by product category (JOIN orders + products)")
    print("=" * 70)
    print('   SQL: SELECT p.category, SUM(o.total_amount) ... JOIN ...')

    qid = run_athena_query(
        athena_client,
        """
        SELECT 
            p.category,
            COUNT(*) as order_count,
            ROUND(SUM(CAST(o.total_amount AS DOUBLE)), 2) as total_revenue,
            ROUND(AVG(CAST(o.total_amount AS DOUBLE)), 2) as avg_order_value
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        GROUP BY p.category
        ORDER BY total_revenue DESC
        """,
        DATABASE
    )
    if qid:
        get_and_print_results(athena_client, qid)

    # ================================================================
    # QUERY 5: Customer tier distribution
    # ================================================================
    print("=" * 70)
    print("QUERY 5: Customer distribution by tier")
    print("=" * 70)

    qid = run_athena_query(
        athena_client,
        """
        SELECT 
            tier,
            COUNT(*) as customer_count,
            COUNT(DISTINCT city) as unique_cities
        FROM customers
        GROUP BY tier
        ORDER BY customer_count DESC
        """,
        DATABASE
    )
    if qid:
        get_and_print_results(athena_client, qid)

    # ================================================================
    # QUERY 6: Top 10 customers by spend (3-way JOIN)
    # ================================================================
    # Demonstrates: Athena can join 3 tables, all backed by S3 CSVs
    print("=" * 70)
    print("QUERY 6: Top 10 customers by total spend (3-way JOIN)")
    print("=" * 70)

    qid = run_athena_query(
        athena_client,
        """
        SELECT 
            c.customer_id,
            c.name,
            c.tier,
            c.city,
            COUNT(o.order_id) as total_orders,
            ROUND(SUM(CAST(o.total_amount AS DOUBLE)), 2) as total_spend
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        GROUP BY c.customer_id, c.name, c.tier, c.city
        ORDER BY total_spend DESC
        LIMIT 10
        """,
        DATABASE
    )
    if qid:
        get_and_print_results(athena_client, qid)

    # ================================================================
    # QUERY 7: Which S3 file is each row from? ($path pseudo-column)
    # ================================================================
    # DEBUGGING TECHNIQUE: $path shows the actual S3 file path per row
    print("=" * 70)
    print("QUERY 7: Identify source S3 file per row ($path)")
    print("=" * 70)
    print('   SQL: SELECT "$path", order_id FROM orders LIMIT 3')

    qid = run_athena_query(
        athena_client,
        """
        SELECT 
            "$path" as source_file,
            order_id,
            status
        FROM orders
        LIMIT 3
        """,
        DATABASE
    )
    if qid:
        get_and_print_results(athena_client, qid)

    # ================================================================
    # QUERY 8: Verify table metadata (SHOW CREATE TABLE)
    # ================================================================
    print("=" * 70)
    print("QUERY 8: Show full table DDL (verify configuration)")
    print("=" * 70)

    qid = run_athena_query(
        athena_client,
        "SHOW CREATE TABLE orders",
        DATABASE
    )
    if qid:
        get_and_print_results(athena_client, qid, max_rows=30)


if __name__ == "__main__":
    run_all_queries()