# file: 64_03_test_federated_queries.py
# Run from: Account B (222222222222)
# Purpose: Test each data source independently before cross-source JOINs

import boto3
import time
from datetime import datetime

REGION = "us-east-2"
WORKGROUP = "federated-queries"
OUTPUT_LOCATION = "s3://quickcart-analytics-athena-results/federated/"

athena = boto3.client("athena", region_name=REGION)


def run_athena_query(sql, description, catalog="AwsDataCatalog", database=None):
    """
    Execute Athena query and wait for result
    
    FEDERATED QUERY FLOW:
    → Athena parses SQL → identifies catalog
    → If native catalog: reads S3 directly
    → If Lambda catalog: invokes connector Lambda
    → Lambda reads source → returns Arrow data → Athena processes
    """
    print(f"\n{'='*60}")
    print(f"📋 {description}")
    print(f"   Catalog: {catalog}")
    print(f"   SQL: {sql[:100]}...")
    print(f"{'='*60}")

    query_params = {
        "QueryString": sql,
        "WorkGroup": WORKGROUP,
        "ResultConfiguration": {
            "OutputLocation": OUTPUT_LOCATION
        }
    }

    # Set catalog context
    if catalog != "AwsDataCatalog":
        query_params["QueryExecutionContext"] = {
            "Catalog": catalog
        }
    elif database:
        query_params["QueryExecutionContext"] = {
            "Database": database
        }

    # Start query
    start_time = time.time()
    response = athena.start_query_execution(**query_params)
    query_id = response["QueryExecutionId"]
    print(f"   Query ID: {query_id}")

    # Poll for completion
    while True:
        status_response = athena.get_query_execution(
            QueryExecutionId=query_id
        )
        status = status_response["QueryExecution"]["Status"]
        state = status["State"]

        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        print(f"   State: {state}...", end="\r")
        time.sleep(2)

    duration = time.time() - start_time
    stats = status_response["QueryExecution"].get("Statistics", {})
    data_scanned_mb = stats.get("DataScannedInBytes", 0) / (1024 * 1024)
    engine_time_ms = stats.get("EngineExecutionTimeInMillis", 0)

    if state == "SUCCEEDED":
        print(f"   ✅ SUCCEEDED in {duration:.2f}s")
        print(f"   Engine time: {engine_time_ms}ms")
        print(f"   Data scanned: {data_scanned_mb:.2f} MB")

        # Fetch results
        result_response = athena.get_query_results(
            QueryExecutionId=query_id,
            MaxResults=10
        )

        rows = result_response["ResultSet"]["Rows"]
        if rows:
            # Print header
            headers = [col["VarCharValue"] for col in rows[0]["Data"]]
            print(f"\n   {'  |  '.join(headers)}")
            print(f"   {'-' * 60}")

            # Print data rows (skip header)
            for row in rows[1:]:
                values = [col.get("VarCharValue", "NULL") for col in row["Data"]]
                print(f"   {'  |  '.join(values)}")

        return {"status": "success", "duration": duration, "query_id": query_id}

    else:
        error_msg = status.get("StateChangeReason", "Unknown error")
        print(f"   ❌ {state}: {error_msg}")
        return {"status": "failed", "error": error_msg, "duration": duration}


def test_native_s3_catalog():
    """
    TEST 1: Native S3 data (Account B — no federation)
    → This should work already (Project 24)
    → Baseline for comparison
    """
    return run_athena_query(
        sql="""
            SELECT order_date, COUNT(*) as order_count, 
                   ROUND(SUM(total_amount), 2) as total_revenue
            FROM analytics_lake.silver_orders
            WHERE year = '2025' AND month = '01'
            GROUP BY order_date
            ORDER BY order_date DESC
            LIMIT 5
        """,
        description="TEST 1: Native S3 Query (Account B — baseline)",
        database="analytics_lake"
    )


def test_mysql_connector():
    """
    TEST 2: MySQL connector → Account A's MariaDB
    → Lambda invoked in Account B's VPC
    → VPC Peering routes to Account A (Project 61)
    → MySQL wire protocol over private network
    
    CATALOG SYNTAX: mysql_acct_a.quickcart.orders
    → mysql_acct_a = catalog name (registered data source)
    → quickcart = database name in MariaDB
    → orders = table name in MariaDB
    """
    return run_athena_query(
        sql="""
            SELECT status, COUNT(*) as order_count,
                   ROUND(SUM(total_amount), 2) as total_revenue
            FROM mysql_acct_a.quickcart.orders
            WHERE order_date >= '2025-01-01'
            GROUP BY status
            ORDER BY total_revenue DESC
        """,
        description="TEST 2: MySQL Federated Query (Account A MariaDB)",
        catalog="mysql_acct_a"
    )


def test_dynamodb_connector():
    """
    TEST 3: DynamoDB connector → Account C's campaign_events
    → Lambda assumes cross-account role in Account C
    → Scans DynamoDB table with predicate pushdown
    
    CATALOG SYNTAX: dynamodb_acct_c.default.campaign_events
    → dynamodb_acct_c = catalog name
    → default = schema (DynamoDB has no schemas — use "default")
    → campaign_events = DynamoDB table name
    
    IMPORTANT: Include partition key in WHERE for efficiency
    → Without PK: full table Scan (expensive)
    → With PK: DynamoDB Query (cheap, fast)
    """
    return run_athena_query(
        sql="""
            SELECT channel, 
                   COUNT(*) as event_count,
                   SUM(CAST(cost AS DOUBLE)) as total_spend
            FROM dynamodb_acct_c.default.campaign_events
            WHERE event_type = 'conversion'
            LIMIT 10
        """,
        description="TEST 3: DynamoDB Federated Query (Account C Marketing)",
        catalog="dynamodb_acct_c"
    )


def test_cross_source_join():
    """
    TEST 4: JOIN across S3 (Account B) and DynamoDB (Account C)
    → This is the CORE use case — campaign ROI analysis
    → Athena handles JOIN logic
    → DynamoDB data fetched via Lambda
    → S3 data read natively
    
    QUERY PLAN:
    1. Athena invokes DynamoDB Lambda → get conversions
    2. Athena reads S3 Parquet → get orders
    3. Athena JOINs on user_id = customer_id
    4. Aggregates by campaign_name
    """
    return run_athena_query(
        sql="""
            SELECT 
                c.campaign_name,
                c.channel,
                COUNT(DISTINCT c.user_id) as converting_users,
                SUM(CAST(c.cost AS DOUBLE)) as campaign_spend,
                COUNT(o.order_id) as resulting_orders,
                ROUND(SUM(o.total_amount), 2) as resulting_revenue,
                ROUND(
                    SUM(o.total_amount) / NULLIF(SUM(CAST(c.cost AS DOUBLE)), 0),
                    2
                ) as roas
            FROM dynamodb_acct_c.default.campaign_events c
            LEFT JOIN AwsDataCatalog.analytics_lake.silver_orders o
                ON c.user_id = o.customer_id
                AND o.year = '2025'
            WHERE c.event_type = 'conversion'
            GROUP BY c.campaign_name, c.channel
            ORDER BY roas DESC
            LIMIT 10
        """,
        description="TEST 4: Cross-Source JOIN — Campaign ROI (DynamoDB + S3)",
        catalog="AwsDataCatalog"
    )


def test_three_source_join():
    """
    TEST 5: JOIN across ALL THREE accounts
    → Account A: MariaDB (live customer details)
    → Account B: S3 (historical orders)
    → Account C: DynamoDB (campaign conversions)
    
    USE CASE: Which campaigns drove high-value customers?
    → Campaign data from Account C
    → Order revenue from Account B
    → Customer tier from Account A
    
    THIS IS THE ULTIMATE FEDERATED QUERY:
    → 3 accounts, 3 different data sources, 1 SQL query
    """
    return run_athena_query(
        sql="""
            SELECT
                camp.campaign_name,
                camp.channel,
                cust.tier as customer_tier,
                COUNT(DISTINCT camp.user_id) as users,
                COUNT(ord.order_id) as orders,
                ROUND(SUM(ord.total_amount), 2) as revenue,
                ROUND(SUM(CAST(camp.cost AS DOUBLE)), 2) as spend,
                ROUND(
                    SUM(ord.total_amount) / NULLIF(SUM(CAST(camp.cost AS DOUBLE)), 0),
                    2
                ) as roas
            FROM dynamodb_acct_c.default.campaign_events camp
            LEFT JOIN AwsDataCatalog.analytics_lake.silver_orders ord
                ON camp.user_id = ord.customer_id
                AND ord.year = '2025'
            LEFT JOIN mysql_acct_a.quickcart.customers cust
                ON camp.user_id = cust.customer_id
            WHERE camp.event_type = 'conversion'
            GROUP BY camp.campaign_name, camp.channel, cust.tier
            ORDER BY revenue DESC
            LIMIT 10
        """,
        description="TEST 5: THREE-SOURCE JOIN — Campaign + Orders + Customers",
        catalog="AwsDataCatalog"
    )


def test_materialize_with_ctas():
    """
    TEST 6: Materialize federated query result as S3 Parquet
    → First query is slow (federated)
    → CTAS stores result as Parquet in Account B's S3
    → Subsequent queries on materialized table = fast native S3
    
    PATTERN: Federated for exploration → CTAS for repeated use
    Already covered in Project 36 — applied here to federated data
    """
    return run_athena_query(
        sql="""
            CREATE TABLE analytics_lake.campaign_roi_materialized
            WITH (
                format = 'PARQUET',
                write_compression = 'SNAPPY',
                external_location = 's3://quickcart-analytics-datalake/gold/campaign_roi/'
            )
            AS
            SELECT
                camp.campaign_name,
                camp.channel,
                camp.user_id,
                CAST(camp.cost AS DOUBLE) as cost,
                ord.order_id,
                ord.total_amount,
                ord.order_date,
                CURRENT_TIMESTAMP as materialized_at
            FROM dynamodb_acct_c.default.campaign_events camp
            LEFT JOIN AwsDataCatalog.analytics_lake.silver_orders ord
                ON camp.user_id = ord.customer_id
                AND ord.year = '2025'
            WHERE camp.event_type = 'conversion'
        """,
        description="TEST 6: CTAS — Materialize Federated Result to S3 Parquet",
        database="analytics_lake"
    )


def run_all_tests():
    """
    Run all tests in sequence, track results
    """
    print("=" * 70)
    print("🧪 ATHENA FEDERATION TEST SUITE")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Workgroup: {WORKGROUP}")
    print("=" * 70)

    tests = [
        ("Native S3 (baseline)", test_native_s3_catalog),
        ("MySQL Connector (Account A)", test_mysql_connector),
        ("DynamoDB Connector (Account C)", test_dynamodb_connector),
        ("Cross-Source JOIN (B + C)", test_cross_source_join),
        ("Three-Source JOIN (A + B + C)", test_three_source_join),
    ]

    results = []
    for name, test_fn in tests:
        try:
            result = test_fn()
            results.append({"name": name, **result})
        except Exception as e:
            print(f"\n❌ Test '{name}' EXCEPTION: {str(e)[:200]}")
            results.append({"name": name, "status": "exception", "error": str(e)[:200]})

    # --- SUMMARY ---
    print(f"\n\n{'='*70}")
    print("📊 TEST SUMMARY")
    print(f"{'='*70}")
    print(f"\n{'Test':<40} {'Status':>10} {'Duration':>12}")
    print("-" * 65)

    for r in results:
        status_icon = "✅" if r["status"] == "success" else "❌"
        duration = f"{r.get('duration', 0):.2f}s"
        print(f"  {r['name']:<40} {status_icon:<10} {duration:>10}")

    passed = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - passed

    print(f"\n  PASSED: {passed}/{len(results)}")
    print(f"  FAILED: {failed}/{len(results)}")

    if failed > 0:
        print(f"\n  ⚠️ DEBUGGING FAILED TESTS:")
        print(f"  1. Check Lambda CloudWatch Logs:")
        print(f"     → /aws/lambda/athena-mysql-connector")
        print(f"     → /aws/lambda/athena-dynamodb-connector")
        print(f"  2. Check cross-account IAM:")
        print(f"     → aws sts assume-role --role-arn {ACCT_C_ROLE_ARN}")
        print(f"  3. Check VPC Peering (for MySQL):")
        print(f"     → Project 61 connectivity test")
        print(f"  4. Check Athena query history:")
        print(f"     → Athena Console → Query history → filter workgroup")

    print(f"{'='*70}")

    return results


# Expose ACCT_C_ROLE_ARN for debugging reference
ACCT_C_ROLE_ARN = f"arn:aws:iam::333333333333:role/athena-federation-cross-account-read"

if __name__ == "__main__":
    run_all_tests()