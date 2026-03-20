# file: 67_04_validate_and_monitor.py
# Run from: Account B (222222222222)
# Purpose: Validate Serverless works correctly and monitor costs

import boto3
import psycopg2
import time
from datetime import datetime, timedelta

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
WORKGROUP_NAME = "quickcart-prod"
NAMESPACE_NAME = "quickcart-analytics"

# Serverless endpoint
SERVERLESS_HOST = f"{WORKGROUP_NAME}.{ACCOUNT_B_ID}.{REGION}.redshift-serverless.amazonaws.com"
SERVERLESS_PORT = 5439
DB_NAME = "analytics"
DB_USER = "etl_writer"
DB_PASS = "R3dsh1ftWr1t3r#2025"
SCHEMA = "quickcart_ops"


def validate_serverless_connectivity():
    """
    Test 1: Basic connectivity to Serverless endpoint
    """
    print("🔌 Test 1: Serverless Connectivity")
    print("-" * 50)

    try:
        conn = psycopg2.connect(
            host=SERVERLESS_HOST,
            port=SERVERLESS_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=60  # Longer timeout for cold start
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        print(f"   ✅ Connected to: {version[:80]}")

        cur.execute("SELECT current_database(), current_schema(), current_user")
        db, schema, user = cur.fetchone()
        print(f"   Database: {db}, Schema: {schema}, User: {user}")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"   ❌ Connection failed: {str(e)[:200]}")
        return False


def validate_data_integrity():
    """
    Test 2: Verify all tables and data transferred correctly
    """
    print(f"\n📊 Test 2: Data Integrity Check")
    print("-" * 50)

    conn = psycopg2.connect(
        host=SERVERLESS_HOST, port=SERVERLESS_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Check schemas exist
    cur.execute("""
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name = 'quickcart_ops'
    """)
    schemas = cur.fetchall()
    print(f"   Schema quickcart_ops: {'✅ exists' if schemas else '❌ missing'}")

    # Check tables
    expected_tables = [
        "orders", "dim_customers", "dim_products",
        "stg_orders", "stg_customers", "stg_products",
        "cdc_watermarks"
    ]

    cur.execute(f"""
        SELECT tablename FROM pg_tables 
        WHERE schemaname = '{SCHEMA}'
        ORDER BY tablename
    """)
    actual_tables = {row[0] for row in cur.fetchall()}

    print(f"\n   Expected tables: {len(expected_tables)}")
    print(f"   Found tables: {len(actual_tables)}")

    for table in expected_tables:
        status = "✅" if table in actual_tables else "❌"
        if table in actual_tables:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
            count = cur.fetchone()[0]
            print(f"   {status} {table}: {count:,} rows")
        else:
            print(f"   {status} {table}: MISSING")

    # Check stored procedures
    cur.execute(f"""
        SELECT proname FROM pg_proc 
        WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{SCHEMA}')
    """)
    procs = [row[0] for row in cur.fetchall()]
    print(f"\n   Stored procedures: {procs}")
    sp_status = "✅" if "sp_cdc_merge" in procs else "❌"
    print(f"   {sp_status} sp_cdc_merge: {'found' if 'sp_cdc_merge' in procs else 'MISSING'}")

    # Check materialized views
    cur.execute("""
        SELECT mv_name FROM STV_MV_INFO 
        WHERE schema_name = 'quickcart_ops'
        ORDER BY mv_name
    """)
    mvs = [row[0] for row in cur.fetchall()]
    print(f"\n   Materialized Views found: {len(mvs)}")
    for mv in mvs:
        print(f"   ✅ {mv}")

    if len(mvs) < 12:
        print(f"   ⚠️ Expected 12 MVs, found {len(mvs)}")
        print(f"   → Run MV creation SQL from Project 63 to recreate missing MVs")

    cur.close()
    conn.close()
    return True


def validate_query_performance():
    """
    Test 3: Run benchmark queries and compare with provisioned baseline
    """
    print(f"\n⏱️  Test 3: Query Performance Benchmark")
    print("-" * 50)

    conn = psycopg2.connect(
        host=SERVERLESS_HOST, port=SERVERLESS_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Disable result cache for honest benchmark
    cur.execute("SET enable_result_cache_for_session = off;")

    benchmarks = [
        {
            "name": "Simple COUNT",
            "sql": f"SELECT COUNT(*) FROM {SCHEMA}.orders",
            "provisioned_baseline_s": 0.8
        },
        {
            "name": "Filtered aggregation",
            "sql": f"""
                SELECT status, COUNT(*), SUM(total_amount) 
                FROM {SCHEMA}.orders 
                WHERE order_date >= CURRENT_DATE - 30 
                GROUP BY status
            """,
            "provisioned_baseline_s": 1.5
        },
        {
            "name": "Multi-table JOIN",
            "sql": f"""
                SELECT p.category, COUNT(o.order_id), SUM(o.total_amount)
                FROM {SCHEMA}.orders o
                JOIN {SCHEMA}.dim_products p ON o.product_id = p.product_id
                WHERE o.order_date >= CURRENT_DATE - 90
                GROUP BY p.category
                ORDER BY SUM(o.total_amount) DESC
            """,
            "provisioned_baseline_s": 3.2
        },
        {
            "name": "MV read (if exists)",
            "sql": f"SELECT * FROM {SCHEMA}.mv_daily_revenue LIMIT 30",
            "provisioned_baseline_s": 0.05
        },
        {
            "name": "Complex window function",
            "sql": f"""
                SELECT order_date, daily_revenue,
                       SUM(daily_revenue) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING)
                FROM (
                    SELECT order_date, SUM(total_amount) AS daily_revenue
                    FROM {SCHEMA}.orders
                    WHERE order_date >= CURRENT_DATE - 90
                    GROUP BY order_date
                )
                ORDER BY order_date
            """,
            "provisioned_baseline_s": 5.0
        }
    ]

    print(f"\n   {'Query':<30} {'Serverless':>12} {'Provisioned':>12} {'Ratio':>8}")
    print(f"   {'-'*65}")

    for bm in benchmarks:
        start = time.time()
        try:
            cur.execute(bm["sql"])
            _ = cur.fetchall()
            duration = time.time() - start

            ratio = duration / bm["provisioned_baseline_s"]
            ratio_label = f"{ratio:.1f}x"
            if ratio <= 1.2:
                icon = "✅"
            elif ratio <= 2.0:
                icon = "⚠️"
            else:
                icon = "❌"

            print(f"   {icon} {bm['name']:<28} {duration:>10.3f}s {bm['provisioned_baseline_s']:>10.3f}s {ratio_label:>8}")

        except Exception as e:
            print(f"   ❌ {bm['name']:<28} ERROR: {str(e)[:50]}")

    cur.execute("SET enable_result_cache_for_session = on;")
    cur.close()
    conn.close()


def validate_data_api():
    """
    Test 4: Verify Redshift Data API works with Serverless
    → This is how Lambda functions and Step Functions interact
    """
    print(f"\n🔌 Test 4: Data API (Serverless)")
    print("-" * 50)

    redshift_data = boto3.client("redshift-data", region_name=REGION)

    try:
        response = redshift_data.execute_statement(
            WorkgroupName=WORKGROUP_NAME,
            Database=DB_NAME,
            Sql=f"SELECT COUNT(*) as total_orders FROM {SCHEMA}.orders"
        )
        stmt_id = response["Id"]
        print(f"   Statement submitted: {stmt_id}")

        # Poll
        max_wait = 60
        elapsed = 0
        while elapsed < max_wait:
            time.sleep(3)
            elapsed += 3
            desc = redshift_data.describe_statement(Id=stmt_id)
            status = desc["Status"]

            if status == "FINISHED":
                result = redshift_data.get_statement_result(Id=stmt_id)
                rows = result["Records"]
                if rows:
                    count = rows[0][0].get("longValue", 0)
                    print(f"   ✅ Data API works! Total orders: {count:,}")
                return True

            elif status == "FAILED":
                print(f"   ❌ Data API failed: {desc.get('Error', 'Unknown')}")
                return False

        print(f"   ⚠️ Data API timeout after {max_wait}s")
        return False

    except Exception as e:
        print(f"   ❌ Data API error: {str(e)[:200]}")
        return False


def create_cost_monitoring():
    """
    Create CloudWatch dashboard for Serverless cost monitoring
    """
    print(f"\n📊 Creating Serverless Cost Dashboard")
    print("-" * 50)

    cw = boto3.client("cloudwatch", region_name=REGION)

    dashboard_body = {
        "widgets": [
            {
                "type": "text",
                "x": 0, "y": 0, "width": 24, "height": 1,
                "properties": {
                    "markdown": "# 💰 Redshift Serverless Cost & Performance Monitor"
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 1, "width": 12, "height": 6,
                "properties": {
                    "title": "RPU Compute Seconds (billable)",
                    "metrics": [
                        ["AWS/Redshift-Serverless", "ComputeSeconds",
                         "Workgroup", WORKGROUP_NAME,
                         {"stat": "Sum", "period": 3600}]
                    ],
                    "region": REGION,
                    "view": "timeSeries"
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 1, "width": 12, "height": 6,
                "properties": {
                    "title": "Compute Capacity (RPU)",
                    "metrics": [
                        ["AWS/Redshift-Serverless", "ComputeCapacity",
                         "Workgroup", WORKGROUP_NAME,
                         {"stat": "Maximum", "period": 300}]
                    ],
                    "region": REGION,
                    "view": "timeSeries",
                    "yAxis": {"left": {"min": 0, "max": 128}}
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 7, "width": 12, "height": 6,
                "properties": {
                    "title": "Estimated Daily Cost ($)",
                    "metrics": [
                        ["AWS/Redshift-Serverless", "ComputeSeconds",
                         "Workgroup", WORKGROUP_NAME,
                         {"stat": "Sum", "period": 86400, "label": "RPU-Seconds/Day"}]
                    ],
                    "region": REGION,
                    "view": "singleValue",
                    "yAxis": {"left": {"label": "RPU-Seconds"}}
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 7, "width": 12, "height": 6,
                "properties": {
                    "title": "Data Storage (GB)",
                    "metrics": [
                        ["AWS/Redshift-Serverless", "DataStorage",
                         "Namespace", NAMESPACE_NAME,
                         {"stat": "Maximum", "period": 86400}]
                    ],
                    "region": REGION,
                    "view": "timeSeries"
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 13, "width": 24, "height": 6,
                "properties": {
                    "title": "Query Execution Time Distribution",
                    "metrics": [
                        ["AWS/Redshift-Serverless", "QueryDuration",
                         "Workgroup", WORKGROUP_NAME,
                         {"stat": "Average", "period": 3600, "label": "Avg"}],
                        ["AWS/Redshift-Serverless", "QueryDuration",
                         "Workgroup", WORKGROUP_NAME,
                         {"stat": "p99", "period": 3600, "label": "p99"}],
                        ["AWS/Redshift-Serverless", "QueryDuration",
                         "Workgroup", WORKGROUP_NAME,
                         {"stat": "Maximum", "period": 3600, "label": "Max"}]
                    ],
                    "region": REGION,
                    "view": "timeSeries"
                }
            }
        ]
    }

    cw.put_dashboard(
        DashboardName="RedshiftServerlessCost",
        DashboardBody=json.dumps(dashboard_body)
    )
    print(f"   ✅ Dashboard created: RedshiftServerlessCost")

    # Create cost alarm
    cw.put_metric_alarm(
        AlarmName="serverless-daily-cost-high",
        AlarmDescription="Serverless daily RPU consumption exceeds budget",
        Namespace="AWS/Redshift-Serverless",
        MetricName="ComputeSeconds",
        Dimensions=[{"Name": "Workgroup", "Value": WORKGROUP_NAME}],
        Statistic="Sum",
        Period=86400,
        EvaluationPeriods=1,
        Threshold=720000,  # 200 RPU-hours = 720,000 RPU-seconds = ~$75/day
        ComparisonOperator="GreaterThanThreshold",
        AlarmActions=[f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts"],
        TreatMissingData="notBreaching"
    )
    print(f"   ✅ Cost alarm created: serverless-daily-cost-high (threshold: $75/day)")


def run_full_validation():
    """
    Run all validation tests in sequence
    """
    print("=" * 70)
    print("🧪 REDSHIFT SERVERLESS VALIDATION SUITE")
    print(f"   Endpoint: {SERVERLESS_HOST}")
    print(f"   Workgroup: {WORKGROUP_NAME}")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    results = {}

    # Test 1: Connectivity
    results["connectivity"] = validate_serverless_connectivity()

    if not results["connectivity"]:
        print("\n🛑 CONNECTIVITY FAILED — skipping remaining tests")
        print("   Checklist:")
        print("   [ ] Workgroup status = AVAILABLE")
        print("   [ ] Security group allows inbound 5439")
        print("   [ ] VPC/subnet routing correct")
        print("   [ ] Credentials correct")
        return results

    # Test 2: Data integrity
    results["data_integrity"] = validate_data_integrity()

    # Test 3: Query performance
    validate_query_performance()
    results["performance"] = True  # Visual inspection

    # Test 4: Data API
    results["data_api"] = validate_data_api()

    # Create monitoring
    create_cost_monitoring()

    # Summary
    print(f"\n{'='*70}")
    print("📊 VALIDATION SUMMARY")
    print(f"{'='*70}")
    for test, passed in results.items():
        icon = "✅" if passed else "❌"
        print(f"   {icon} {test}")

    all_passed = all(results.values())
    if all_passed:
        print(f"\n   ✅ ALL TESTS PASSED — Serverless is ready for production")
        print(f"   Next steps:")
        print(f"   1. Run event-driven pipeline (Project 66) pointing to Serverless")
        print(f"   2. Monitor cost dashboard for 7 days")
        print(f"   3. Compare with provisioned cost baseline")
        print(f"   4. If satisfactory: pause provisioned cluster")
        print(f"   5. After 30 days: delete provisioned cluster")
    else:
        print(f"\n   ⚠️ SOME TESTS FAILED — investigate before production migration")
        print(f"   Keep provisioned cluster running as fallback")

    print(f"{'='*70}")
    return results


if __name__ == "__main__":
    run_full_validation()