# file: 36_01_athena_ctas_executor.py
# Purpose: Execute CTAS queries to materialize gold layer tables
# Run from: Your machine, Lambda, or scheduled via Glue/Step Functions

import boto3
import time
from datetime import datetime

AWS_REGION = "us-east-2"
S3_BUCKET = "quickcart-datalake-prod"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"
GOLD_S3_PATH = f"s3://{S3_BUCKET}/gold"
GOLD_DATABASE = "gold_quickcart"
SILVER_DATABASE = "silver_quickcart"

athena = boto3.client("athena", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)
glue = boto3.client("glue", region_name=AWS_REGION)


# ═══════════════════════════════════════════════════════════════════
# CORE: Execute Athena Query and Wait
# ═══════════════════════════════════════════════════════════════════
def execute_athena_query(query, database=GOLD_DATABASE, description=""):
    """
    Execute an Athena query and wait for completion
    
    Returns: {
        query_id, status, data_scanned_bytes,
        execution_time_ms, output_location
    }
    """
    if description:
        print(f"   📝 {description}")

    # Show first 120 chars of query for logging
    query_preview = query.strip().replace('\n', ' ')[:120]
    print(f"   SQL: {query_preview}...")

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup="primary"
    )

    query_id = response["QueryExecutionId"]

    # Poll for completion
    while True:
        status_response = athena.get_query_execution(
            QueryExecutionId=query_id
        )
        execution = status_response["QueryExecution"]
        state = execution["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    # Extract results
    result = {
        "query_id": query_id,
        "status": state,
        "data_scanned_bytes": 0,
        "execution_time_ms": 0,
        "output_location": ""
    }

    if state == "SUCCEEDED":
        stats = execution.get("Statistics", {})
        result["data_scanned_bytes"] = stats.get("DataScannedInBytes", 0)
        result["execution_time_ms"] = stats.get("EngineExecutionTimeInMillis", 0)
        result["output_location"] = execution["ResultConfiguration"]["OutputLocation"]

        scanned_mb = round(result["data_scanned_bytes"] / (1024 * 1024), 2)
        cost = round(result["data_scanned_bytes"] / (1024**4) * 5, 6)
        time_sec = round(result["execution_time_ms"] / 1000, 2)

        print(f"   ✅ Succeeded | {time_sec}s | {scanned_mb} MB scanned | ${cost:.6f}")

    elif state == "FAILED":
        error = execution["Status"].get("StateChangeReason", "Unknown error")
        print(f"   ❌ Failed: {error}")
        result["error"] = error

    return result


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Ensure Gold Database Exists
# ═══════════════════════════════════════════════════════════════════
def ensure_gold_database():
    """Create gold_quickcart database in Glue Catalog"""
    print("\n📚 Ensuring gold database exists...")

    try:
        glue.create_database(
            DatabaseInput={
                "Name": GOLD_DATABASE,
                "Description": "Gold layer — materialized, business-ready tables"
            }
        )
        print(f"   ✅ Created: {GOLD_DATABASE}")
    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Already exists: {GOLD_DATABASE}")


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Clean Up Before CTAS (Required)
# ═══════════════════════════════════════════════════════════════════
def cleanup_before_ctas(table_name):
    """
    Remove existing table and S3 data before CTAS
    
    REQUIRED because:
    1. CTAS fails if table already exists (no IF NOT EXISTS)
    2. external_location must be empty (no pre-existing files)
    
    ORDER:
    1. DROP TABLE (removes Catalog entry)
    2. Delete S3 files (removes data)
    """
    print(f"\n🧹 Cleaning up: {GOLD_DATABASE}.{table_name}")

    # Step 1: Drop table from Catalog
    drop_query = f"DROP TABLE IF EXISTS {GOLD_DATABASE}.{table_name}"
    execute_athena_query(
        drop_query,
        database=GOLD_DATABASE,
        description=f"Drop existing table: {table_name}"
    )

    # Step 2: Delete S3 data
    s3_prefix = f"gold/{table_name}/"
    print(f"   🗑️  Deleting S3 data at: s3://{S3_BUCKET}/{s3_prefix}")

    paginator = s3.get_paginator("list_objects_v2")
    objects_to_delete = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=s3_prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                objects_to_delete.append({"Key": obj["Key"]})

    if objects_to_delete:
        # Delete in batches of 1000 (S3 limit)
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i:i+1000]
            s3.delete_objects(
                Bucket=S3_BUCKET,
                Delete={"Objects": batch}
            )
        print(f"   ✅ Deleted {len(objects_to_delete)} files")
    else:
        print(f"   ℹ️  No existing files to delete")


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Define Materialized Tables
# ═══════════════════════════════════════════════════════════════════
MATERIALIZED_TABLES = {

    # ── TABLE 1: Revenue by Category by Month ──
    "revenue_by_category_month": {
        "description": "Monthly revenue aggregation by product category",
        "refresh_frequency": "daily",
        "source_scan_gb": 15,
        "estimated_rows": 200,
        "partition_by": "year",
        "ctas_sql": f"""
            CREATE TABLE {GOLD_DATABASE}.revenue_by_category_month
            WITH (
                format = 'PARQUET',
                write_compression = 'SNAPPY',
                external_location = '{GOLD_S3_PATH}/revenue_by_category_month/',
                partitioned_by = ARRAY['year']
            )
            AS
            SELECT
                product_category,
                CAST(order_month AS VARCHAR) AS month,
                status,
                COUNT(DISTINCT order_id) AS order_count,
                COUNT(DISTINCT customer_id) AS unique_customers,
                ROUND(SUM(revenue), 2) AS total_revenue,
                ROUND(AVG(revenue), 2) AS avg_order_value,
                ROUND(MIN(revenue), 2) AS min_order_value,
                ROUND(MAX(revenue), 2) AS max_order_value,
                SUM(quantity) AS total_quantity,
                ROUND(AVG(discount_pct), 1) AS avg_discount_pct,
                CAST(order_year AS VARCHAR) AS year
            FROM {SILVER_DATABASE}.order_details
            WHERE status = 'completed'
            GROUP BY product_category, order_month, order_year, status
            ORDER BY total_revenue DESC
        """
    },

    # ── TABLE 2: Daily Operations Summary ──
    "daily_operations_summary": {
        "description": "Daily order volume by status, category, and customer city",
        "refresh_frequency": "daily",
        "source_scan_gb": 15,
        "estimated_rows": 3000,
        "partition_by": "order_date",
        "ctas_sql": f"""
            CREATE TABLE {GOLD_DATABASE}.daily_operations_summary
            WITH (
                format = 'PARQUET',
                write_compression = 'SNAPPY',
                external_location = '{GOLD_S3_PATH}/daily_operations_summary/',
                partitioned_by = ARRAY['order_date']
            )
            AS
            SELECT
                status,
                product_category,
                customer_city,
                customer_tier,
                COUNT(DISTINCT order_id) AS order_count,
                ROUND(SUM(revenue), 2) AS total_revenue,
                SUM(quantity) AS total_items,
                COUNT(DISTINCT customer_id) AS unique_customers,
                CAST(order_date AS VARCHAR) AS order_date
            FROM {SILVER_DATABASE}.order_details
            GROUP BY status, product_category, customer_city,
                     customer_tier, order_date
        """
    },

    # ── TABLE 3: Customer Segments Summary ──
    "customer_segments_summary": {
        "description": "Customer count and value by RFM segment and tier",
        "refresh_frequency": "daily",
        "source_scan_gb": 2,
        "estimated_rows": 50,
        "partition_by": None,
        "ctas_sql": f"""
            CREATE TABLE {GOLD_DATABASE}.customer_segments_summary
            WITH (
                format = 'PARQUET',
                write_compression = 'SNAPPY',
                external_location = '{GOLD_S3_PATH}/customer_segments_summary/'
            )
            AS
            SELECT
                rfm_segment,
                customer_tier,
                COUNT(*) AS customer_count,
                ROUND(AVG(monetary), 2) AS avg_lifetime_value,
                ROUND(AVG(frequency), 1) AS avg_order_frequency,
                ROUND(AVG(recency_days), 0) AS avg_recency_days,
                ROUND(SUM(monetary), 2) AS total_segment_revenue,
                ROUND(AVG(avg_order_value), 2) AS avg_order_value,
                MIN(first_order_date) AS earliest_customer,
                MAX(last_order_date) AS latest_order
            FROM {SILVER_DATABASE}.customer_rfm
            GROUP BY rfm_segment, customer_tier
            ORDER BY total_segment_revenue DESC
        """
    },

    # ── TABLE 4: Product Performance ──
    "product_performance": {
        "description": "Product-level sales performance metrics",
        "refresh_frequency": "daily",
        "source_scan_gb": 15,
        "estimated_rows": 5000,
        "partition_by": None,
        "ctas_sql": f"""
            CREATE TABLE {GOLD_DATABASE}.product_performance
            WITH (
                format = 'PARQUET',
                write_compression = 'SNAPPY',
                external_location = '{GOLD_S3_PATH}/product_performance/',
                bucketed_by = ARRAY['product_category'],
                bucket_count = 8
            )
            AS
            SELECT
                product_id,
                product_name,
                product_category,
                price_tier,
                COUNT(DISTINCT order_id) AS times_ordered,
                COUNT(DISTINCT customer_id) AS unique_buyers,
                SUM(quantity) AS total_units_sold,
                ROUND(SUM(revenue), 2) AS total_revenue,
                ROUND(AVG(revenue), 2) AS avg_revenue_per_order,
                ROUND(AVG(discount_pct), 1) AS avg_discount_given,
                MIN(order_date) AS first_sale_date,
                MAX(order_date) AS last_sale_date
            FROM {SILVER_DATABASE}.order_details
            WHERE status = 'completed'
            GROUP BY product_id, product_name, product_category, price_tier
            ORDER BY total_revenue DESC
        """
    },

    # ── TABLE 5: Conversion Funnel (Using INSERT INTO pattern) ──
    "daily_conversion_metrics": {
        "description": "Daily conversion metrics — append-only historical tracking",
        "refresh_frequency": "daily",
        "source_scan_gb": 15,
        "estimated_rows": 1,
        "partition_by": "snapshot_date",
        "is_append": True,
        "create_table_sql": f"""
            CREATE TABLE IF NOT EXISTS {GOLD_DATABASE}.daily_conversion_metrics (
                total_orders BIGINT,
                completed_orders BIGINT,
                cancelled_orders BIGINT,
                refunded_orders BIGINT,
                completion_rate DOUBLE,
                cancellation_rate DOUBLE,
                total_revenue DOUBLE,
                avg_order_value DOUBLE,
                unique_customers BIGINT,
                snapshot_date VARCHAR
            )
            WITH (
                format = 'PARQUET',
                write_compression = 'SNAPPY',
                external_location = '{GOLD_S3_PATH}/daily_conversion_metrics/',
                partitioned_by = ARRAY['snapshot_date']
            )
        """,
        "insert_sql": f"""
            INSERT INTO {GOLD_DATABASE}.daily_conversion_metrics
            SELECT
                COUNT(DISTINCT order_id) AS total_orders,
                COUNT(DISTINCT CASE WHEN status = 'completed' THEN order_id END) AS completed_orders,
                COUNT(DISTINCT CASE WHEN status = 'cancelled' THEN order_id END) AS cancelled_orders,
                COUNT(DISTINCT CASE WHEN status = 'refunded' THEN order_id END) AS refunded_orders,
                ROUND(
                    CAST(COUNT(DISTINCT CASE WHEN status = 'completed' THEN order_id END) AS DOUBLE)
                    / NULLIF(COUNT(DISTINCT order_id), 0) * 100
                , 1) AS completion_rate,
                ROUND(
                    CAST(COUNT(DISTINCT CASE WHEN status = 'cancelled' THEN order_id END) AS DOUBLE)
                    / NULLIF(COUNT(DISTINCT order_id), 0) * 100
                , 1) AS cancellation_rate,
                ROUND(SUM(CASE WHEN status = 'completed' THEN revenue ELSE 0 END), 2) AS total_revenue,
                ROUND(AVG(CASE WHEN status = 'completed' THEN revenue END), 2) AS avg_order_value,
                COUNT(DISTINCT customer_id) AS unique_customers,
                '{{snapshot_date}}' AS snapshot_date
            FROM {SILVER_DATABASE}.order_details
        """
    }
}


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Materialize All Gold Tables
# ═══════════════════════════════════════════════════════════════════
def materialize_gold_tables(extraction_date=None):
    """
    Run all CTAS queries to refresh gold layer
    
    EXECUTION ORDER:
    1. Ensure gold database exists
    2. For each table:
       a. Clean up (drop table + delete S3)
       b. Run CTAS
       c. Verify output
    """
    if extraction_date is None:
        extraction_date = datetime.now().strftime("%Y-%m-%d")

    overall_start = datetime.now()
    results = []

    print("=" * 70)
    print("🚀 GOLD LAYER MATERIALIZATION")
    print(f"   Date: {extraction_date}")
    print(f"   Tables: {len(MATERIALIZED_TABLES)}")
    print("=" * 70)

    # Ensure database
    ensure_gold_database()

    for table_name, config in MATERIALIZED_TABLES.items():
        table_start = datetime.now()
        print(f"\n{'─' * 70}")
        print(f"📊 Materializing: {GOLD_DATABASE}.{table_name}")
        print(f"   Description: {config['description']}")
        print(f"   Source scan: ~{config['source_scan_gb']} GB")
        print(f"   Expected rows: ~{config['estimated_rows']}")

        try:
            if config.get("is_append"):
                # ── APPEND PATTERN (INSERT INTO) ──
                # Create table if not exists, then insert today's data
                print(f"   Pattern: INSERT INTO (append)")

                # Create table structure (idempotent)
                execute_athena_query(
                    config["create_table_sql"],
                    database=GOLD_DATABASE,
                    description="Create table if not exists"
                )

                # Insert today's data
                insert_sql = config["insert_sql"].replace(
                    "{{snapshot_date}}", extraction_date
                )
                result = execute_athena_query(
                    insert_sql,
                    database=GOLD_DATABASE,
                    description=f"Insert data for {extraction_date}"
                )

            else:
                # ── FULL REFRESH PATTERN (DROP + CTAS) ──
                print(f"   Pattern: DROP + CTAS (full refresh)")

                # Clean up
                cleanup_before_ctas(table_name)

                # Run CTAS
                result = execute_athena_query(
                    config["ctas_sql"],
                    database=GOLD_DATABASE,
                    description="Create materialized table"
                )

            table_time = (datetime.now() - table_start).total_seconds()

            results.append({
                "table": table_name,
                "status": result["status"],
                "data_scanned_mb": round(
                    result.get("data_scanned_bytes", 0) / (1024 * 1024), 2
                ),
                "execution_ms": result.get("execution_time_ms", 0),
                "total_seconds": table_time,
                "cost": round(
                    result.get("data_scanned_bytes", 0) / (1024**4) * 5, 6
                )
            })

        except Exception as e:
            print(f"   ❌ Error: {e}")
            results.append({
                "table": table_name,
                "status": "ERROR",
                "error": str(e)
            })

    # ── FINAL SUMMARY ──
    overall_time = (datetime.now() - overall_start).total_seconds()

    print(f"\n{'=' * 70}")
    print("📊 MATERIALIZATION SUMMARY")
    print("=" * 70)

    total_scanned = 0
    total_cost = 0
    succeeded = 0
    failed = 0

    for r in results:
        icon = "✅" if r["status"] == "SUCCEEDED" else "❌"
        scanned = r.get("data_scanned_mb", 0)
        cost = r.get("cost", 0)
        total_scanned += scanned
        total_cost += cost

        if r["status"] == "SUCCEEDED":
            succeeded += 1
        else:
            failed += 1

        print(
            f"   {icon} {r['table']:35s} | "
            f"{scanned:8.1f} MB | ${cost:.6f} | "
            f"{r.get('total_seconds', 0):.1f}s"
        )

    print(f"\n   Total data scanned: {total_scanned:.1f} MB ({total_scanned/1024:.2f} GB)")
    print(f"   Total CTAS cost: ${total_cost:.6f}")
    print(f"   Total time: {overall_time:.1f}s ({overall_time/60:.1f} min)")
    print(f"   Succeeded: {succeeded} | Failed: {failed}")

    # Cost comparison
    daily_analyst_queries = 45
    per_query_cost_without_gold = total_cost  # Same scan cost
    daily_cost_without_gold = per_query_cost_without_gold * daily_analyst_queries
    daily_cost_with_gold = total_cost + (daily_analyst_queries * 0.00005)  # 10 MB minimum per query
    monthly_savings = (daily_cost_without_gold - daily_cost_with_gold) * 30

    print(f"\n   💰 COST IMPACT:")
    print(f"      Without gold: ${daily_cost_without_gold:.4f}/day → ${daily_cost_without_gold*30:.2f}/month")
    print(f"      With gold:    ${daily_cost_with_gold:.4f}/day → ${daily_cost_with_gold*30:.2f}/month")
    print(f"      Savings:      ${monthly_savings:.2f}/month ({monthly_savings/(daily_cost_without_gold*30)*100:.0f}%)")

    print("=" * 70)

    return results


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Verify Gold Tables
# ═══════════════════════════════════════════════════════════════════
def verify_gold_tables():
    """Verify all gold tables have data and are queryable"""
    print(f"\n🔍 Verifying Gold Tables...")
    print("-" * 60)

    for table_name in MATERIALIZED_TABLES:
        count_query = f"SELECT COUNT(*) as cnt FROM {GOLD_DATABASE}.{table_name}"

        result = execute_athena_query(
            count_query,
            database=GOLD_DATABASE,
            description=f"Count rows in {table_name}"
        )

        if result["status"] == "SUCCEEDED":
            # Get actual count from result
            query_results = athena.get_query_results(
                QueryExecutionId=result["query_id"]
            )
            rows = query_results["ResultSet"]["Rows"]
            if len(rows) > 1:
                count = rows[1]["Data"][0]["VarCharValue"]
                scanned = round(result["data_scanned_bytes"] / 1024, 1)
                print(f"   ✅ {table_name:35s} | {count:>10s} rows | {scanned} KB scanned")
            else:
                print(f"   ⚠️  {table_name}: no data returned")
        else:
            print(f"   ❌ {table_name}: query failed")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    action = sys.argv[1] if len(sys.argv) > 1 else "materialize"
    date = sys.argv[2] if len(sys.argv) > 2 else None

    if action == "materialize":
        results = materialize_gold_tables(extraction_date=date)
        verify_gold_tables()

    elif action == "verify":
        verify_gold_tables()

    elif action == "cleanup":
        # Clean up ALL gold tables (nuclear option)
        print("🧹 Cleaning up ALL gold tables...")
        for table_name in MATERIALIZED_TABLES:
            cleanup_before_ctas(table_name)
        print("✅ All gold tables cleaned up")

    else:
        print(f"""
USAGE:
    python 36_01_athena_ctas_executor.py materialize [YYYY-MM-DD]
    python 36_01_athena_ctas_executor.py verify
    python 36_01_athena_ctas_executor.py cleanup
        """)

    print("\n🏁 DONE")