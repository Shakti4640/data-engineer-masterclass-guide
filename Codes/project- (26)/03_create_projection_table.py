# file: 03_create_projection_table.py
# Method B: Partition Projection — recommended for production
# No MSCK REPAIR needed, no crawler needed, instant partition discovery

import boto3
import time

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"
BUCKET_NAME = "quickcart-raw-data-prod"


class AthenaExecutor:
    """Same executor from Step 2"""
    
    def __init__(self, workgroup="data_engineering"):
        self.athena = boto3.client("athena", region_name=REGION)
        self.workgroup = workgroup

    def run(self, sql, description=""):
        if description:
            print(f"   ⏳ {description}...")

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
                mb = round(stats.get("DataScannedInBytes", 0) / (1024*1024), 3)
                ms = stats.get("TotalExecutionTimeInMillis", 0)
                print(f"   ✅ Done ({ms}ms, {mb} MB scanned)")
                results = self.athena.get_query_results(QueryExecutionId=query_id)
                return results["ResultSet"]["Rows"], mb
            elif state in ["FAILED", "CANCELLED"]:
                error = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                print(f"   ❌ {state}: {error}")
                return None, 0
            time.sleep(2)


def create_partition_projection_table():
    """
    Create table with Partition Projection
    
    PROJECTION PROPERTIES EXPLAINED:
    ─────────────────────────────────
    projection.enabled = true
    → Enables projection for this table
    
    projection.order_date.type = date
    → Partition key is a date type
    → Other types: integer, enum, injected
    
    projection.order_date.range = 2025-01-01,NOW
    → Valid date range: from Jan 1 2025 to current date
    → NOW = dynamic, always current date
    → Athena won't generate partitions outside this range
    
    projection.order_date.format = yyyy-MM-dd
    → Date format matching S3 folder names
    → order_date=2025-01-15 uses yyyy-MM-dd
    
    projection.order_date.interval = 1
    projection.order_date.interval.unit = DAYS
    → One partition per day
    → Other units: HOURS, MINUTES, MONTHS, YEARS
    
    storage.location.template = s3://bucket/path/order_date=${order_date}/
    → Template for constructing S3 paths
    → ${order_date} replaced with actual date value
    → Athena constructs path directly without catalog lookup
    """
    executor = AthenaExecutor()

    print("=" * 60)
    print("📋 METHOD B: PARTITION PROJECTION TABLE")
    print("=" * 60)

    # --- DROP IF EXISTS ---
    executor.run(
        "DROP TABLE IF EXISTS silver_orders_projected;",
        "Dropping existing table"
    )

    # --- CREATE TABLE WITH PROJECTION ---
    create_sql = f"""
    CREATE EXTERNAL TABLE silver_orders_projected (
        order_id      STRING    COMMENT 'Unique order identifier',
        customer_id   STRING    COMMENT 'Customer reference',
        product_id    STRING    COMMENT 'Product reference',
        quantity      INT       COMMENT 'Units ordered',
        unit_price    DOUBLE    COMMENT 'Price per unit',
        total_amount  DOUBLE    COMMENT 'Total order value',
        status        STRING    COMMENT 'Order status'
    )
    PARTITIONED BY (
        order_date STRING
    )
    STORED AS PARQUET
    LOCATION 's3://{BUCKET_NAME}/silver_partitioned/orders/'
    TBLPROPERTIES (
        'projection.enabled'                    = 'true',
        'projection.order_date.type'            = 'date',
        'projection.order_date.range'           = '2025-01-01,NOW',
        'projection.order_date.format'          = 'yyyy-MM-dd',
        'projection.order_date.interval'        = '1',
        'projection.order_date.interval.unit'   = 'DAYS',
        'storage.location.template'             = 
            's3://{BUCKET_NAME}/silver_partitioned/orders/order_date=${{order_date}}/',
        'parquet.compression'                   = 'SNAPPY',
        'classification'                        = 'parquet'
    );
    """

    executor.run(create_sql, "Creating projection table")

    print(f"\n   📋 Projection configuration:")
    print(f"      Type:     date")
    print(f"      Range:    2025-01-01 → NOW (dynamic)")
    print(f"      Interval: 1 DAY")
    print(f"      Template: .../order_date=${{order_date}}/")
    print(f"\n   💡 NO MSCK REPAIR needed!")
    print(f"   💡 NO Crawler needed!")
    print(f"   💡 New dates auto-discovered via math!")

    # --- TEST QUERIES ---
    print(f"\n{'─' * 60}")
    print(f"🧪 TESTING PARTITION PROJECTION")
    print(f"{'─' * 60}")

    # Test 1: Full scan
    rows, full_mb = executor.run(
        "SELECT COUNT(*) as cnt FROM silver_orders_projected;",
        "Full table scan"
    )
    if rows and len(rows) > 1:
        total_count = rows[1]["Data"][0].get("VarCharValue", "0")
        print(f"      Total rows: {total_count}")

    # Test 2: Single date
    rows, single_mb = executor.run(
        "SELECT COUNT(*) as cnt FROM silver_orders_projected WHERE order_date = '2025-01-15';",
        "Single date partition"
    )
    if rows and len(rows) > 1:
        date_count = rows[1]["Data"][0].get("VarCharValue", "0")
        print(f"      Rows for 2025-01-15: {date_count}")

    # Test 3: Date range (1 week)
    rows, week_mb = executor.run(
        """SELECT COUNT(*) as cnt FROM silver_orders_projected 
           WHERE order_date BETWEEN '2025-01-10' AND '2025-01-16';""",
        "7-day range partition"
    )
    if rows and len(rows) > 1:
        week_count = rows[1]["Data"][0].get("VarCharValue", "0")
        print(f"      Rows for 7-day range: {week_count}")

    # Test 4: Column selection + partition filter
    _, col_mb = executor.run(
        """SELECT status, SUM(total_amount) as revenue
           FROM silver_orders_projected 
           WHERE order_date = '2025-01-15'
           GROUP BY status;""",
        "Aggregation on single partition"
    )

    # --- RESULTS COMPARISON ---
    print(f"\n{'─' * 60}")
    print(f"💰 PARTITION PRUNING COST COMPARISON")
    print(f"{'─' * 60}")
    print(f"   {'Query Type':<35} {'Data Scanned':>12} {'Est Cost':>10}")
    print(f"   {'─'*35} {'─'*12} {'─'*10}")
    
    for label, mb in [
        ("Full table scan", full_mb),
        ("Single date (1 partition)", single_mb),
        ("7-day range (7 partitions)", week_mb),
        ("Aggregation + single partition", col_mb)
    ]:
        cost = round((mb / 1024) * 5.0, 6)
        print(f"   {label:<35} {mb:>10.3f} MB ${cost:>9.6f}")

    if full_mb > 0 and single_mb > 0:
        savings = round((1 - single_mb / full_mb) * 100, 1)
        print(f"\n   🎯 Single-date savings vs full scan: {savings}%")


if __name__ == "__main__":
    create_partition_projection_table()