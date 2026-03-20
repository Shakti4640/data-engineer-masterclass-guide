# file: 02_create_hive_partitioned_table.py
# Method A: Traditional Hive-style partitioned table
# Uses MSCK REPAIR TABLE to discover partitions from S3

import boto3
import time

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"
BUCKET_NAME = "quickcart-raw-data-prod"


class AthenaExecutor:
    """Reusable Athena query executor from Project 24"""
    
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
                rows = results["ResultSet"]["Rows"]
                return rows, mb

            elif state in ["FAILED", "CANCELLED"]:
                error = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                print(f"   ❌ {state}: {error}")
                return None, 0

            time.sleep(2)


def create_hive_partitioned_table():
    """
    Create partitioned table using traditional Hive approach
    
    STEPS:
    1. CREATE EXTERNAL TABLE with PARTITIONED BY clause
    2. MSCK REPAIR TABLE to discover existing partitions
    3. Verify partitions registered
    """
    executor = AthenaExecutor()

    print("=" * 60)
    print("📋 METHOD A: HIVE-STYLE PARTITIONED TABLE")
    print("=" * 60)

    # --- STEP 1: DROP IF EXISTS ---
    executor.run(
        "DROP TABLE IF EXISTS silver_orders_hive_partitioned;",
        "Dropping existing table"
    )

    # --- STEP 2: CREATE TABLE ---
    create_sql = f"""
    CREATE EXTERNAL TABLE silver_orders_hive_partitioned (
        order_id      STRING    COMMENT 'Unique order identifier',
        customer_id   STRING    COMMENT 'Customer reference',
        product_id    STRING    COMMENT 'Product reference',
        quantity      INT       COMMENT 'Units ordered',
        unit_price    DOUBLE    COMMENT 'Price per unit',
        total_amount  DOUBLE    COMMENT 'Total order value',
        status        STRING    COMMENT 'Order status'
    )
    PARTITIONED BY (
        order_date DATE COMMENT 'Date order was placed'
    )
    STORED AS PARQUET
    LOCATION 's3://{BUCKET_NAME}/silver_partitioned/orders/'
    TBLPROPERTIES (
        'parquet.compression' = 'SNAPPY',
        'classification' = 'parquet'
    );
    """

    executor.run(create_sql, "Creating partitioned table")

    # --- STEP 3: MSCK REPAIR TABLE ---
    # Scans S3 for partition folders and registers them in catalog
    print(f"\n   📂 Discovering partitions via MSCK REPAIR TABLE...")
    start = time.time()
    executor.run(
        "MSCK REPAIR TABLE silver_orders_hive_partitioned;",
        "Running MSCK REPAIR"
    )
    msck_time = round(time.time() - start, 1)
    print(f"   ⏱️  MSCK REPAIR took {msck_time}s")

    # --- STEP 4: VERIFY PARTITIONS ---
    rows, _ = executor.run(
        "SHOW PARTITIONS silver_orders_hive_partitioned;",
        "Listing partitions"
    )

    if rows:
        # Skip header row
        partitions = [r["Data"][0].get("VarCharValue", "") for r in rows[1:]]
        print(f"\n   📊 Partitions registered: {len(partitions)}")
        for p in partitions[:5]:
            print(f"      {p}")
        if len(partitions) > 5:
            print(f"      ... and {len(partitions) - 5} more")

    # --- STEP 5: TEST PARTITION PRUNING ---
    print(f"\n{'─' * 60}")
    print(f"🧪 TESTING PARTITION PRUNING")
    print(f"{'─' * 60}")

    # Full scan
    _, full_mb = executor.run(
        "SELECT COUNT(*) FROM silver_orders_hive_partitioned;",
        "Full table scan"
    )

    # Single partition
    _, part_mb = executor.run(
        "SELECT COUNT(*) FROM silver_orders_hive_partitioned WHERE order_date = DATE '2025-01-15';",
        "Single partition scan"
    )

    if full_mb > 0:
        savings = round((1 - part_mb / full_mb) * 100, 1)
        print(f"\n   💰 Partition pruning results:")
        print(f"      Full scan:      {full_mb} MB")
        print(f"      Single date:    {part_mb} MB")
        print(f"      Savings:        {savings}%")


if __name__ == "__main__":
    create_hive_partitioned_table()