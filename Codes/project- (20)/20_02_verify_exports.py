# file: 20_02_verify_exports.py
# Purpose: List exported files in S3 and verify Athena can read them
# Demonstrates: the export files from 20_01 are immediately Athena-queryable

import boto3
import time
from datetime import date, datetime

REGION = "us-east-2"
EXPORT_BUCKET = "quickcart-raw-data-prod"
EXPORT_PREFIX = "mariadb_exports"
ATHENA_OUTPUT = "s3://quickcart-athena-results/"


def list_exports(export_date=None):
    """List all exported files in S3"""
    s3_client = boto3.client("s3", region_name=REGION)

    if export_date is None:
        export_date = date.today()

    print("=" * 70)
    print(f"📦 EXPORTED FILES IN S3 (date: {export_date})")
    print("=" * 70)

    prefix = f"{EXPORT_PREFIX}/"

    paginator = s3_client.get_paginator("list_objects_v2")
    total_files = 0
    total_size = 0

    for page in paginator.paginate(Bucket=EXPORT_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size_kb = obj["Size"] / 1024
            modified = obj["LastModified"].strftime("%Y-%m-%d %H:%M")

            # Only show files for the requested date
            date_str = f"day={export_date.day:02d}"
            if date_str in key or export_date is None:
                print(f"   {key}")
                print(f"      Size: {size_kb:.1f} KB | Modified: {modified}")

                # Show S3 metadata
                meta_response = s3_client.head_object(
                    Bucket=EXPORT_BUCKET, Key=key
                )
                metadata = meta_response.get("Metadata", {})
                if metadata:
                    row_count = metadata.get("row-count", "?")
                    source = metadata.get("source-table", "?")
                    print(f"      Rows: {row_count} | Source: {source}")

                total_files += 1
                total_size += obj["Size"]
                print()

    total_mb = total_size / (1024 * 1024)
    print(f"   Total: {total_files} files, {total_mb:.2f} MB")


def verify_with_athena():
    """
    Create Athena table pointing to exports and query it
    Proves: Hive-partitioned exports are immediately Athena-queryable
    Connects to: Project 16 (Athena basics), Project 26 (partitioning)
    """
    athena_client = boto3.client("athena", region_name=REGION)

    print("\n" + "=" * 70)
    print("🔍 ATHENA VERIFICATION: Can Athena read the exports?")
    print("=" * 70)

    # ── CREATE TABLE POINTING TO EXPORTS ──
    create_table_ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS quickcart_raw.exported_orders (
        order_id        STRING,
        customer_id     STRING,
        product_id      STRING,
        quantity         INT,
        unit_price      DOUBLE,
        total_amount    DOUBLE,
        status          STRING,
        order_date      STRING
    )
    PARTITIONED BY (
        year STRING,
        month STRING,
        day STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar'     = '"'
    )
    STORED AS TEXTFILE
    LOCATION 's3://{EXPORT_BUCKET}/{EXPORT_PREFIX}/full/orders/'
    TBLPROPERTIES (
        'skip.header.line.count' = '1'
    )
    """

    print("   Creating Athena table for exported orders...")

    # Run DDL
    response = athena_client.start_query_execution(
        QueryString=create_table_ddl,
        QueryExecutionContext={"Database": "quickcart_raw"},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    query_id = response["QueryExecutionId"]

    # Wait for completion
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            print("   ✅ Table created")
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            print(f"   ❌ {state}: {reason}")
            return
        time.sleep(1)

    # ── REPAIR PARTITIONS ──
    # MSCK REPAIR discovers Hive-style partitions (year=.../month=.../day=...)
    print("   Discovering partitions (MSCK REPAIR)...")

    response = athena_client.start_query_execution(
        QueryString="MSCK REPAIR TABLE quickcart_raw.exported_orders",
        QueryExecutionContext={"Database": "quickcart_raw"},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )

    query_id = response["QueryExecutionId"]
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            print("   ✅ Partitions discovered")
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            print(f"   ⚠️  {state}: {reason}")
            print("   → This is OK if no partition directories exist yet")
            break
        time.sleep(1)

    # ── QUERY THE EXPORTED DATA ──
    print("   Querying exported data via Athena...")

    response = athena_client.start_query_execution(
        QueryString="""
            SELECT year, month, day,
                   COUNT(*) as row_count,
                   ROUND(SUM(CAST(total_amount AS DOUBLE)), 2) as total_revenue
            FROM exported_orders
            GROUP BY year, month, day
            ORDER BY year, month, day
        """,
        QueryExecutionContext={"Database": "quickcart_raw"},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )

    query_id = response["QueryExecutionId"]
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            stats = status["QueryExecution"].get("Statistics", {})
            scanned = stats.get("DataScannedInBytes", 0)
            print(f"   ✅ Query succeeded (scanned: {scanned:,} bytes)")

            # Fetch results
            results = athena_client.get_query_results(QueryExecutionId=query_id)
            rows = results["ResultSet"]["Rows"]

            if len(rows) > 1:
                print("\n   Athena Query Results (from MariaDB exports):")
                print("   " + "-" * 55)
                for row in rows:
                    vals = [col.get("VarCharValue", "") for col in row["Data"]]
                    print(f"   {' | '.join(v.ljust(12) for v in vals)}")
                print()
                print("   🎉 MariaDB data exported to S3 and queryable via Athena!")
            else:
                print("   ℹ️  No data rows (export may not have run yet)")
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            print(f"   ❌ {state}: {reason}")
            break
        time.sleep(1)


if __name__ == "__main__":
    list_exports()
    verify_with_athena()