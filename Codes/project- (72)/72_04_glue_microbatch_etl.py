# file: 72_04_glue_microbatch_etl.py
# Glue ETL job script: runs every micro-batch cycle
# Bronze (raw Firehose Parquet) → Silver (deduplicated) → Gold (compacted, enriched)
# Deployed as Glue Job — triggered by Step Functions

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# --- GLUE JOB PARAMETERS ---
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BUCKET_NAME",
    "BRONZE_PREFIX",
    "SILVER_PREFIX",
    "GOLD_PREFIX",
    "GLUE_DB_GOLD",
    "GLUE_TABLE_GOLD",
    "PROCESSING_WINDOW_MINUTES"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuration from parameters
BUCKET = args["BUCKET_NAME"]
BRONZE_PREFIX = args["BRONZE_PREFIX"]   # bronze/clickstream/
SILVER_PREFIX = args["SILVER_PREFIX"]   # silver/clickstream/
GOLD_PREFIX = args["GOLD_PREFIX"]       # gold/clickstream_events/
GLUE_DB = args["GLUE_DB_GOLD"]         # orders_gold
GLUE_TABLE = args["GLUE_TABLE_GOLD"]   # clickstream_events
WINDOW_MINUTES = int(args["PROCESSING_WINDOW_MINUTES"])  # 15


def calculate_time_window():
    """
    Determine which Bronze partition(s) to process
    
    Strategy: process the LAST N minutes of data
    → Prevents re-processing old data
    → Handles late-arriving files within window
    → Works with Firehose's hourly partitioning
    """
    now = datetime.utcnow()
    window_start = now - timedelta(minutes=WINDOW_MINUTES)

    # Generate list of hour partitions that overlap with window
    partitions = set()
    current = window_start
    while current <= now:
        partitions.add((
            current.strftime("%Y"),
            current.strftime("%m"),
            current.strftime("%d"),
            current.strftime("%H")
        ))
        current += timedelta(hours=1)

    return partitions, window_start, now


def read_bronze_data(partitions):
    """
    Read raw Parquet files from Bronze layer for the time window
    
    IMPORTANT: Read specific partitions only — not full table scan
    → At 70 GB/day, full scan would be expensive and slow
    → Partition pruning: only read the 1-2 hours we need
    """
    bronze_paths = []
    for year, month, day, hour in partitions:
        path = (
            f"s3://{BUCKET}/{BRONZE_PREFIX}"
            f"year={year}/month={month}/day={day}/hour={hour}/"
        )
        bronze_paths.append(path)

    print(f"📥 Reading Bronze data from {len(bronze_paths)} partition(s):")
    for p in bronze_paths:
        print(f"   → {p}")

    # Read all partitions
    # Using Spark directly (not DynamicFrame) for better control
    try:
        bronze_df = spark.read.parquet(*bronze_paths)
        record_count = bronze_df.count()
        print(f"   ✅ Read {record_count:,} records from Bronze")
        return bronze_df
    except Exception as e:
        print(f"   ⚠️  No data found in Bronze partitions: {e}")
        return None


def bronze_to_silver(bronze_df):
    """
    Bronze → Silver transformation:
    1. Deduplicate on event_id (at-least-once → exactly-once)
    2. Validate schema (drop malformed records)
    3. Standardize timestamps
    4. Add processing metadata
    
    DEDUPLICATION STRATEGY:
    → Kinesis delivers at-least-once
    → Same event_id may appear multiple times
    → Keep the FIRST occurrence (earliest event_timestamp)
    → Window function: ROW_NUMBER() PARTITION BY event_id ORDER BY event_timestamp
    """
    print("\n🔄 Bronze → Silver transformation:")

    # Step 1: Count before dedup
    total_before = bronze_df.count()

    # Step 2: Deduplicate on event_id
    # Keep the record with the earliest timestamp for each event_id
    dedup_window = Window.partitionBy("event_id").orderBy("event_timestamp")

    silver_df = (
        bronze_df
        .withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    total_after_dedup = silver_df.count()
    duplicates_removed = total_before - total_after_dedup
    print(f"   Dedup: {total_before:,} → {total_after_dedup:,} "
          f"({duplicates_removed:,} duplicates removed)")

    # Step 3: Validate required fields (drop nulls in critical columns)
    required_columns = ["event_id", "event_type", "user_id", "event_timestamp"]
    for col_name in required_columns:
        null_count = silver_df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            print(f"   ⚠️  Dropping {null_count} records with null {col_name}")

    silver_df = silver_df.dropna(subset=required_columns)
    total_after_validation = silver_df.count()
    print(f"   Validation: {total_after_dedup:,} → {total_after_validation:,}")

    # Step 4: Standardize and enrich
    silver_df = (
        silver_df
        # Parse timestamp string to proper timestamp type
        .withColumn(
            "event_ts",
            F.to_timestamp("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        # Add processing metadata
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_processing_batch", F.lit(datetime.utcnow().strftime("%Y%m%d%H%M")))
        # Normalize event_type to lowercase
        .withColumn("event_type", F.lower(F.col("event_type")))
        # Extract date parts for partitioning
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
    )

    print(f"   ✅ Silver dataset: {total_after_validation:,} clean records")

    return silver_df


def silver_to_gold(silver_df):
    """
    Silver → Gold transformation:
    1. Select only data product columns (drop internal fields)
    2. Coalesce to optimal file count (Project 62 — small file problem)
    3. Partition by year/month/day/hour
    
    COMPACTION STRATEGY:
    → Firehose writes many small files (1 per buffer flush)
    → Gold layer compacts into fewer, larger files
    → Target: 128 MB per Parquet file (optimal for Athena/Spectrum)
    → Use coalesce() to control output file count
    """
    print("\n🔄 Silver → Gold transformation:")

    # Step 1: Select data product columns
    gold_df = silver_df.select(
        "event_id",
        "event_type",
        "user_id",
        "session_id",
        "event_ts",
        "event_timestamp",
        "page_url",
        "product_id",
        "search_query",
        "device_type",
        "browser",
        "ip_country",
        "referrer",
        "cart_value",
        "items_in_cart",
        "event_date",
        "event_hour",
        "_processed_at"
    )

    # Step 2: Add partition columns for S3 Hive-style partitioning
    gold_df = (
        gold_df
        .withColumn("year", F.date_format("event_ts", "yyyy"))
        .withColumn("month", F.date_format("event_ts", "MM"))
        .withColumn("day", F.date_format("event_ts", "dd"))
        .withColumn("hour", F.lpad(F.col("event_hour").cast("string"), 2, "0"))
    )

    # Step 3: Calculate optimal file count
    # Rough estimate: 1 KB per event → 200K events = ~200 MB
    # Target: ~128 MB per file → 2 files for 200K events
    record_count = gold_df.count()
    estimated_size_mb = record_count * 0.001  # ~1 KB per record
    optimal_files = max(1, int(estimated_size_mb / 128))
    print(f"   Records: {record_count:,}")
    print(f"   Estimated size: {estimated_size_mb:.0f} MB")
    print(f"   Compacting to: {optimal_files} file(s) per partition")

    # Step 4: Coalesce per partition (reduce file count)
    gold_df = gold_df.coalesce(max(1, optimal_files))

    print(f"   ✅ Gold dataset ready: {record_count:,} records")

    return gold_df


def write_gold_to_s3(gold_df):
    """
    Write Gold Parquet to S3 with Hive-style partitioning
    
    MODE: append (not overwrite)
    → Each micro-batch ADDS new partition data
    → Previous batches remain untouched
    → If re-processing needed: delete partition first, then re-run
    """
    print("\n💾 Writing Gold layer to S3:")
    gold_path = f"s3://{BUCKET}/{GOLD_PREFIX}"
    print(f"   Path: {gold_path}")

    (
        gold_df
        .write
        .mode("append")
        .partitionBy("year", "month", "day", "hour")
        .format("parquet")
        .option("compression", "snappy")
        .save(gold_path)
    )

    print(f"   ✅ Gold Parquet written successfully")
    return gold_path


def write_silver_to_s3(silver_df):
    """Write Silver layer (intermediate — for debugging and reprocessing)"""
    silver_path = f"s3://{BUCKET}/{SILVER_PREFIX}"

    (
        silver_df
        .write
        .mode("append")
        .partitionBy("year", "month", "day", "hour")
        .format("parquet")
        .option("compression", "snappy")
        .save(silver_path)
    )

    # Add partition columns for write
    silver_df_partitioned = (
        silver_df
        .withColumn("year", F.date_format("event_ts", "yyyy"))
        .withColumn("month", F.date_format("event_ts", "MM"))
        .withColumn("day", F.date_format("event_ts", "dd"))
        .withColumn("hour", F.lpad(F.col("event_hour").cast("string"), 2, "0"))
    )

    (
        silver_df_partitioned
        .write
        .mode("append")
        .partitionBy("year", "month", "day", "hour")
        .format("parquet")
        .option("compression", "snappy")
        .save(silver_path)
    )

    print(f"   ✅ Silver Parquet written: {silver_path}")


def update_glue_catalog_partitions(gold_df):
    """
    Register new partitions in Glue Catalog
    → Required for Athena and Spectrum to discover new data
    → Alternative: MSCK REPAIR TABLE (slower for large tables)
    → Best: explicit add_partition via boto3
    """
    import boto3

    glue_client = boto3.client("glue", region_name="us-east-2")

    # Get unique partition values from the Gold data
    partitions = (
        gold_df
        .select("year", "month", "day", "hour")
        .distinct()
        .collect()
    )

    print(f"\n📋 Registering {len(partitions)} partition(s) in Glue Catalog:")

    for row in partitions:
        partition_values = [row["year"], row["month"], row["day"], row["hour"]]
        partition_path = (
            f"s3://{BUCKET}/{GOLD_PREFIX}"
            f"year={row['year']}/month={row['month']}/"
            f"day={row['day']}/hour={row['hour']}/"
        )

        try:
            glue_client.create_partition(
                DatabaseName=GLUE_DB,
                TableName=GLUE_TABLE,
                PartitionInput={
                    "Values": partition_values,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "event_id", "Type": "string"},
                            {"Name": "event_type", "Type": "string"},
                            {"Name": "user_id", "Type": "string"},
                            {"Name": "session_id", "Type": "string"},
                            {"Name": "event_ts", "Type": "timestamp"},
                            {"Name": "event_timestamp", "Type": "string"},
                            {"Name": "page_url", "Type": "string"},
                            {"Name": "product_id", "Type": "string"},
                            {"Name": "search_query", "Type": "string"},
                            {"Name": "device_type", "Type": "string"},
                            {"Name": "browser", "Type": "string"},
                            {"Name": "ip_country", "Type": "string"},
                            {"Name": "referrer", "Type": "string"},
                            {"Name": "cart_value", "Type": "double"},
                            {"Name": "items_in_cart", "Type": "int"},
                            {"Name": "event_date", "Type": "date"},
                            {"Name": "event_hour", "Type": "int"},
                            {"Name": "_processed_at", "Type": "timestamp"}
                        ],
                        "Location": partition_path,
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                        },
                        "Compressed": True
                    },
                    "Parameters": {
                        "classification": "parquet",
                        "compressionType": "snappy"
                    }
                }
            )
            print(f"   ✅ Partition: {'/'.join(partition_values)}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"   ℹ️  Partition exists: {'/'.join(partition_values)}")


def emit_processing_metrics(bronze_count, silver_count, gold_count, start_time):
    """
    Publish custom CloudWatch metrics for pipeline monitoring
    → Records processed per batch
    → Dedup rate
    → Processing duration
    → Used by governance dashboard (Project 71)
    """
    import boto3

    cw_client = boto3.client("cloudwatch", region_name="us-east-2")
    elapsed = (datetime.utcnow() - start_time).total_seconds()
    dedup_rate = ((bronze_count - silver_count) / max(bronze_count, 1)) * 100

    cw_client.put_metric_data(
        Namespace="QuickCart/DataMesh/Clickstream",
        MetricData=[
            {
                "MetricName": "RecordsProcessed",
                "Value": gold_count,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": "clickstream-microbatch"},
                    {"Name": "Layer", "Value": "gold"}
                ]
            },
            {
                "MetricName": "DeduplicationRate",
                "Value": round(dedup_rate, 2),
                "Unit": "Percent",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": "clickstream-microbatch"}
                ]
            },
            {
                "MetricName": "ProcessingDurationSeconds",
                "Value": round(elapsed, 1),
                "Unit": "Seconds",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": "clickstream-microbatch"}
                ]
            }
        ]
    )
    print(f"\n📊 Metrics published:")
    print(f"   Records: {gold_count:,} | Dedup: {dedup_rate:.1f}% | Duration: {elapsed:.0f}s")


# ═══════════════════════════════════════════
# MAIN EXECUTION FLOW
# ═══════════════════════════════════════════
print("=" * 70)
print("⚡ GLUE MICRO-BATCH ETL: Clickstream Bronze → Silver → Gold")
print(f"   Processing window: last {WINDOW_MINUTES} minutes")
print("=" * 70)

processing_start = datetime.utcnow()

# Step 1: Determine time window
partitions, window_start, window_end = calculate_time_window()
print(f"\n⏰ Time window: {window_start.isoformat()} → {window_end.isoformat()}")

# Step 2: Read Bronze
bronze_df = read_bronze_data(partitions)

if bronze_df is None or bronze_df.count() == 0:
    print("\n⚠️  No data in Bronze for this window — exiting cleanly")
    job.commit()
    sys.exit(0)

bronze_count = bronze_df.count()

# Step 3: Bronze → Silver (dedup + validate)
silver_df = bronze_to_silver(bronze_df)
silver_count = silver_df.count()

# Step 4: Write Silver (optional — for debugging/reprocessing)
write_silver_to_s3(silver_df)

# Step 5: Silver → Gold (compact + enrich)
gold_df = silver_to_gold(silver_df)
gold_count = gold_df.count()

# Step 6: Write Gold
write_gold_to_s3(gold_df)

# Step 7: Update Glue Catalog
update_glue_catalog_partitions(gold_df)

# Step 8: Emit metrics
emit_processing_metrics(bronze_count, silver_count, gold_count, processing_start)

print("\n" + "=" * 70)
print("✅ MICRO-BATCH ETL COMPLETE")
print(f"   Bronze: {bronze_count:,} → Silver: {silver_count:,} → Gold: {gold_count:,}")
print("=" * 70)

job.commit()