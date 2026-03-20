# file: 33_02_custom_watermark_dynamodb.py
# Purpose: Custom watermark tracking for JDBC sources
# Uses: DynamoDB table to store last processed timestamp
# This is the pattern used in Project 42 (CDC) — introduced here

import sys
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ═══════════════════════════════════════════════════════════════════
# STEP 1: Initialize
# ═══════════════════════════════════════════════════════════════════
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_connection",       # Glue JDBC connection (Project 31)
    "source_table",            # orders
    "watermark_column",        # updated_at
    "target_s3_path",          # s3://bucket/bronze/mariadb/orders/
    "watermark_table",         # DynamoDB table name
    "region"                   # us-east-2
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

SOURCE_CONNECTION = args["source_connection"]
SOURCE_TABLE = args["source_table"]
WATERMARK_COLUMN = args["watermark_column"]
TARGET_PATH = args["target_s3_path"]
WATERMARK_DDB_TABLE = args["watermark_table"]
REGION = args["region"]


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Watermark Manager (DynamoDB)
# ═══════════════════════════════════════════════════════════════════
class WatermarkManager:
    """
    Manages incremental processing state using DynamoDB
    
    DynamoDB Table Schema:
    ┌──────────────────────────────────────────────────────┐
    │ job_name (PK)  │ source_table (SK) │ watermark_value │
    ├──────────────────────────────────────────────────────┤
    │ quickcart-     │ orders            │ 2025-01-15      │
    │ mariadb-incr   │                   │ 23:59:59        │
    ├──────────────────────────────────────────────────────┤
    │ quickcart-     │ customers         │ 2025-01-15      │
    │ mariadb-incr   │                   │ 18:30:00        │
    └──────────────────────────────────────────────────────┘
    
    WHY DYNAMODB:
    → Atomic writes (no race conditions)
    → Sub-millisecond reads (fast watermark lookup)
    → Serverless (no infrastructure to manage)
    → $1/month for this use case
    → Queryable (can check all watermarks across all jobs)
    """

    def __init__(self, table_name, region):
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.job_name = args["JOB_NAME"]

    def get_watermark(self, source_table):
        """
        Get last processed watermark value
        Returns None if no previous run (first time)
        """
        try:
            response = self.table.get_item(
                Key={
                    "job_name": self.job_name,
                    "source_table": source_table
                }
            )
            if "Item" in response:
                watermark = response["Item"]["watermark_value"]
                logger.info(f"   📌 Previous watermark: {watermark}")
                return watermark
            else:
                logger.info(f"   📌 No previous watermark — first run")
                return None
        except Exception as e:
            logger.error(f"   ❌ Failed to read watermark: {e}")
            return None

    def set_watermark(self, source_table, watermark_value):
        """
        Update watermark after successful processing
        
        CRITICAL: Only call this AFTER successful write
        If you update before write and write fails → data loss
        """
        try:
            self.table.put_item(
                Item={
                    "job_name": self.job_name,
                    "source_table": source_table,
                    "watermark_value": str(watermark_value),
                    "updated_at": datetime.now().isoformat(),
                    "updated_by": "glue_job"
                }
            )
            logger.info(f"   📌 Watermark updated to: {watermark_value}")
        except Exception as e:
            logger.error(f"   ❌ Failed to update watermark: {e}")
            raise  # Fail the job — watermark must be updated


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Read Incrementally from JDBC Using Watermark
# ═══════════════════════════════════════════════════════════════════
def read_incremental(watermark_mgr):
    """
    Read only NEW/CHANGED rows from MariaDB using watermark
    
    HOW IT WORKS:
    → Get last watermark: "2025-01-14 23:59:59"
    → Build SQL: SELECT * FROM orders WHERE updated_at > '2025-01-14 23:59:59'
    → Only returns rows inserted/updated since last run
    → If first run (no watermark): read everything
    
    COMPARISON WITH JOB BOOKMARKS:
    ┌───────────────────────┬──────────────────┬──────────────────────┐
    │                       │ Job Bookmarks    │ Custom Watermark     │
    ├───────────────────────┼──────────────────┼──────────────────────┤
    │ Tracks updates?       │ No (new rows     │ Yes (uses updated_at)│
    │                       │ only for JDBC)   │                      │
    ├───────────────────────┼──────────────────┼──────────────────────┤
    │ Custom filter logic?  │ No               │ Yes (any SQL WHERE)  │
    ├───────────────────────┼──────────────────┼──────────────────────┤
    │ Visible state?        │ Opaque (Glue     │ Transparent (DynamoDB│
    │                       │ internal)        │ table, queryable)    │
    ├───────────────────────┼──────────────────┼──────────────────────┤
    │ Reset granularity?    │ All or nothing   │ Per table, per job   │
    ├───────────────────────┼──────────────────┼──────────────────────┤
    │ Code required?        │ Zero             │ ~50 lines            │
    └───────────────────────┴──────────────────┴──────────────────────┘
    """
    # Get last watermark
    last_watermark = watermark_mgr.get_watermark(SOURCE_TABLE)

    # Build query
    if last_watermark:
        # Incremental: only rows after watermark
        query = (
            f"(SELECT * FROM {SOURCE_TABLE} "
            f"WHERE {WATERMARK_COLUMN} > '{last_watermark}' "
            f"ORDER BY {WATERMARK_COLUMN}) as incremental_read"
        )
        logger.info(f"   📖 Incremental read: {WATERMARK_COLUMN} > '{last_watermark}'")
    else:
        # Full load: first run, read everything
        query = f"(SELECT * FROM {SOURCE_TABLE}) as full_read"
        logger.info(f"   📖 Full load (first run): reading all rows")

    # Read via JDBC (using Glue Connection from Project 31)
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": SOURCE_CONNECTION,
            "dbtable": query,
            "hashfield": "order_id" if not last_watermark else None,
            "hashpartitions": "10" if not last_watermark else "4"
        },
        transformation_ctx=f"read_incremental_{SOURCE_TABLE}"
    )

    row_count = dyf.count()
    logger.info(f"   Rows read: {row_count:,}")

    # Get the NEW watermark value (max of watermark column)
    new_watermark = None
    if row_count > 0:
        df = dyf.toDF()
        max_row = df.agg(F.max(F.col(WATERMARK_COLUMN))).collect()[0]
        new_watermark = str(max_row[0])
        logger.info(f"   New watermark will be: {new_watermark}")

    return dyf, row_count, new_watermark


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Transform (reuse pattern from Project 31)
# ═══════════════════════════════════════════════════════════════════
def transform_incremental(dyf):
    """Add metadata columns — same pattern as Project 31 Step 5"""
    df = dyf.toDF()

    today = datetime.now()
    df = df.withColumn("_extraction_date", F.lit(today.strftime("%Y-%m-%d")))
    df = df.withColumn("_extraction_timestamp", F.lit(today.isoformat()))
    df = df.withColumn("_source_system", F.lit("mariadb"))
    df = df.withColumn("_source_database", F.lit("quickcart_shop"))
    df = df.withColumn("_source_table", F.lit(SOURCE_TABLE))
    df = df.withColumn("_load_type", F.lit("incremental"))

    # Partition columns
    df = df.withColumn("year", F.lit(today.strftime("%Y")))
    df = df.withColumn("month", F.lit(today.strftime("%m")))
    df = df.withColumn("day", F.lit(today.strftime("%d")))

    return DynamicFrame.fromDF(df, glueContext, "transform_incremental")


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Write to S3 (Append Mode)
# ═══════════════════════════════════════════════════════════════════
def write_incremental(dyf):
    """Write incremental data — APPEND to existing partitions"""
    logger.info(f"\n💾 Writing incremental data to: {TARGET_PATH}")

    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": TARGET_PATH,
            "partitionKeys": ["year", "month", "day"]
        },
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx=f"write_incremental_{SOURCE_TABLE}"
    )

    logger.info(f"   ✅ Write complete")


# ═══════════════════════════════════════════════════════════════════
# STEP 6: Main Execution
# ═══════════════════════════════════════════════════════════════════
start_time = datetime.now()
watermark_mgr = WatermarkManager(WATERMARK_DDB_TABLE, REGION)

logger.info("=" * 60)
logger.info("🚀 INCREMENTAL EXTRACTION WITH CUSTOM WATERMARK")
logger.info(f"   Table: {SOURCE_TABLE}")
logger.info(f"   Watermark column: {WATERMARK_COLUMN}")
logger.info("=" * 60)

# Read incremental
dyf, row_count, new_watermark = read_incremental(watermark_mgr)

if row_count > 0:
    # Transform
    dyf_transformed = transform_incremental(dyf)

    # Write to S3
    write_incremental(dyf_transformed)

    # Update watermark ONLY after successful write
    # ORDER MATTERS: write first, then update watermark
    # If write fails → watermark not updated → retry processes same rows
    watermark_mgr.set_watermark(SOURCE_TABLE, new_watermark)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"\n✅ Incremental: {row_count:,} rows in {duration:.1f}s")
    logger.info(f"   Watermark moved to: {new_watermark}")
else:
    logger.info(f"\nℹ️  No new data since last watermark")

job.commit()
logger.info("📌 Job committed")