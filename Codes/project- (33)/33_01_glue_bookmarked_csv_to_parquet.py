
# file: 33_01_glue_bookmarked_csv_to_parquet.py
# Purpose: Convert partner CSV feeds to Parquet — only process NEW files
# Uses: Glue Job Bookmarks (built-in)
# Upload to: s3://quickcart-datalake-prod/scripts/glue/bookmarked_csv_to_parquet.py

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ═══════════════════════════════════════════════════════════════════
# STEP 1: Initialize with Bookmark Support
# ═══════════════════════════════════════════════════════════════════
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_s3_path",         # s3://bucket/raw/partner_feeds/
    "target_s3_path",         # s3://bucket/bronze/partner_feeds/
    "source_format"           # csv
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ── CRITICAL: init with args enables bookmark tracking ──
# Bookmark state is tied to JOB_NAME
# job.init() loads the previous bookmark state (if any)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

SOURCE_PATH = args["source_s3_path"]
TARGET_PATH = args["target_s3_path"]
SOURCE_FORMAT = args["source_format"]

logger.info(f"Job started: {args['JOB_NAME']}")
logger.info(f"Source: {SOURCE_PATH}")
logger.info(f"Target: {TARGET_PATH}")
logger.info(f"Bookmark: ENABLED (via --job-bookmark-option)")


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Read Source with Bookmark Tracking
# ═══════════════════════════════════════════════════════════════════
def read_with_bookmark():
    """
    Read from S3 using DynamicFrame API WITH bookmark tracking
    
    CRITICAL DETAILS:
    → transformation_ctx MUST be specified
    → This string is the KEY in the bookmark state
    → Bookmark uses this to know: "for this read operation,
      which files were already processed?"
    → If you omit transformation_ctx: bookmark does NOTHING
    
    WHAT HAPPENS INTERNALLY:
    → Glue lists all files at source_s3_path
    → Loads previous bookmark state for transformation_ctx="read_partner_csvs"
    → Filters: only files with timestamp > bookmark watermark
    → Reads only the filtered (new) files
    → Returns DynamicFrame with ONLY new data
    """
    logger.info(f"\n📖 Reading new files from: {SOURCE_PATH}")
    logger.info(f"   transformation_ctx: 'read_partner_csvs'")

    # ── READ WITH BOOKMARK ──
    # The MAGIC happens here:
    # → "read_partner_csvs" is the bookmark key
    # → Glue checks: "what did I last process for this key?"
    # → Filters files automatically
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [SOURCE_PATH],
            "recurse": True,

            # ── BOOKMARK-SPECIFIC OPTIONS ──
            # groupFiles: group small files for efficiency
            "groupFiles": "inPartition",
            # groupSize: target group size in bytes (128 MB)
            "groupSize": "134217728"
        },
        format=SOURCE_FORMAT,
        format_options={
            "withHeader": True,
            "separator": ","
        },
        # ══════════════════════════════════════════════
        # THIS IS THE BOOKMARK KEY — DO NOT CHANGE AFTER FIRST RUN
        # ══════════════════════════════════════════════
        transformation_ctx="read_partner_csvs"
    )

    row_count = dyf.count()
    logger.info(f"   Rows from new files: {row_count:,}")

    if row_count == 0:
        logger.info(f"   ℹ️  No new files to process — bookmark is current")
        logger.info(f"   → Job will complete with no output")

    return dyf, row_count


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Transform (same as Project 25 but bookmark-aware)
# ═══════════════════════════════════════════════════════════════════
def transform(dyf):
    """
    Apply transformations to new data only
    
    IMPORTANT: These transforms run ONLY on new files
    → If bookmark filtered to 1 file (100K rows)
    → Transform runs on 100K rows, not 9M rows
    → 90x less compute than without bookmark
    """
    logger.info(f"\n🔄 Transforming new data...")

    df = dyf.toDF()

    # Add processing metadata
    df = df.withColumn("_processed_at", F.current_timestamp())
    df = df.withColumn("_source_format", F.lit("csv"))
    df = df.withColumn("_job_name", F.lit(args["JOB_NAME"]))

    # Add date partition columns from processing date
    today = datetime.now()
    df = df.withColumn("year", F.lit(today.strftime("%Y")))
    df = df.withColumn("month", F.lit(today.strftime("%m")))
    df = df.withColumn("day", F.lit(today.strftime("%d")))

    # Standard cleanup (same as Project 25 — not repeated in detail)
    # → Trim strings, normalize NULLs, cast types

    # Convert back to DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df, glueContext, "transform_partner")

    logger.info(f"   ✅ Transform complete: {df.count():,} rows")
    return dyf_transformed


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Write Output (Append Mode — Critical for Bookmarks)
# ═══════════════════════════════════════════════════════════════════
def write_output(dyf):
    """
    Write transformed data to S3 as Parquet
    
    CRITICAL: Use APPEND mode, not OVERWRITE
    
    WHY:
    → Run 1: writes files A, B, C → output has A, B, C
    → Run 2: bookmark → only new file D → writes D
    → With APPEND: output has A, B, C, D ✅
    → With OVERWRITE: output has only D ❌ (A, B, C deleted!)
    
    PARTITIONING:
    → Write to date-partitioned paths
    → Each run's output goes to its own partition
    → Avoids overwriting previous run's data
    """
    logger.info(f"\n💾 Writing to: {TARGET_PATH}")

    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": TARGET_PATH,
            "partitionKeys": ["year", "month", "day"]
        },
        format="parquet",
        format_options={
            "compression": "snappy"
        },
        # ── BOOKMARK KEY FOR WRITE ──
        # Tracks what was written (for potential re-processing)
        transformation_ctx="write_partner_parquet"
    )

    logger.info(f"   ✅ Write complete")


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Main Execution
# ═══════════════════════════════════════════════════════════════════
start_time = datetime.now()

logger.info("=" * 60)
logger.info("🚀 BOOKMARKED CSV → PARQUET JOB")
logger.info("=" * 60)

# Read (bookmark filters to new files only)
dyf, row_count = read_with_bookmark()

if row_count > 0:
    # Transform
    dyf_transformed = transform(dyf)

    # Write
    write_output(dyf_transformed)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"\n✅ Processed {row_count:,} rows in {duration:.1f}s")
else:
    logger.info(f"\nℹ️  Nothing to process — all files already bookmarked")

# ══════════════════════════════════════════════════════════════
# CRITICAL: job.commit() saves the bookmark state
# Without this: bookmark is NOT updated
# Next run would reprocess the same files
# ══════════════════════════════════════════════════════════════
job.commit()
logger.info("📌 Bookmark committed — state saved for next run")