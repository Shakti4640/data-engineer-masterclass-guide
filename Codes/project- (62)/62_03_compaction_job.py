# file: 62_03_compaction_job.py
# Purpose: Glue job that compacts existing small files in S3
# Run: scheduled weekly or triggered after detecting small file problem
# Upload to: s3://quickcart-analytics-scripts/glue/compact_small_files.py

import sys
import math
import boto3
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_path",           # e.g., s3://bucket/silver/orders/
    "target_path",           # e.g., s3://bucket/silver/orders_compacted/
    "target_file_size_mb",   # e.g., 128
    "file_format",           # e.g., parquet
    "partition_keys",        # e.g., "year,month,day" or "none"
    "min_files_threshold"    # e.g., 100 — only compact if file count exceeds this
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

SOURCE_PATH = args["source_path"]
TARGET_PATH = args["target_path"]
TARGET_FILE_SIZE_MB = int(args["target_file_size_mb"])
FILE_FORMAT = args["file_format"]
PARTITION_KEYS = args["partition_keys"].split(",") if args["partition_keys"] != "none" else []
MIN_FILES_THRESHOLD = int(args["min_files_threshold"])


def analyze_current_state(path):
    """
    Analyze existing files in S3 path
    → Count files, total size, average size
    → Determine if compaction is needed
    """
    logger.info(f"🔍 Analyzing: {path}")

    # Use Spark to read and count
    try:
        df = spark.read.format(FILE_FORMAT).load(path)
        total_rows = df.count()

        # Count physical files using Spark's input files
        input_files = df.inputFiles()
        file_count = len(input_files)

        # Estimate total size
        # Use S3 API for accurate sizes
        s3 = boto3.client("s3")
        bucket = path.replace("s3://", "").split("/")[0]
        prefix = "/".join(path.replace("s3://", "").split("/")[1:])

        total_bytes = 0
        s3_file_count = 0
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(f".{FILE_FORMAT}") or \
                   obj["Key"].endswith(".snappy.parquet"):
                    total_bytes += obj["Size"]
                    s3_file_count += 1

        total_mb = total_bytes / (1024 * 1024)
        avg_mb = total_mb / s3_file_count if s3_file_count > 0 else 0

        analysis = {
            "total_rows": total_rows,
            "file_count": s3_file_count,
            "total_mb": round(total_mb, 2),
            "avg_file_mb": round(avg_mb, 4),
            "needs_compaction": s3_file_count > MIN_FILES_THRESHOLD and avg_mb < 64
        }

        logger.info(f"   📊 Analysis results:")
        logger.info(f"      Total rows: {total_rows:,}")
        logger.info(f"      File count: {s3_file_count:,}")
        logger.info(f"      Total size: {total_mb:.2f} MB")
        logger.info(f"      Avg file size: {avg_mb:.4f} MB")
        logger.info(f"      Needs compaction: {analysis['needs_compaction']}")

        return analysis, df

    except Exception as e:
        logger.error(f"   ❌ Analysis failed: {str(e)}")
        raise


def compact_dataframe(df, total_mb):
    """
    Compact DataFrame to optimal file count
    
    STRATEGY:
    → Calculate target file count based on data size
    → Use repartition (not coalesce) for EVEN distribution
    → WHY repartition here: input is many small files = likely skewed
    → coalesce would merge adjacent = still skewed output
    → repartition shuffles for even output
    → Cost of shuffle is acceptable for a maintenance job
    """
    optimal_count = max(1, math.ceil(total_mb / TARGET_FILE_SIZE_MB))

    logger.info(f"🔄 Compacting:")
    logger.info(f"   Current partitions: {df.rdd.getNumPartitions()}")
    logger.info(f"   Target file count: {optimal_count}")
    logger.info(f"   Using: repartition (full shuffle for even output)")

    # repartition for EVEN file sizes
    df_compacted = df.repartition(optimal_count)

    return df_compacted, optimal_count


def write_compacted(df_compacted, target_path, file_count):
    """
    Write compacted data to target path
    """
    logger.info(f"📝 Writing {file_count} compacted file(s) to: {target_path}")

    writer = df_compacted.write \
        .mode("overwrite") \
        .format(FILE_FORMAT) \
        .option("compression", "snappy")

    if PARTITION_KEYS:
        writer = writer.partitionBy(*PARTITION_KEYS)

    writer.save(target_path)
    logger.info(f"   ✅ Write complete")


def validate_compaction(source_path, target_path, expected_rows):
    """
    CRITICAL: Verify compacted data matches original
    
    CHECKS:
    1. Row count matches
    2. Target files exist
    3. Target file sizes are in optimal range
    
    IF VALIDATION FAILS: do NOT delete source files
    """
    logger.info(f"🔍 Validating compaction...")

    try:
        # Read compacted data
        df_compacted = spark.read.format(FILE_FORMAT).load(target_path)
        compacted_rows = df_compacted.count()

        # Check 1: Row count
        row_match = compacted_rows == expected_rows
        logger.info(f"   Row count — Source: {expected_rows:,}, Target: {compacted_rows:,}")

        if not row_match:
            logger.error(f"   ❌ ROW COUNT MISMATCH!")
            logger.error(f"   → Source: {expected_rows:,}")
            logger.error(f"   → Target: {compacted_rows:,}")
            logger.error(f"   → Difference: {abs(expected_rows - compacted_rows):,}")
            return False

        logger.info(f"   ✅ Row counts match: {compacted_rows:,}")

        # Check 2: File count and sizes in target
        s3 = boto3.client("s3")
        bucket = target_path.replace("s3://", "").split("/")[0]
        prefix = "/".join(target_path.replace("s3://", "").split("/")[1:])

        target_files = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet") or \
                   obj["Key"].endswith(".snappy.parquet"):
                    target_files.append({
                        "key": obj["Key"],
                        "size_mb": round(obj["Size"] / (1024 * 1024), 2)
                    })

        if len(target_files) == 0:
            logger.error(f"   ❌ No target files found at {target_path}")
            return False

        logger.info(f"   ✅ Target file count: {len(target_files)}")

        # Check 3: File size distribution
        sizes = [f["size_mb"] for f in target_files]
        min_size = min(sizes)
        max_size = max(sizes)
        avg_size = sum(sizes) / len(sizes)

        logger.info(f"   📐 File sizes:")
        logger.info(f"      Min: {min_size:.2f} MB")
        logger.info(f"      Max: {max_size:.2f} MB")
        logger.info(f"      Avg: {avg_size:.2f} MB")

        # Check for skew: max should not be > 3x min (unless single file)
        if len(sizes) > 1 and max_size > 0 and min_size > 0:
            skew_ratio = max_size / min_size
            if skew_ratio > 3.0:
                logger.warn(f"   ⚠️ File size skew detected: {skew_ratio:.1f}x")
                logger.warn(f"   → Consider repartition() instead of coalesce()")
            else:
                logger.info(f"   ✅ File size skew ratio: {skew_ratio:.1f}x (acceptable)")

        # Check 4: All files above minimum useful size
        tiny_files = [f for f in target_files if f["size_mb"] < 1.0]
        if tiny_files:
            logger.warn(f"   ⚠️ {len(tiny_files)} compacted files still < 1 MB")
            for tf in tiny_files[:5]:
                logger.warn(f"      → {tf['key']}: {tf['size_mb']} MB")

        logger.info(f"   ✅ Compaction validation PASSED")
        return True

    except Exception as e:
        logger.error(f"   ❌ Validation failed: {str(e)}")
        return False


def cleanup_source_files(source_path):
    """
    Delete original small files AFTER validation passes
    
    CAUTION:
    → This is DESTRUCTIVE — cannot be undone (unless versioning is enabled)
    → Only called after validate_compaction returns True
    → Uses S3 batch delete for efficiency (1000 keys per request)
    
    ALTERNATIVE STRATEGY (safer):
    → Don't delete originals
    → Update Glue Catalog to point to target_path
    → Delete originals after 7 days (lifecycle rule)
    → This gives time to catch issues
    """
    logger.info(f"🗑️ Cleaning up source files: {source_path}")

    s3 = boto3.client("s3")
    bucket = source_path.replace("s3://", "").split("/")[0]
    prefix = "/".join(source_path.replace("s3://", "").split("/")[1:])

    deleted_count = 0
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        if not contents:
            continue

        # Batch delete (max 1000 per request)
        objects_to_delete = [{"Key": obj["Key"]} for obj in contents]

        # Process in batches of 1000
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i:i + 1000]
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": batch, "Quiet": True}
            )
            deleted_count += len(batch)

    logger.info(f"   ✅ Deleted {deleted_count:,} source files")
    return deleted_count


def swap_paths(source_path, target_path):
    """
    Move compacted files from target_path to source_path
    
    STRATEGY:
    1. Source files already deleted (cleanup_source_files)
    2. Copy compacted files from target to source location
    3. Delete compacted files from target location
    
    WHY NOT just write to source directly with overwrite?
    → Overwrite in Spark: deletes ALL files first, THEN writes
    → If write fails: ALL data lost (original AND new)
    → Write-to-temp → validate → swap is SAFE
    
    ALTERNATIVE: Just update Glue Catalog to point to target_path
    → Avoids copy entirely
    → But: downstream jobs may hardcode source_path
    """
    logger.info(f"🔄 Swapping: {target_path} → {source_path}")

    s3 = boto3.client("s3")
    source_bucket = source_path.replace("s3://", "").split("/")[0]
    target_bucket = target_path.replace("s3://", "").split("/")[0]
    target_prefix = "/".join(target_path.replace("s3://", "").split("/")[1:])
    source_prefix = "/".join(source_path.replace("s3://", "").split("/")[1:])

    copied_count = 0
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=target_bucket, Prefix=target_prefix):
        for obj in page.get("Contents", []):
            target_key = obj["Key"]
            # Replace target prefix with source prefix
            relative_key = target_key[len(target_prefix):]
            source_key = source_prefix + relative_key

            s3.copy_object(
                Bucket=source_bucket,
                Key=source_key,
                CopySource={"Bucket": target_bucket, "Key": target_key}
            )
            copied_count += 1

    logger.info(f"   ✅ Copied {copied_count} files to source path")

    # Clean up target (temporary) path
    for page in paginator.paginate(Bucket=target_bucket, Prefix=target_prefix):
        contents = page.get("Contents", [])
        if contents:
            s3.delete_objects(
                Bucket=target_bucket,
                Delete={
                    "Objects": [{"Key": obj["Key"]} for obj in contents],
                    "Quiet": True
                }
            )

    logger.info(f"   ✅ Cleaned up temporary target path")
    return copied_count


def update_glue_catalog(database, table_name, new_location=None):
    """
    Update Glue Catalog after compaction
    
    IF we swapped paths back to original location:
    → Catalog already points correctly — just run MSCK REPAIR
    
    IF we kept compacted files in new location:
    → Must update table's Location property
    """
    glue_client = boto3.client("glue")

    if new_location:
        # Update table location to point to compacted path
        try:
            table = glue_client.get_table(
                DatabaseName=database,
                Name=table_name
            )

            table_input = table["Table"]
            # Remove fields that can't be in update request
            for key in ["DatabaseName", "CreateTime", "UpdateTime",
                        "CreatedBy", "IsRegisteredWithLakeFormation",
                        "CatalogId", "VersionId"]:
                table_input.pop(key, None)

            table_input["StorageDescriptor"]["Location"] = new_location

            glue_client.update_table(
                DatabaseName=database,
                TableInput=table_input
            )
            logger.info(f"   ✅ Catalog updated: {database}.{table_name} → {new_location}")

        except Exception as e:
            logger.error(f"   ❌ Catalog update failed: {str(e)}")
            raise
    else:
        # Just repair partitions (location unchanged)
        logger.info(f"   ℹ️ Location unchanged — partitions will be detected by crawler")


def run_compaction():
    """
    Main compaction orchestrator
    
    FLOW:
    1. Analyze source files → determine if compaction needed
    2. Read all source data into DataFrame
    3. Repartition to optimal file count
    4. Write to temporary target path
    5. Validate: row count match + file sizes
    6. If valid: delete source → move target to source
    7. If invalid: abort, keep source intact, alert
    """
    logger.info("=" * 70)
    logger.info("🔧 S3 SMALL FILE COMPACTION JOB")
    logger.info(f"   Source: {SOURCE_PATH}")
    logger.info(f"   Target: {TARGET_PATH}")
    logger.info(f"   Target file size: {TARGET_FILE_SIZE_MB} MB")
    logger.info(f"   Format: {FILE_FORMAT}")
    logger.info(f"   Partition keys: {PARTITION_KEYS if PARTITION_KEYS else 'None'}")
    logger.info(f"   Min files threshold: {MIN_FILES_THRESHOLD}")
    logger.info("=" * 70)

    # --- STEP 1: ANALYZE ---
    analysis, df_source = analyze_current_state(SOURCE_PATH)

    if not analysis["needs_compaction"]:
        logger.info(f"\n✅ NO COMPACTION NEEDED")
        logger.info(f"   File count ({analysis['file_count']}) <= threshold ({MIN_FILES_THRESHOLD})")
        logger.info(f"   OR avg file size ({analysis['avg_file_mb']:.2f} MB) >= 64 MB")
        logger.info(f"   Skipping compaction.")
        job.commit()
        return

    logger.info(f"\n⚠️ COMPACTION NEEDED")
    logger.info(f"   {analysis['file_count']:,} files averaging {analysis['avg_file_mb']:.4f} MB")

    # --- STEP 2: COMPACT ---
    df_compacted, target_file_count = compact_dataframe(
        df_source, analysis["total_mb"]
    )

    # --- STEP 3: WRITE TO TEMP TARGET ---
    write_compacted(df_compacted, TARGET_PATH, target_file_count)

    # --- STEP 4: VALIDATE ---
    is_valid = validate_compaction(
        SOURCE_PATH, TARGET_PATH, analysis["total_rows"]
    )

    if not is_valid:
        logger.error("❌ VALIDATION FAILED — ABORTING COMPACTION")
        logger.error("   Source files preserved at original location")
        logger.error("   Target files at temporary location — manual cleanup needed")
        raise Exception("Compaction validation failed — source data preserved")

    # --- STEP 5: SWAP ---
    logger.info("\n🔄 Validation passed — performing atomic swap")

    # Delete original small files
    cleanup_source_files(SOURCE_PATH)

    # Move compacted files to original location
    swap_paths(SOURCE_PATH, TARGET_PATH)

    # --- STEP 6: SUMMARY ---
    logger.info("\n" + "=" * 70)
    logger.info("📊 COMPACTION SUMMARY")
    logger.info(f"   Before: {analysis['file_count']:,} files, "
                f"{analysis['avg_file_mb']:.4f} MB avg")
    logger.info(f"   After:  {target_file_count} files, "
                f"~{analysis['total_mb'] / target_file_count:.1f} MB avg")
    logger.info(f"   Reduction: {analysis['file_count']:,} → {target_file_count} files "
                f"({(1 - target_file_count / analysis['file_count']) * 100:.1f}% reduction)")
    logger.info(f"   Row count: {analysis['total_rows']:,} (preserved)")
    logger.info("=" * 70)


# --- EXECUTE ---
run_compaction()
job.commit()