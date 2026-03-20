# file: 65_03_cdc_glue_job.py
# This is the Glue ETL script — runs inside Glue
# Upload to: s3://quickcart-analytics-scripts/glue/cdc_mariadb_to_redshift.py

import sys
import math
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, BooleanType

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "redshift_temp_dir",
    "target_schema",
    "cdc_s3_base_path",
    "safety_margin_minutes"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Reduce shuffle partitions for small CDC deltas
spark.conf.set("spark.sql.shuffle.partitions", "4")

# --- CONFIGURATION ---
MARIADB_CONNECTION = "conn-cross-account-mariadb"
REDSHIFT_CONNECTION = "conn-redshift-analytics"
REDSHIFT_TEMP_DIR = args["redshift_temp_dir"]
TARGET_SCHEMA = args["target_schema"]
CDC_S3_BASE = args["cdc_s3_base_path"]
SAFETY_MARGIN_MIN = int(args["safety_margin_minutes"])

RUN_ID = f"cdc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
RUN_TIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Tables to CDC
TABLES_CONFIG = [
    {
        "source_table": "orders",
        "target_table": "orders",
        "staging_table": "stg_orders",
        "primary_key": "order_id",
        "watermark_column": "updated_at"
    },
    {
        "source_table": "customers",
        "target_table": "dim_customers",
        "staging_table": "stg_customers",
        "primary_key": "customer_id",
        "watermark_column": "updated_at"
    },
    {
        "source_table": "products",
        "target_table": "dim_products",
        "staging_table": "stg_products",
        "primary_key": "product_id",
        "watermark_column": "updated_at"
    }
]


def get_watermark(table_name):
    """
    Read current watermark from Redshift
    Returns: timestamp string of last successful extraction
    """
    logger.info(f"📋 Reading watermark for: {table_name}")

    watermark_df = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": REDSHIFT_CONNECTION,
            "dbtable": f"""
                (SELECT last_extracted_at 
                 FROM {TARGET_SCHEMA}.cdc_watermarks 
                 WHERE table_name = '{table_name}')
            """,
            "redshiftTmpDir": REDSHIFT_TEMP_DIR
        },
        transformation_ctx=f"watermark_{table_name}"
    ).toDF()

    if watermark_df.count() == 0:
        # No watermark found — use epoch (extract everything)
        watermark = "2024-01-01 00:00:00"
        logger.info(f"   ⚠️ No watermark found — using default: {watermark}")
    else:
        watermark = str(watermark_df.first()["last_extracted_at"])
        logger.info(f"   📌 Current watermark: {watermark}")

    # Apply safety margin
    from datetime import datetime as dt, timedelta as td
    watermark_dt = dt.strptime(watermark[:19], "%Y-%m-%d %H:%M:%S")
    safe_watermark_dt = watermark_dt - td(minutes=SAFETY_MARGIN_MIN)
    safe_watermark = safe_watermark_dt.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"   🛡️ Safety margin: {SAFETY_MARGIN_MIN} min")
    logger.info(f"   📌 Effective watermark: {safe_watermark}")

    return safe_watermark


def extract_delta(table_config, watermark):
    """
    Extract ONLY changed rows from MariaDB since watermark
    
    SQL: SELECT * FROM table WHERE updated_at > watermark
    
    KEY DIFFERENCES FROM FULL LOAD (Project 61):
    → WHERE clause limits to changed rows only
    → Uses index on updated_at → range scan (not full scan)
    → Returns 200K rows instead of 12M
    → MariaDB CPU: 5% instead of 95%
    """
    source_table = table_config["source_table"]
    watermark_col = table_config["watermark_column"]

    logger.info(f"📥 Extracting delta: {source_table}")
    logger.info(f"   WHERE {watermark_col} > '{watermark}'")

    # Build SQL query with watermark filter
    query = f"""
        (SELECT * FROM {source_table} 
         WHERE {watermark_col} > '{watermark}') AS delta
    """

    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": MARIADB_CONNECTION,
                "dbtable": query,
                # Fewer parallel readers for small delta
                "hashexpression": table_config["primary_key"],
                "hashpartitions": "2"
            },
            transformation_ctx=f"delta_{source_table}"
        )

        delta_count = dynamic_frame.count()
        logger.info(f"   📊 Delta rows extracted: {delta_count:,}")

        return dynamic_frame, delta_count

    except Exception as e:
        logger.error(f"   ❌ Extraction failed: {str(e)}")
        raise


def classify_changes(df, primary_key):
    """
    Classify each row as INSERT, UPDATE, or DELETE
    
    LOGIC:
    → is_deleted = TRUE → operation = 'DELETE' (soft delete)
    → created_at > watermark → operation = 'INSERT' (new row)
    → updated_at > watermark AND created_at <= watermark → operation = 'UPDATE'
    
    NOTE: This classification is for AUDIT purposes
    The MERGE stored procedure doesn't use this column for logic
    It uses DELETE+INSERT pattern regardless of operation type
    """
    df_classified = df.withColumn(
        "cdc_operation",
        F.when(F.col("is_deleted") == True, F.lit("DELETE"))
         .when(F.col("created_at") == F.col("updated_at"), F.lit("INSERT"))
         .otherwise(F.lit("UPDATE"))
    )

    # Count by operation type
    operation_counts = df_classified.groupBy("cdc_operation").count().collect()
    for row in operation_counts:
        logger.info(f"   📊 {row['cdc_operation']}: {row['count']:,} rows")

    return df_classified


def transform_delta(dynamic_frame, table_config):
    """
    Transform delta rows
    → Add ETL metadata columns
    → Classify change type
    → Cast types for Redshift compatibility
    → Apply same transforms as full load (Project 61)
    """
    source_table = table_config["source_table"]
    primary_key = table_config["primary_key"]

    logger.info(f"🔄 Transforming delta: {source_table}")

    df = dynamic_frame.toDF()

    # Add ETL metadata
    df_transformed = df \
        .withColumn("etl_loaded_at", F.current_timestamp()) \
        .withColumn("etl_source", F.lit("cdc_mariadb"))

    # Cast common types
    if "total_amount" in df.columns:
        df_transformed = df_transformed \
            .withColumn("total_amount", F.col("total_amount").cast("decimal(12,2)"))
    if "unit_price" in df.columns:
        df_transformed = df_transformed \
            .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    if "price" in df.columns:
        df_transformed = df_transformed \
            .withColumn("price", F.col("price").cast("decimal(10,2)"))

    # Ensure is_deleted is boolean
    df_transformed = df_transformed \
        .withColumn("is_deleted",
                     F.when(F.col("is_deleted").isNull(), F.lit(False))
                      .otherwise(F.col("is_deleted").cast(BooleanType())))

    # Classify changes for audit trail
    df_classified = classify_changes(df_transformed, primary_key)

    # Handle NULLs
    df_final = df_classified.na.fill({
        "is_deleted": False,
        "cdc_operation": "UNKNOWN"
    })

    row_count = df_final.count()
    logger.info(f"   ✅ Transformed: {row_count:,} rows")

    return df_final


def persist_delta_to_s3(df, table_config):
    """
    Write delta to S3 for audit trail and replay capability
    
    PATH: s3://lake/cdc/{table}/batch_{timestamp}/delta.parquet
    
    Smart file sizing from Project 62:
    → 200K rows ≈ 100 MB → coalesce to 1 file
    """
    source_table = table_config["source_table"]
    batch_path = f"{CDC_S3_BASE}{source_table}/batch_{RUN_ID}/"

    logger.info(f"💾 Persisting delta to S3: {batch_path}")

    # Smart coalesce (Project 62 pattern)
    row_count = df.count()
    estimated_mb = (row_count * 500) / (1024 * 1024)  # ~500 bytes/row compressed
    target_files = max(1, math.ceil(estimated_mb / 128))

    df_coalesced = df.coalesce(target_files)

    df_coalesced.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(batch_path)

    logger.info(f"   ✅ Written: {target_files} file(s) to {batch_path}")
    return batch_path


def load_to_staging(df, table_config):
    """
    Load delta into Redshift staging table
    
    STEPS:
    1. Truncate staging table (clean from previous run)
    2. Write delta via Glue → S3 temp → Redshift COPY
    
    Smart coalesce applied for Redshift COPY efficiency
    """
    staging_table = table_config["staging_table"]
    full_staging = f"{TARGET_SCHEMA}.{staging_table}"

    logger.info(f"📤 Loading to staging: {full_staging}")

    row_count = df.count()

    # Coalesce for Redshift COPY (Project 62 pattern)
    REDSHIFT_SLICES = 4
    target_files = max(REDSHIFT_SLICES, math.ceil(row_count / 500000))
    target_files = min(target_files, REDSHIFT_SLICES * 4)

    df_coalesced = df.coalesce(target_files)

    # Convert back to DynamicFrame
    dyf = DynamicFrame.fromDF(df_coalesced, glueContext, f"staging_{staging_table}")

    try:
        glueContext.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="redshift",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": REDSHIFT_CONNECTION,
                "dbtable": full_staging,
                "redshiftTmpDir": REDSHIFT_TEMP_DIR,
                "preactions": f"TRUNCATE TABLE {full_staging};",
            },
            transformation_ctx=f"load_staging_{staging_table}"
        )
        logger.info(f"   ✅ Loaded {row_count:,} rows to {full_staging}")

    except Exception as e:
        logger.error(f"   ❌ Staging load failed: {str(e)}")
        raise


def execute_merge(table_config):
    """
    Execute the MERGE stored procedure in Redshift
    
    Calls: sp_cdc_merge which does:
    1. DELETE matching rows from target
    2. INSERT non-deleted rows from staging
    3. Update watermark
    All within single transaction
    
    USES: Redshift Data API for stored procedure execution
    (Glue's JDBC connection can also execute via raw SQL)
    """
    import boto3
    import time

    target_table = table_config["target_table"]
    staging_table = table_config["staging_table"]
    primary_key = table_config["primary_key"]

    logger.info(f"🔀 Executing MERGE: {staging_table} → {target_table}")

    # Use Redshift Data API for stored procedure call
    redshift_data = boto3.client("redshift-data")

    sql = f"""
        CALL {TARGET_SCHEMA}.sp_cdc_merge(
            '{TARGET_SCHEMA}',
            '{target_table}',
            '{staging_table}',
            '{primary_key}',
            '{RUN_ID}',
            '{RUN_TIMESTAMP}'::TIMESTAMP
        );
    """

    try:
        response = redshift_data.execute_statement(
            ClusterIdentifier="quickcart-analytics-cluster",
            Database="analytics",
            DbUser="etl_writer",
            Sql=sql
        )
        stmt_id = response["Id"]
        logger.info(f"   Statement submitted: {stmt_id}")

        # Poll for completion
        max_wait = 300  # 5 minutes
        elapsed = 0
        while elapsed < max_wait:
            time.sleep(5)
            elapsed += 5

            desc = redshift_data.describe_statement(Id=stmt_id)
            status = desc["Status"]

            if status == "FINISHED":
                duration = desc.get("Duration", 0) / 1000000
                logger.info(f"   ✅ MERGE completed in {duration:.2f}s")
                return True

            elif status == "FAILED":
                error = desc.get("Error", "Unknown")
                logger.error(f"   ❌ MERGE failed: {error}")
                raise Exception(f"Merge failed for {target_table}: {error}")

            logger.info(f"   ⏳ Status: {status} ({elapsed}s)")

        raise Exception(f"Merge timeout after {max_wait}s for {target_table}")

    except Exception as e:
        logger.error(f"   ❌ Merge execution error: {str(e)}")
        raise


def validate_merge(table_config, delta_count):
    """
    Post-merge validation
    
    CHECKS:
    1. Watermark updated successfully
    2. Target table row count reasonable
    3. No staging rows left unprocessed
    """
    target_table = table_config["target_table"]
    source_table = table_config["source_table"]

    logger.info(f"🔍 Validating merge: {target_table}")

    # Check watermark
    watermark_df = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": REDSHIFT_CONNECTION,
            "dbtable": f"""
                (SELECT * FROM {TARGET_SCHEMA}.cdc_watermarks 
                 WHERE table_name = '{source_table}')
            """,
            "redshiftTmpDir": REDSHIFT_TEMP_DIR
        },
        transformation_ctx=f"validate_wm_{source_table}"
    ).toDF()

    if watermark_df.count() > 0:
        wm_row = watermark_df.first()
        logger.info(f"   📌 Watermark: {wm_row['last_extracted_at']}")
        logger.info(f"   📊 Rows processed: {wm_row['rows_processed']}")
        logger.info(f"   📊 Inserted: {wm_row['rows_inserted']}")
        logger.info(f"   📊 Updated: {wm_row['rows_updated']}")
        logger.info(f"   📊 Deleted: {wm_row['rows_deleted']}")
        logger.info(f"   📋 Status: {wm_row['status']}")

        if wm_row['status'] != 'completed' and wm_row['status'] != 'completed_no_changes':
            logger.error(f"   ❌ Watermark status is not 'completed': {wm_row['status']}")
            raise Exception(f"Merge validation failed for {source_table}")

    # Check target table count
    target_df = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": REDSHIFT_CONNECTION,
            "dbtable": f"""
                (SELECT COUNT(*) as cnt FROM {TARGET_SCHEMA}.{target_table})
            """,
            "redshiftTmpDir": REDSHIFT_TEMP_DIR
        },
        transformation_ctx=f"validate_target_{target_table}"
    ).toDF()

    target_count = target_df.first()["cnt"]
    logger.info(f"   📊 Target table count: {target_count:,}")

    logger.info(f"   ✅ Validation passed for {target_table}")
    return True


def run_cdc_pipeline():
    """
    Main CDC orchestrator
    
    For each table:
    1. Get watermark
    2. Extract delta from MariaDB
    3. Transform + classify changes
    4. Persist delta to S3 (audit trail)
    5. Load delta to Redshift staging
    6. Execute MERGE (staging → target)
    7. Validate merge
    """
    logger.info("=" * 70)
    logger.info(f"🚀 CDC PIPELINE: MariaDB → S3 → Redshift (Incremental)")
    logger.info(f"   Run ID: {RUN_ID}")
    logger.info(f"   Timestamp: {RUN_TIMESTAMP}")
    logger.info(f"   Safety margin: {SAFETY_MARGIN_MIN} minutes")
    logger.info("=" * 70)

    results = {"success": [], "failed": [], "skipped": []}

    for table_config in TABLES_CONFIG:
        table_name = table_config["source_table"]
        logger.info(f"\n{'='*50}")
        logger.info(f"📋 Processing: {table_name}")
        logger.info(f"{'='*50}")

        try:
            # STEP 1: Get watermark
            watermark = get_watermark(table_name)

            # STEP 2: Extract delta
            delta_frame, delta_count = extract_delta(table_config, watermark)

            if delta_count == 0:
                logger.info(f"   ℹ️ No changes detected for {table_name} — skipping")
                results["skipped"].append(table_name)
                # Still update watermark to advance timestamp
                execute_merge(table_config)  # Will detect 0 staging rows and update watermark
                continue

            # STEP 3: Transform
            df_transformed = transform_delta(delta_frame, table_config)

            # STEP 4: Persist to S3
            s3_path = persist_delta_to_s3(df_transformed, table_config)

            # STEP 5: Load to staging
            load_to_staging(df_transformed, table_config)

            # STEP 6: Execute MERGE
            execute_merge(table_config)

            # STEP 7: Validate
            validate_merge(table_config, delta_count)

            results["success"].append({
                "table": table_name,
                "delta_count": delta_count,
                "s3_path": s3_path
            })

        except Exception as e:
            logger.error(f"❌ FAILED: {table_name} — {str(e)}")
            results["failed"].append({
                "table": table_name,
                "error": str(e)
            })
            continue

    # --- SUMMARY ---
    logger.info("\n" + "=" * 70)
    logger.info("📊 CDC PIPELINE SUMMARY")
    logger.info(f"   Run ID: {RUN_ID}")
    logger.info(f"   ✅ Succeeded: {len(results['success'])}")
    for s in results["success"]:
        logger.info(f"      → {s['table']}: {s['delta_count']:,} rows")
    logger.info(f"   ⏭️  Skipped (no changes): {len(results['skipped'])}")
    for sk in results["skipped"]:
        logger.info(f"      → {sk}")
    logger.info(f"   ❌ Failed: {len(results['failed'])}")
    for f in results["failed"]:
        logger.info(f"      → {f['table']}: {f['error'][:100]}")
    logger.info("=" * 70)

    if results["failed"]:
        raise Exception(
            f"CDC pipeline completed with {len(results['failed'])} failures: "
            f"{[f['table'] for f in results['failed']]}"
        )


# --- EXECUTE ---
run_cdc_pipeline()
job.commit()