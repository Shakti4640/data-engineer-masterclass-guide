# file: 32_02_glue_etl_s3_to_redshift.py
# Purpose: Read Parquet from S3 bronze layer → Transform → Load into Redshift
# Execution: Runs INSIDE Glue service
# Upload to: s3://quickcart-datalake-prod/scripts/glue/s3_to_redshift.py

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
    "source_s3_path",            # s3://bucket/bronze/mariadb/
    "target_tables",             # Comma-separated: orders,customers,products
    "redshift_connection",       # Glue connection name for Redshift
    "redshift_schema",           # analytics
    "redshift_tmp_dir",          # s3://bucket/tmp/glue-redshift/
    "redshift_iam_role",         # ARN of Redshift S3 read role
    "extraction_date"            # YYYY-MM-DD — which partition to load
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

# ═══════════════════════════════════════════════════════════════════
# STEP 2: Parse Parameters
# ═══════════════════════════════════════════════════════════════════
SOURCE_S3_PATH = args["source_s3_path"]
TARGET_TABLES = args["target_tables"].split(",")
REDSHIFT_CONN = args["redshift_connection"]
REDSHIFT_SCHEMA = args["redshift_schema"]
REDSHIFT_TMP_DIR = args["redshift_tmp_dir"]
REDSHIFT_IAM_ROLE = args["redshift_iam_role"]
EXTRACTION_DATE = args["extraction_date"]

ext_date = datetime.strptime(EXTRACTION_DATE, "%Y-%m-%d")
YEAR = ext_date.strftime("%Y")
MONTH = ext_date.strftime("%m")
DAY = ext_date.strftime("%d")

logger.info(f"Loading S3 Parquet → Redshift")
logger.info(f"Source: {SOURCE_S3_PATH}")
logger.info(f"Tables: {TARGET_TABLES}")
logger.info(f"Date: {EXTRACTION_DATE}")

# ═══════════════════════════════════════════════════════════════════
# STEP 3: Table Configuration — Source-to-Target Mapping
# ═══════════════════════════════════════════════════════════════════
TABLE_CONFIG = {
    "orders": {
        "source_prefix": "orders",
        "redshift_table": "fact_orders",
        "primary_key": "order_id",
        "column_order": [
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "status", "order_date",
            "_extraction_date", "_extraction_timestamp",
            "_source_system", "_source_database", "_source_table"
        ]
    },
    "customers": {
        "source_prefix": "customers",
        "redshift_table": "dim_customers",
        "primary_key": "customer_id",
        "column_order": [
            "customer_id", "name", "email",
            "city", "state", "tier", "signup_date",
            "_extraction_date", "_extraction_timestamp",
            "_source_system", "_source_database", "_source_table"
        ]
    },
    "products": {
        "source_prefix": "products",
        "redshift_table": "dim_products",
        "primary_key": "product_id",
        "column_order": [
            "product_id", "name", "category",
            "price", "stock_quantity", "is_active",
            "_extraction_date", "_extraction_timestamp",
            "_source_system", "_source_database", "_source_table"
        ]
    }
}


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Read Source Parquet from S3
# ═══════════════════════════════════════════════════════════════════
def read_source_parquet(table_name, config):
    """
    Read specific date partition from S3 bronze layer
    
    Path: s3://bucket/bronze/mariadb/orders/year=2025/month=01/day=15/
    
    WHY read specific partition (not entire table):
    → We only want TODAY's extraction
    → Reading all partitions = reading entire history = wasteful
    → Partition pruning: Spark only reads files in the target path
    """
    source_prefix = config["source_prefix"]
    partition_path = (
        f"{SOURCE_S3_PATH}{source_prefix}/"
        f"year={YEAR}/month={MONTH}/day={DAY}/"
    )

    logger.info(f"\n📖 Reading: {partition_path}")

    try:
        # Read Parquet using Spark (faster than Glue DynamicFrame for S3)
        df = spark.read.parquet(partition_path)
        row_count = df.count()

        logger.info(f"   Rows read: {row_count:,}")

        if row_count == 0:
            logger.warn(f"   ⚠️  No data found for {table_name} on {EXTRACTION_DATE}")
            return None, 0

        return df, row_count

    except Exception as e:
        logger.error(f"   ❌ Failed to read {partition_path}: {e}")
        return None, 0


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Transform for Redshift Compatibility
# ═══════════════════════════════════════════════════════════════════
def transform_for_redshift(df, table_name, config):
    """
    Ensure data matches Redshift table schema:
    
    1. Column order must match Redshift DDL
       → COPY maps by position, not by name
    2. Data types must be compatible
    3. Remove partition columns (year, month, day) — not in Redshift table
    4. Handle NULLs — Redshift has strict NULL handling
    """
    logger.info(f"\n🔄 Transforming {table_name} for Redshift compatibility...")

    # ── REMOVE S3 PARTITION COLUMNS ──
    # These were added for S3/Athena partitioning — not needed in Redshift
    columns_to_drop = ["year", "month", "day"]
    for col in columns_to_drop:
        if col in df.columns:
            df = df.drop(col)
            logger.info(f"   Dropped partition column: {col}")

    # ── REORDER COLUMNS TO MATCH REDSHIFT DDL ──
    # CRITICAL: COPY command maps columns by position
    # If column order doesn't match → data goes to wrong columns
    target_columns = config["column_order"]
    
    # Verify all expected columns exist
    missing = set(target_columns) - set(df.columns)
    if missing:
        logger.warn(f"   ⚠️  Missing columns: {missing}")
        # Add missing columns as NULL
        for col_name in missing:
            df = df.withColumn(col_name, F.lit(None).cast("string"))
            logger.info(f"   Added missing column as NULL: {col_name}")

    extra = set(df.columns) - set(target_columns)
    if extra:
        logger.info(f"   Dropping extra columns: {extra}")
        for col_name in extra:
            df = df.drop(col_name)

    # Reorder to match Redshift DDL
    df = df.select(target_columns)
    logger.info(f"   Column order: {target_columns}")

    # ── TYPE CASTING ──
    # Ensure string lengths don't exceed Redshift VARCHAR limits
    from pyspark.sql.types import StringType
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            # Truncate to 200 chars (max VARCHAR in our DDL)
            df = df.withColumn(
                field.name,
                F.substring(F.col(field.name), 1, 200)
            )

    # Cast order_date to proper date format if it's a string
    if table_name == "orders" and "order_date" in df.columns:
        df = df.withColumn(
            "order_date",
            F.to_date(F.col("order_date"), "yyyy-MM-dd")
        )

    if table_name == "customers" and "signup_date" in df.columns:
        df = df.withColumn(
            "signup_date",
            F.to_date(F.col("signup_date"), "yyyy-MM-dd")
        )

    logger.info(f"   ✅ Transform complete: {len(df.columns)} columns")
    return df


# ═══════════════════════════════════════════════════════════════════
# STEP 6: Build Pre-Actions and Post-Actions SQL
# ═══════════════════════════════════════════════════════════════════
def build_sql_actions(table_name, config):
    """
    Build the SQL statements for staging table pattern
    
    FLOW:
    PRE-ACTION:
    → Create staging table (clone of target)
    → Truncate staging table (safety)
    
    [Glue loads data into staging table via COPY]
    
    POST-ACTION:
    → BEGIN transaction
    → DELETE from target WHERE primary_key IN staging
    → INSERT into target SELECT * FROM staging
    → COMMIT
    → DROP staging table
    
    WHY NOT just TRUNCATE target + COPY directly:
    → If COPY fails, target table is empty → dashboard shows nothing
    → With staging: if anything fails, target table is untouched
    → Staging pattern = zero-downtime loading
    """
    schema = REDSHIFT_SCHEMA
    target_table = config["redshift_table"]
    staging_table = f"stg_{target_table}"
    primary_key = config["primary_key"]

    # ── PRE-ACTIONS ──
    # Run BEFORE Glue loads data into Redshift
    # Creates a clean staging table to receive the incoming data
    preactions = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{staging_table} (LIKE {schema}.{target_table});
        TRUNCATE TABLE {schema}.{staging_table};
    """

    # ── POST-ACTIONS ──
    # Run AFTER Glue loads data into staging table
    # Merges staging into target using DELETE + INSERT pattern
    #
    # WHY BEGIN...COMMIT:
    # → DELETE + INSERT must be ATOMIC
    # → If INSERT fails after DELETE, we'd lose data
    # → Transaction ensures all-or-nothing
    #
    # WHY DELETE + INSERT (not UPDATE):
    # → Redshift UPDATE is slow (rewrites entire row)
    # → DELETE + INSERT is Amazon's recommended upsert pattern
    # → Internally: Redshift marks old rows as deleted, appends new rows
    postactions = f"""
        BEGIN;

        -- Step 1: Delete existing rows that will be replaced
        DELETE FROM {schema}.{target_table}
        USING {schema}.{staging_table}
        WHERE {schema}.{target_table}.{primary_key} 
            = {schema}.{staging_table}.{primary_key};

        -- Step 2: Insert all rows from staging (new + updated)
        INSERT INTO {schema}.{target_table}
        SELECT * FROM {schema}.{staging_table};

        -- Step 3: Commit the transaction
        COMMIT;

        -- Step 4: Drop staging table (outside transaction — cleanup)
        DROP TABLE IF EXISTS {schema}.{staging_table};

        -- Step 5: Update table statistics for query optimizer
        ANALYZE {schema}.{target_table};
    """

    logger.info(f"\n📝 SQL Actions for {target_table}:")
    logger.info(f"   Pre-action: CREATE + TRUNCATE {staging_table}")
    logger.info(f"   Post-action: DELETE/INSERT merge on {primary_key} + ANALYZE")

    return preactions, postactions, staging_table


# ═══════════════════════════════════════════════════════════════════
# STEP 7: Write to Redshift via Glue Connector
# ═══════════════════════════════════════════════════════════════════
def write_to_redshift(df, table_name, config):
    """
    Write DataFrame to Redshift using Glue's built-in connector
    
    WHAT HAPPENS INTERNALLY:
    1. Glue executes preactions SQL on Redshift
    2. Glue writes DataFrame as CSV to redshiftTmpDir on S3
    3. Glue issues COPY command to load from temp S3 → staging table
    4. Glue executes postactions SQL on Redshift (merge + cleanup)
    5. Glue deletes temp CSV files from S3
    
    CONNECTION OPTIONS EXPLAINED:
    → useConnectionProperties: use Glue Connection for JDBC URL + creds
    → connectionName: which Glue Connection to use
    → dbtable: target table name (staging table — Glue loads here)
    → preactions: SQL to run BEFORE load
    → postactions: SQL to run AFTER load
    → redshiftTmpDir: S3 path for temp CSV staging files
    → aws_iam_role: Redshift's IAM Role ARN for COPY command
    """
    preactions, postactions, staging_table = build_sql_actions(
        table_name, config
    )

    target_table = config["redshift_table"]
    full_staging_table = f"{REDSHIFT_SCHEMA}.{staging_table}"

    logger.info(f"\n🚀 Writing {table_name} → Redshift {REDSHIFT_SCHEMA}.{target_table}")
    logger.info(f"   Via staging table: {full_staging_table}")
    logger.info(f"   Temp S3 dir: {REDSHIFT_TMP_DIR}")

    start_time = datetime.now()

    # Convert Spark DataFrame → Glue DynamicFrame
    dyf = DynamicFrame.fromDF(df, glueContext, f"redshift_{table_name}")

    # ── WRITE TO REDSHIFT ──
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection=REDSHIFT_CONN,
        connection_options={
            # Target: staging table (NOT the final target)
            # Glue loads into staging; postactions merge into target
            "dbtable": full_staging_table,

            # Database schema
            "database": REDSHIFT_SCHEMA,

            # Pre-actions: create + truncate staging
            "preactions": preactions,

            # Post-actions: merge staging → target + cleanup
            "postactions": postactions,

            # Temp S3 directory for COPY staging files
            # Each write creates a unique subfolder automatically
            "redshiftTmpDir": REDSHIFT_TMP_DIR,

            # Redshift's IAM Role for reading S3 during COPY
            # This is NOT Glue's role — it's the role attached to Redshift cluster
            "aws_iam_role": REDSHIFT_IAM_ROLE
        },
        redshift_tmp_dir=REDSHIFT_TMP_DIR,
        transformation_ctx=f"write_redshift_{table_name}"
    )

    write_time = (datetime.now() - start_time).total_seconds()

    logger.info(f"   ✅ Write complete in {write_time:.1f} seconds")
    logger.info(f"   → Data merged into {REDSHIFT_SCHEMA}.{target_table}")

    return write_time


# ═══════════════════════════════════════════════════════════════════
# STEP 8: Post-Load Validation (Query Redshift from Glue)
# ═══════════════════════════════════════════════════════════════════
def validate_redshift_load(table_name, config, source_count):
    """
    After loading, verify data in Redshift matches source count
    
    HOW: Read from Redshift via JDBC, count rows
    This uses Glue's JDBC connection to query Redshift
    """
    target_table = config["redshift_table"]
    full_table = f"{REDSHIFT_SCHEMA}.{target_table}"

    logger.info(f"\n🔍 Validating {full_table} in Redshift...")

    try:
        # Read row count from Redshift
        # Use Spark JDBC directly for a simple count query
        count_query = f"(SELECT COUNT(*) as cnt FROM {full_table}) tmp"

        count_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:redshift://{args.get('redshift_host', 'quickcart-dwh.cxyz123.us-east-2.redshift.amazonaws.com')}:5439/quickcart_warehouse") \
            .option("dbtable", count_query) \
            .option("user", "glue_loader") \
            .option("password", "RETRIEVED_FROM_CONNECTION") \
            .load()

        # Alternative: use Glue connection for count
        # This is simpler but less flexible
        count_dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": REDSHIFT_CONN,
                "dbtable": count_query,
                "redshiftTmpDir": REDSHIFT_TMP_DIR
            },
            transformation_ctx=f"validate_{table_name}"
        )

        if count_dyf.count() > 0:
            target_count = count_dyf.toDF().collect()[0]["cnt"]
            logger.info(f"   Source rows (S3):     {source_count:,}")
            logger.info(f"   Target rows (Redshift): {target_count:,}")

            if target_count >= source_count:
                logger.info(f"   ✅ Validation PASSED")
                return True
            else:
                logger.warn(f"   ⚠️  Row count mismatch!")
                logger.warn(f"   → Target has fewer rows than source")
                logger.warn(f"   → Check STL_LOAD_ERRORS on Redshift")
                return False
        else:
            logger.warn(f"   ⚠️  Could not read count from Redshift")
            return False

    except Exception as e:
        logger.error(f"   ❌ Validation failed: {e}")
        logger.error(f"   → Run manual check: SELECT COUNT(*) FROM {full_table}")
        return False


# ═══════════════════════════════════════════════════════════════════
# STEP 9: Alternative Write Method — More Control
# ═══════════════════════════════════════════════════════════════════
def write_to_redshift_advanced(df, table_name, config):
    """
    Alternative approach with more explicit control
    
    WHEN TO USE THIS vs STEP 7:
    → Step 7 (from_jdbc_conf): simpler, handles most cases
    → This method: when you need custom COPY options,
      error handling, or manual staging control
    
    This manually:
    1. Writes to S3 as CSV
    2. Builds COPY command
    3. Executes via JDBC
    4. Handles errors explicitly
    """
    import uuid

    staging_table = f"stg_{config['redshift_table']}"
    target_table = config["redshift_table"]
    primary_key = config["primary_key"]
    run_id = str(uuid.uuid4())[:8]

    # Unique temp path for this run (prevents conflicts)
    temp_path = f"{REDSHIFT_TMP_DIR}{table_name}/{run_id}/"

    logger.info(f"\n🚀 Advanced write: {table_name} → Redshift")
    logger.info(f"   Temp path: {temp_path}")

    # ── STEP A: Write DataFrame to S3 as CSV ──
    logger.info(f"   Writing temp CSV files to S3...")
    df.coalesce(4).write \
        .mode("overwrite") \
        .option("header", "false") \
        .option("delimiter", "|") \
        .option("nullValue", "") \
        .option("emptyValue", "") \
        .csv(temp_path)

    logger.info(f"   ✅ Temp CSVs written to {temp_path}")

    # ── STEP B: Build and Execute SQL via JDBC ──
    # Use Spark JDBC to run arbitrary SQL on Redshift
    schema = REDSHIFT_SCHEMA

    merge_sql = f"""
        -- Create staging table
        CREATE TABLE IF NOT EXISTS {schema}.{staging_table} 
            (LIKE {schema}.{target_table});
        TRUNCATE TABLE {schema}.{staging_table};

        -- COPY from S3 into staging
        COPY {schema}.{staging_table}
        FROM '{temp_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        DELIMITER '|'
        BLANKSASNULL
        EMPTYASNULL
        TRUNCATECOLUMNS
        TIMEFORMAT 'auto'
        DATEFORMAT 'auto'
        REGION '{args.get("region", "us-east-2")}';

        -- Merge: delete + insert
        BEGIN;
        DELETE FROM {schema}.{target_table}
        USING {schema}.{staging_table}
        WHERE {schema}.{target_table}.{primary_key} 
            = {schema}.{staging_table}.{primary_key};
        
        INSERT INTO {schema}.{target_table}
        SELECT * FROM {schema}.{staging_table};
        COMMIT;

        -- Cleanup
        DROP TABLE IF EXISTS {schema}.{staging_table};
        ANALYZE {schema}.{target_table};
    """

    logger.info(f"   Executing merge SQL on Redshift...")
    logger.info(f"   → COPY + DELETE/INSERT + ANALYZE")

    # NOTE: In production, you'd execute this via psycopg2 or 
    # redshift-connector library, not via Spark JDBC
    # Spark JDBC doesn't support multi-statement execution well
    # This is shown for educational purposes

    return merge_sql


# ═══════════════════════════════════════════════════════════════════
# STEP 10: Main Orchestration Loop
# ═══════════════════════════════════════════════════════════════════
load_summary = []
overall_start = datetime.now()

logger.info("=" * 70)
logger.info("🚀 S3 PARQUET → REDSHIFT LOAD STARTING")
logger.info(f"   Source: {SOURCE_S3_PATH}")
logger.info(f"   Target: Redshift {REDSHIFT_SCHEMA}")
logger.info(f"   Date: {EXTRACTION_DATE}")
logger.info(f"   Tables: {TARGET_TABLES}")
logger.info("=" * 70)

for table_name in TARGET_TABLES:
    table_name = table_name.strip()
    table_start = datetime.now()

    config = TABLE_CONFIG.get(table_name)
    if not config:
        logger.warn(f"⚠️  No configuration for table: {table_name} — skipping")
        load_summary.append({
            "table": table_name, "status": "SKIPPED",
            "rows": 0, "reason": "No config found"
        })
        continue

    try:
        # ── READ SOURCE ──
        df, source_count = read_source_parquet(table_name, config)
        if df is None:
            load_summary.append({
                "table": table_name, "status": "SKIPPED",
                "rows": 0, "reason": "No source data"
            })
            continue

        # ── TRANSFORM ──
        df_transformed = transform_for_redshift(df, table_name, config)

        # ── WRITE TO REDSHIFT ──
        write_time = write_to_redshift(df_transformed, table_name, config)

        # ── RECORD SUCCESS ──
        table_time = (datetime.now() - table_start).total_seconds()
        load_summary.append({
            "table": table_name,
            "status": "SUCCESS",
            "rows": source_count,
            "write_seconds": write_time,
            "total_seconds": table_time
        })

        logger.info(f"\n✅ {table_name}: {source_count:,} rows → "
                     f"Redshift in {table_time:.1f}s")

    except Exception as e:
        logger.error(f"\n❌ FAILED: {table_name}")
        logger.error(f"   Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

        load_summary.append({
            "table": table_name,
            "status": "FAILED",
            "rows": 0,
            "error": str(e)
        })
        continue


# ═══════════════════════════════════════════════════════════════════
# STEP 11: Final Summary
# ═══════════════════════════════════════════════════════════════════
overall_time = (datetime.now() - overall_start).total_seconds()

logger.info("\n" + "=" * 70)
logger.info("📊 REDSHIFT LOAD SUMMARY")
logger.info("=" * 70)

total_rows = 0
success_count = 0
fail_count = 0

for entry in load_summary:
    icon = "✅" if entry["status"] == "SUCCESS" else "❌" if entry["status"] == "FAILED" else "⏭️"
    logger.info(
        f"  {icon} {entry['table']:20s} | {entry['status']:8s} | "
        f"{entry['rows']:>12,} rows"
    )

    if entry["status"] == "SUCCESS":
        total_rows += entry["rows"]
        success_count += 1
        logger.info(
            f"     Write: {entry['write_seconds']:.1f}s | "
            f"Total: {entry['total_seconds']:.1f}s"
        )
    elif entry["status"] == "FAILED":
        fail_count += 1
        logger.info(f"     Error: {entry.get('error', 'Unknown')[:100]}")

logger.info("-" * 70)
logger.info(f"  Total rows loaded: {total_rows:,}")
logger.info(f"  Succeeded: {success_count} | Failed: {fail_count}")
logger.info(f"  Total time: {overall_time:.1f}s ({overall_time/60:.1f} min)")
logger.info("=" * 70)

# Commit job bookmarks
job.commit()

if fail_count > 0:
    raise Exception(
        f"Load completed with {fail_count} failures. Check logs."
    )