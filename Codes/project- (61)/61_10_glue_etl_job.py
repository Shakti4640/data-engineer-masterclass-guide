# file: 61_10_glue_etl_job.py
# This is the ACTUAL Glue ETL script that runs inside Glue
# Upload to: s3://quickcart-analytics-scripts/glue/cross_account_mariadb_to_redshift.py
# Run from: Account B (222222222222)

# ============================================================
# GLUE JOB: Cross-Account MariaDB → Redshift ETL
# ============================================================
# Source: MariaDB on EC2 in Account A (via VPC Peering)
# Target: Redshift in Account B
# Frequency: Nightly at 2 AM
# Mode: Full load (incremental covered in Project 65)
# ============================================================

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# --- INIT GLUE CONTEXT ---
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "redshift_temp_dir",      # S3 path for Redshift staging
    "target_schema"            # Redshift target schema
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

# --- CONFIGURATION ---
MARIADB_CONNECTION = "conn-cross-account-mariadb"
REDSHIFT_CONNECTION = "conn-redshift-analytics"
REDSHIFT_TEMP_DIR = args["redshift_temp_dir"]
TARGET_SCHEMA = args["target_schema"]

# Tables to extract from MariaDB
TABLES_CONFIG = [
    {
        "source_table": "orders",
        "target_table": "orders",
        "primary_key": "order_id",
        "partition_column": "order_id",      # For parallel reads
        "expected_min_rows": 1000            # Validation threshold
    },
    {
        "source_table": "customers",
        "target_table": "dim_customers",
        "primary_key": "customer_id",
        "partition_column": "customer_id",
        "expected_min_rows": 100
    },
    {
        "source_table": "products",
        "target_table": "dim_products",
        "primary_key": "product_id",
        "partition_column": "product_id",
        "expected_min_rows": 50
    }
]


def extract_from_mariadb(table_config):
    """
    Extract table from MariaDB via JDBC over VPC Peering
    
    HOW THIS WORKS:
    → Glue ENI (10.2.6.x) sends JDBC request
    → Route table: 10.1.0.0/16 → pcx-xxx → Account A
    → MariaDB SG: allows 10.2.6.0/24:3306
    → MySQL wire protocol: SELECT * FROM table
    → Data streams back through same path
    → Glue converts ResultSet → DynamicFrame
    
    IMPORTANT FOR LARGE TABLES:
    → Without partitioning: single JDBC connection reads ALL rows sequentially
    → With hashexpression: Glue opens N parallel connections
    → Each reads 1/N of the data based on hash of partition column
    → MariaDB sees N concurrent queries — be mindful of load
    """
    source_table = table_config["source_table"]
    logger.info(f"📥 Extracting: {source_table}")

    try:
        # Read from MariaDB via Glue Connection
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": MARIADB_CONNECTION,
                "dbtable": source_table,
                # Parallel read optimization
                # hashexpression: Glue creates N parallel queries
                # Each query: SELECT * FROM table WHERE MOD(HASH(col), N) = partition_id
                "hashexpression": table_config["partition_column"],
                "hashpartitions": "4"    # 4 parallel readers — gentle on MariaDB
            },
            transformation_ctx=f"source_{source_table}"
        )

        row_count = dynamic_frame.count()
        logger.info(f"   Rows extracted: {row_count:,}")

        # --- VALIDATION: minimum row count ---
        if row_count < table_config["expected_min_rows"]:
            raise Exception(
                f"Row count validation FAILED for {source_table}. "
                f"Expected >= {table_config['expected_min_rows']}, "
                f"Got: {row_count}"
            )

        logger.info(f"   ✅ Validation passed: {row_count} >= {table_config['expected_min_rows']}")
        return dynamic_frame

    except Exception as e:
        logger.error(f"   ❌ Extraction failed for {source_table}: {str(e)}")
        raise


def transform_orders(dynamic_frame):
    """
    Transform orders data for Redshift
    
    TRANSFORMATIONS:
    → Add ETL metadata columns (load timestamp, source system)
    → Cast data types for Redshift compatibility
    → Handle NULL values
    → Add derived columns
    """
    logger.info("🔄 Transforming: orders")

    # Convert to Spark DataFrame for richer transformations
    df = dynamic_frame.toDF()

    df_transformed = df \
        .withColumn(
            "etl_loaded_at",
            F.current_timestamp()
        ) \
        .withColumn(
            "etl_source",
            F.lit("account_a_mariadb")
        ) \
        .withColumn(
            "total_amount",
            F.col("total_amount").cast("decimal(12,2)")
        ) \
        .withColumn(
            "unit_price",
            F.col("unit_price").cast("decimal(10,2)")
        ) \
        .withColumn(
            "order_date",
            F.col("order_date").cast("date")
        ) \
        .withColumn(
            "is_high_value",
            F.when(F.col("total_amount") > 500, True).otherwise(False)
        ) \
        .na.fill({
            "status": "unknown",
            "quantity": 0
        })

    row_count = df_transformed.count()
    logger.info(f"   Transformed rows: {row_count:,}")

    # Convert back to DynamicFrame for Glue write
    return DynamicFrame.fromDF(df_transformed, glueContext, "orders_transformed")


def transform_customers(dynamic_frame):
    """
    Transform customers for Redshift dimension table
    """
    logger.info("🔄 Transforming: customers")

    df = dynamic_frame.toDF()

    df_transformed = df \
        .withColumn("etl_loaded_at", F.current_timestamp()) \
        .withColumn("etl_source", F.lit("account_a_mariadb")) \
        .withColumn("name", F.trim(F.col("name"))) \
        .withColumn("email", F.lower(F.trim(F.col("email")))) \
        .withColumn("tier", F.upper(F.col("tier"))) \
        .na.fill({
            "city": "Unknown",
            "tier": "BRONZE"
        })

    return DynamicFrame.fromDF(df_transformed, glueContext, "customers_transformed")


def transform_products(dynamic_frame):
    """
    Transform products for Redshift dimension table
    """
    logger.info("🔄 Transforming: products")

    df = dynamic_frame.toDF()

    df_transformed = df \
        .withColumn("etl_loaded_at", F.current_timestamp()) \
        .withColumn("etl_source", F.lit("account_a_mariadb")) \
        .withColumn("price", F.col("price").cast("decimal(10,2)")) \
        .withColumn("category", F.trim(F.upper(F.col("category")))) \
        .withColumn(
            "price_tier",
            F.when(F.col("price") < 25, "budget")
             .when(F.col("price") < 100, "mid_range")
             .when(F.col("price") < 500, "premium")
             .otherwise("luxury")
        )

    return DynamicFrame.fromDF(df_transformed, glueContext, "products_transformed")


# Map table names to transform functions
TRANSFORM_MAP = {
    "orders": transform_orders,
    "customers": transform_customers,
    "products": transform_products
}


def load_to_redshift(dynamic_frame, table_config):
    """
    Load transformed data into Redshift
    
    HOW GLUE → REDSHIFT WORKS INTERNALLY:
    1. Glue writes DynamicFrame to S3 as temp Parquet/CSV files
       → Location: redshift_temp_dir (e.g., s3://bucket/glue-temp/)
    2. Glue issues Redshift COPY command pointing to those temp files
       → COPY schema.table FROM 's3://bucket/glue-temp/...' IAM_ROLE '...'
    3. Redshift bulk-loads from S3 (fast, parallel)
    4. Glue cleans up temp files
    
    preactions: SQL to run BEFORE loading (e.g., TRUNCATE for full load)
    postactions: SQL to run AFTER loading (e.g., ANALYZE, GRANT)
    
    SAME VPC — no peering needed for this hop:
    → Glue ENI (10.2.6.x) → Redshift (10.2.5.x) — local route
    """
    target_table = table_config["target_table"]
    full_target = f"{TARGET_SCHEMA}.{target_table}"

    logger.info(f"📤 Loading to Redshift: {full_target}")

    try:
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": REDSHIFT_CONNECTION,
                "dbtable": full_target,
                "redshiftTmpDir": REDSHIFT_TEMP_DIR,
                # FULL LOAD: truncate then insert
                "preactions": f"TRUNCATE TABLE {full_target};",
                # POST LOAD: update statistics + grant access
                "postactions": (
                    f"ANALYZE {full_target}; "
                    f"GRANT SELECT ON {full_target} TO GROUP analysts;"
                )
            },
            transformation_ctx=f"target_{target_table}"
        )

        logger.info(f"   ✅ Loaded to {full_target}")

    except Exception as e:
        logger.error(f"   ❌ Load failed for {full_target}: {str(e)}")
        raise


def run_etl():
    """
    Main ETL orchestrator
    
    FLOW:
    For each table:
      1. EXTRACT from MariaDB (Account A via VPC Peering)
      2. TRANSFORM (clean, cast, enrich)
      3. LOAD to Redshift (Account B, same VPC)
    """
    logger.info("=" * 60)
    logger.info("🚀 CROSS-ACCOUNT ETL: MariaDB (Account A) → Redshift (Account B)")
    logger.info("=" * 60)

    results = {"success": [], "failed": []}

    for table_config in TABLES_CONFIG:
        table_name = table_config["source_table"]
        logger.info(f"\n{'='*40}")
        logger.info(f"Processing: {table_name}")
        logger.info(f"{'='*40}")

        try:
            # --- EXTRACT ---
            raw_frame = extract_from_mariadb(table_config)

            # --- TRANSFORM ---
            transform_fn = TRANSFORM_MAP.get(table_name)
            if transform_fn:
                transformed_frame = transform_fn(raw_frame)
            else:
                logger.info(f"   No custom transform for {table_name} — loading raw")
                transformed_frame = raw_frame

            # --- LOAD ---
            load_to_redshift(transformed_frame, table_config)

            results["success"].append(table_name)

        except Exception as e:
            logger.error(f"❌ FAILED: {table_name} — {str(e)}")
            results["failed"].append({"table": table_name, "error": str(e)})
            # Continue with next table — don't fail entire job
            continue

    # --- SUMMARY ---
    logger.info("\n" + "=" * 60)
    logger.info("📊 ETL SUMMARY")
    logger.info(f"   ✅ Succeeded: {len(results['success'])}")
    for t in results["success"]:
        logger.info(f"      → {t}")
    logger.info(f"   ❌ Failed: {len(results['failed'])}")
    for f in results["failed"]:
        logger.info(f"      → {f['table']}: {f['error']}")
    logger.info("=" * 60)

    # Fail job if ANY table failed
    if results["failed"]:
        raise Exception(
            f"ETL completed with {len(results['failed'])} failures: "
            f"{[f['table'] for f in results['failed']]}"
        )


# --- EXECUTE ---
run_etl()
job.commit()