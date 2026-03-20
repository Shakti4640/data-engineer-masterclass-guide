# file: 62_02_patched_etl_with_smart_write.py
# Purpose: Show how Project 61's Glue job changes to prevent small files
# Only showing the CHANGED parts — rest is same as Project 61

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
import math

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "redshift_temp_dir", "target_schema"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# ============================================================
# KEY CHANGE #1: Set shuffle partitions appropriately
# ============================================================
# DEFAULT: 200 — creates 200 files after any groupBy/join
# FOR 200 MB DATA: 200 partitions = 1 MB each = terrible
# RULE OF THUMB: target_data_size / 128 MB, minimum 2
# For 200 MB: ceil(200/128) = 2 — but keep some parallelism for processing
spark.conf.set("spark.sql.shuffle.partitions", "10")
logger.info("✅ Set shuffle.partitions = 10 (was default 200)")

TARGET_FILE_SIZE_MB = 128


def estimate_df_size_mb(df):
    """Same as Part 1 — estimate DataFrame size"""
    total_rows = df.count()
    if total_rows == 0:
        return 0.0
    sample_fraction = min(10000 / total_rows, 1.0)
    try:
        sample_pd = df.sample(fraction=sample_fraction, seed=42).toPandas()
        sample_bytes = sample_pd.memory_usage(deep=True).sum()
        if len(sample_pd) == 0:
            return 0.0
        avg_row_bytes = sample_bytes / len(sample_pd)
        return (total_rows * avg_row_bytes / 5.0) / (1024 * 1024)
    except Exception:
        return (total_rows * 500) / (1024 * 1024)


def extract_from_mariadb(table_config):
    """Same as Project 61 — no changes needed"""
    source_table = table_config["source_table"]
    logger.info(f"📥 Extracting: {source_table}")

    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": "conn-cross-account-mariadb",
            "dbtable": source_table,
            "hashexpression": table_config["partition_column"],
            "hashpartitions": "4"
        },
        transformation_ctx=f"source_{source_table}"
    )

    row_count = dynamic_frame.count()
    logger.info(f"   Rows extracted: {row_count:,}")
    return dynamic_frame


def transform_orders(dynamic_frame):
    """Same as Project 61 — no changes needed"""
    df = dynamic_frame.toDF()
    df_transformed = df \
        .withColumn("etl_loaded_at", F.current_timestamp()) \
        .withColumn("etl_source", F.lit("account_a_mariadb")) \
        .withColumn("total_amount", F.col("total_amount").cast("decimal(12,2)")) \
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)")) \
        .withColumn("order_date", F.col("order_date").cast("date")) \
        .withColumn("is_high_value",
                     F.when(F.col("total_amount") > 500, True).otherwise(False)) \
        .na.fill({"status": "unknown", "quantity": 0})
    return df_transformed


def load_to_redshift_smart(df, table_config):
    """
    ============================================================
    KEY CHANGE #2: coalesce before Redshift write
    ============================================================
    
    WHY THIS MATTERS FOR REDSHIFT:
    → Glue writes to S3 temp dir BEFORE issuing COPY
    → 200 partitions → 200 temp files in S3
    → Redshift COPY reads ALL 200 files → 200 S3 GET requests
    → With coalesce: 2 temp files → 2 GET requests → faster COPY
    
    BONUS: Redshift COPY is most efficient when:
    → Number of files = multiple of number of slices
    → dc2.large: 2 slices per node
    → 2-node cluster: 4 slices → ideal file count: 4 or 8
    """
    target_table = table_config["target_table"]
    full_target = f"{args['target_schema']}.{target_table}"

    # Calculate optimal file count
    estimated_mb = estimate_df_size_mb(df)
    # For Redshift: align to cluster slice count
    REDSHIFT_SLICES = 4  # 2-node dc2.large cluster
    optimal = max(REDSHIFT_SLICES, math.ceil(estimated_mb / TARGET_FILE_SIZE_MB))
    # Round UP to nearest multiple of slices
    optimal = math.ceil(optimal / REDSHIFT_SLICES) * REDSHIFT_SLICES

    logger.info(f"📐 Redshift write sizing:")
    logger.info(f"   Estimated: {estimated_mb:.1f} MB")
    logger.info(f"   Redshift slices: {REDSHIFT_SLICES}")
    logger.info(f"   Coalescing to: {optimal} files")

    # Coalesce BEFORE converting back to DynamicFrame
    df_coalesced = df.coalesce(optimal)

    # Convert back to DynamicFrame
    dyf_coalesced = DynamicFrame.fromDF(
        df_coalesced, glueContext, f"coalesced_{target_table}"
    )

    logger.info(f"📤 Loading to Redshift: {full_target}")

    glueContext.write_dynamic_frame.from_options(
        frame=dyf_coalesced,
        connection_type="redshift",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": "conn-redshift-analytics",
            "dbtable": full_target,
            "redshiftTmpDir": args["redshift_temp_dir"],
            "preactions": f"TRUNCATE TABLE {full_target};",
            "postactions": (
                f"ANALYZE {full_target}; "
                f"GRANT SELECT ON {full_target} TO GROUP analysts;"
            )
        },
        transformation_ctx=f"target_{target_table}"
    )

    logger.info(f"   ✅ Loaded to {full_target} ({optimal} staging files)")


def write_to_s3_smart(df, output_path, partition_cols=None):
    """
    ============================================================
    KEY CHANGE #3: Smart S3 write for data lake layers
    ============================================================
    
    Used for Bronze/Silver/Gold S3 writes
    → Prevents 48K tiny files problem completely
    """
    estimated_mb = estimate_df_size_mb(df)
    current_partitions = df.rdd.getNumPartitions()
    optimal = max(1, math.ceil(estimated_mb / TARGET_FILE_SIZE_MB))
    optimal = min(optimal, current_partitions)

    logger.info(f"📐 S3 write sizing:")
    logger.info(f"   Estimated: {estimated_mb:.1f} MB → {optimal} file(s)")

    df_coalesced = df.coalesce(optimal)

    writer = df_coalesced.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)
    logger.info(f"   ✅ Written to {output_path}")


# ============================================================
# KEY CHANGE #4: Also write to Silver S3 layer (not just Redshift)
# ============================================================
TABLES_CONFIG = [
    {
        "source_table": "orders",
        "target_table": "orders",
        "primary_key": "order_id",
        "partition_column": "order_id",
        "expected_min_rows": 1000,
        "s3_silver_path": "s3://quickcart-analytics-datalake/silver/orders/",
        "s3_partition_cols": ["order_date"]
    },
    {
        "source_table": "customers",
        "target_table": "dim_customers",
        "primary_key": "customer_id",
        "partition_column": "customer_id",
        "expected_min_rows": 100,
        "s3_silver_path": "s3://quickcart-analytics-datalake/silver/customers/",
        "s3_partition_cols": None
    },
    {
        "source_table": "products",
        "target_table": "dim_products",
        "primary_key": "product_id",
        "partition_column": "product_id",
        "expected_min_rows": 50,
        "s3_silver_path": "s3://quickcart-analytics-datalake/silver/products/",
        "s3_partition_cols": None
    }
]

TRANSFORM_MAP = {
    "orders": transform_orders,
    # customers and products transforms same as Project 61
}


def run_etl():
    logger.info("=" * 60)
    logger.info("🚀 CROSS-ACCOUNT ETL (WITH SMALL FILE PREVENTION)")
    logger.info("=" * 60)

    for table_config in TABLES_CONFIG:
        table_name = table_config["source_table"]
        logger.info(f"\n{'='*40}")
        logger.info(f"Processing: {table_name}")

        # EXTRACT (same as before)
        raw_frame = extract_from_mariadb(table_config)

        # TRANSFORM
        transform_fn = TRANSFORM_MAP.get(table_name)
        if transform_fn:
            df_transformed = transform_fn(raw_frame)
        else:
            df_transformed = raw_frame.toDF()

        # LOAD TO REDSHIFT (with smart coalesce)
        load_to_redshift_smart(df_transformed, table_config)

        # WRITE TO S3 SILVER LAYER (with smart coalesce)
        write_to_s3_smart(
            df=df_transformed,
            output_path=table_config["s3_silver_path"],
            partition_cols=table_config.get("s3_partition_cols")
        )

    logger.info("✅ ETL complete — all writes optimized for file size")


run_etl()
job.commit()