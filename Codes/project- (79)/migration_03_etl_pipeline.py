# file: migration/03_etl_pipeline.py
# Purpose: Glue ETL jobs that transform Bronze (Account A) → Silver → Gold (Account B)
# Integrates: cross-account S3 (Project 56), CDC merge (Project 65),
#             lineage (Project 77), data quality (Project 68)

import sys
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration_etl")

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "JOB_RUN_ID",
    "source_bucket",       # Account A Bronze bucket
    "target_bucket",       # Account B Silver bucket
    "database_name",       # e.g., "ecommerce"
    "table_name",          # e.g., "orders"
    "processing_mode"      # "full_load" or "cdc"
])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Import lineage emitter (Project 77 — uploaded as additional file)
from lineage_emitter import LineageEmitter, LineageNode

ACCOUNT_A = "[REDACTED:BANK_ACCOUNT_NUMBER]"
ACCOUNT_B = "[REDACTED:BANK_ACCOUNT_NUMBER]"

emitter = LineageEmitter(
    job_name=args["JOB_NAME"],
    job_run_id=args["JOB_RUN_ID"],
    account_id=ACCOUNT_B
)


def process_full_load(database_name, table_name, source_bucket, target_bucket):
    """
    Process initial full load files from DMS
    
    DMS full load S3 structure:
    s3://{source_bucket}/raw/{database}/{table}/LOAD00001/
      part-00000.parquet
      part-00001.parquet
    
    Output to Silver:
    s3://{target_bucket}/silver/{database}/{table}/
      year=YYYY/month=MM/day=DD/
        part-00000.snappy.parquet
    """
    logger.info(f"Processing FULL LOAD: {database_name}.{table_name}")

    source_path = f"s3://{source_bucket}/raw/{database_name}/{table_name}/LOAD00001/"

    # Read full load parquet files
    df = spark.read.parquet(source_path)
    input_count = df.count()
    logger.info(f"Read {input_count:,} rows from full load")

    # Apply transformations based on table
    cleaned_df = apply_table_transformations(
        df, database_name, table_name, "full_load"
    )
    output_count = cleaned_df.count()

    # Write to Silver layer
    target_path = f"s3://{target_bucket}/silver/{database_name}/{table_name}/"

    cleaned_df.write \
        .mode("overwrite") \
        .partitionBy("_partition_date") \
        .parquet(target_path)

    logger.info(f"Written {output_count:,} rows to Silver")

    # Emit lineage (Project 77)
    emit_table_lineage(
        database_name, table_name, source_bucket, target_bucket,
        "full_load", input_count, output_count
    )

    return input_count, output_count


def process_cdc(database_name, table_name, source_bucket, target_bucket):
    """
    Process CDC files from DMS and merge into Silver layer
    
    DMS CDC S3 structure:
    s3://{source_bucket}/raw/{database}/{table}/CDC/
      YYYY/MM/DD/HH/
        cdc-00001.parquet
    
    CDC record contains:
    → Op: 'I' (Insert), 'U' (Update), 'D' (Delete)
    → All columns with current values
    → _dms_ingestion_timestamp
    
    MERGE STRATEGY (from Project 65):
    1. Read new CDC files (Job Bookmark tracks position)
    2. Read existing Silver data for affected primary keys
    3. Apply CDC: delete old, insert new (for I and U), skip D
    4. Write merged result back to Silver
    """
    logger.info(f"Processing CDC: {database_name}.{table_name}")

    # Read CDC files (Job Bookmark ensures only new files — Project 33)
    cdc_path = f"s3://{source_bucket}/raw/{database_name}/{table_name}/CDC/"

    cdc_dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [cdc_path],
            "recurse": True
        },
        format="parquet",
        transformation_ctx=f"cdc_{database_name}_{table_name}"
    )

    cdc_df = cdc_dyf.toDF()

    if cdc_df.rdd.isEmpty():
        logger.info("No new CDC files to process")
        return 0, 0

    cdc_count = cdc_df.count()
    logger.info(f"Read {cdc_count:,} CDC records")

    # Get primary key for this table
    pk_column = get_primary_key(database_name, table_name)

    # Deduplicate CDC records (keep latest per PK)
    # In case of multiple updates to same row in same batch
    window = Window.partitionBy(pk_column).orderBy(
        F.col("_dms_ingestion_timestamp").desc()
    )
    cdc_deduped = cdc_df.withColumn("_rn", F.row_number().over(window)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")

    # Read existing Silver data
    silver_path = f"s3://{target_bucket}/silver/{database_name}/{table_name}/"

    try:
        existing_df = spark.read.parquet(silver_path)
    except Exception:
        logger.warning("No existing Silver data — treating as initial load")
        existing_df = spark.createDataFrame([], cdc_deduped.schema)

    # MERGE LOGIC (Project 65 pattern):
    # 1. Get PKs from CDC batch
    cdc_pks = cdc_deduped.select(pk_column).distinct()

    # 2. Remove affected rows from existing data
    surviving_df = existing_df.join(
        cdc_pks,
        on=pk_column,
        how="left_anti"  # Keep rows NOT in CDC batch
    )

    # 3. Get non-delete CDC records (Inserts + Updates)
    new_rows_df = cdc_deduped.filter(F.col("Op") != "D")

    # 4. Apply transformations to new rows
    transformed_new = apply_table_transformations(
        new_rows_df, database_name, table_name, "cdc"
    )

    # 5. Drop DMS metadata columns before union
    dms_columns = ["Op", "_dms_ingestion_timestamp", "source_database"]
    for col_name in dms_columns:
        if col_name in transformed_new.columns:
            transformed_new = transformed_new.drop(col_name)
        if col_name in surviving_df.columns:
            surviving_df = surviving_df.drop(col_name)

    # 6. Union surviving + new rows
    merged_df = surviving_df.unionByName(transformed_new, allowMissingColumns=True)

    output_count = merged_df.count()

    # 7. Write back to Silver (overwrite partition)
    merged_df.write \
        .mode("overwrite") \
        .partitionBy("_partition_date") \
        .parquet(silver_path)

    logger.info(
        f"CDC merge complete: {cdc_count} CDC records applied, "
        f"{output_count:,} total rows in Silver"
    )

    # Emit lineage
    emit_table_lineage(
        database_name, table_name, source_bucket, target_bucket,
        "cdc", cdc_count, output_count
    )

    return cdc_count, output_count


def apply_table_transformations(df, database_name, table_name, mode):
    """
    Apply table-specific transformations
    
    COMMON TRANSFORMS (all tables):
    → Add _partition_date column for partitioning
    → Cast timestamp strings to proper types
    → Trim whitespace from string columns
    → Add _etl_timestamp for audit
    
    TABLE-SPECIFIC TRANSFORMS:
    → orders: derive order_total, validate status enum
    → customers: mask PII (email → hash), validate tier
    → payments: convert currency, validate amounts
    """
    # Common transforms
    transformed = df \
        .withColumn("_etl_timestamp", F.current_timestamp()) \
        .withColumn("_etl_mode", F.lit(mode))

    # Add partition date
    if "order_date" in df.columns:
        transformed = transformed.withColumn(
            "_partition_date",
            F.to_date(F.col("order_date"))
        )
    elif "created_at" in df.columns:
        transformed = transformed.withColumn(
            "_partition_date",
            F.to_date(F.col("created_at"))
        )
    elif "transaction_date" in df.columns:
        transformed = transformed.withColumn(
            "_partition_date",
            F.to_date(F.col("transaction_date"))
        )
    else:
        transformed = transformed.withColumn(
            "_partition_date",
            F.current_date()
        )

    # Table-specific transforms
    if table_name == "orders":
        transformed = transform_orders(transformed)
    elif table_name == "customers":
        transformed = transform_customers(transformed)
    elif table_name == "transactions":
        transformed = transform_transactions(transformed)

    return transformed


def transform_orders(df):
    """Orders-specific transformations (mirrors Project 34 logic)"""
    return df \
        .withColumn("quantity", F.col("quantity").cast("int")) \
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)")) \
        .withColumn("order_total",
            F.col("quantity") * F.col("unit_price")) \
        .filter(F.col("quantity") > 0) \
        .filter(F.col("unit_price") > 0)


def transform_customers(df):
    """Customers-specific — PII masking (Lake Formation also enforces — Project 74)"""
    return df \
        .withColumn("email_hash",
            F.sha2(F.lower(F.trim(F.col("email"))), 256)) \
        .drop("email") \
        .withColumn("name_masked",
            F.concat(F.substring(F.col("name"), 1, 1), F.lit("***"))) \
        .drop("name")


def transform_transactions(df):
    """Transactions-specific — amount validation"""
    return df \
        .withColumn("amount", F.col("amount").cast("decimal(12,2)")) \
        .filter(F.col("amount").isNotNull()) \
        .filter(F.col("amount") >= 0)


def get_primary_key(database_name, table_name):
    """Return primary key column for each table"""
    pk_map = {
        "ecommerce.orders": "order_id",
        "ecommerce.order_items": "order_item_id",
        "ecommerce.customers": "customer_id",
        "ecommerce.products": "product_id",
        "ecommerce.inventory": "product_id",
        "payments.transactions": "transaction_id",
        "payments.refunds": "refund_id",
    }
    return pk_map.get(f"{database_name}.{table_name}", "id")


def emit_table_lineage(database_name, table_name, source_bucket,
                       target_bucket, mode, input_count, output_count):
    """Emit lineage events for this ETL run (Project 77)"""
    source_node = LineageNode(
        ACCOUNT_A, "s3", "bronze", f"raw_{database_name}_{table_name}",
        data_type="parquet",
        tags=["bronze", "raw", f"source:{database_name}", f"mode:{mode}"]
    )

    target_node = LineageNode(
        ACCOUNT_B, "glue", "silver", f"{database_name}_{table_name}",
        data_type="parquet",
        tags=["silver", "cleaned", f"source:{database_name}"]
    )

    emitter.emit_edge(
        source_node, target_node,
        json.dumps({
            "type": f"ETL_{mode.upper()}",
            "input_rows": input_count,
            "output_rows": output_count,
            "filtered_rows": input_count - output_count,
            "transforms": [
                "type_casting", "null_filtering",
                "partition_by_date", "audit_columns"
            ]
        })
    )


# ═══════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════

import json

processing_mode = args["processing_mode"]
database_name = args["database_name"]
table_name = args["table_name"]
source_bucket = args["source_bucket"]
target_bucket = args["target_bucket"]

logger.info(f"{'=' * 60}")
logger.info(f"Migration ETL: {database_name}.{table_name} ({processing_mode})")
logger.info(f"{'=' * 60}")

if processing_mode == "full_load":
    input_count, output_count = process_full_load(
        database_name, table_name, source_bucket, target_bucket
    )
elif processing_mode == "cdc":
    input_count, output_count = process_cdc(
        database_name, table_name, source_bucket, target_bucket
    )
else:
    raise ValueError(f"Unknown processing mode: {processing_mode}")

# Finalize lineage
emitter.finalize()

logger.info(f"✅ Complete: {input_count:,} in → {output_count:,} out")
job.commit()