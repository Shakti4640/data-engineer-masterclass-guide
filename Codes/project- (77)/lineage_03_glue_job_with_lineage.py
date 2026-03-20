# file: lineage/03_glue_job_with_lineage.py
# Purpose: Demonstrate a real Glue ETL job that emits column-level lineage
# This is the "job_clean_orders" job from the problem statement

import sys
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Import our lineage library (uploaded as additional python file in Glue)
from lineage_emitter import LineageEmitter, LineageNode

# --- SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_clean_orders")

args = getResolvedOptions(sys.argv, ["JOB_NAME", "JOB_RUN_ID"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --- LINEAGE SETUP ---
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"  # Processing account (Account B)
SOURCE_ACCOUNT = "[REDACTED:BANK_ACCOUNT_NUMBER]"  # Ingestion account (Account A)

emitter = LineageEmitter(
    job_name=args["JOB_NAME"],
    job_run_id=args["JOB_RUN_ID"],
    account_id=ACCOUNT_ID
)

# ═══════════════════════════════════════════════════
# STEP 1: READ FROM BRONZE LAYER
# ═══════════════════════════════════════════════════

logger.info("Reading bronze.raw_orders from S3...")

# Using Glue Catalog table (created by Crawler — Project 23)
raw_orders_dyf = glue_context.create_dynamic_frame.from_catalog(
    database="bronze",
    table_name="raw_orders",
    transformation_ctx="raw_orders_source"
    # Job Bookmark enabled (Project 33) — only reads new files
)

raw_orders_df = raw_orders_dyf.toDF()
input_count = raw_orders_df.count()
logger.info(f"Read {input_count} rows from bronze.raw_orders")

# --- DEFINE SOURCE NODES (what we read) ---
src_order_id = LineageNode(
    SOURCE_ACCOUNT, "glue", "bronze", "raw_orders", "order_id",
    data_type="string", tags=["business-key"]
)
src_qty = LineageNode(
    SOURCE_ACCOUNT, "glue", "bronze", "raw_orders", "qty",
    data_type="string", tags=["financial"]
)
src_unit_price = LineageNode(
    SOURCE_ACCOUNT, "glue", "bronze", "raw_orders", "unit_price",
    data_type="string", tags=["financial"]
)
src_status = LineageNode(
    SOURCE_ACCOUNT, "glue", "bronze", "raw_orders", "status",
    data_type="string", tags=["categorical"]
)
src_order_date = LineageNode(
    SOURCE_ACCOUNT, "glue", "bronze", "raw_orders", "order_date",
    data_type="string", tags=["temporal"]
)

# ═══════════════════════════════════════════════════
# STEP 2: TRANSFORM — CLEAN + RENAME + CAST + DERIVE
# ═══════════════════════════════════════════════════

logger.info("Applying transformations...")

cleaned_df = (
    raw_orders_df
    # RENAME: qty → quantity
    .withColumnRenamed("qty", "quantity")
    # CAST: quantity from string to integer
    .withColumn("quantity", F.col("quantity").cast("int"))
    # CAST: unit_price from string to decimal
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    # DERIVE: order_total = quantity × unit_price (NEW COLUMN)
    .withColumn("order_total", F.col("quantity") * F.col("unit_price"))
    # FILTER: remove cancelled orders and invalid quantities
    .filter(F.col("status") != "cancelled")
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") > 0)
    # CAST: order_date from string to date
    .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
    # DROP: raw status column (not needed in silver)
    .drop("status")
)

output_count = cleaned_df.count()
logger.info(f"After cleaning: {output_count} rows (filtered {input_count - output_count})")

# --- DEFINE TARGET NODES (what we write) ---
tgt_order_id = LineageNode(
    ACCOUNT_ID, "glue", "silver", "orders_cleaned", "order_id",
    data_type="string", tags=["business-key"]
)
tgt_quantity = LineageNode(
    ACCOUNT_ID, "glue", "silver", "orders_cleaned", "quantity",
    data_type="int", tags=["financial"]
)
tgt_unit_price = LineageNode(
    ACCOUNT_ID, "glue", "silver", "orders_cleaned", "unit_price",
    data_type="decimal(10,2)", tags=["financial"]
)
tgt_order_total = LineageNode(
    ACCOUNT_ID, "glue", "silver", "orders_cleaned", "order_total",
    data_type="decimal(10,2)", tags=["financial", "derived"]
)
tgt_order_date = LineageNode(
    ACCOUNT_ID, "glue", "silver", "orders_cleaned", "order_date",
    data_type="date", tags=["temporal"]
)

# ═══════════════════════════════════════════════════
# STEP 3: EMIT COLUMN-LEVEL LINEAGE
# ═══════════════════════════════════════════════════

logger.info("Emitting lineage events...")

# order_id: direct passthrough (no transformation)
emitter.emit_column_mapping(
    src_order_id, tgt_order_id,
    {"type": "PASSTHROUGH", "note": "no transformation applied"}
)

# qty → quantity: RENAME + CAST
emitter.emit_column_mapping(
    src_qty, tgt_quantity,
    {
        "type": "RENAME_AND_CAST",
        "rename": {"from": "qty", "to": "quantity"},
        "cast": {"from_type": "string", "to_type": "int"},
        "filter": "quantity > 0 (rows with qty <= 0 dropped)"
    }
)

# unit_price: CAST only
emitter.emit_column_mapping(
    src_unit_price, tgt_unit_price,
    {
        "type": "CAST",
        "cast": {"from_type": "string", "to_type": "decimal(10,2)"},
        "filter": "unit_price > 0 (rows with price <= 0 dropped)"
    }
)

# order_total: DERIVED from two source columns
# This is a MULTI-INPUT → SINGLE-OUTPUT edge
emitter.emit_column_mapping(
    src_qty, tgt_order_total,
    {
        "type": "EXPRESSION",
        "expression": "CAST(qty AS INT) * CAST(unit_price AS DECIMAL)",
        "role": "multiplicand",
        "note": "order_total = quantity × unit_price"
    }
)
emitter.emit_column_mapping(
    src_unit_price, tgt_order_total,
    {
        "type": "EXPRESSION",
        "expression": "CAST(qty AS INT) * CAST(unit_price AS DECIMAL)",
        "role": "multiplier",
        "note": "order_total = quantity × unit_price"
    }
)

# order_date: CAST string to date
emitter.emit_column_mapping(
    src_order_date, tgt_order_date,
    {
        "type": "CAST",
        "cast": {"from_type": "string", "to_type": "date"},
        "format": "yyyy-MM-dd"
    }
)

# ═══════════════════════════════════════════════════
# STEP 4: WRITE TO SILVER LAYER
# ═══════════════════════════════════════════════════

logger.info("Writing to silver.orders_cleaned...")

from awsglue.dynamicframe import DynamicFrame

cleaned_dyf = DynamicFrame.fromDF(cleaned_df, glue_context, "cleaned")

glue_context.write_dynamic_frame.from_options(
    frame=cleaned_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://quickcart-silver-prod/orders_cleaned/",
        "partitionKeys": ["order_date"]
    },
    format="parquet",
    transformation_ctx="silver_orders_sink"
)

logger.info(f"✅ Written {output_count} rows to silver.orders_cleaned")

# ═══════════════════════════════════════════════════
# STEP 5: FINALIZE LINEAGE + COMMIT JOB
# ═══════════════════════════════════════════════════

logger.info("Finalizing lineage...")

# Archive all edges to S3 for historical queryability
emitter.finalize()

logger.info(
    f"📊 Job Summary:\n"
    f"   Input rows:    {input_count}\n"
    f"   Output rows:   {output_count}\n"
    f"   Filtered rows: {input_count - output_count}\n"
    f"   Lineage nodes: {len(emitter.nodes_registered)}\n"
    f"   Lineage edges: {len(emitter.edges_emitted)}"
)

# Commit Glue job bookmark (Project 33)
job.commit()
logger.info("✅ Job complete")