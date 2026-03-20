# file: 02_csv_to_parquet_job.py
# THIS IS THE GLUE JOB SCRIPT — runs INSIDE Glue's Spark environment
# Upload to S3: s3://quickcart-raw-data-prod/glue-scripts/csv_to_parquet_job.py

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, when, trim, lit
from pyspark.sql.types import IntegerType, DoubleType, DateType

# ═══════════════════════════════════════════════════
# PHASE 1: INITIALIZATION
# ═══════════════════════════════════════════════════

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'source_table',
    'target_s3_path',
    'target_database',
    'target_table'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extract parameters
SOURCE_DATABASE = args['source_database']
SOURCE_TABLE = args['source_table']
TARGET_S3_PATH = args['target_s3_path']
TARGET_DATABASE = args['target_database']
TARGET_TABLE = args['target_table']

print(f"=" * 60)
print(f"🚀 CSV TO PARQUET CONVERSION")
print(f"   Source: {SOURCE_DATABASE}.{SOURCE_TABLE}")
print(f"   Target: {TARGET_S3_PATH}")
print(f"=" * 60)

# ═══════════════════════════════════════════════════
# PHASE 2: READ SOURCE DATA
# ═══════════════════════════════════════════════════

# Read from Glue Catalog (uses table defined by crawler — Project 23)
# Job Bookmarks track which files were already processed (Project 33)
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE,
    transformation_ctx="source_dyf"   # Required for Job Bookmarks
)

input_count = source_dyf.count()
print(f"\n📥 Source records read: {input_count:,}")

if input_count == 0:
    print("⚠️  No new records to process — exiting")
    job.commit()
    sys.exit(0)

# Show schema before transformation
print(f"\n📋 Source schema:")
source_dyf.printSchema()

# ═══════════════════════════════════════════════════
# PHASE 3: TRANSFORM — TYPE CASTING
# ═══════════════════════════════════════════════════

# ApplyMapping: rename columns + cast types in one operation
# This is where STRING → proper types happens
mapped_dyf = ApplyMapping.apply(
    frame=source_dyf,
    mappings=[
        # (source_col, source_type, target_col, target_type)
        ("order_id",    "string", "order_id",     "string"),
        ("customer_id", "string", "customer_id",  "string"),
        ("product_id",  "string", "product_id",   "string"),
        ("quantity",    "string", "quantity",      "int"),
        ("unit_price",  "string", "unit_price",    "double"),
        ("total_amount","string", "total_amount",  "double"),
        ("status",      "string", "status",        "string"),
        ("order_date",  "string", "order_date",    "string")  # Cast to date in Spark
    ],
    transformation_ctx="mapped_dyf"
)

# ═══════════════════════════════════════════════════
# PHASE 4: TRANSFORM — RESOLVE CHOICE TYPES
# ═══════════════════════════════════════════════════

# ResolveChoice handles columns where crawler detected mixed types
# Example: quantity column has "5" (int) and "N/A" (string)
# cast:int → converts valid values, nulls invalid ones
resolved_dyf = ResolveChoice.apply(
    frame=mapped_dyf,
    choice="cast:int",                # For ambiguous int columns
    transformation_ctx="resolved_dyf"
)

# ═══════════════════════════════════════════════════
# PHASE 5: TRANSFORM — ADVANCED CLEANING (Spark DataFrame)
# ═══════════════════════════════════════════════════

# Convert DynamicFrame → Spark DataFrame for complex operations
df = resolved_dyf.toDF()

# --- Cast order_date string to proper DATE type ---
# CSV dates are "YYYY-MM-DD" strings — Spark can parse them
df = df.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)

# --- Clean null/invalid values ---
# Replace null quantities with 0
df = df.withColumn(
    "quantity",
    when(col("quantity").isNull(), lit(0)).otherwise(col("quantity"))
)

# Replace null amounts with 0.0
df = df.withColumn(
    "total_amount",
    when(col("total_amount").isNull(), lit(0.0)).otherwise(col("total_amount"))
)

# --- Trim whitespace from string columns ---
df = df.withColumn("status", trim(col("status")))
df = df.withColumn("order_id", trim(col("order_id")))

# --- Filter out invalid rows ---
# Remove rows where order_id is null (corrupt records)
valid_df = df.filter(col("order_id").isNotNull())

# Remove rows where order_date couldn't be parsed
valid_df = valid_df.filter(col("order_date").isNotNull())

# Track dropped rows
dropped_count = df.count() - valid_df.count()
valid_count = valid_df.count()

print(f"\n🔧 Transformation results:")
print(f"   Input records:   {input_count:,}")
print(f"   Valid records:   {valid_count:,}")
print(f"   Dropped records: {dropped_count:,}")

if dropped_count > 0:
    drop_pct = round(100.0 * dropped_count / input_count, 2)
    print(f"   Drop rate:       {drop_pct}%")
    if drop_pct > 5.0:
        print(f"   ⚠️  WARNING: Drop rate exceeds 5% — investigate source data quality")

# ═══════════════════════════════════════════════════
# PHASE 6: OPTIMIZE OUTPUT — COALESCE FILES
# ═══════════════════════════════════════════════════

# Control number of output Parquet files
# Target: 128-512 MB per file
# 1.5 GB CSV → ~200 MB Parquet → aim for 4 files (~50 MB each)
# Adjust based on your data volume

data_size_mb = valid_count * 85 / (1024 * 1024)  # Rough estimate: 85 bytes/row
target_file_size_mb = 128
num_output_files = max(1, int(data_size_mb / target_file_size_mb))
num_output_files = min(num_output_files, 32)  # Cap at 32 files

print(f"\n📦 Output configuration:")
print(f"   Estimated data size: {round(data_size_mb)} MB")
print(f"   Target files: {num_output_files}")

# Coalesce (not repartition — avoids expensive shuffle)
optimized_df = valid_df.coalesce(num_output_files)

# ═══════════════════════════════════════════════════
# PHASE 7: WRITE PARQUET TO S3
# ═══════════════════════════════════════════════════

# Convert back to DynamicFrame for Glue catalog integration
output_dyf = DynamicFrame.fromDF(optimized_df, glueContext, "output_dyf")

# Show output schema
print(f"\n📋 Output schema:")
output_dyf.printSchema()

# Write Parquet with Glue Catalog update
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": TARGET_S3_PATH,
        "partitionKeys": []          # No partitioning (added in Project 26)
    },
    format="glueparquet",            # Glue-optimized Parquet writer
    format_options={
        "compression": "snappy"      # Default and recommended
    },
    transformation_ctx="write_parquet"
)

print(f"\n✅ Parquet written to: {TARGET_S3_PATH}")

# ═══════════════════════════════════════════════════
# PHASE 8: UPDATE GLUE CATALOG
# ═══════════════════════════════════════════════════

# Programmatically create/update catalog table pointing to Parquet
# Alternative: run crawler on TARGET_S3_PATH (Project 23)

from awsglue.catalog import GlueCatalog

try:
    # Get output schema from DynamicFrame
    output_schema = output_dyf.schema()
    
    print(f"\n📋 Catalog table: {TARGET_DATABASE}.{TARGET_TABLE}")
    print(f"   Location: {TARGET_S3_PATH}")
    print(f"   Format: Parquet (Snappy)")
    
except Exception as e:
    print(f"⚠️  Catalog update note: {e}")
    print(f"   Run crawler on {TARGET_S3_PATH} to update catalog")

# ═══════════════════════════════════════════════════
# PHASE 9: JOB COMPLETION
# ═══════════════════════════════════════════════════

print(f"\n{'=' * 60}")
print(f"📊 JOB SUMMARY")
print(f"{'=' * 60}")
print(f"   Source:          {SOURCE_DATABASE}.{SOURCE_TABLE}")
print(f"   Target:          {TARGET_S3_PATH}")
print(f"   Input records:   {input_count:,}")
print(f"   Output records:  {valid_count:,}")
print(f"   Dropped:         {dropped_count:,}")
print(f"   Output files:    {num_output_files}")
print(f"   Format:          Parquet (Snappy compression)")
print(f"{'=' * 60}")

# Commit job (required for Job Bookmarks to work)
job.commit()
print(f"\n✅ Job committed successfully")