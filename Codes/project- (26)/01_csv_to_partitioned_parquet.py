# file: 01_csv_to_partitioned_parquet.py
# Modified Glue ETL job that writes PARTITIONED Parquet output
# Upload to S3: s3://bucket/glue-scripts/csv_to_partitioned_parquet.py
# Builds on Project 25 job script — changes marked with ★

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, when, trim, lit

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'source_database', 'source_table',
    'target_s3_path', 'target_database', 'target_table'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_DATABASE = args['source_database']
SOURCE_TABLE = args['source_table']
TARGET_S3_PATH = args['target_s3_path']

print(f"🚀 CSV → PARTITIONED PARQUET CONVERSION")
print(f"   Source: {SOURCE_DATABASE}.{SOURCE_TABLE}")
print(f"   Target: {TARGET_S3_PATH}")

# ═══════════════════════════════════════════════════
# READ SOURCE (same as Project 25)
# ═══════════════════════════════════════════════════
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE,
    transformation_ctx="source_dyf"
)

input_count = source_dyf.count()
print(f"📥 Source records: {input_count:,}")

if input_count == 0:
    print("⚠️  No records — exiting")
    job.commit()
    sys.exit(0)

# ═══════════════════════════════════════════════════
# TRANSFORM — TYPE CASTING (same as Project 25)
# ═══════════════════════════════════════════════════
mapped_dyf = ApplyMapping.apply(
    frame=source_dyf,
    mappings=[
        ("order_id",     "string", "order_id",     "string"),
        ("customer_id",  "string", "customer_id",  "string"),
        ("product_id",   "string", "product_id",   "string"),
        ("quantity",     "string", "quantity",      "int"),
        ("unit_price",   "string", "unit_price",    "double"),
        ("total_amount", "string", "total_amount",  "double"),
        ("status",       "string", "status",        "string"),
        ("order_date",   "string", "order_date",    "string")
    ],
    transformation_ctx="mapped_dyf"
)

resolved_dyf = ResolveChoice.apply(
    frame=mapped_dyf,
    choice="cast:int",
    transformation_ctx="resolved_dyf"
)

# Convert to DataFrame for advanced transforms
df = resolved_dyf.toDF()
df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
df = df.withColumn("quantity",
    when(col("quantity").isNull(), lit(0)).otherwise(col("quantity")))
df = df.withColumn("total_amount",
    when(col("total_amount").isNull(), lit(0.0)).otherwise(col("total_amount")))
df = df.withColumn("status", trim(col("status")))

# Filter invalid rows
valid_df = df.filter(col("order_id").isNotNull() & col("order_date").isNotNull())

valid_count = valid_df.count()
dropped = input_count - valid_count
print(f"🔧 Valid: {valid_count:,}, Dropped: {dropped:,}")

# ═══════════════════════════════════════════════════
# ★ KEY CHANGE: COALESCE PER PARTITION
# ═══════════════════════════════════════════════════

# ★ For partitioned output, we want 1 file per partition (day)
# ★ repartition("order_date") groups rows by date
# ★ Then each partition gets its own folder with 1 file

# ★ repartition() (not coalesce) because we WANT to shuffle by partition key
# ★ coalesce() would merge WITHOUT grouping by date → mixed partitions
partitioned_df = valid_df.repartition("order_date")

print(f"📦 Repartitioned by order_date")
print(f"   Distinct dates: {valid_df.select('order_date').distinct().count()}")

# ═══════════════════════════════════════════════════
# ★ KEY CHANGE: WRITE WITH PARTITION KEYS
# ═══════════════════════════════════════════════════

output_dyf = DynamicFrame.fromDF(partitioned_df, glueContext, "output_dyf")

# ★ partitionKeys=["order_date"] tells Glue to create Hive-style folders
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": TARGET_S3_PATH,
        "partitionKeys": ["order_date"]    # ★ THIS IS THE KEY CHANGE
    },
    format="glueparquet",
    format_options={
        "compression": "snappy"
    },
    transformation_ctx="write_partitioned_parquet"
)

print(f"\n✅ Partitioned Parquet written to: {TARGET_S3_PATH}")
print(f"   Structure: {TARGET_S3_PATH}order_date=YYYY-MM-DD/file.parquet")

# ═══════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════
print(f"\n{'=' * 60}")
print(f"📊 JOB SUMMARY")
print(f"   Input:      {input_count:,} records")
print(f"   Output:     {valid_count:,} records")
print(f"   Partitions: order_date (Hive-style)")
print(f"   Format:     Parquet + Snappy")
print(f"{'=' * 60}")

job.commit()