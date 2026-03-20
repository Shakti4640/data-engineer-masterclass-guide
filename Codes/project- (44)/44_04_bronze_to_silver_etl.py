# file: 44_04_bronze_to_silver_etl.py
# Purpose: Glue ETL script that transforms Bronze (raw CSV) → Silver (clean Parquet)
# This script runs INSIDE AWS Glue
# Builds on Project 25 (CSV→Parquet) + Project 34 (transforms) + Project 42 (CDC)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType, DateType, BooleanType
)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "entity_name",
    "source_zone",       # "bronze"
    "target_zone",       # "silver"
    "datalake_bucket",
    "process_date"       # "2025-01-15"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

entity = args["entity_name"]
bucket = args["datalake_bucket"]
process_date = args["process_date"]
year, month, day = process_date.split("-")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCHEMA DEFINITIONS (Silver zone enforces strict schemas)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SILVER_SCHEMAS = {
    "orders": StructType([
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DecimalType(10, 2), nullable=False),
        StructField("total_amount", DecimalType(12, 2), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("order_date", DateType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False)
    ]),
    "customers": StructType([
        StructField("customer_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("tier", StringType(), nullable=True),
        StructField("signup_date", DateType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False)
    ]),
    "products": StructType([
        StructField("product_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("price", DecimalType(10, 2), nullable=True),
        StructField("stock_quantity", IntegerType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False)
    ])
}

# Primary keys for deduplication
PRIMARY_KEYS = {
    "orders": "order_id",
    "customers": "customer_id",
    "products": "product_id"
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 1: READ FROM BRONZE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Read BOTH full dump AND CDC for this date
# CDC takes priority for rows that appear in both

bronze_cdc_path = (
    f"s3://{bucket}/bronze/mariadb/cdc/{entity}/"
    f"year={year}/month={month}/day={day}/"
)
bronze_full_path = (
    f"s3://{bucket}/bronze/mariadb/full_dump/{entity}/"
    f"year={year}/month={month}/day={day}/"
)

print(f"📥 Reading Bronze data for: {entity} ({process_date})")
print(f"   CDC path: {bronze_cdc_path}")
print(f"   Full dump path: {bronze_full_path}")

# Try CDC first (preferred — smaller, incremental)
cdc_df = None
full_df = None

try:
    cdc_df = spark.read.option("header", "true").csv(bronze_cdc_path)
    cdc_count = cdc_df.count()
    print(f"   ✅ CDC: {cdc_count} rows")
except Exception:
    print(f"   ℹ️  No CDC file for this date")
    cdc_count = 0

try:
    full_df = spark.read.option("header", "true").csv(bronze_full_path)
    full_count = full_df.count()
    print(f"   ✅ Full dump: {full_count} rows")
except Exception:
    print(f"   ℹ️  No full dump for this date")
    full_count = 0

# Determine source strategy
if cdc_count > 0 and full_count > 0:
    print(f"   📋 Strategy: UNION (full + CDC, deduplicate with CDC priority)")
    # Union both, CDC rows take priority (have later updated_at)
    raw_df = full_df.unionByName(cdc_df, allowMissingColumns=True)
elif cdc_count > 0:
    print(f"   📋 Strategy: CDC only")
    raw_df = cdc_df
elif full_count > 0:
    print(f"   📋 Strategy: Full dump only")
    raw_df = full_df
else:
    print(f"   ⚠️  No Bronze data found for {entity} on {process_date}")
    job.commit()
    sys.exit(0)

total_raw = raw_df.count()
print(f"   📊 Total raw rows: {total_raw}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 2: SCHEMA ENFORCEMENT (Bronze → Silver quality gate)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🔧 Applying schema enforcement...")

target_schema = SILVER_SCHEMAS[entity]

# Cast each column to the target type
typed_df = raw_df
for field in target_schema.fields:
    col_name = field.name
    col_type = field.dataType

    if col_name in raw_df.columns:
        typed_df = typed_df.withColumn(
            col_name,
            F.col(col_name).cast(col_type)
        )
    else:
        # Column missing in source → add as null (if nullable)
        if field.nullable:
            typed_df = typed_df.withColumn(col_name, F.lit(None).cast(col_type))
            print(f"   ⚠️  Missing column '{col_name}' — added as NULL")
        else:
            raise ValueError(
                f"Required column '{col_name}' missing in Bronze data for {entity}"
            )

# Select only schema columns (drop any extra columns from source)
typed_df = typed_df.select([field.name for field in target_schema.fields])
print(f"   ✅ Schema enforced: {len(target_schema.fields)} columns")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 3: NULL HANDLING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🔧 Handling NULLs...")

pk_column = PRIMARY_KEYS[entity]

# Drop rows where primary key is null (invalid data)
before_null_drop = typed_df.count()
typed_df = typed_df.filter(F.col(pk_column).isNotNull())
after_null_drop = typed_df.count()
null_pk_dropped = before_null_drop - after_null_drop

if null_pk_dropped > 0:
    print(f"   ⚠️  Dropped {null_pk_dropped} rows with NULL primary key ({pk_column})")
else:
    print(f"   ✅ No NULL primary keys found")

# Log null counts per column
print(f"   📊 NULL counts per column:")
for field in target_schema.fields:
    null_count = typed_df.filter(F.col(field.name).isNull()).count()
    if null_count > 0:
        pct = (null_count / max(after_null_drop, 1)) * 100
        print(f"      {field.name}: {null_count} ({pct:.1f}%)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 4: DEDUPLICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🔧 Deduplicating on {pk_column}...")

from pyspark.sql.window import Window

# Keep the row with the LATEST updated_at per primary key
window = Window.partitionBy(pk_column).orderBy(F.col("updated_at").desc())
deduped_df = typed_df.withColumn("_row_num", F.row_number().over(window)) \
                      .filter(F.col("_row_num") == 1) \
                      .drop("_row_num")

deduped_count = deduped_df.count()
dups_removed = after_null_drop - deduped_count

print(f"   Before: {after_null_drop}, After: {deduped_count}, Dups removed: {dups_removed}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 5: ADD METADATA COLUMNS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🔧 Adding metadata columns...")

enriched_df = deduped_df \
    .withColumn("_etl_loaded_at", F.current_timestamp()) \
    .withColumn("_etl_source_zone", F.lit("bronze")) \
    .withColumn("_etl_process_date", F.lit(process_date))

print(f"   ✅ Added: _etl_loaded_at, _etl_source_zone, _etl_process_date")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 6: WRITE TO SILVER (Parquet, partitioned by date)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
silver_path = f"s3://{bucket}/silver/{entity}/"

# Add partition columns
output_df = enriched_df \
    .withColumn("year", F.lit(year)) \
    .withColumn("month", F.lit(month)) \
    .withColumn("day", F.lit(day))

# Coalesce to control file count (prevents small file problem — Project 62)
num_output_files = max(1, deduped_count // 250000)  # ~250K rows per file
num_output_files = min(num_output_files, 8)  # Max 8 files
output_df = output_df.coalesce(num_output_files)

print(f"\n📤 Writing to Silver: {silver_path}")
print(f"   Rows: {deduped_count}")
print(f"   Files: {num_output_files}")
print(f"   Partition: year={year}/month={month}/day={day}/")

output_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .option("compression", "snappy") \
    .parquet(silver_path)

print(f"   ✅ Silver write complete")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 7: QUALITY SUMMARY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n📊 BRONZE → SILVER QUALITY REPORT: {entity}")
print(f"{'─'*50}")
print(f"   Input (Bronze raw):     {total_raw:>10,} rows")
print(f"   NULL PK dropped:        {null_pk_dropped:>10,} rows")
print(f"   Duplicates removed:     {dups_removed:>10,} rows")
print(f"   Output (Silver clean):  {deduped_count:>10,} rows")
print(f"   Retention rate:         {(deduped_count/max(total_raw,1))*100:>9.1f}%")
print(f"   Format: Parquet (Snappy)")
print(f"   Partitioned by: year/month/day")
print(f"{'─'*50}")

job.commit()