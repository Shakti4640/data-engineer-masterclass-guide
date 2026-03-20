# file: 45_04_adaptive_bronze_to_silver.py
# Purpose: Enhanced Bronze→Silver ETL that handles schema evolution
# Replaces the strict ETL from Project 44 (44_04)
# Runs INSIDE AWS Glue

import sys
import json
import boto3
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# These modules must be packaged with the Glue job
# or included via --extra-py-files
from schema_registry import get_active_schema
from schema_drift_detector import detect_schema_drift, print_change_report
from adaptive_transformer import (
    apply_schema_adaptations, save_drift_report,
    send_drift_alert, map_type_string_to_spark
)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "entity_name",
    "datalake_bucket",
    "process_date",
    "sns_topic_arn"
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

# Set SNS topic for alerts
import adaptive_transformer
adaptive_transformer.SNS_TOPIC_ARN = args.get("sns_topic_arn", "")
adaptive_transformer.DATALAKE_BUCKET = bucket


print("╔" + "═" * 68 + "╗")
print("║" + f"  ADAPTIVE BRONZE → SILVER ETL: {entity}".center(68) + "║")
print("║" + f"  Date: {process_date}".center(68) + "║")
print("╚" + "═" * 68 + "╝")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 1: READ BRONZE DATA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n📥 PHASE 1: Reading Bronze data...")

bronze_cdc_path = f"s3://{bucket}/bronze/mariadb/cdc/{entity}/year={year}/month={month}/day={day}/"
bronze_full_path = f"s3://{bucket}/bronze/mariadb/full_dump/{entity}/year={year}/month={month}/day={day}/"

cdc_df = None
full_df = None

try:
    cdc_df = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_cdc_path)
    cdc_count = cdc_df.count()
    print(f"   CDC: {cdc_count} rows")
except Exception:
    cdc_count = 0
    print(f"   CDC: no file")

try:
    full_df = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_full_path)
    full_count = full_df.count()
    print(f"   Full dump: {full_count} rows")
except Exception:
    full_count = 0
    print(f"   Full dump: no file")

if cdc_count > 0 and full_count > 0:
    raw_df = full_df.unionByName(cdc_df, allowMissingColumns=True)
elif cdc_count > 0:
    raw_df = cdc_df
elif full_count > 0:
    raw_df = full_df
else:
    print(f"   ⚠️  No Bronze data — exiting")
    job.commit()
    sys.exit(0)

total_raw = raw_df.count()
print(f"   Total raw: {total_raw} rows")
print(f"   Incoming columns: {raw_df.columns}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 2: SCHEMA DRIFT DETECTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🔍 PHASE 2: Detecting schema drift...")

# Get baseline schema from registry
baseline_schema = get_active_schema(entity)
print(f"   Baseline: v{baseline_schema['version']} ({len(baseline_schema['columns'])} columns)")
print(f"   Alias map: {baseline_schema.get('alias_map', {})}")

# Build incoming type map from Spark's inferred schema
incoming_columns = raw_df.columns
incoming_types = {}
for field in raw_df.schema.fields:
    type_name = field.dataType.simpleString()
    incoming_types[field.name] = type_name

print(f"   Incoming types: {incoming_types}")

# Detect drift
drift_report = detect_schema_drift(baseline_schema, incoming_columns, incoming_types)
print_change_report(drift_report)

# Save drift report to S3 (regardless of severity)
save_drift_report(drift_report, entity, process_date)

# Send alert if any changes detected
if drift_report["total_changes"] > 0:
    send_drift_alert(drift_report, entity)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 3: APPLY SCHEMA ADAPTATIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🔧 PHASE 3: Applying adaptations...")

if drift_report["pipeline_action"] == "FAIL":
    print(f"\n❌ PIPELINE CANNOT CONTINUE")
    print(f"   Reason: {drift_report['manual_required']} changes require manual intervention")
    print(f"   Action: Update schema registry alias_map or column definitions")
    print(f"   Then re-run this job")

    # Still commit job (don't leave it hanging)
    job.commit()
    # Raise exception so Step Functions catches it
    raise RuntimeError(
        f"Schema drift for {entity}: {drift_report['manual_required']} "
        f"changes require manual intervention. "
        f"Pipeline action: FAIL. See drift report in S3."
    )

# Apply adaptations
adapted_df, should_continue = apply_schema_adaptations(
    raw_df, entity, baseline_schema, drift_report
)

if not should_continue:
    raise RuntimeError(f"Schema adaptation failed for {entity}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 4: STANDARD SILVER TRANSFORMATIONS
# (same as Project 44, but on ADAPTED DataFrame)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🔧 PHASE 4: Silver transformations...")

# Get baseline columns for type casting
baseline_columns = {col["name"]: col for col in baseline_schema["columns"]}

# Cast baseline columns to expected types
for col_name, col_def in baseline_columns.items():
    if col_name in adapted_df.columns:
        spark_type = map_type_string_to_spark(col_def["type"])
        adapted_df = adapted_df.withColumn(col_name, F.col(col_name).cast(spark_type))

# Primary key mapping
pk_map = {"orders": "order_id", "customers": "customer_id", "products": "product_id"}
pk_column = pk_map.get(entity, adapted_df.columns[0])

# Drop rows with NULL primary key
before_count = adapted_df.count()
adapted_df = adapted_df.filter(F.col(pk_column).isNotNull())
after_null_drop = adapted_df.count()
null_dropped = before_count - after_null_drop
print(f"   NULL PK dropped: {null_dropped}")

# Deduplicate on primary key (keep latest updated_at)
if "updated_at" in adapted_df.columns:
    window = Window.partitionBy(pk_column).orderBy(F.col("updated_at").desc())
    adapted_df = adapted_df.withColumn("_rn", F.row_number().over(window)) \
                           .filter(F.col("_rn") == 1) \
                           .drop("_rn")

deduped_count = adapted_df.count()
dups_removed = after_null_drop - deduped_count
print(f"   Duplicates removed: {dups_removed}")

# Add ETL metadata columns
adapted_df = adapted_df \
    .withColumn("_etl_loaded_at", F.current_timestamp()) \
    .withColumn("_etl_source_zone", F.lit("bronze")) \
    .withColumn("_etl_process_date", F.lit(process_date)) \
    .withColumn("_etl_schema_version", F.lit(baseline_schema["version"])) \
    .withColumn("_etl_schema_drift", F.lit(drift_report["total_changes"] > 0))

print(f"   ETL metadata added")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 5: QUALITY GATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🔍 PHASE 5: Quality gate...")

quality_passed = True
quality_issues = []

# Check 1: Row count reasonable
if deduped_count == 0:
    quality_issues.append("ZERO rows after transformation")
    quality_passed = False

# Check 2: Required columns non-null
for col_def in baseline_schema["columns"]:
    if col_def.get("required_for_silver") and col_def["name"] in adapted_df.columns:
        null_count = adapted_df.filter(F.col(col_def["name"]).isNull()).count()
        if null_count > 0:
            pct = (null_count / max(deduped_count, 1)) * 100
            if pct > 5.0:
                quality_issues.append(
                    f"Required column '{col_def['name']}' has {null_count} NULLs ({pct:.1f}%)"
                )
                quality_passed = False
            else:
                print(f"   ⚠️  {col_def['name']}: {null_count} NULLs ({pct:.1f}%) — within tolerance")

# Check 3: No schema drift caused >10% data loss
if total_raw > 0:
    retention_rate = (deduped_count / total_raw) * 100
    if retention_rate < 50:
        quality_issues.append(f"Low retention rate: {retention_rate:.1f}% (expected >50%)")
        quality_passed = False

if quality_passed:
    print(f"   ✅ Quality gate PASSED")
else:
    print(f"   ❌ Quality gate FAILED:")
    for issue in quality_issues:
        print(f"      → {issue}")
    raise RuntimeError(f"Quality gate failed for {entity}: {quality_issues}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 6: WRITE TO SILVER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n📤 PHASE 6: Writing to Silver...")

# Add partition columns
output_df = adapted_df \
    .withColumn("year", F.lit(year)) \
    .withColumn("month", F.lit(month)) \
    .withColumn("day", F.lit(day))

# Coalesce for file count control (Project 62)
num_files = max(1, min(8, deduped_count // 250000))
output_df = output_df.coalesce(num_files)

silver_path = f"s3://{bucket}/silver/{entity}/"

output_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .option("compression", "snappy") \
    .parquet(silver_path)

print(f"   ✅ Written: {deduped_count:,} rows to {silver_path}")
print(f"   Files: {num_files}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 7: SUMMARY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n{'='*60}")
print(f"📊 ADAPTIVE BRONZE → SILVER REPORT: {entity}")
print(f"{'='*60}")
print(f"   Schema baseline: v{baseline_schema['version']}")
print(f"   Schema drift: {drift_report['total_changes']} changes "
      f"({drift_report['auto_handled']} auto, {drift_report['manual_required']} manual)")
print(f"   Pipeline action: {drift_report['pipeline_action']}")
print(f"   Input (Bronze):  {total_raw:>10,} rows")
print(f"   NULL PK dropped: {null_dropped:>10,} rows")
print(f"   Dups removed:    {dups_removed:>10,} rows")
print(f"   Output (Silver): {deduped_count:>10,} rows")
print(f"   Quality gate:    {'PASSED' if quality_passed else 'FAILED'}")
print(f"   Output columns:  {len(output_df.columns)}")
print(f"{'='*60}")

job.commit()