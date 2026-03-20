# file: 31_02_glue_etl_mariadb_to_s3.py
# Purpose: Glue PySpark job that extracts MariaDB tables → S3 Parquet
# Execution: Runs INSIDE Glue service (not on your machine)
# Upload to: s3://quickcart-datalake-prod/scripts/glue/mariadb_to_s3.py

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ═══════════════════════════════════════════════════════════════════
# STEP 1: Initialize Glue Context
# ═══════════════════════════════════════════════════════════════════
# Glue passes job parameters via sys.argv
# getResolvedOptions parses them into a dict
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_database",        # MariaDB database name
    "source_tables",          # Comma-separated table names
    "target_s3_path",         # s3://bucket/bronze/mariadb/
    "glue_connection_name",   # Glue JDBC connection name
    "extraction_date"         # YYYY-MM-DD — for partitioning
])

# SparkContext → GlueContext → Job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Logger for CloudWatch
logger = glueContext.get_logger()

# ═══════════════════════════════════════════════════════════════════
# STEP 2: Parse Parameters
# ═══════════════════════════════════════════════════════════════════
SOURCE_DATABASE = args["source_database"]            # "quickcart_shop"
SOURCE_TABLES = args["source_tables"].split(",")     # ["orders", "customers", "products"]
TARGET_S3_PATH = args["target_s3_path"]              # "s3://bucket/bronze/mariadb/"
CONNECTION_NAME = args["glue_connection_name"]        # "quickcart-mariadb-conn"
EXTRACTION_DATE = args["extraction_date"]             # "2025-01-15"

# Parse date components for partitioning
ext_date = datetime.strptime(EXTRACTION_DATE, "%Y-%m-%d")
YEAR = ext_date.strftime("%Y")
MONTH = ext_date.strftime("%m")
DAY = ext_date.strftime("%d")

logger.info(f"Starting extraction: {SOURCE_DATABASE} → {TARGET_S3_PATH}")
logger.info(f"Tables: {SOURCE_TABLES}")
logger.info(f"Extraction date: {EXTRACTION_DATE}")


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Define Table Configurations
# ═══════════════════════════════════════════════════════════════════
# Each table may need different extraction settings
# Large tables need parallel reads; small tables don't

TABLE_CONFIG = {
    "orders": {
        "hashfield": "order_id",          # Column to hash for parallel reads
        "hashpartitions": "10",           # Number of parallel JDBC connections
        "estimated_rows": 45_000_000,
        "partition_columns": ["year", "month", "day"],  # S3 output partitions
        "coalesce_files": 10              # Target number of output files
    },
    "customers": {
        "hashfield": "customer_id",
        "hashpartitions": "4",
        "estimated_rows": 2_000_000,
        "partition_columns": ["year", "month", "day"],
        "coalesce_files": 4
    },
    "products": {
        "hashfield": None,                # Small table — no parallel read needed
        "hashpartitions": None,
        "estimated_rows": 50_000,
        "partition_columns": ["year", "month", "day"],
        "coalesce_files": 1               # Single file for small table
    },
    "order_items": {
        "hashfield": "order_item_id",
        "hashpartitions": "10",
        "estimated_rows": 120_000_000,
        "partition_columns": ["year", "month", "day"],
        "coalesce_files": 20
    },
    "payments": {
        "hashfield": "payment_id",
        "hashpartitions": "5",
        "estimated_rows": 45_000_000,
        "partition_columns": ["year", "month", "day"],
        "coalesce_files": 8
    }
}


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Extract Function (Core Logic)
# ═══════════════════════════════════════════════════════════════════
def extract_table(table_name):
    """
    Extract a single table from MariaDB via JDBC
    
    HOW create_dynamic_frame.from_options WORKS WITH JDBC:
    
    1. Glue reads the Connection object (URL, creds, VPC config)
    2. Spark opens JDBC connection(s) to MariaDB
    3. If hashfield specified:
       → Opens N connections (hashpartitions)
       → Each runs: SELECT * FROM table WHERE MOD(HASH(hashfield), N) = i
       → Reads happen IN PARALLEL across Spark executors
    4. If no hashfield:
       → Single connection: SELECT * FROM table
       → Sequential read — fine for small tables
    5. Results stored as DynamicFrame (Glue's DataFrame wrapper)
    """
    config = TABLE_CONFIG.get(table_name, {
        "hashfield": None,
        "hashpartitions": None,
        "estimated_rows": 0,
        "partition_columns": ["year", "month", "day"],
        "coalesce_files": 2
    })

    logger.info(f"\n{'='*60}")
    logger.info(f"EXTRACTING: {SOURCE_DATABASE}.{table_name}")
    logger.info(f"Estimated rows: {config['estimated_rows']:,}")
    logger.info(f"Parallel connections: {config['hashpartitions'] or 1}")
    logger.info(f"{'='*60}")

    # ── BUILD CONNECTION OPTIONS ──
    connection_options = {
        "useConnectionProperties": "true",
        "connectionName": CONNECTION_NAME,
        "dbtable": table_name,

        # IMPORTANT: Tell Glue which JDBC driver to use
        # MariaDB is MySQL-compatible
        "connectionType": "mysql"
    }

    # ── ADD PARALLEL READ CONFIG (for large tables) ──
    if config["hashfield"]:
        connection_options["hashfield"] = config["hashfield"]
        connection_options["hashpartitions"] = config["hashpartitions"]

        logger.info(f"Parallel read enabled: hashfield={config['hashfield']}, "
                     f"partitions={config['hashpartitions']}")

    # ── READ FROM MARIADB ──
    start_time = datetime.now()

    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options=connection_options,
        transformation_ctx=f"read_{table_name}"
    )

    read_time = (datetime.now() - start_time).total_seconds()
    row_count = dyf.count()

    logger.info(f"Read complete: {row_count:,} rows in {read_time:.1f} seconds")

    # ── VALIDATE ──
    if row_count == 0:
        logger.warn(f"⚠️  WARNING: {table_name} returned 0 rows!")
        logger.warn("Possible causes: empty table, wrong database, permission issue")
        return None

    # Print schema for debugging / CloudWatch logs
    logger.info(f"Schema for {table_name}:")
    dyf.printSchema()

    return dyf, config, row_count, read_time


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Transform Function
# ═══════════════════════════════════════════════════════════════════
def transform_table(dyf, table_name):
    """
    Apply common transformations before writing to S3:
    
    1. Add extraction metadata columns
    2. Resolve ambiguous types (Project 34 covers this deeply)
    3. Handle NULL values
    
    WHY add metadata columns:
    → When you look at Parquet files in S3 weeks later
    → You need to know: WHEN was this extracted? FROM where?
    → These columns are your audit trail
    """

    # Convert to Spark DataFrame for richer transformations
    df = dyf.toDF()

    # ── ADD AUDIT COLUMNS ──
    df = df.withColumn("_extraction_date", F.lit(EXTRACTION_DATE))
    df = df.withColumn("_extraction_timestamp", F.lit(datetime.now().isoformat()))
    df = df.withColumn("_source_system", F.lit("mariadb"))
    df = df.withColumn("_source_database", F.lit(SOURCE_DATABASE))
    df = df.withColumn("_source_table", F.lit(table_name))
    # ── ADD PARTITION COLUMNS ──
    # These become the Hive-style partition directories in S3
    # year=2025/month=01/day=15/
    df = df.withColumn("year", F.lit(YEAR))
    df = df.withColumn("month", F.lit(MONTH))
    df = df.withColumn("day", F.lit(DAY))

    # ── HANDLE COMMON TYPE ISSUES ──
    # MariaDB DECIMAL → Spark reads as java.math.BigDecimal
    # Parquet doesn't handle BigDecimal well → cast to double
    from pyspark.sql.types import DecimalType

    for field in df.schema.fields:
        if isinstance(field.dataType, DecimalType):
            logger.info(f"  Casting {field.name}: Decimal → Double")
            df = df.withColumn(field.name, F.col(field.name).cast("double"))

    # ── HANDLE NULL STRINGS ──
    # MariaDB may return literal "NULL" strings or empty strings
    # Normalize to actual NULL for proper Parquet encoding
    from pyspark.sql.types import StringType

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(
                    (F.col(field.name) == "") | 
                    (F.upper(F.col(field.name)) == "NULL"),
                    F.lit(None)
                ).otherwise(F.col(field.name))
            )

    # Convert back to DynamicFrame for Glue-native write
    dyf_transformed = DynamicFrame.fromDF(df, glueContext, f"transform_{table_name}")

    logger.info(f"Transform complete for {table_name}")
    logger.info(f"  Final column count: {len(df.columns)}")
    logger.info(f"  Columns: {df.columns}")

    return dyf_transformed, df


# ═══════════════════════════════════════════════════════════════════
# STEP 6: Write Function (S3 Parquet Output)
# ═══════════════════════════════════════════════════════════════════
def write_table_to_s3(dyf, df, table_name, config):
    """
    Write DynamicFrame to S3 as partitioned Parquet
    
    OUTPUT STRUCTURE:
    s3://quickcart-datalake-prod/bronze/mariadb/orders/
        year=2025/
            month=01/
                day=15/
                    part-00000-abc123.snappy.parquet
                    part-00001-def456.snappy.parquet
                    ...
    
    WHY PARTITION BY DATE:
    → Each extraction creates its own partition
    → Athena can query specific dates efficiently (partition pruning)
    → No overwriting of previous extractions
    → Easy to delete/reprocess a specific date
    
    WHY COALESCE:
    → Spark default: 1 file per partition (Spark partition, not Hive partition)
    → 10 DPUs with 10 hashpartitions → 100+ tiny files possible
    → Coalesce reduces to target file count → better for Athena
    → Project 62 covers the small file problem in depth
    """
    output_path = f"{TARGET_S3_PATH}{table_name}/"
    target_files = config.get("coalesce_files", 4)

    logger.info(f"\nWriting {table_name} to: {output_path}")
    logger.info(f"  Partition columns: {config['partition_columns']}")
    logger.info(f"  Target file count: {target_files}")

    # ── COALESCE TO CONTROL FILE COUNT ──
    # Repartition by partition columns, then coalesce within each
    df_coalesced = df.coalesce(target_files)

    # Convert back to DynamicFrame after coalesce
    dyf_coalesced = DynamicFrame.fromDF(
        df_coalesced, glueContext, f"coalesce_{table_name}"
    )

    start_time = datetime.now()

    # ── WRITE AS PARTITIONED PARQUET ──
    glueContext.write_dynamic_frame.from_options(
        frame=dyf_coalesced,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": config["partition_columns"]
        },
        format="parquet",
        format_options={
            "compression": "snappy"   # Fast compression, good balance
            # Other options: "gzip" (smaller but slower), "none", "lzo"
        },
        transformation_ctx=f"write_{table_name}"
    )

    write_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"  ✅ Write complete in {write_time:.1f} seconds")
    logger.info(f"  → Output: {output_path}year={YEAR}/month={MONTH}/day={DAY}/")

    return write_time


# ═══════════════════════════════════════════════════════════════════
# STEP 7: Register in Glue Catalog (So Athena Can Query)
# ═══════════════════════════════════════════════════════════════════
def register_in_catalog(table_name, df):
    """
    Create or update Glue Catalog table so Athena/Spectrum can query
    
    TWO APPROACHES:
    A) Run a Glue Crawler after this job (Project 23) — automatic
    B) Register manually via Spark — more control, immediate
    
    We use approach B here — avoids running a separate crawler
    """
    catalog_database = "bronze_mariadb"
    catalog_table = table_name
    table_path = f"{TARGET_S3_PATH}{table_name}/"

    logger.info(f"\n📋 Registering in Glue Catalog: {catalog_database}.{catalog_table}")

    # ── ENSURE CATALOG DATABASE EXISTS ──
    try:
        glue_client = boto3.client("glue")
        glue_client.create_database(
            DatabaseInput={
                "Name": catalog_database,
                "Description": "Bronze layer - raw MariaDB extracts"
            }
        )
        logger.info(f"  Created catalog database: {catalog_database}")
    except Exception:
        # Already exists — fine
        logger.info(f"  Catalog database already exists: {catalog_database}")

    # ── WRITE TABLE DEFINITION VIA SPARK CATALOG ──
    # This creates/updates the Glue Catalog table using Spark's Hive metastore integration
    # Glue IS the Hive metastore in AWS

    # Create a temp view, then write as external table
    df_sample = df.limit(0)  # Schema only, no data
    
    # Use Spark SQL to create external table
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_database}")
    
    # Register via saveAsTable — overwrites table definition
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .option("path", table_path) \
        .partitionBy("year", "month", "day") \
        .saveAsTable(f"{catalog_database}.{catalog_table}")

    logger.info(f"  ✅ Catalog table registered: {catalog_database}.{catalog_table}")
    logger.info(f"  → Athena query: SELECT * FROM {catalog_database}.{catalog_table} LIMIT 10")

    return f"{catalog_database}.{catalog_table}"


# ═══════════════════════════════════════════════════════════════════
# STEP 8: Alternative Simpler Catalog Registration
# ═══════════════════════════════════════════════════════════════════
def register_in_catalog_via_boto3(table_name, df):
    """
    Alternative: Register using boto3 Glue API directly
    More explicit, works when Spark catalog integration has issues
    
    USE THIS when you want precise control over:
    → Column types in catalog
    → SerDe properties
    → Table parameters
    """
    import boto3
    glue_client = boto3.client("glue", region_name="us-east-2")

    catalog_database = "bronze_mariadb"
    table_path = f"{TARGET_S3_PATH}{table_name}/"

    # ── BUILD COLUMN DEFINITIONS FROM SPARK SCHEMA ──
    type_mapping = {
        "StringType": "string",
        "IntegerType": "int",
        "LongType": "bigint",
        "DoubleType": "double",
        "FloatType": "float",
        "BooleanType": "boolean",
        "TimestampType": "timestamp",
        "DateType": "date"
    }

    columns = []
    for field in df.schema.fields:
        if field.name not in ["year", "month", "day"]:  # Exclude partition cols
            spark_type = type(field.dataType).__name__
            glue_type = type_mapping.get(spark_type, "string")
            columns.append({
                "Name": field.name,
                "Type": glue_type,
                "Comment": f"From MariaDB {table_name}.{field.name}"
            })

    # ── PARTITION KEYS ──
    partition_keys = [
        {"Name": "year", "Type": "string"},
        {"Name": "month", "Type": "string"},
        {"Name": "day", "Type": "string"}
    ]

    # ── CREATE OR UPDATE TABLE ──
    table_input = {
        "Name": table_name,
        "Description": f"Bronze layer extract from MariaDB {table_name}",
        "StorageDescriptor": {
            "Columns": columns,
            "Location": table_path,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": True,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {
                    "serialization.format": "1"
                }
            }
        },
        "PartitionKeys": partition_keys,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "compressionType": "snappy",
            "typeOfData": "file",
            "source_system": "mariadb",
            "source_database": "quickcart_shop",
            "source_table": table_name,
            "extraction_script": "31_02_glue_etl_mariadb_to_s3.py"
        }
    }

    try:
        glue_client.create_table(
            DatabaseName=catalog_database,
            TableInput=table_input
        )
        logger.info(f"  ✅ Created catalog table: {catalog_database}.{table_name}")
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_table(
            DatabaseName=catalog_database,
            TableInput=table_input
        )
        logger.info(f"  ✅ Updated catalog table: {catalog_database}.{table_name}")

    # ── ADD PARTITION FOR TODAY'S DATA ──
    try:
        glue_client.create_partition(
            DatabaseName=catalog_database,
            TableName=table_name,
            PartitionInput={
                "Values": [YEAR, MONTH, DAY],
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": f"{table_path}year={YEAR}/month={MONTH}/day={DAY}/",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    }
                }
            }
        )
        logger.info(f"  ✅ Partition added: year={YEAR}/month={MONTH}/day={DAY}")
    except glue_client.exceptions.AlreadyExistsException:
        logger.info(f"  ℹ️  Partition already exists: year={YEAR}/month={MONTH}/day={DAY}")


# ═══════════════════════════════════════════════════════════════════
# STEP 9: Main Orchestration Loop
# ═══════════════════════════════════════════════════════════════════
import boto3

extraction_summary = []
overall_start = datetime.now()

logger.info("=" * 70)
logger.info("🚀 MARIADB → S3 PARQUET EXTRACTION STARTING")
logger.info(f"   Database: {SOURCE_DATABASE}")
logger.info(f"   Tables: {SOURCE_TABLES}")
logger.info(f"   Target: {TARGET_S3_PATH}")
logger.info(f"   Date: {EXTRACTION_DATE}")
logger.info("=" * 70)

for table_name in SOURCE_TABLES:
    table_name = table_name.strip()  # Remove whitespace from CSV parsing
    table_start = datetime.now()

    try:
        # ── EXTRACT ──
        result = extract_table(table_name)
        if result is None:
            logger.warn(f"Skipping {table_name} — no data returned")
            extraction_summary.append({
                "table": table_name,
                "status": "SKIPPED",
                "rows": 0,
                "reason": "Empty table"
            })
            continue

        dyf, config, row_count, read_time = result

        # ── TRANSFORM ──
        dyf_transformed, df_transformed = transform_table(dyf, table_name)

        # ── WRITE ──
        write_time = write_table_to_s3(
            dyf_transformed, df_transformed, table_name, config
        )

        # ── REGISTER IN CATALOG ──
        register_in_catalog_via_boto3(table_name, df_transformed)

        # ── RECORD SUCCESS ──
        table_time = (datetime.now() - table_start).total_seconds()
        extraction_summary.append({
            "table": table_name,
            "status": "SUCCESS",
            "rows": row_count,
            "read_seconds": read_time,
            "write_seconds": write_time,
            "total_seconds": table_time
        })

        logger.info(f"\n✅ {table_name}: {row_count:,} rows in {table_time:.1f}s")

    except Exception as e:
        logger.error(f"\n❌ FAILED: {table_name}")
        logger.error(f"   Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

        extraction_summary.append({
            "table": table_name,
            "status": "FAILED",
            "rows": 0,
            "error": str(e)
        })
        # Continue to next table — don't let one failure stop all
        continue


# ═══════════════════════════════════════════════════════════════════
# STEP 10: Final Summary
# ═══════════════════════════════════════════════════════════════════
overall_time = (datetime.now() - overall_start).total_seconds()

logger.info("\n" + "=" * 70)
logger.info("📊 EXTRACTION SUMMARY")
logger.info("=" * 70)

total_rows = 0
success_count = 0
fail_count = 0

for entry in extraction_summary:
    status_icon = "✅" if entry["status"] == "SUCCESS" else "❌" if entry["status"] == "FAILED" else "⏭️"
    logger.info(f"  {status_icon} {entry['table']:20s} | {entry['status']:8s} | {entry['rows']:>12,} rows")

    if entry["status"] == "SUCCESS":
        total_rows += entry["rows"]
        success_count += 1
        logger.info(f"     Read: {entry['read_seconds']:.1f}s | Write: {entry['write_seconds']:.1f}s | Total: {entry['total_seconds']:.1f}s")
    elif entry["status"] == "FAILED":
        fail_count += 1
        logger.info(f"     Error: {entry.get('error', 'Unknown')}")

logger.info("-" * 70)
logger.info(f"  Total rows extracted: {total_rows:,}")
logger.info(f"  Succeeded: {success_count} | Failed: {fail_count}")
logger.info(f"  Total time: {overall_time:.1f} seconds ({overall_time/60:.1f} minutes)")
logger.info("=" * 70)

# ═══════════════════════════════════════════════════════════════════
# STEP 11: Commit Job (Required for Job Bookmarks — Project 33)
# ═══════════════════════════════════════════════════════════════════
job.commit()

# If any table failed, raise exception so Glue marks job as FAILED
# This triggers CloudWatch Alarm → SNS notification (Project 54)
if fail_count > 0:
    raise Exception(
        f"Extraction completed with {fail_count} failures. "
        f"Check logs for details."
    )