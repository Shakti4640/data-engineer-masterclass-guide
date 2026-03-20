# file: 60_01_cross_account_glue_etl.py
# Purpose: Glue ETL job that reads from Account A, transforms, writes to Account B's Redshift
# This script runs AS a Glue job in Account B

import sys
import json
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# ═══════════════════════════════════════════════════════════════
# INITIALIZATION
# ═══════════════════════════════════════════════════════════════

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_catalog_id",          # Account A's catalog ID
    "source_database",            # Database in Account A
    "cross_account_role_arn",     # Account A's cross-account role
    "external_id",                # Confused deputy prevention
    "target_redshift_connection", # Account B's Redshift connection name
    "target_database",            # Account B's Redshift database
    "target_schema",              # Account B's Redshift schema
    "staging_s3_path",            # Account B's staging S3 path
    "execution_date"              # Processing date
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Extract arguments
SOURCE_CATALOG_ID = args["source_catalog_id"]
SOURCE_DATABASE = args["source_database"]
CROSS_ACCOUNT_ROLE = args["cross_account_role_arn"]
EXTERNAL_ID = args["external_id"]
REDSHIFT_CONNECTION = args["target_redshift_connection"]
TARGET_DB = args["target_database"]
TARGET_SCHEMA = args["target_schema"]
STAGING_PATH = args["staging_s3_path"]
EXEC_DATE = args["execution_date"]

logger.info(f"═══════════════════════════════════════════════════")
logger.info(f"CROSS-ACCOUNT ANALYTICS ETL — Started")
logger.info(f"Source Catalog: {SOURCE_CATALOG_ID}")
logger.info(f"Source Database: {SOURCE_DATABASE}")
logger.info(f"Target: {TARGET_DB}.{TARGET_SCHEMA}")
logger.info(f"Execution Date: {EXEC_DATE}")
logger.info(f"═══════════════════════════════════════════════════")


# ═══════════════════════════════════════════════════════════════
# PHASE 1: ASSUME ROLE INTO ACCOUNT A
# ═══════════════════════════════════════════════════════════════

logger.info("PHASE 1: Assuming cross-account role...")

sts_client = boto3.client("sts")

try:
    assume_response = sts_client.assume_role(
        RoleArn=CROSS_ACCOUNT_ROLE,
        RoleSessionName=f"glue-etl-{EXEC_DATE}",
        ExternalId=EXTERNAL_ID,
        DurationSeconds=14400  # 4 hours — enough for large jobs
    )
    cross_creds = assume_response["Credentials"]
    logger.info(f"AssumeRole succeeded — expires: {cross_creds['Expiration']}")

except Exception as e:
    logger.error(f"AssumeRole FAILED: {e}")
    logger.error(f"Check: trust policy on {CROSS_ACCOUNT_ROLE}")
    logger.error(f"Check: sts:AssumeRole permission on this Glue role")
    raise


# ═══════════════════════════════════════════════════════════════
# PHASE 2: READ FROM ACCOUNT A's CATALOG + S3
# ═══════════════════════════════════════════════════════════════

logger.info("PHASE 2: Reading from Account A...")

# Create cross-account Glue client
cross_glue = boto3.client(
    "glue",
    region_name="us-east-2",
    aws_access_key_id=cross_creds["AccessKeyId"],
    aws_secret_access_key=cross_creds["SecretAccessKey"],
    aws_session_token=cross_creds["SessionToken"]
)

# --- Read orders_gold table metadata ---
logger.info("Reading orders_gold metadata from Account A's Catalog...")
try:
    orders_table = cross_glue.get_table(
        CatalogId=SOURCE_CATALOG_ID,
        DatabaseName=SOURCE_DATABASE,
        Name="orders_gold"
    )
    orders_s3_location = orders_table["Table"]["StorageDescriptor"]["Location"]
    orders_columns = [
        col["Name"] for col in orders_table["Table"]["StorageDescriptor"]["Columns"]
    ]
    logger.info(f"orders_gold S3 location: {orders_s3_location}")
    logger.info(f"orders_gold columns: {orders_columns}")
except Exception as e:
    logger.error(f"Failed to read orders_gold metadata: {e}")
    raise

# --- Read customers_silver table metadata ---
logger.info("Reading customers_silver metadata from Account A's Catalog...")
try:
    customers_table = cross_glue.get_table(
        CatalogId=SOURCE_CATALOG_ID,
        DatabaseName=SOURCE_DATABASE,
        Name="customers_silver"
    )
    customers_s3_location = customers_table["Table"]["StorageDescriptor"]["Location"]
    logger.info(f"customers_silver S3 location: {customers_s3_location}")
except Exception as e:
    logger.error(f"Failed to read customers_silver metadata: {e}")
    raise

# --- Configure Spark to read from Account A's S3 ---
logger.info("Configuring Spark for cross-account S3 read...")
hadoop_conf = sc._jsc.hadoopConfiguration()

# Save original credentials (to restore later)
original_access_key = hadoop_conf.get("fs.s3a.access.key", "")
original_secret_key = hadoop_conf.get("fs.s3a.secret.key", "")
original_session_token = hadoop_conf.get("fs.s3a.session.token", "")

# Set Account A's credentials
hadoop_conf.set("fs.s3a.access.key", cross_creds["AccessKeyId"])
hadoop_conf.set("fs.s3a.secret.key", cross_creds["SecretAccessKey"])
hadoop_conf.set("fs.s3a.session.token", cross_creds["SessionToken"])
hadoop_conf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

# --- Read data from Account A's S3 ---
logger.info(f"Reading orders data from: {orders_s3_location}")
try:
    # Use s3a:// protocol for Hadoop S3A connector
    orders_s3a = orders_s3_location.replace("s3://", "s3a://")
    orders_df = spark.read.parquet(orders_s3a)
    orders_count = orders_df.count()
    logger.info(f"Orders loaded: {orders_count:,} rows")
except Exception as e:
    logger.error(f"Failed to read orders from Account A's S3: {e}")
    logger.error("Check: cross-account role has s3:GetObject permission")
    raise

logger.info(f"Reading customers data from: {customers_s3_location}")
try:
    customers_s3a = customers_s3_location.replace("s3://", "s3a://")
    customers_df = spark.read.parquet(customers_s3a)
    customers_count = customers_df.count()
    logger.info(f"Customers loaded: {customers_count:,} rows")
except Exception as e:
    logger.error(f"Failed to read customers from Account A's S3: {e}")
    raise


# ═══════════════════════════════════════════════════════════════
# PHASE 3: RESET CREDENTIALS TO ACCOUNT B
# (Critical — all subsequent operations use Account B's creds)
# ═══════════════════════════════════════════════════════════════

logger.info("PHASE 3: Resetting credentials to Account B...")

# Clear Account A's credentials from Hadoop
hadoop_conf.unset("fs.s3a.access.key")
hadoop_conf.unset("fs.s3a.secret.key")
hadoop_conf.unset("fs.s3a.session.token")
hadoop_conf.set("fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")

logger.info("Hadoop credentials reset to Account B's Glue role")


# ═══════════════════════════════════════════════════════════════
# PHASE 4: READ ACCOUNT B's LOCAL DATA
# ═══════════════════════════════════════════════════════════════

logger.info("PHASE 4: Reading Account B's local reference data...")

try:
    # Read local forecast data (Account B's own S3)
    forecast_df = spark.read.parquet(f"{STAGING_PATH}reference/forecast/")
    logger.info(f"Forecast data loaded: {forecast_df.count():,} rows")
except Exception as e:
    logger.warn(f"No forecast data found — creating empty: {e}")
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    forecast_schema = StructType([
        StructField("order_date", StringType(), True),
        StructField("forecast_revenue", DoubleType(), True)
    ])
    forecast_df = spark.createDataFrame([], forecast_schema)


# ═══════════════════════════════════════════════════════════════
# PHASE 5: TRANSFORM (Account B's compute — the business logic)
# ═══════════════════════════════════════════════════════════════

logger.info("PHASE 5: Transforming data...")

# --- Transform 1: Enrich orders with customer data ---
enriched_orders = orders_df.join(
    customers_df.select(
        "customer_id", "name", "city", "state", "tier"
    ),
    on="customer_id",
    how="left"
)
logger.info(f"Enriched orders: {enriched_orders.count():,} rows")

# --- Transform 2: Daily aggregation ---
daily_summary = enriched_orders.groupBy("order_date").agg(
    F.count("order_id").alias("order_count"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.sum("total_amount").alias("total_revenue"),
    F.avg("total_amount").alias("avg_order_value"),
    F.max("total_amount").alias("max_order_value")
)

# --- Transform 3: Join with forecast (Account B's local data) ---
if forecast_df.count() > 0:
    daily_with_forecast = daily_summary.join(
        forecast_df,
        on="order_date",
        how="left"
    ).withColumn(
        "variance",
        F.col("total_revenue") - F.col("forecast_revenue")
    ).withColumn(
        "variance_pct",
        F.round(
            (F.col("total_revenue") / F.col("forecast_revenue") - 1) * 100,
            2
        )
    )
else:
    daily_with_forecast = daily_summary.withColumn(
        "forecast_revenue", F.lit(None)
    ).withColumn(
        "variance", F.lit(None)
    ).withColumn(
        "variance_pct", F.lit(None)
    )

# --- Transform 4: Customer tier summary ---
tier_summary = enriched_orders.groupBy("tier", "order_date").agg(
    F.count("order_id").alias("order_count"),
    F.sum("total_amount").alias("revenue"),
    F.countDistinct("customer_id").alias("customers")
)

# --- Transform 5: Add metadata columns ---
daily_with_forecast = daily_with_forecast.withColumn(
    "etl_timestamp", F.lit(datetime.utcnow().isoformat())
).withColumn(
    "source_account", F.lit(SOURCE_CATALOG_ID)
).withColumn(
    "execution_date", F.lit(EXEC_DATE)
)

tier_summary = tier_summary.withColumn(
    "etl_timestamp", F.lit(datetime.utcnow().isoformat())
).withColumn(
    "source_account", F.lit(SOURCE_CATALOG_ID)
)

logger.info(f"Daily summary: {daily_with_forecast.count():,} rows")
logger.info(f"Tier summary: {tier_summary.count():,} rows")


# ═══════════════════════════════════════════════════════════════
# PHASE 6: WRITE TO ACCOUNT B's REDSHIFT
# ═══════════════════════════════════════════════════════════════

logger.info("PHASE 6: Writing to Account B's Redshift...")

# Convert to DynamicFrames for Glue connector
daily_dyf = DynamicFrame.fromDF(daily_with_forecast, glueContext, "daily")
tier_dyf = DynamicFrame.fromDF(tier_summary, glueContext, "tier")

# --- Write daily summary ---
logger.info("Writing daily_revenue_summary to Redshift...")
try:
    glueContext.write_dynamic_frame.from_options(
        frame=daily_dyf,
        connection_type="redshift",
        connection_options={
            "url": f"jdbc:redshift://workgroup.account-b.us-east-2.redshift-serverless.amazonaws.com:5439/{TARGET_DB}",
            "dbtable": f"{TARGET_SCHEMA}.daily_revenue_summary",
            "redshiftTmpDir": f"{STAGING_PATH}redshift-temp/daily/",
            "preactions": f"DELETE FROM {TARGET_SCHEMA}.daily_revenue_summary WHERE execution_date = '{EXEC_DATE}';",
            "connectionName": REDSHIFT_CONNECTION
        },
        transformation_ctx="write_daily"
    )
    logger.info("daily_revenue_summary written successfully")
except Exception as e:
    logger.error(f"Failed to write daily_revenue_summary: {e}")
    raise

# --- Write tier summary ---
logger.info("Writing tier_revenue_summary to Redshift...")
try:
    glueContext.write_dynamic_frame.from_options(
        frame=tier_dyf,
        connection_type="redshift",
        connection_options={
            "url": f"jdbc:redshift://workgroup.account-b.us-east-2.redshift-serverless.amazonaws.com:5439/{TARGET_DB}",
            "dbtable": f"{TARGET_SCHEMA}.tier_revenue_summary",
            "redshiftTmpDir": f"{STAGING_PATH}redshift-temp/tier/",
            "preactions": f"DELETE FROM {TARGET_SCHEMA}.tier_revenue_summary WHERE execution_date = '{EXEC_DATE}';",
            "connectionName": REDSHIFT_CONNECTION
        },
        transformation_ctx="write_tier"
    )
    logger.info("tier_revenue_summary written successfully")
except Exception as e:
    logger.error(f"Failed to write tier_revenue_summary: {e}")
    raise


# ═══════════════════════════════════════════════════════════════
# PHASE 7: PUBLISH COMPLETION EVENT + CLEANUP
# ═══════════════════════════════════════════════════════════════

logger.info("PHASE 7: Publishing completion event...")

# Publish to Account B's own SNS topic
sns_client = boto3.client("sns", region_name="us-east-2")
try:
    sns_client.publish(
        TopicArn="arn:aws:sns:us-east-2:222222222222:quickcart-analytics-pipeline-events",
        Message=json.dumps({
            "event_type": "analytics_etl_completed",
            "pipeline": "cross-account-analytics",
            "execution_date": EXEC_DATE,
            "source_account": SOURCE_CATALOG_ID,
            "tables_written": ["daily_revenue_summary", "tier_revenue_summary"],
            "rows_processed": {
                "orders_read": orders_count,
                "customers_read": customers_count,
                "daily_summary_written": daily_with_forecast.count(),
                "tier_summary_written": tier_summary.count()
            },
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }),
        MessageAttributes={
            "event_type": {
                "DataType": "String",
                "StringValue": "analytics_etl_completed"
            },
            "severity": {
                "DataType": "String",
                "StringValue": "info"
            }
        }
    )
    logger.info("Completion event published to SNS")
except Exception as e:
    logger.warn(f"Failed to publish completion event: {e}")
    # Non-fatal — job succeeded even if notification fails

# Commit job
job.commit()

logger.info("═══════════════════════════════════════════════════")
logger.info("CROSS-ACCOUNT ANALYTICS ETL — COMPLETED SUCCESSFULLY")
logger.info(f"Orders read from Account A: {orders_count:,}")
logger.info(f"Customers read from Account A: {customers_count:,}")
logger.info(f"Daily summary written to Account B: {daily_with_forecast.count():,}")
logger.info(f"Tier summary written to Account B: {tier_summary.count():,}")
logger.info("═══════════════════════════════════════════════════")