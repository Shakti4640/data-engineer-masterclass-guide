# file: 57_05_glue_etl_cross_account.py
# Purpose: Demonstrate Glue ETL reading from Account A's Catalog in Account B

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/catalog_sharing_config.json", "r") as f:
        return json.load(f)


def generate_glue_etl_script():
    """
    Generate a Glue ETL script that reads from Account A's Catalog
    
    KEY DIFFERENCES from same-account Glue (Project 25):
    → Must specify catalog_id in from_catalog()
    → Must have both: Catalog read permissions + S3 read permissions
    → Job runs in Account B but reads Account A's metadata + data
    """
    config = load_config()
    account_a_id = config["account_a_id"]

    glue_script = f'''
# ═══════════════════════════════════════════════════════════════
# CROSS-ACCOUNT GLUE ETL JOB
# Runs in Account B — reads Account A's Catalog + S3 data
# ═══════════════════════════════════════════════════════════════

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# --- Initialize ---
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_catalog_id",     # Account A's catalog ID
    "source_database",       # Database in Account A
    "output_path"            # S3 path in Account B for output
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_catalog_id = args["source_catalog_id"]   # "111111111111"
source_database = args["source_database"]         # "quickcart_db"
output_path = args["output_path"]                 # "s3://account-b-bucket/transformed/"

print(f"Reading from Account A Catalog: {{source_catalog_id}}")
print(f"Database: {{source_database}}")

# ═══════════════════════════════════════
# METHOD 1: from_catalog with catalog_id
# (Preferred — uses Glue native integration)
# ═══════════════════════════════════════

# Read orders from Account A's Catalog
orders_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=source_database,
    table_name="orders_gold",
    additional_options={{
        "catalogId": source_catalog_id    # ← KEY PARAMETER
    }},
    transformation_ctx="orders_source"
)

print(f"Orders schema:")
orders_dyf.printSchema()
print(f"Orders count: {{orders_dyf.count()}}")

# Read customers from Account A's Catalog
customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=source_database,
    table_name="customers_silver",
    additional_options={{
        "catalogId": source_catalog_id    # ← KEY PARAMETER
    }},
    transformation_ctx="customers_source"
)

print(f"Customers count: {{customers_dyf.count()}}")

# ═══════════════════════════════════════
# METHOD 2: Direct S3 read using Catalog metadata
# (Alternative — when from_catalog has issues)
# ═══════════════════════════════════════

# Get table metadata from Account A's Catalog via boto3
import boto3
glue_client = boto3.client("glue", region_name="{REGION}")

table_response = glue_client.get_table(
    CatalogId=source_catalog_id,
    DatabaseName=source_database,
    Name="orders_gold"
)

s3_location = table_response["Table"]["StorageDescriptor"]["Location"]
input_format = table_response["Table"]["StorageDescriptor"].get(
    "InputFormat", ""
)

print(f"S3 Location from Catalog: {{s3_location}}")
print(f"Format: {{input_format}}")

# Read directly from S3 using discovered location
# (Uses cross-account S3 access from Project 56)
if "parquet" in input_format.lower() or s3_location.endswith("parquet/"):
    orders_df = spark.read.parquet(s3_location)
else:
    orders_df = spark.read.csv(s3_location, header=True, inferSchema=True)

print(f"Direct S3 read count: {{orders_df.count()}}")

# ═══════════════════════════════════════
# TRANSFORM (runs in Account B)
# ═══════════════════════════════════════

# Convert to DynamicFrame for Glue transforms
orders_dyf = DynamicFrame.fromDF(orders_df, glueContext, "orders")

# Example: aggregate orders by date
orders_df = orders_dyf.toDF()
summary_df = orders_df.groupBy("order_date").agg(
    {{"order_id": "count", "total_amount": "sum"}}
).withColumnRenamed("count(order_id)", "order_count") \\
 .withColumnRenamed("sum(total_amount)", "total_revenue")

print(f"Summary rows: {{summary_df.count()}}")
summary_df.show(5)

# ═══════════════════════════════════════
# WRITE OUTPUT (to Account B's S3)
# ═══════════════════════════════════════

summary_dyf = DynamicFrame.fromDF(summary_df, glueContext, "summary")

glueContext.write_dynamic_frame.from_options(
    frame=summary_dyf,
    connection_type="s3",
    connection_options={{
        "path": output_path
    }},
    format="parquet",
    transformation_ctx="output_sink"
)

print(f"Output written to: {{output_path}}")

job.commit()
print("Job completed successfully!")
'''

    print("=" * 70)
    print("📝 CROSS-ACCOUNT GLUE ETL SCRIPT")
    print("=" * 70)
    print(glue_script)

    # Save script
    script_path = "/tmp/cross_account_glue_etl.py"
    with open(script_path, "w") as f:
        f.write(glue_script)
    print(f"\n💾 Script saved to: {script_path}")

    # Show how to create the Glue job
    print(f"\n{'─' * 50}")
    print(f"📌 TO CREATE THIS GLUE JOB IN ACCOUNT B:")
    print(f"{'─' * 50}")
    print(f"""
    aws glue create-job \\
      --name "cross-account-orders-etl" \\
      --role "arn:aws:iam::{config['account_b_id']}:role/QuickCart-Analytics-GlueRole" \\
      --command '{{
        "Name": "glueetl",
        "ScriptLocation": "s3://account-b-scripts/cross_account_glue_etl.py",
        "PythonVersion": "3"
      }}' \\
      --default-arguments '{{
        "--source_catalog_id": "{account_a_id}",
        "--source_database": "quickcart_db",
        "--output_path": "s3://account-b-results/transformed/",
        "--job-language": "python"
      }}' \\
      --glue-version "4.0" \\
      --number-of-workers 5 \\
      --worker-type "G.1X"
    """)

    return glue_script


def generate_redshift_spectrum_example():
    """
    Show how Redshift Spectrum references cross-account Catalog
    """
    config = load_config()
    account_a_id = config["account_a_id"]
    account_b_id = config["account_b_id"]

    print(f"\n{'=' * 70}")
    print(f"📝 REDSHIFT SPECTRUM CROSS-ACCOUNT CATALOG EXAMPLE")
    print(f"{'=' * 70}")

    sql = f"""
-- ═══════════════════════════════════════════════════════════
-- Run this in Account B's Redshift cluster
-- References Account A's Glue Catalog for external tables
-- ═══════════════════════════════════════════════════════════

-- Step 1: Create external schema pointing to Account A's Catalog
CREATE EXTERNAL SCHEMA IF NOT EXISTS acct_a_quickcart
FROM DATA CATALOG
DATABASE 'quickcart_db'
CATALOG_ID '{account_a_id}'
IAM_ROLE 'arn:aws:iam::{account_b_id}:role/QuickCart-Analytics-RedshiftRole';

-- Step 2: Query Account A's tables via Spectrum
SELECT order_date,
       COUNT(*) as order_count,
       SUM(total_amount) as revenue
FROM acct_a_quickcart.orders_gold
WHERE order_date >= '2025-01-01'
GROUP BY order_date
ORDER BY order_date;

-- Step 3: Join Spectrum (Account A) with local Redshift tables (Account B)
SELECT s.order_date,
       s.order_count,
       s.revenue,
       l.forecast_revenue
FROM (
    SELECT order_date, COUNT(*) as order_count, SUM(total_amount) as revenue
    FROM acct_a_quickcart.orders_gold
    GROUP BY order_date
) s
JOIN local_schema.revenue_forecast l
ON s.order_date = l.forecast_date;

-- NOTE: Redshift IAM role needs:
-- 1. glue:GetTable, glue:GetPartitions on Account A's Catalog
-- 2. s3:GetObject on Account A's S3 bucket
-- 3. Both via cross-account (Project 56 + Project 57)
"""
    print(sql)

    return sql


if __name__ == "__main__":
    generate_glue_etl_script()
    generate_redshift_spectrum_example()