# file: 41_06_create_glue_resources.py
# Purpose: Create the Glue Crawler and ETL Job that Step Functions will orchestrate
# Builds on Projects 23 (Crawlers) and 25 (ETL Jobs)

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
BUCKET_NAME = "quickcart-raw-data-prod"
GLUE_DATABASE = "quickcart_raw"

# Glue service role (should exist from Project 23/25)
GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/quickcart-glue-service-role"

# Entities to create resources for
ENTITIES = ["orders", "customers", "products"]


def create_glue_database(glue_client):
    """Create the Glue Catalog database if not exists (Project 15 concept)"""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": GLUE_DATABASE,
                "Description": "QuickCart raw data catalog — CSV source files"
            }
        )
        print(f"✅ Glue Database created: {GLUE_DATABASE}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Glue Database already exists: {GLUE_DATABASE}")


def create_crawler_for_entity(glue_client, entity_name):
    """
    Create a Glue Crawler for a specific entity.
    
    WHY per-entity crawlers:
    → Each entity has different schema
    → Step Functions triggers the RIGHT crawler based on file prefix
    → Avoids crawling ALL data when only one entity has new files
    
    Naming convention: quickcart-{entity}-crawler
    → quickcart-orders-crawler
    → quickcart-customers-crawler
    → quickcart-products-crawler
    """
    crawler_name = f"quickcart-{entity_name}-crawler"
    s3_target = f"s3://{BUCKET_NAME}/daily_exports/{entity_name}/"

    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=GLUE_ROLE_ARN,
            DatabaseName=GLUE_DATABASE,
            Description=f"Crawls {entity_name} CSV files from daily exports",
            Targets={
                "S3Targets": [
                    {
                        "Path": s3_target,
                        "Exclusions": [
                            "**/_temporary/**",
                            "**/.spark-staging/**"
                        ]
                    }
                ]
            },
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG"
            },
            RecrawlPolicy={
                "RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"
            },
            Tags={
                "Environment": "Production",
                "Entity": entity_name,
                "Project": "QuickCart-ETL"
            }
        )
        print(f"✅ Crawler created: {crawler_name} → {s3_target}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Crawler already exists: {crawler_name}")


def create_etl_job_for_entity(glue_client, entity_name):
    """
    Create a Glue ETL Job for a specific entity.
    
    This job:
    → Reads CSV from source S3 path
    → Converts to Parquet
    → Writes to curated S3 path (Bronze → Silver transition, Project 44 concept)
    
    Naming convention: quickcart-{entity}-etl
    → quickcart-orders-etl
    → quickcart-customers-etl
    → quickcart-products-etl
    
    The actual ETL script is stored in S3.
    Step Functions passes --source_bucket, --source_key, --entity_name as arguments.
    """
    job_name = f"quickcart-{entity_name}-etl"
    script_location = f"s3://{BUCKET_NAME}/glue-scripts/{entity_name}_etl.py"
    output_path = f"s3://{BUCKET_NAME}/curated/{entity_name}/"
    temp_dir = f"s3://{BUCKET_NAME}/glue-temp/{entity_name}/"

    try:
        glue_client.create_job(
            Name=job_name,
            Description=f"Transform {entity_name} CSV to Parquet",
            Role=GLUE_ROLE_ARN,
            Command={
                "Name": "glueetl",
                "ScriptLocation": script_location,
                "PythonVersion": "3"
            },
            DefaultArguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--TempDir": temp_dir,
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--output_path": output_path,
                "--source_database": GLUE_DATABASE,
                "--source_table": entity_name,

                # Optimization: coalesce output to avoid small files (Project 62)
                "--output_file_count": "4"
            },
            MaxRetries=0,
            GlueVersion="4.0",
            NumberOfWorkers=2,
            WorkerType="G.1X",
            Timeout=30,
            Tags={
                "Environment": "Production",
                "Entity": entity_name,
                "Project": "QuickCart-ETL"
            }
        )
        print(f"✅ ETL Job created: {job_name}")
        print(f"   Script: {script_location}")
        print(f"   Output: {output_path}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  ETL Job already exists: {job_name}")


def upload_etl_script(entity_name):
    """
    Upload the Glue ETL script to S3.
    
    This is the actual PySpark code that runs inside the Glue job.
    Step Functions passes dynamic arguments via --source_key, --entity_name.
    """
    s3_client = boto3.client("s3", region_name=REGION)

    script_content = f'''
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET ARGUMENTS (passed by Step Functions)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_bucket",
    "source_key",
    "entity_name",
    "output_path",
    "source_database",
    "source_table",
    "output_file_count"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# READ: Source CSV from Glue Catalog
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"Reading from catalog: {{args['source_database']}}.{{args['source_table']}}")

source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args["source_database"],
    table_name=args["source_table"],
    transformation_ctx="source_dyf"
)

record_count = source_dyf.count()
print(f"Records read: {{record_count}}")

if record_count == 0:
    print("WARNING: No records found. Skipping write.")
    job.commit()
    sys.exit(0)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TRANSFORM: Basic cleaning
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Resolve any ambiguous types (Project 34 concept)
resolved_dyf = ResolveChoice.apply(
    frame=source_dyf,
    choice="match_catalog",
    database=args["source_database"],
    table_name=args["source_table"],
    transformation_ctx="resolved_dyf"
)

# Drop null primary key rows
cleaned_dyf = DropNullFields.apply(
    frame=resolved_dyf,
    transformation_ctx="cleaned_dyf"
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WRITE: Parquet to curated zone
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
output_path = args["output_path"]
num_files = int(args["output_file_count"])

print(f"Writing {{record_count}} records to: {{output_path}}")
print(f"Output files: {{num_files}}")

# Coalesce to control file count (prevents small file problem — Project 62)
output_df = cleaned_dyf.toDF().coalesce(num_files)
output_dyf = DynamicFrame.fromDF(output_df, glueContext, "output_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={{
        "path": output_path,
        "partitionKeys": []
    }},
    format="parquet",
    format_options={{
        "compression": "snappy"
    }},
    transformation_ctx="write_output"
)

print(f"SUCCESS: {{record_count}} records written to {{output_path}}")

job.commit()
'''

    script_key = f"glue-scripts/{entity_name}_etl.py"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=script_key,
        Body=script_content.encode("utf-8"),
        ContentType="text/x-python"
    )

    print(f"✅ ETL Script uploaded: s3://{BUCKET_NAME}/{script_key}")


def setup_all_entities():
    """Create Glue resources for all entities"""
    glue_client = boto3.client("glue", region_name=REGION)

    print("=" * 60)
    print("🔧 SETTING UP GLUE RESOURCES")
    print("=" * 60)

    # Database
    create_glue_database(glue_client)

    # Per-entity resources
    for entity in ENTITIES:
        print(f"\n--- {entity.upper()} ---")
        create_crawler_for_entity(glue_client, entity)
        create_etl_job_for_entity(glue_client, entity)
        upload_etl_script(entity)

    print("\n" + "=" * 60)
    print("✅ ALL GLUE RESOURCES READY")
    print("=" * 60)


if __name__ == "__main__":
    setup_all_entities()