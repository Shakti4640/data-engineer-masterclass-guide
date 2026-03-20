# file: 62_04_deploy_compaction_job.py
# Run from: Account B (222222222222)
# Purpose: Create the compaction Glue job and schedule it

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID [REDACTED:BANK_ACCOUNT_NUMBER]222"
GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_B_ID}:role/GlueCrossAccountETLRole"
SCRIPT_BUCKET = "quickcart-analytics-scripts"
SCRIPT_KEY = "glue/compact_small_files.py"
SCRIPT_S3_PATH = f"s3://{SCRIPT_BUCKET}/{SCRIPT_KEY}"


def upload_compaction_script():
    """Upload compaction script to S3"""
    s3 = boto3.client("s3", region_name=REGION)

    with open("62_03_compaction_job.py", "rb") as f:
        s3.upload_fileobj(f, SCRIPT_BUCKET, SCRIPT_KEY)

    print(f"✅ Compaction script uploaded to {SCRIPT_S3_PATH}")


def create_compaction_job():
    """
    Create parameterized compaction job
    
    PARAMETERIZED: same job works for ANY table
    → Pass source_path, target_path, etc. as arguments at runtime
    → One job definition, many triggers with different params
    → No need for separate job per table
    """
    glue = boto3.client("glue", region_name=REGION)

    try:
        glue.create_job(
            Name="s3-small-file-compaction",
            Description=(
                "Compacts small Parquet files in S3 data lake. "
                "Parameterized: pass source_path, target_path, etc."
            ),
            Role=GLUE_ROLE_ARN,
            Command={
                "Name": "glueetl",
                "ScriptLocation": SCRIPT_S3_PATH,
                "PythonVersion": "3"
            },
            DefaultArguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-disable",
                "--TempDir": "s3://quickcart-analytics-temp/compaction/",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                # Defaults — can be overridden per run
                "--target_file_size_mb": "128",
                "--file_format": "parquet",
                "--partition_keys": "none",
                "--min_files_threshold": "100"
            },
            GlueVersion="4.0",
            WorkerType="G.1X",
            NumberOfWorkers=5,
            Timeout=120,  # 2 hours max
            MaxRetries=0,  # Don't retry compaction automatically
            Tags={
                "Environment": "Production",
                "Purpose": "SmallFileCompaction",
                "ManagedBy": "DataEngineering"
            }
        )
        print("✅ Compaction job created: s3-small-file-compaction")

    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Compaction job already exists")


def create_weekly_schedule():
    """
    Schedule compaction for each table weekly
    
    STRATEGY:
    → Run every Sunday at 4 AM UTC
    → After all daily ETL jobs have completed
    → Separate trigger per table (different paths)
    """
    glue = boto3.client("glue", region_name=REGION)

    tables_to_compact = [
        {
            "name": "orders",
            "source": "s3://quickcart-analytics-datalake/silver/orders/",
            "target": "s3://quickcart-analytics-datalake/silver/orders_compacting/",
            "partition_keys": "order_date",
            "schedule": "cron(0 4 ? * SUN *)"  # Sunday 4 AM
        },
        {
            "name": "customers",
            "source": "s3://quickcart-analytics-datalake/silver/customers/",
            "target": "s3://quickcart-analytics-datalake/silver/customers_compacting/",
            "partition_keys": "none",
            "schedule": "cron(15 4 ? * SUN *)"  # Sunday 4:15 AM
        },
        {
            "name": "products",
            "source": "s3://quickcart-analytics-datalake/silver/products/",
            "target": "s3://quickcart-analytics-datalake/silver/products_compacting/",
            "partition_keys": "none",
            "schedule": "cron(30 4 ? * SUN *)"  # Sunday 4:30 AM
        }
    ]

    for table in tables_to_compact:
        trigger_name = f"trigger-compact-{table['name']}-weekly"

        try:
            glue.create_trigger(
                Name=trigger_name,
                Type="SCHEDULED",
                Schedule=table["schedule"],
                Actions=[
                    {
                        "JobName": "s3-small-file-compaction",
                        "Arguments": {
                            "--source_path": table["source"],
                            "--target_path": table["target"],
                            "--target_file_size_mb": "128",
                            "--file_format": "parquet",
                            "--partition_keys": table["partition_keys"],
                            "--min_files_threshold": "100"
                        }
                    }
                ],
                Description=f"Weekly compaction for {table['name']} table",
                StartOnCreation=True
            )
            print(f"✅ Trigger created: {trigger_name}")
            print(f"   Schedule: {table['schedule']}")
            print(f"   Source: {table['source']}")

        except glue.exceptions.AlreadyExistsException:
            print(f"ℹ️  Trigger already exists: {trigger_name}")


def run_compaction_manually(table_name, source_path, target_path, partition_keys="none"):
    """
    Trigger compaction immediately for testing or ad-hoc needs
    """
    glue = boto3.client("glue", region_name=REGION)

    response = glue.start_job_run(
        JobName="s3-small-file-compaction",
        Arguments={
            "--source_path": source_path,
            "--target_path": target_path,
            "--target_file_size_mb": "128",
            "--file_format": "parquet",
            "--partition_keys": partition_keys,
            "--min_files_threshold": "50"  # Lower threshold for testing
        }
    )

    run_id = response["JobRunId"]
    print(f"🚀 Compaction started for {table_name}")
    print(f"   Run ID: {run_id}")
    print(f"   Monitor: CloudWatch → /aws-glue/jobs/output")
    return run_id


if __name__ == "__main__":
    print("=" * 60)
    print("🔧 DEPLOYING COMPACTION INFRASTRUCTURE")
    print("=" * 60)

    upload_compaction_script()
    print()
    create_compaction_job()
    print()
    create_weekly_schedule()
    print()

    # Optionally run immediately for testing
    # run_compaction_manually(
    #     "orders",
    #     "s3://quickcart-analytics-datalake/silver/orders/",
    #     "s3://quickcart-analytics-datalake/silver/orders_compacting/",
    #     "order_date"
    # )
