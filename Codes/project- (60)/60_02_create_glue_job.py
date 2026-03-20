# file: 60_02_create_glue_job.py
# Purpose: Create the Glue job in Account B with proper configuration

import boto3
import json

REGION = "us-east-2"
ACCOUNT_A_ID [REDACTED:BANK_ACCOUNT_NUMBER]111"
ACCOUNT_B_ID [REDACTED:BANK_ACCOUNT_NUMBER]222"

JOB_NAME = "cross-account-analytics-etl"
GLUE_ROLE_NAME = "QuickCart-Analytics-GlueRole"
SCRIPT_S3_PATH = f"s3://quickcart-analytics-scripts-{ACCOUNT_B_ID}/glue/cross_account_etl.py"
CROSS_ACCOUNT_ROLE = f"arn:aws:iam::{ACCOUNT_A_ID}:role/CrossAccount-S3Read-FromAnalytics"
EXTERNAL_ID = "quickcart-analytics-2025-xK9mP2"


def create_cross_account_glue_job():
    """Create Glue job in Account B configured for cross-account ETL"""
    glue_client = boto3.client("glue", region_name=REGION)
    iam_client = boto3.client("iam")

    print("=" * 70)
    print("🔧 CREATING CROSS-ACCOUNT GLUE ETL JOB (Account B)")
    print("=" * 70)

    # Get Glue role ARN
    role = iam_client.get_role(RoleName=GLUE_ROLE_NAME)
    role_arn = role["Role"]["Arn"]
    print(f"  Glue Role: {role_arn}")

    # Create the Glue job
    try:
        glue_client.create_job(
            Name=JOB_NAME,
            Description=(
                "Cross-account ETL: reads from Account A's S3/Catalog, "
                "transforms in Account B, loads Account B's Redshift"
            ),
            Role=role_arn,
            Command={
                "Name": "glueetl",
                "ScriptLocation": SCRIPT_S3_PATH,
                "PythonVersion": "3"
            },
            DefaultArguments={
                "--source_catalog_id": ACCOUNT_A_ID,
                "--source_database": "quickcart_db",
                "--cross_account_role_arn": CROSS_ACCOUNT_ROLE,
                "--external_id": EXTERNAL_ID,
                "--target_redshift_connection": "quickcart-analytics-redshift",
                "--target_database": "analytics",
                "--target_schema": "bi",
                "--staging_s3_path": f"s3://quickcart-analytics-staging-{ACCOUNT_B_ID}/",
                "--execution_date": "auto",
                "--job-language": "python",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--TempDir": f"s3://quickcart-analytics-staging-{ACCOUNT_B_ID}/glue-temp/"
            },
            GlueVersion="4.0",
            WorkerType="G.1X",
            NumberOfWorkers=10,
            Timeout=240,  # 4 hours max
            MaxRetries=1,
            Tags={
                "Team": "Analytics",
                "Pipeline": "cross-account-analytics",
                "SourceAccount": ACCOUNT_A_ID,
                "Environment": "Production"
            }
        )
        print(f"  ✅ Glue job created: {JOB_NAME}")

    except glue_client.exceptions.AlreadyExistsException:
        # Update existing
        glue_client.update_job(
            JobName=JOB_NAME,
            JobUpdate={
                "Role": role_arn,
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": SCRIPT_S3_PATH,
                    "PythonVersion": "3"
                },
                "DefaultArguments": {
                    "--source_catalog_id": ACCOUNT_A_ID,
                    "--source_database": "quickcart_db",
                    "--cross_account_role_arn": CROSS_ACCOUNT_ROLE,
                    "--external_id": EXTERNAL_ID,
                    "--target_redshift_connection": "quickcart-analytics-redshift",
                    "--target_database": "analytics",
                    "--target_schema": "bi",
                    "--staging_s3_path": f"s3://quickcart-analytics-staging-{ACCOUNT_B_ID}/",
                    "--execution_date": "auto",
                    "--job-language": "python",
                    "--enable-metrics": "true",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--TempDir": f"s3://quickcart-analytics-staging-{ACCOUNT_B_ID}/glue-temp/"
                },
                "GlueVersion": "4.0",
                "WorkerType": "G.1X",
                "NumberOfWorkers": 10,
                "Timeout": 240,
                "MaxRetries": 1
            }
        )
        print(f"  ℹ️  Glue job updated: {JOB_NAME}")

    # Display job configuration
    print(f"\n  📋 JOB CONFIGURATION:")
    print(f"  ─────────────────────────────────")
    print(f"  Job Name:        {JOB_NAME}")
    print(f"  Glue Version:    4.0")
    print(f"  Workers:         10 × G.1X")
    print(f"  Timeout:         4 hours")
    print(f"  Script:          {SCRIPT_S3_PATH}")
    print(f"  Source (Acct A):  Catalog {ACCOUNT_A_ID} / quickcart_db")
    print(f"  Target (Acct B):  Redshift analytics.bi")
    print(f"  Cross-account:    {CROSS_ACCOUNT_ROLE}")

    # Save config
    config = {
        "job_name": JOB_NAME,
        "role_arn": role_arn,
        "source_catalog_id": ACCOUNT_A_ID,
        "cross_account_role": CROSS_ACCOUNT_ROLE,
        "external_id": EXTERNAL_ID
    }
    with open("/tmp/cross_account_glue_config.json", "w") as f:
        json.dump(config, f, indent=2)

    return JOB_NAME


if __name__ == "__main__":
    create_cross_account_glue_job()