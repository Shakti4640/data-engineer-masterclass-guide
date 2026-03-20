# file: 65_04_deploy_cdc_job.py
# Run from: Account B (222222222222)
# Purpose: Create and schedule the CDC Glue job

import boto3

REGION = "us-east-2"
ACCOUNT_B_ID =[REDACTED:BANK_ACCOUNT_NUMBER]22"
GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_B_ID}:role/GlueCrossAccountETLRole"
SCRIPT_BUCKET = "quickcart-analytics-scripts"
SCRIPT_KEY = "glue/cdc_mariadb_to_redshift.py"
SCRIPT_S3_PATH = f"s3://{SCRIPT_BUCKET}/{SCRIPT_KEY}"


def upload_script():
    s3 = boto3.client("s3", region_name=REGION)
    with open("65_03_cdc_glue_job.py", "rb") as f:
        s3.upload_fileobj(f, SCRIPT_BUCKET, SCRIPT_KEY)
    print(f"✅ CDC script uploaded to {SCRIPT_S3_PATH}")


def create_cdc_job():
    """
    Create the CDC Glue job
    
    KEY DIFFERENCES FROM FULL LOAD JOB (Project 61):
    → Fewer DPUs: 3 instead of 10 (smaller data volume)
    → Bookmarks DISABLED: we manage our own watermarks
    → Additional args: CDC S3 path, safety margin
    → Shorter timeout: 30 min instead of 60 (delta is small)
    """
    glue = boto3.client("glue", region_name=REGION)

    try:
        glue.create_job(
            Name="cdc-mariadb-to-redshift",
            Description="Incremental CDC: Extract changed rows from MariaDB, merge into Redshift",
            Role=GLUE_ROLE_ARN,
            Command={
                "Name": "glueetl",
                "ScriptLocation": SCRIPT_S3_PATH,
                "PythonVersion": "3"
            },
            DefaultArguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-disable",
                "--TempDir": "s3://quickcart-analytics-temp/cdc-temp/",
                "--redshift_temp_dir": "s3://quickcart-analytics-temp/cdc-redshift-staging/",
                "--target_schema": "quickcart_ops",
                "--cdc_s3_base_path": "s3://quickcart-analytics-datalake/cdc/",
                "--safety_margin_minutes": "5",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--additional-python-modules": "boto3==1.28.0"
            },
            Connections={
                "Connections": [
                    "conn-cross-account-mariadb",
                    "conn-redshift-analytics"
                ]
            },
            GlueVersion="4.0",
            WorkerType="G.1X",
            NumberOfWorkers=3,   # Fewer DPUs — delta is small
            Timeout=30,          # 30 min max — delta should be fast
            MaxRetries=1,
            Tags={
                "Environment": "Production",
                "Pipeline": "CDC-Incremental",
                "Source": "AccountA-MariaDB",
                "Target": "AccountB-Redshift"
            }
        )
        print("✅ CDC Glue job created: cdc-mariadb-to-redshift")
        print(f"   Workers: 3 × G.1X (vs 10 for full load)")
        print(f"   Timeout: 30 min (vs 60 for full load)")
        print(f"   Estimated cost: $0.13/run (vs $1.85 for full load)")

    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Job already exists — updating...")
        glue.update_job(
            JobName="cdc-mariadb-to-redshift",
            JobUpdate={
                "Role": GLUE_ROLE_ARN,
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": SCRIPT_S3_PATH,
                    "PythonVersion": "3"
                },
                "DefaultArguments": {
                    "--job-language": "python",
                    "--job-bookmark-option": "job-bookmark-disable",
                    "--TempDir": "s3://quickcart-analytics-temp/cdc-temp/",
                    "--redshift_temp_dir": "s3://quickcart-analytics-temp/cdc-redshift-staging/",
                    "--target_schema": "quickcart_ops",
                    "--cdc_s3_base_path": "s3://quickcart-analytics-datalake/cdc/",
                    "--safety_margin_minutes": "5",
                    "--enable-metrics": "true",
                    "--enable-continuous-cloudwatch-log": "true"
                },
                "Connections": {
                    "Connections": [
                        "conn-cross-account-mariadb",
                        "conn-redshift-analytics"
                    ]
                },
                "GlueVersion": "4.0",
                "WorkerType": "G.1X",
                "NumberOfWorkers": 3,
                "Timeout": 30,
                "MaxRetries": 1
            }
        )
        print("✅ Job updated")


def create_schedule():
    """
    Replace old full-load schedule with CDC schedule
    
    SCHEDULE:
    → CDC: nightly at 2:00 AM (same as before)
    → Full reconciliation: weekly Sunday at 3:00 AM
    """
    glue = boto3.client("glue", region_name=REGION)

    # Daily CDC trigger
    try:
        glue.create_trigger(
            Name="trigger-nightly-cdc",
            Type="SCHEDULED",
            Schedule="cron(0 2 * * ? *)",
            Actions=[{"JobName": "cdc-mariadb-to-redshift"}],
            Description="Nightly CDC: incremental MariaDB → Redshift",
            StartOnCreation=True
        )
        print("✅ Daily CDC trigger created: 2:00 AM UTC")
    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Daily trigger already exists")

    # Weekly full reconciliation trigger (keep full load job from Project 61)
    try:
        glue.create_trigger(
            Name="trigger-weekly-full-reconciliation",
            Type="SCHEDULED",
            Schedule="cron(0 3 ? * SUN *)",
            Actions=[{
                "JobName": "cross-account-mariadb-to-redshift",
                "Arguments": {"--target_schema": "quickcart_ops"}
            }],
            Description="Weekly full load for reconciliation (Sunday 3 AM)",
            StartOnCreation=True
        )
        print("✅ Weekly reconciliation trigger created: Sunday 3:00 AM UTC")
    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Weekly trigger already exists")

    # Disable old nightly full-load trigger
    try:
        glue.update_trigger(
            Name="trigger-nightly-cross-account-etl",
            TriggerUpdate={
                "Name": "trigger-nightly-cross-account-etl",
                "Actions": [{"JobName": "cross-account-mariadb-to-redshift"}],
                "Schedule": "cron(0 2 * * ? *)",
                "Description": "DISABLED — replaced by CDC trigger"
            }
        )
        glue.stop_trigger(Name="trigger-nightly-cross-account-etl")
        print("✅ Old full-load nightly trigger DISABLED")
    except Exception as e:
        print(f"⚠️ Could not disable old trigger: {str(e)[:100]}")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 DEPLOYING CDC PIPELINE")
    print("=" * 60)

    upload_script()
    print()
    create_cdc_job()
    print()
    create_schedule()

    print(f"\n{'='*60}")
    print("✅ CDC PIPELINE DEPLOYED")
    print("   Daily CDC: 2:00 AM (incremental — 6 min)")
    print("   Weekly Full: Sunday 3:00 AM (reconciliation — 25 min)")
    print("   Old full load trigger: DISABLED")
    print(f"{'='*60}")