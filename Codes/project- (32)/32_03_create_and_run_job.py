# file: 32_03_create_and_run_job.py
# Purpose: Create Glue Job for S3 → Redshift and trigger a run
# Run from: Your local machine or CI/CD pipeline

import boto3
import time
from datetime import datetime

# ─── CONFIGURATION ───────────────────────────────────────────────
AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

S3_BUCKET = "quickcart-datalake-prod"
SCRIPT_S3_PATH = f"s3://{S3_BUCKET}/scripts/glue/s3_to_redshift.py"
REDSHIFT_TMP_DIR = f"s3://{S3_BUCKET}/tmp/glue-redshift/"
SOURCE_S3_PATH = f"s3://{S3_BUCKET}/bronze/mariadb/"

GLUE_JOB_NAME = "quickcart-s3-to-redshift"
GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/GlueMariaDBExtractorRole"
REDSHIFT_CONN = "quickcart-redshift-conn"
REDSHIFT_IAM_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/RedshiftS3ReadRole"

glue = boto3.client("glue", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


def upload_script():
    """Upload Glue ETL script to S3"""
    print("📤 Uploading Glue ETL script...")
    s3.upload_file(
        Filename="32_02_glue_etl_s3_to_redshift.py",
        Bucket=S3_BUCKET,
        Key="scripts/glue/s3_to_redshift.py"
    )
    print(f"   ✅ Uploaded to: {SCRIPT_S3_PATH}")


def create_glue_job():
    """Create Glue Job for S3 → Redshift loading"""
    print("\n⚙️  Creating Glue Job...")

    try:
        glue.create_job(
            Name=GLUE_JOB_NAME,
            Description="Load S3 Parquet (bronze layer) into Redshift analytics schema",
            Role=GLUE_ROLE_ARN,
            Command={
                "Name": "glueetl",
                "ScriptLocation": SCRIPT_S3_PATH,
                "PythonVersion": "3"
            },
            # BOTH connections needed:
            # → Redshift connection (JDBC to Redshift)
            # → MariaDB connection not needed here (source is S3)
            # But Glue uses the connection for VPC ENI provisioning
            Connections={
                "Connections": [REDSHIFT_CONN]
            },
            DefaultArguments={
                "--source_s3_path": SOURCE_S3_PATH,
                "--target_tables": "orders,customers,products",
                "--redshift_connection": REDSHIFT_CONN,
                "--redshift_schema": "analytics",
                "--redshift_tmp_dir": REDSHIFT_TMP_DIR,
                "--redshift_iam_role": REDSHIFT_IAM_ROLE,
                "--extraction_date": datetime.now().strftime("%Y-%m-%d"),
                "--job-language": "python",
                "--TempDir": REDSHIFT_TMP_DIR,
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-auto-scaling": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{S3_BUCKET}/spark-logs/{GLUE_JOB_NAME}/",
                "--job-bookmark-option": "job-bookmark-disable",
                "--conf": "spark.sql.parquet.compression.codec=snappy"
            },
            GlueVersion="4.0",
            WorkerType="G.1X",
            NumberOfWorkers=10,
            Timeout=60,
            MaxRetries=1,
            Tags={
                "Environment": "Production",
                "Team": "Data-Engineering",
                "Pipeline": "S3-to-Redshift",
                "CostCenter": "ENG-001"
            }
        )
        print(f"   ✅ Job created: {GLUE_JOB_NAME}")

    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Job already exists — updating...")
        glue.update_job(
            JobName=GLUE_JOB_NAME,
            JobUpdate={
                "Role": GLUE_ROLE_ARN,
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": SCRIPT_S3_PATH,
                    "PythonVersion": "3"
                },
                "Connections": {"Connections": [REDSHIFT_CONN]},
                "GlueVersion": "4.0",
                "WorkerType": "G.1X",
                "NumberOfWorkers": 10,
                "Timeout": 60
            }
        )
        print(f"   ✅ Job updated: {GLUE_JOB_NAME}")


def run_job(tables="orders,customers,products", extraction_date=None):
    """Start Glue Job Run with parameter overrides"""
    if extraction_date is None:
        extraction_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n🚀 Starting Job Run...")
    print(f"   Tables: {tables}")
    print(f"   Date: {extraction_date}")

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--target_tables": tables,
            "--extraction_date": extraction_date
        }
    )
    run_id = response["JobRunId"]
    print(f"   ✅ Run ID: {run_id}")
    return run_id


def monitor_job(run_id):
    """Poll job status until completion — same pattern as Project 31"""
    print(f"\n📊 Monitoring: {run_id}")
    print("-" * 60)

    terminal_states = {"SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR"}

    while True:
        response = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
        run = response["JobRun"]
        state = run["JobRunState"]
        duration = run.get("ExecutionTime", 0)
        dpu_seconds = run.get("DPUSeconds", 0)

        ts = datetime.now().strftime("%H:%M:%S")
        print(f"   [{ts}] {state:12s} | {duration}s | {dpu_seconds} DPU-sec")

        if state in terminal_states:
            print("-" * 60)
            if state == "SUCCEEDED":
                cost = round(dpu_seconds * 0.44 / 3600, 4)
                print(f"   ✅ SUCCEEDED in {duration}s | Cost: ${cost:.4f}")
            elif state == "FAILED":
                print(f"   ❌ FAILED: {run.get('ErrorMessage', 'Check logs')}")
            return state

        time.sleep(15)


def verify_redshift_data(tables, extraction_date):
    """
    Print Redshift verification queries
    (Execute these on Redshift Query Editor)
    """
    print(f"\n🔍 VERIFICATION QUERIES — Run on Redshift:")
    print("=" * 60)

    for table in tables.split(","):
        table = table.strip()
        config = {
            "orders": "fact_orders",
            "customers": "dim_customers",
            "products": "dim_products"
        }
        redshift_table = config.get(table, table)

        print(f"""
-- Verify {redshift_table}
SELECT COUNT(*) as total_rows FROM analytics.{redshift_table};

SELECT COUNT(*) as todays_rows FROM analytics.{redshift_table} 
WHERE _extraction_date = '{extraction_date}';

SELECT * FROM analytics.{redshift_table} LIMIT 5;

-- Check for load errors
SELECT * FROM stl_load_errors 
WHERE filename LIKE '%{table}%' 
ORDER BY starttime DESC LIMIT 10;

-- Check load history
SELECT query, filename, lines_scanned, 
       num_files, num_files_unknown 
FROM stl_load_commits 
ORDER BY updated DESC LIMIT 5;
""")

    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    today = datetime.now().strftime("%Y-%m-%d")
    tables = "orders,customers,products"

    upload_script()
    create_glue_job()
    run_id = run_job(tables=tables, extraction_date=today)
    final_state = monitor_job(run_id)

    if final_state == "SUCCEEDED":
        verify_redshift_data(tables, today)

    print("\n🏁 DONE")