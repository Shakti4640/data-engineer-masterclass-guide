# file: 31_03_create_and_run_job.py
# Purpose: Create the Glue Job definition and trigger a run
# Run from: Your local machine or CI/CD pipeline

import boto3
import json
import time
from datetime import datetime

# ─── CONFIGURATION ───────────────────────────────────────────────
AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

S3_BUCKET = "quickcart-datalake-prod"
SCRIPT_S3_PATH = f"s3://{S3_BUCKET}/scripts/glue/mariadb_to_s3.py"
TEMP_S3_PATH = f"s3://{S3_BUCKET}/tmp/glue/"
TARGET_S3_PATH = f"s3://{S3_BUCKET}/bronze/mariadb/"

GLUE_JOB_NAME = "quickcart-mariadb-to-s3-parquet"
GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/GlueMariaDBExtractorRole"
GLUE_CONNECTION_NAME = "quickcart-mariadb-conn"

glue = boto3.client("glue", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Upload ETL Script to S3
# ═══════════════════════════════════════════════════════════════════
def upload_script():
    """Upload the Glue ETL script to S3 where Glue can access it"""
    print("📤 Uploading Glue ETL script to S3...")

    script_file = "31_02_glue_etl_mariadb_to_s3.py"

    s3.upload_file(
        Filename=script_file,
        Bucket=S3_BUCKET,
        Key="scripts/glue/mariadb_to_s3.py"
    )
    print(f"   ✅ Uploaded to: {SCRIPT_S3_PATH}")


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Create Glue Job Definition
# ═══════════════════════════════════════════════════════════════════
def create_glue_job():
    """
    Create the Glue Job with:
    → Glue 4.0 (Spark 3.3, Python 3.10)
    → 10 G.1X workers (4 vCPU, 16 GB each)
    → Auto-scaling enabled
    → JDBC Connection attached
    → Metrics and logging enabled
    → 60 minute timeout
    
    WORKER TYPES:
    ┌──────────┬────────┬────────┬─────────────────────────────────┐
    │ Type     │ vCPU   │ RAM    │ Use Case                        │
    ├──────────┼────────┼────────┼─────────────────────────────────┤
    │ Standard │ 4      │ 16 GB  │ Legacy — don't use              │
    │ G.1X     │ 4      │ 16 GB  │ Most ETL jobs                   │
    │ G.2X     │ 8      │ 32 GB  │ Memory-intensive transforms     │
    │ G.4X     │ 16     │ 64 GB  │ ML training, huge joins         │
    │ G.8X     │ 32     │ 128 GB │ Extreme workloads               │
    │ G.025X   │ 2      │ 4 GB   │ Light streaming jobs            │
    └──────────┴────────┴────────┴─────────────────────────────────┘
    
    For MariaDB extraction: G.1X is sufficient
    → Bottleneck is JDBC network I/O, not compute
    """
    print("\n⚙️  Creating Glue Job definition...")

    try:
        glue.create_job(
            Name=GLUE_JOB_NAME,
            Description="Extract MariaDB tables to S3 as partitioned Parquet",
            Role=GLUE_ROLE_ARN,

            # ── SCRIPT LOCATION ──
            Command={
                "Name": "glueetl",          # glueetl = Spark job
                "ScriptLocation": SCRIPT_S3_PATH,
                "PythonVersion": "3"        # Python 3.10 with Glue 4.0
            },

            # ── JDBC CONNECTION ──
            # Attaching connection tells Glue to provision ENIs in that VPC
            Connections={
                "Connections": [GLUE_CONNECTION_NAME]
            },

            # ── DEFAULT JOB PARAMETERS ──
            # These can be overridden at runtime
            DefaultArguments={
                # Script parameters
                "--source_database": "quickcart_shop",
                "--source_tables": "orders,customers,products",
                "--target_s3_path": TARGET_S3_PATH,
                "--glue_connection_name": GLUE_CONNECTION_NAME,
                "--extraction_date": datetime.now().strftime("%Y-%m-%d"),

                # Glue configuration parameters
                "--job-language": "python",
                "--TempDir": TEMP_S3_PATH,

                # ── MONITORING (Critical for debugging) ──
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-continuous-log-filter": "true",
                "--continuous-log-logGroup": f"/aws-glue/jobs/{GLUE_JOB_NAME}",

                # ── PERFORMANCE ──
                "--enable-auto-scaling": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{S3_BUCKET}/spark-logs/{GLUE_JOB_NAME}/",

                # ── JOB BOOKMARKS (for incremental — Project 33) ──
                "--job-bookmark-option": "job-bookmark-disable",
                # Change to "job-bookmark-enable" when implementing incremental

                # ── SPARK CONFIGURATION ──
                "--conf": "spark.sql.parquet.compression.codec=snappy"
            },

            # ── COMPUTE CONFIGURATION ──
            GlueVersion="4.0",              # Spark 3.3 + Python 3.10
            WorkerType="G.1X",              # 4 vCPU, 16 GB per worker
            NumberOfWorkers=10,             # Max workers (auto-scaling can reduce)
            Timeout=60,                     # Kill after 60 minutes
            MaxRetries=1,                   # Retry once on failure

            # ── TAGS ──
            Tags={
                "Environment": "Production",
                "Team": "Data-Engineering",
                "Source": "MariaDB",
                "CostCenter": "ENG-001"
            }
        )
        print(f"   ✅ Job created: {GLUE_JOB_NAME}")

    except glue.exceptions.IdempotentParameterMismatchException:
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
                "Connections": {"Connections": [GLUE_CONNECTION_NAME]},
                "GlueVersion": "4.0",
                "WorkerType": "G.1X",
                "NumberOfWorkers": 10,
                "Timeout": 60,
                "MaxRetries": 1
            }
        )
        print(f"   ✅ Job updated: {GLUE_JOB_NAME}")

    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Job already exists: {GLUE_JOB_NAME}")


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Run the Glue Job
# ═══════════════════════════════════════════════════════════════════
def run_glue_job(tables="orders,customers,products", extraction_date=None):
    """
    Start a Glue Job Run with runtime parameter overrides
    
    WHY override at runtime:
    → Same job definition, different parameters per run
    → Monday: extract orders,customers
    → Tuesday: extract products,order_items
    → Or: re-extract a specific date after fixing data quality issue
    """
    if extraction_date is None:
        extraction_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n🚀 Starting Glue Job Run...")
    print(f"   Tables: {tables}")
    print(f"   Date: {extraction_date}")

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--source_tables": tables,
            "--extraction_date": extraction_date
        }
    )

    run_id = response["JobRunId"]
    print(f"   ✅ Job Run started: {run_id}")

    return run_id


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Monitor Job Run
# ═══════════════════════════════════════════════════════════════════
def monitor_job_run(run_id):
    """
    Poll job status until completion
    
    GLUE JOB STATES:
    STARTING → RUNNING → SUCCEEDED
                      → FAILED
                      → TIMEOUT
                      → STOPPED (manual cancel)
                      → ERROR (infrastructure issue)
    
    STARTING phase includes:
    → Allocating DPUs
    → Provisioning ENIs in VPC
    → Starting Spark cluster
    → Loading script
    → ~30-60 seconds for Glue 4.0 (vs 2-3 min for Glue 2.0)
    """
    print(f"\n📊 Monitoring Job Run: {run_id}")
    print("-" * 60)

    terminal_states = {"SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR"}
    poll_interval = 15  # seconds

    while True:
        response = glue.get_job_run(
            JobName=GLUE_JOB_NAME,
            RunId=run_id
        )

        run = response["JobRun"]
        state = run["JobRunState"]
        started = run.get("StartedOn", "")
        duration = run.get("ExecutionTime", 0)
        dpu_seconds = run.get("DPUSeconds", 0)

        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"   [{timestamp}] State: {state:12s} | Duration: {duration}s | DPU-seconds: {dpu_seconds}")

        if state in terminal_states:
            print("-" * 60)

            if state == "SUCCEEDED":
                cost = round(dpu_seconds * 0.44 / 3600, 4)
                print(f"   ✅ JOB SUCCEEDED")
                print(f"   → Total Duration: {duration} seconds ({duration/60:.1f} minutes)")
                print(f"   → DPU-seconds: {dpu_seconds}")
                print(f"   → Estimated Cost: ${cost:.4f}")

            elif state == "FAILED":
                error_msg = run.get("ErrorMessage", "No error message")
                print(f"   ❌ JOB FAILED")
                print(f"   → Error: {error_msg}")
                print(f"   → Check CloudWatch Logs: /aws-glue/jobs/{GLUE_JOB_NAME}")
                print(f"   → Common fixes:")
                print(f"      1. Security Group self-referencing rule missing")
                print(f"      2. S3 VPC Endpoint missing")
                print(f"      3. MariaDB credentials wrong")
                print(f"      4. Subnet has no available IPs")

            elif state == "TIMEOUT":
                print(f"   ⏰ JOB TIMED OUT (limit: 60 minutes)")
                print(f"   → Increase timeout or reduce data volume")

            return state

        time.sleep(poll_interval)


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Verify Output in S3
# ═══════════════════════════════════════════════════════════════════
def verify_output(tables, extraction_date):
    """Check that Parquet files were actually written"""
    print(f"\n🔍 Verifying S3 Output...")

    year, month, day = extraction_date.split("-")

    for table in tables.split(","):
        table = table.strip()
        prefix = f"bronze/mariadb/{table}/year={year}/month={month}/day={day}/"

        response = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix
        )

        if "Contents" in response:
            files = response["Contents"]
            total_size = sum(f["Size"] for f in files)
            total_mb = round(total_size / (1024 * 1024), 2)

            print(f"\n   📁 {table}:")
            print(f"      Path: s3://{S3_BUCKET}/{prefix}")
            print(f"      Files: {len(files)}")
            print(f"      Total Size: {total_mb} MB")

            for f in files[:3]:  # Show first 3 files
                size_kb = round(f["Size"] / 1024, 1)
                print(f"      → {f['Key'].split('/')[-1]} ({size_kb} KB)")
            if len(files) > 3:
                print(f"      → ... and {len(files) - 3} more files")
        else:
            print(f"\n   ❌ {table}: No files found at {prefix}")
            print(f"      → Check Glue job logs for errors")


# ═══════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    today = datetime.now().strftime("%Y-%m-%d")
    tables = "orders,customers,products"

    # Step 1: Upload script
    upload_script()

    # Step 2: Create job
    create_glue_job()

    # Step 3: Run job
    run_id = run_glue_job(
        tables=tables,
        extraction_date=today
    )

    # Step 4: Monitor
    final_state = monitor_job_run(run_id)

    # Step 5: Verify
    if final_state == "SUCCEEDED":
        verify_output(tables, today)

    print("\n" + "=" * 60)
    print("🏁 DONE")
    print("=" * 60)