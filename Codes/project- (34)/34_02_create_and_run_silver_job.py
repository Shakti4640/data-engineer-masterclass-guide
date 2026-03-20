# file: 34_02_create_and_run_silver_job.py
# Purpose: Create Glue Job for silver layer transformation and trigger run
# Run from: Your local machine or CI/CD pipeline

import boto3
import time
from datetime import datetime

# ─── CONFIGURATION ───────────────────────────────────────────────
AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

S3_BUCKET = "quickcart-datalake-prod"
SCRIPT_S3_PATH = f"s3://{S3_BUCKET}/scripts/glue/silver_layer_transforms.py"

GLUE_JOB_NAME = "quickcart-silver-layer-transforms"
GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/GlueMariaDBExtractorRole"

glue = boto3.client("glue", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


def upload_script():
    """Upload Glue ETL script to S3"""
    print("📤 Uploading silver layer transform script...")
    s3.upload_file(
        Filename="34_01_glue_silver_layer_transforms.py",
        Bucket=S3_BUCKET,
        Key="scripts/glue/silver_layer_transforms.py"
    )
    print(f"   ✅ Uploaded to: {SCRIPT_S3_PATH}")


def create_glue_job():
    """
    Create Glue Job for silver layer transformations
    
    KEY DIFFERENCES from Project 31/32 jobs:
    → No JDBC connection needed (source is S3, not database)
    → No VPC needed (S3 access via public endpoint)
    → Higher DPUs for join/aggregate operations
    → G.2X workers for memory-intensive joins
    """
    print("\n⚙️  Creating Silver Layer Glue Job...")

    try:
        glue.create_job(
            Name=GLUE_JOB_NAME,
            Description="Transform bronze → silver: clean, dedup, join, aggregate",
            Role=GLUE_ROLE_ARN,
            Command={
                "Name": "glueetl",
                "ScriptLocation": SCRIPT_S3_PATH,
                "PythonVersion": "3"
            },
            # No Connections needed — reading from S3, not JDBC
            # Without Connection: Glue runs WITHOUT VPC ENIs
            # → Faster startup (no ENI provisioning)
            # → Direct S3 access via public endpoint
            DefaultArguments={
                # Script parameters
                "--bronze_s3_path": f"s3://{S3_BUCKET}/bronze/mariadb/",
                "--silver_s3_path": f"s3://{S3_BUCKET}/silver/",
                "--extraction_date": datetime.now().strftime("%Y-%m-%d"),
                "--catalog_database": "silver_quickcart",

                # Glue configuration
                "--job-language": "python",
                "--TempDir": f"s3://{S3_BUCKET}/tmp/glue-silver/",

                # Monitoring
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-auto-scaling": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": (
                    f"s3://{S3_BUCKET}/spark-logs/{GLUE_JOB_NAME}/"
                ),

                # Bookmark disabled — we use date partition filtering
                "--job-bookmark-option": "job-bookmark-disable",

                # Spark tuning for joins
                "--conf": (
                    "spark.sql.autoBroadcastJoinThreshold=209715200"  # 200MB
                    " --conf spark.sql.shuffle.partitions=50"
                    " --conf spark.sql.parquet.compression.codec=snappy"
                )
            },
            # ── G.2X for memory-intensive joins ──
            # 45M orders joined with 2M customers needs memory
            # G.2X: 8 vCPU, 32 GB per worker (vs G.1X: 4 vCPU, 16 GB)
            GlueVersion="4.0",
            WorkerType="G.2X",
            NumberOfWorkers=15,
            Timeout=90,               # 90 min timeout (joins take longer)
            MaxRetries=1,
            ExecutionProperty={
                "MaxConcurrentRuns": 1  # One run at a time
            },
            Tags={
                "Environment": "Production",
                "Team": "Data-Engineering",
                "Pipeline": "Silver-Layer",
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
                "GlueVersion": "4.0",
                "WorkerType": "G.2X",
                "NumberOfWorkers": 15,
                "Timeout": 90
            }
        )
        print(f"   ✅ Job updated: {GLUE_JOB_NAME}")


def run_job(extraction_date=None):
    """Start the silver layer job"""
    if extraction_date is None:
        extraction_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n🚀 Starting Silver Layer Job...")
    print(f"   Date: {extraction_date}")

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--extraction_date": extraction_date
        }
    )
    run_id = response["JobRunId"]
    print(f"   ✅ Run ID: {run_id}")
    return run_id


def monitor_job(run_id):
    """Poll job status until completion"""
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
                print(f"   ✅ SUCCEEDED in {duration}s ({duration/60:.1f} min)")
                print(f"   → DPU-seconds: {dpu_seconds}")
                print(f"   → Estimated cost: ${cost:.4f}")
            elif state == "FAILED":
                error = run.get("ErrorMessage", "Check CloudWatch logs")
                print(f"   ❌ FAILED: {error[:200]}")
            return state

        time.sleep(20)


def verify_silver_output(extraction_date):
    """Verify silver layer output files exist and have data"""
    print(f"\n🔍 Verifying Silver Layer Output...")

    year, month, day = extraction_date.split("-")

    silver_tables = {
        "clean_orders": f"year={year}/month={month}/day={day}/",
        "clean_customers": f"year={year}/month={month}/day={day}/",
        "clean_products": f"year={year}/month={month}/day={day}/",
        "order_details": f"year={year}/month={month}/day={day}/",
        "daily_revenue_summary": f"year={year}/month={month}/day={day}/",
        "monthly_category_summary": f"year={year}/month={month}/",
        "customer_rfm": f"year={year}/month={month}/day={day}/"
    }

    for table_name, partition_path in silver_tables.items():
        prefix = f"silver/{table_name}/{partition_path}"

        response = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix
        )

        if "Contents" in response:
            files = response["Contents"]
            total_size = sum(f["Size"] for f in files)
            total_mb = round(total_size / (1024 * 1024), 2)
            print(f"   ✅ {table_name:35s} | {len(files):3d} files | {total_mb:8.2f} MB")
        else:
            print(f"   ❌ {table_name:35s} | NO FILES at {prefix}")

    # Print Athena verification queries
    print(f"\n📋 ATHENA VERIFICATION QUERIES:")
    print("=" * 60)
    print(f"""
-- Row counts
SELECT 'clean_orders' as tbl, COUNT(*) as cnt FROM silver_quickcart.clean_orders
UNION ALL SELECT 'order_details', COUNT(*) FROM silver_quickcart.order_details
UNION ALL SELECT 'daily_revenue_summary', COUNT(*) FROM silver_quickcart.daily_revenue_summary
UNION ALL SELECT 'customer_rfm', COUNT(*) FROM silver_quickcart.customer_rfm;

-- Sample enriched order details (verify joins worked)
SELECT order_id, customer_name, product_name, product_category,
       quantity, unit_price, revenue, customer_tier, price_tier
FROM silver_quickcart.order_details
LIMIT 10;

-- Daily revenue summary (verify aggregation)
SELECT order_date, product_category, customer_tier,
       order_count, total_revenue, avg_order_value, unique_customers
FROM silver_quickcart.daily_revenue_summary
ORDER BY total_revenue DESC
LIMIT 20;

-- Customer RFM segments
SELECT rfm_segment, COUNT(*) as customer_count,
       AVG(monetary) as avg_monetary,
       AVG(frequency) as avg_frequency,
       AVG(recency_days) as avg_recency
FROM silver_quickcart.customer_rfm
GROUP BY rfm_segment
ORDER BY avg_monetary DESC;

-- Data quality check: NULLs in joined columns
SELECT 
    COUNT(*) as total_rows,
    COUNT(product_name) as with_product_name,
    COUNT(*) - COUNT(product_name) as missing_product_name,
    COUNT(customer_name) as with_customer_name,
    COUNT(*) - COUNT(customer_name) as missing_customer_name
FROM silver_quickcart.order_details;
""")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    today = datetime.now().strftime("%Y-%m-%d")

    upload_script()
    create_glue_job()
    run_id = run_job(extraction_date=today)
    final_state = monitor_job(run_id)

    if final_state == "SUCCEEDED":
        verify_silver_output(today)

    print("\n🏁 DONE")