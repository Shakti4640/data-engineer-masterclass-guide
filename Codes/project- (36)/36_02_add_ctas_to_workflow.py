# file: 36_02_add_ctas_to_workflow.py
# Purpose: Add gold materialization step to nightly workflow
# Extends Project 35 workflow with CTAS execution via Lambda

import boto3
import json
import time

AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

glue = boto3.client("glue", region_name=AWS_REGION)
iam = boto3.client("iam")
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

WORKFLOW_NAME = "quickcart-nightly-pipeline"
S3_BUCKET = "quickcart-datalake-prod"


# ═══════════════════════════════════════════════════════════════════
# APPROACH: Schedule CTAS via Glue Python Shell Job
# ═══════════════════════════════════════════════════════════════════
# WHY Python Shell instead of Lambda:
# → Glue Workflow can only trigger Glue Jobs and Crawlers
# → Cannot trigger Lambda directly
# → Python Shell Job: lightweight (0.0625 DPU), runs boto3 script
# → Perfect for executing Athena queries
# → Alternative: Step Functions (Project 55) can trigger Lambda

def create_ctas_glue_script():
    """
    Create a lightweight Glue Python Shell job that runs CTAS queries
    
    Python Shell Job:
    → 0.0625 DPU (1/16th of standard)
    → Max 1 DPU
    → Runs Python with boto3
    → No Spark overhead
    → Perfect for: API calls, light processing, orchestration
    → Cost: $0.44 × 0.0625 DPU × time = ~$0.005 for 3-minute run
    """
    script_content = '''
import sys
import boto3
import time
from datetime import datetime

# Get parameters
from awsglue.utils import getResolvedOptions
args = getResolvedOptions(sys.argv, ["JOB_NAME", "extraction_date", "gold_database", "silver_database", "s3_bucket"])

EXTRACTION_DATE = args["extraction_date"]
GOLD_DB = args["gold_database"]
SILVER_DB = args["silver_database"]
S3_BUCKET = args["s3_bucket"]
GOLD_PATH = f"s3://{S3_BUCKET}/gold"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

athena = boto3.client("athena")
s3 = boto3.client("s3")

print(f"Materializing gold layer for {EXTRACTION_DATE}")


def run_query(sql, description=""):
    """Execute Athena query and wait"""
    print(f"  Running: {description}")
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GOLD_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    qid = response["QueryExecutionId"]
    
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    
    if state == "SUCCEEDED":
        scanned = status["QueryExecution"].get("Statistics", {}).get("DataScannedInBytes", 0)
        print(f"  ✅ {description}: {round(scanned/1024/1024, 1)} MB scanned")
    else:
        error = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        print(f"  ❌ {description}: {error}")
        raise Exception(f"Query failed: {description} - {error}")
    
    return state


def cleanup_table(table_name):
    """Drop table and delete S3 data"""
    run_query(f"DROP TABLE IF EXISTS {GOLD_DB}.{table_name}", f"Drop {table_name}")
    
    prefix = f"gold/{table_name}/"
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({"Key": obj["Key"]})
    if objects:
        for i in range(0, len(objects), 1000):
            s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": objects[i:i+1000]})


# ── Materialize revenue summary ──
cleanup_table("revenue_by_category_month")
run_query(f"""
    CREATE TABLE {GOLD_DB}.revenue_by_category_month
    WITH (format=\'PARQUET\', write_compression=\'SNAPPY\',
          external_location=\'{GOLD_PATH}/revenue_by_category_month/\',
          partitioned_by=ARRAY[\'year\'])
    AS
    SELECT product_category, CAST(order_month AS VARCHAR) AS month,
           COUNT(DISTINCT order_id) AS order_count,
           ROUND(SUM(revenue), 2) AS total_revenue,
           ROUND(AVG(revenue), 2) AS avg_order_value,
           COUNT(DISTINCT customer_id) AS unique_customers,
           CAST(order_year AS VARCHAR) AS year
    FROM {SILVER_DB}.order_details
    WHERE status = \'completed\'
    GROUP BY product_category, order_month, order_year
""", "CTAS revenue_by_category_month")

# ── Materialize customer segments ──
cleanup_table("customer_segments_summary")
run_query(f"""
    CREATE TABLE {GOLD_DB}.customer_segments_summary
    WITH (format=\'PARQUET\', write_compression=\'SNAPPY\',
          external_location=\'{GOLD_PATH}/customer_segments_summary/\')
    AS
    SELECT rfm_segment, customer_tier,
           COUNT(*) AS customer_count,
           ROUND(AVG(monetary), 2) AS avg_lifetime_value,
           ROUND(AVG(frequency), 1) AS avg_frequency,
           ROUND(AVG(recency_days), 0) AS avg_recency,
           ROUND(SUM(monetary), 2) AS total_segment_revenue
    FROM {SILVER_DB}.customer_rfm
    GROUP BY rfm_segment, customer_tier
""", "CTAS customer_segments_summary")

# ── Materialize product performance ──
cleanup_table("product_performance")
run_query(f"""
    CREATE TABLE {GOLD_DB}.product_performance
    WITH (format=\'PARQUET\', write_compression=\'SNAPPY\',
          external_location=\'{GOLD_PATH}/product_performance/\')
    AS
    SELECT product_id, product_name, product_category, price_tier,
           COUNT(DISTINCT order_id) AS times_ordered,
           COUNT(DISTINCT customer_id) AS unique_buyers,
           SUM(quantity) AS total_units_sold,
           ROUND(SUM(revenue), 2) AS total_revenue,
           ROUND(AVG(revenue), 2) AS avg_revenue_per_order
    FROM {SILVER_DB}.order_details
    WHERE status = \'completed\'
    GROUP BY product_id, product_name, product_category, price_tier
""", "CTAS product_performance")

# ── Append daily conversion metrics ──
run_query(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_DB}.daily_conversion_metrics (
        total_orders BIGINT,
        completed_orders BIGINT,
        cancelled_orders BIGINT,
        completion_rate DOUBLE,
        total_revenue DOUBLE,
        avg_order_value DOUBLE,
        unique_customers BIGINT,
        snapshot_date VARCHAR
    )
    WITH (format=\'PARQUET\', write_compression=\'SNAPPY\',
          external_location=\'{GOLD_PATH}/daily_conversion_metrics/\',
          partitioned_by=ARRAY[\'snapshot_date\'])
""", "Create conversion metrics table")

run_query(f"""
    INSERT INTO {GOLD_DB}.daily_conversion_metrics
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN status=\'completed\' THEN order_id END) AS completed_orders,
        COUNT(DISTINCT CASE WHEN status=\'cancelled\' THEN order_id END) AS cancelled_orders,
        ROUND(CAST(COUNT(DISTINCT CASE WHEN status=\'completed\' THEN order_id END) AS DOUBLE)
              / NULLIF(COUNT(DISTINCT order_id), 0) * 100, 1) AS completion_rate,
        ROUND(SUM(CASE WHEN status=\'completed\' THEN revenue ELSE 0 END), 2) AS total_revenue,
        ROUND(AVG(CASE WHEN status=\'completed\' THEN revenue END), 2) AS avg_order_value,
        COUNT(DISTINCT customer_id) AS unique_customers,
        \'{EXTRACTION_DATE}\' AS snapshot_date
    FROM {SILVER_DB}.order_details
""", f"Insert conversion metrics for {EXTRACTION_DATE}")

print(f"\\n✅ Gold layer materialization complete for {EXTRACTION_DATE}")
'''

    # Upload script to S3
    print("\n📤 Uploading CTAS Python Shell script...")
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key="scripts/glue/gold_layer_ctas.py",
        Body=script_content,
        ContentType="text/x-python"
    )
    print(f"   ✅ Uploaded to: s3://{S3_BUCKET}/scripts/glue/gold_layer_ctas.py")

    return f"s3://{S3_BUCKET}/scripts/glue/gold_layer_ctas.py"


# ═══════════════════════════════════════════════════════════════════
# Create Glue Python Shell Job for CTAS
# ═══════════════════════════════════════════════════════════════════
def create_ctas_glue_job(script_path):
    """
    Create a Glue Python Shell job for gold layer materialization
    
    Python Shell vs glueetl:
    ┌────────────────────┬─────────────────┬──────────────────┐
    │                    │ Python Shell    │ glueetl (Spark)  │
    ├────────────────────┼─────────────────┼──────────────────┤
    │ Engine             │ Python + boto3  │ Apache Spark     │
    │ Max DPU            │ 1 DPU           │ 100+ DPUs        │
    │ Cold start         │ ~5 seconds      │ 30-60 seconds    │
    │ Min billing        │ 1 minute        │ 10 minutes       │
    │ Cost for 3 min     │ $0.022          │ $0.73            │
    │ Use case           │ API calls,      │ Data transforms, │
    │                    │ orchestration   │ large datasets   │
    └────────────────────┴─────────────────┴──────────────────┘
    """
    print("\n⚙️  Creating CTAS Glue Python Shell Job...")

    job_name = "quickcart-gold-layer-ctas"
    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/GlueMariaDBExtractorRole"

    try:
        glue.create_job(
            Name=job_name,
            Description="Materialize gold layer tables via Athena CTAS",
            Role=role_arn,
            Command={
                "Name": "pythonshell",           # Python Shell, NOT glueetl
                "ScriptLocation": script_path,
                "PythonVersion": "3.9"
            },
            DefaultArguments={
                "--extraction_date": "2025-01-15",
                "--gold_database": "gold_quickcart",
                "--silver_database": "silver_quickcart",
                "--s3_bucket": S3_BUCKET,
                "--job-language": "python",
                "--additional-python-modules": "boto3"
            },
            MaxCapacity=0.0625,   # 1/16 DPU — minimum for Python Shell
            Timeout=30,           # 30 minutes max
            MaxRetries=1,
            GlueVersion="3.0",    # Python Shell supports up to 3.0
            ExecutionProperty={
                "MaxConcurrentRuns": 1
            },
            Tags={
                "Environment": "Production",
                "Team": "Data-Engineering",
                "Pipeline": "Gold-Layer-CTAS",
                "CostCenter": "ENG-001"
            }
        )
        print(f"   ✅ Job created: {job_name}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_job(
            JobName=job_name,
            JobUpdate={
                "Role": role_arn,
                "Command": {
                    "Name": "pythonshell",
                    "ScriptLocation": script_path,
                    "PythonVersion": "3.9"
                },
                "MaxCapacity": 0.0625,
                "Timeout": 30
            }
        )
        print(f"   ✅ Job updated: {job_name}")

    return job_name


# ═══════════════════════════════════════════════════════════════════
# Update Glue IAM Role for Athena Access
# ═══════════════════════════════════════════════════════════════════
def update_iam_for_athena():
    """
    Glue role needs Athena permissions for CTAS execution
    Already has S3 and Glue Catalog access from Projects 31-35
    Need to add: athena:StartQueryExecution, athena:GetQueryExecution, etc.
    """
    print("\n🔐 Updating IAM Role for Athena access...")

    athena_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AthenaQueryAccess",
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:StopQueryExecution",
                    "athena:GetWorkGroup"
                ],
                "Resource": [
                    f"arn:aws:athena:{AWS_REGION}:{ACCOUNT_ID}:workgroup/primary"
                ]
            }
        ]
    }

    iam.put_role_policy(
        RoleName="GlueMariaDBExtractorRole",
        PolicyName="GlueAthenaAccess",
        PolicyDocument=json.dumps(athena_policy)
    )
    print(f"   ✅ Athena policy added to GlueMariaDBExtractorRole")


# ═══════════════════════════════════════════════════════════════════
# Add CTAS Job to Existing Workflow
# ═══════════════════════════════════════════════════════════════════
def add_ctas_to_workflow(ctas_job_name):
    """
    Add gold layer CTAS as the FINAL step in the nightly workflow
    
    UPDATED WORKFLOW:
    trigger-start → Job 1 (Extract)
      → Crawler A (Bronze)
        → Job 2 (Silver)
          → Crawler B (Silver)
            → Job 3 (Redshift)
              → Job 4 (Gold CTAS)  ← NEW STEP
    
    This means gold tables refresh AFTER Redshift load
    Alternative: run CTAS parallel with Redshift load
    (both read from silver — no dependency between them)
    """
    print(f"\n🔗 Adding CTAS job to workflow: {WORKFLOW_NAME}")

    trigger_name = "trigger-after-redshift-load"

    try:
        glue.create_trigger(
            Name=trigger_name,
            WorkflowName=WORKFLOW_NAME,
            Type="CONDITIONAL",
            Description="Materialize gold layer after Redshift load completes",
            StartOnCreation=False,
            Predicate={
                "Logical": "AND",
                "Conditions": [
                    {
                        "LogicalOperator": "EQUALS",
                        "JobName": "quickcart-s3-to-redshift",  # Project 32 job
                        "State": "SUCCEEDED"
                    }
                ]
            },
            Actions=[
                {
                    "JobName": ctas_job_name,
                    "Arguments": {
                        "--extraction_date": "{{extraction_date}}"
                    },
                    "Timeout": 30
                }
            ],
            Tags={"Step": "6-GoldCTAS"}
        )
        print(f"   ✅ Trigger created: {trigger_name}")
    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Trigger already exists: {trigger_name}")

    # Activate trigger
    try:
        glue.start_trigger(Name=trigger_name)
        print(f"   ✅ Trigger activated: {trigger_name}")
    except Exception as e:
        print(f"   ⚠️  Activation: {e}")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 ADDING GOLD LAYER CTAS TO PIPELINE")
    print("=" * 60)

    # Step 1: Upload Python Shell script
    script_path = create_ctas_glue_script()

    # Step 2: Update IAM
    update_iam_for_athena()

    # Step 3: Create Glue Python Shell job
    ctas_job_name = create_ctas_glue_job(script_path)

    # Step 4: Add to workflow
    add_ctas_to_workflow(ctas_job_name)

    print("\n" + "=" * 60)
    print("✅ GOLD LAYER CTAS INTEGRATED INTO PIPELINE")
    print("=" * 60)
    print(f"""
   UPDATED WORKFLOW (6 steps):
   ┌──────────────────────────────────────────────────────┐
   │ ⏰ 2:00 AM → Job 1 (Extract)                        │
   │          → Crawler A (Bronze)                        │
   │            → Job 2 (Silver)                          │
   │              → Crawler B (Silver)                    │
   │                → Job 3 (Redshift)                    │
   │                  → Job 4 (Gold CTAS)  ← NEW         │
   └──────────────────────────────────────────────────────┘
   
   Gold CTAS cost: ~$0.022 (Python Shell) + ~$0.23 (Athena scans)
   Total additional cost: ~$0.25/day → ~$7.50/month
   Savings from analyst queries: ~$99/month
   NET SAVINGS: ~$91.50/month
""")