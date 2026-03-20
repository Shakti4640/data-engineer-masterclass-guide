# file: 03_create_glue_job.py
# Creates the Glue ETL job definition and uploads the script to S3
# Then runs the job

import boto3
import time

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
ROLE_ARN = "arn:aws:iam::123456789012:role/GlueETLJobRole"
JOB_NAME = "quickcart-csv-to-parquet-orders"
SCRIPT_S3_KEY = "glue-scripts/csv_to_parquet_job.py"


def upload_job_script():
    """Upload Glue job script to S3"""
    s3_client = boto3.client("s3", region_name=REGION)

    # Read the script file
    script_path = "02_csv_to_parquet_job.py"
    
    try:
        with open(script_path, "r") as f:
            script_content = f.read()
    except FileNotFoundError:
        print(f"❌ Script not found: {script_path}")
        print(f"   Create the script first (Step 2)")
        return None

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=SCRIPT_S3_KEY,
        Body=script_content,
        ContentType="text/x-python",
        ServerSideEncryption="AES256"
    )

    script_s3_path = f"s3://{BUCKET_NAME}/{SCRIPT_S3_KEY}"
    print(f"✅ Script uploaded: {script_s3_path}")
    return script_s3_path


def create_glue_job():
    """
    Create Glue ETL job definition
    
    KEY SETTINGS:
    → GlueVersion: "4.0" (latest, Python 3.10 + Spark 3.3)
    → NumberOfWorkers: 2 (minimum for small datasets)
    → WorkerType: "G.1X" (4 vCPU, 16 GB — standard)
    → Timeout: 30 minutes (auto-kill if stuck)
    → MaxRetries: 1 (retry once on failure)
    → JobBookmark: enabled for incremental processing
    → DefaultArguments: job parameters
    """
    glue_client = boto3.client("glue", region_name=REGION)

    script_location = f"s3://{BUCKET_NAME}/{SCRIPT_S3_KEY}"
    target_s3_path = f"s3://{BUCKET_NAME}/silver/orders/"

    job_config = {
        "Name": JOB_NAME,
        "Description": "Converts raw CSV orders to optimized Parquet format",
        "Role": ROLE_ARN,
        "Command": {
            "Name": "glueetl",             # ETL job type
            "ScriptLocation": script_location,
            "PythonVersion": "3"
        },
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",              # 4 vCPU, 16 GB RAM per worker
        "NumberOfWorkers": 2,              # Minimum for small datasets
        "Timeout": 30,                     # Minutes — auto-kill if stuck
        "MaxRetries": 1,                   # Retry once on transient failure
        "ExecutionProperty": {
            "MaxConcurrentRuns": 1         # Prevent parallel runs
        },
        "DefaultArguments": {
            # Job parameters (referenced in script via getResolvedOptions)
            "--source_database": "quickcart_datalake",
            "--source_table": "raw_orders",
            "--target_s3_path": target_s3_path,
            "--target_database": "quickcart_datalake",
            "--target_table": "silver_orders",

            # Glue system arguments
            "--job-bookmark-option": "job-bookmark-enable",  # Incremental processing
            "--enable-metrics": "true",                       # CloudWatch metrics
            "--enable-continuous-cloudwatch-log": "true",     # Real-time logging
            "--enable-spark-ui": "true",                      # Spark UI for debugging
            "--spark-event-logs-path": f"s3://{BUCKET_NAME}/glue-spark-logs/",

            # Spark configuration
            "--conf": "spark.sql.parquet.compression.codec=snappy"
        },
        "Tags": {
            "Environment": "Production",
            "Team": "Data-Engineering",
            "Pipeline": "CSV-to-Parquet"
        }
    }

    try:
        glue_client.create_job(**job_config)
        print(f"✅ Job created: {JOB_NAME}")
    except glue_client.exceptions.AlreadyExistsException:
        # Update existing job
        del job_config["Name"]
        del job_config["Tags"]
        glue_client.update_job(
            JobName=JOB_NAME,
            JobUpdate=job_config
        )
        print(f"ℹ️  Job updated: {JOB_NAME}")

    print(f"   Script: {script_location}")
    print(f"   Workers: 2 × G.1X (4 vCPU, 16 GB each)")
    print(f"   Timeout: 30 minutes")
    print(f"   Bookmarks: enabled")
    print(f"   Target: {target_s3_path}")

    return JOB_NAME


def run_glue_job():
    """Start the Glue job and wait for completion"""
    glue_client = boto3.client("glue", region_name=REGION)

    print(f"\n🚀 Starting job: {JOB_NAME}")

    response = glue_client.start_job_run(
        JobName=JOB_NAME,
        Arguments={
            # Can override default arguments per run
            "--source_database": "quickcart_datalake",
            "--source_table": "raw_orders",
            "--target_s3_path": f"s3://{BUCKET_NAME}/silver/orders/",
            "--target_database": "quickcart_datalake",
            "--target_table": "silver_orders"
        }
    )

    run_id = response["JobRunId"]
    print(f"   Run ID: {run_id}")

    # Poll for completion
    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        status_response = glue_client.get_job_run(
            JobName=JOB_NAME,
            RunId=run_id
        )

        state = status_response["JobRun"]["JobRunState"]
        
        if state == "SUCCEEDED":
            exec_time = status_response["JobRun"].get("ExecutionTime", 0)
            dpu_seconds = status_response["JobRun"].get("DPUSeconds", 0)
            cost = round(dpu_seconds * 0.44 / 3600, 4)

            print(f"\n✅ Job SUCCEEDED")
            print(f"   Execution time: {exec_time} seconds")
            print(f"   DPU-seconds:    {dpu_seconds}")
            print(f"   Estimated cost: ${cost:.4f}")
            return True

        elif state in ["FAILED", "ERROR", "TIMEOUT"]:
            error_msg = status_response["JobRun"].get("ErrorMessage", "Unknown")
            print(f"\n❌ Job {state}: {error_msg}")
            return False

        elif state == "STOPPED":
            print(f"\n⚠️  Job STOPPED (manually cancelled)")
            return False

        else:
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)
            print(f"   ⏳ {state} ({mins}m {secs}s elapsed)")
            time.sleep(15)


if __name__ == "__main__":
    # 1. Upload script
    upload_job_script()

    # 2. Create job
    create_glue_job()

    # 3. Run job
    run_glue_job()