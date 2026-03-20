# file: 04_run_partitioned_job.py
# Creates Glue job using the partitioned script and runs it

import boto3
import time

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
ROLE_ARN = "arn:aws:iam::123456789012:role/GlueETLJobRole"
JOB_NAME = "quickcart-csv-to-partitioned-parquet-orders"
SCRIPT_KEY = "glue-scripts/csv_to_partitioned_parquet.py"


def upload_and_create_job():
    """Upload partitioned script and create Glue job"""
    s3_client = boto3.client("s3", region_name=REGION)
    glue_client = boto3.client("glue", region_name=REGION)

    # Upload script
    try:
        with open("01_csv_to_partitioned_parquet.py", "r") as f:
            s3_client.put_object(
                Bucket=BUCKET_NAME, Key=SCRIPT_KEY,
                Body=f.read(), ContentType="text/x-python"
            )
        print(f"✅ Script uploaded: s3://{BUCKET_NAME}/{SCRIPT_KEY}")
    except FileNotFoundError:
        print(f"⚠️  Script file not found — using existing S3 copy")

    # Create job
    target_path = f"s3://{BUCKET_NAME}/silver_partitioned/orders/"
    
    try:
        glue_client.create_job(
            Name=JOB_NAME,
            Description="Convert CSV orders to partitioned Parquet by order_date",
            Role=ROLE_ARN,
            Command={
                "Name": "glueetl",
                "ScriptLocation": f"s3://{BUCKET_NAME}/{SCRIPT_KEY}",
                "PythonVersion": "3"
            },
            GlueVersion="4.0",
            WorkerType="G.1X",
            NumberOfWorkers=3,      # Slightly more for partitioned write
            Timeout=30,
            MaxRetries=1,
            DefaultArguments={
                "--source_database": "quickcart_datalake",
                "--source_table": "raw_orders",
                "--target_s3_path": target_path,
                "--target_database": "quickcart_datalake",
                "--target_table": "silver_orders_projected",
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--conf": "spark.sql.parquet.compression.codec=snappy"
            }
        )
        print(f"✅ Job created: {JOB_NAME}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Job already exists: {JOB_NAME}")

    # Run job
    response = glue_client.start_job_run(JobName=JOB_NAME)
    run_id = response["JobRunId"]
    print(f"\n🚀 Job started: {run_id}")

    # Wait for completion
    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        if elapsed > 900:
            print(f"❌ Timeout")
            return False

        status = glue_client.get_job_run(JobName=JOB_NAME, RunId=run_id)
        state = status["JobRun"]["JobRunState"]

        if state == "SUCCEEDED":
            exec_time = status["JobRun"].get("ExecutionTime", 0)
            dpu_seconds = status["JobRun"].get("DPUSeconds", 0)
            cost = round(dpu_seconds * 0.44 / 3600, 4)
            print(f"\n✅ SUCCEEDED — {exec_time}s, ${cost}")
            return True
        elif state in ["FAILED", "ERROR", "TIMEOUT", "STOPPED"]:
            error = status["JobRun"].get("ErrorMessage", "Unknown")
            print(f"\n❌ {state}: {error}")
            return False
        else:
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)
            print(f"   ⏳ {state} ({mins}m {secs}s)")
            time.sleep(15)


if __name__ == "__main__":
    upload_and_create_job()