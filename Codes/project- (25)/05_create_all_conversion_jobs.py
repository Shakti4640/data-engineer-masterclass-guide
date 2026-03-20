# file: 05_create_all_conversion_jobs.py
# Creates Glue ETL jobs for ALL three tables (orders, customers, products)
# Reuses the same script with different parameters

import boto3
import time

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
ROLE_ARN = "arn:aws:iam::123456789012:role/GlueETLJobRole"
SCRIPT_LOCATION = f"s3://{BUCKET_NAME}/glue-scripts/csv_to_parquet_job.py"


def create_conversion_job(glue_client, job_name, source_table, target_table,
                          target_s3_path, workers=2):
    """Create a Glue job for a specific table conversion"""
    job_config = {
        "Name": job_name,
        "Description": f"Convert {source_table} CSV to Parquet",
        "Role": ROLE_ARN,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": SCRIPT_LOCATION,
            "PythonVersion": "3"
        },
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": workers,
        "Timeout": 30,
        "MaxRetries": 1,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
        "DefaultArguments": {
            "--source_database": "quickcart_datalake",
            "--source_table": source_table,
            "--target_s3_path": target_s3_path,
            "--target_database": "quickcart_datalake",
            "--target_table": target_table,
            "--job-bookmark-option": "job-bookmark-enable",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--conf": "spark.sql.parquet.compression.codec=snappy"
        },
        "Tags": {
            "Environment": "Production",
            "Pipeline": "CSV-to-Parquet"
        }
    }

    try:
        glue_client.create_job(**job_config)
        print(f"✅ Created: {job_name}")
    except glue_client.exceptions.AlreadyExistsException:
        del job_config["Name"]
        del job_config["Tags"]
        glue_client.update_job(JobName=job_name, JobUpdate=job_config)
        print(f"ℹ️  Updated: {job_name}")

    print(f"   Source: raw_{source_table.split('_')[-1]} → Target: {target_s3_path}")


def run_job_and_wait(glue_client, job_name, timeout_sec=600):
    """Run job and poll for completion"""
    response = glue_client.start_job_run(JobName=job_name)
    run_id = response["JobRunId"]
    print(f"\n🚀 Started: {job_name} (Run: {run_id})")

    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_sec:
            print(f"   ❌ Timeout after {timeout_sec}s")
            return False

        status = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        state = status["JobRun"]["JobRunState"]

        if state == "SUCCEEDED":
            exec_time = status["JobRun"].get("ExecutionTime", 0)
            dpu_seconds = status["JobRun"].get("DPUSeconds", 0)
            cost = round(dpu_seconds * 0.44 / 3600, 4)
            print(f"   ✅ SUCCEEDED — {exec_time}s, ${cost:.4f}")
            return True
        elif state in ["FAILED", "ERROR", "TIMEOUT", "STOPPED"]:
            error = status["JobRun"].get("ErrorMessage", "Unknown")
            print(f"   ❌ {state}: {error}")
            return False
        else:
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)
            print(f"   ⏳ {state} ({mins}m {secs}s)")
            time.sleep(15)


def create_and_run_all_jobs():
    """Create and run conversion jobs for all tables"""
    glue_client = boto3.client("glue", region_name=REGION)

    jobs = [
        {
            "job_name": "quickcart-csv-to-parquet-orders",
            "source_table": "raw_orders",
            "target_table": "silver_orders",
            "target_s3_path": f"s3://{BUCKET_NAME}/silver/orders/",
            "workers": 2
        },
        {
            "job_name": "quickcart-csv-to-parquet-customers",
            "source_table": "raw_customers",
            "target_table": "silver_customers",
            "target_s3_path": f"s3://{BUCKET_NAME}/silver/customers/",
            "workers": 2
        },
        {
            "job_name": "quickcart-csv-to-parquet-products",
            "source_table": "raw_products",
            "target_table": "silver_products",
            "target_s3_path": f"s3://{BUCKET_NAME}/silver/products/",
            "workers": 2
        }
    ]

    print("=" * 60)
    print("🏭 CREATING ALL CSV → PARQUET CONVERSION JOBS")
    print("=" * 60)

    for job in jobs:
        create_conversion_job(
            glue_client=glue_client,
            job_name=job["job_name"],
            source_table=job["source_table"],
            target_table=job["target_table"],
            target_s3_path=job["target_s3_path"],
            workers=job["workers"]
        )

    print(f"\n{'=' * 60}")
    print("🚀 RUNNING ALL JOBS")
    print("=" * 60)

    results = {}
    for job in jobs:
        success = run_job_and_wait(glue_client, job["job_name"])
        results[job["job_name"]] = success

    print(f"\n{'=' * 60}")
    print("📊 EXECUTION SUMMARY")
    print("=" * 60)

    total_success = 0
    for name, success in results.items():
        status = "✅ SUCCEEDED" if success else "❌ FAILED"
        print(f"   {status} — {name}")
        if success:
            total_success += 1

    print(f"\n   {total_success}/{len(jobs)} jobs completed successfully")
    return results


if __name__ == "__main__":
    create_and_run_all_jobs()