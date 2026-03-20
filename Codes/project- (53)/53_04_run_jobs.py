# file: 53_04_run_jobs.py
# Purpose: Execute Profile Job and Recipe Job, monitor status

import boto3
import json
import time

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
ROLE_NAME = "QuickCart-DataBrew-Role"
PROFILE_JOB_NAME = "quickcart-customers-profile"
RECIPE_NAME = "quickcart-customers-recipe"
RECIPE_JOB_NAME = "quickcart-customers-clean-job"


def get_role_arn():
    iam_client = boto3.client("iam")
    role = iam_client.get_role(RoleName=ROLE_NAME)
    return role["Role"]["Arn"]


def run_profile_job(databrew_client):
    """
    Run Profile Job to analyze data quality
    
    OUTPUT: JSON file in S3 with per-column statistics
    Sarah uses this to understand what needs cleaning
    """
    print("\n📌 Starting Profile Job...")

    try:
        response = databrew_client.start_job_run(Name=PROFILE_JOB_NAME)
        run_id = response["RunId"]
        print(f"  ✅ Profile Job started: Run ID = {run_id}")
    except databrew_client.exceptions.ConflictException:
        print(f"  ⚠️  Profile Job already running")
        return None

    # Monitor job
    return monitor_job(databrew_client, PROFILE_JOB_NAME, run_id)


def create_and_run_recipe_job(databrew_client, role_arn):
    """
    Create and run Recipe Job — applies cleaning recipe to FULL dataset
    
    Output: Parquet files in S3 Silver zone
    """
    # First: publish the recipe (make it immutable for this job run)
    try:
        databrew_client.publish_recipe(
            Name=RECIPE_NAME,
            Description="Published for production recipe job"
        )
        print(f"  ✅ Recipe published: {RECIPE_NAME}")
    except Exception as e:
        print(f"  ℹ️  Recipe publish: {e}")

    # Create Recipe Job
    try:
        databrew_client.create_recipe_job(
            Name=RECIPE_JOB_NAME,
            DatasetName="quickcart-customers-raw",
            RecipeReference={
                "Name": RECIPE_NAME,
                "RecipeVersion": "1.0"     # Use latest published version
            },
            RoleArn=role_arn,
            Outputs=[
                {
                    "Location": {
                        "Bucket": BUCKET_NAME,
                        "Key": "cleaned/customers/"
                    },
                    "Format": "PARQUET",
                    "Overwrite": True,
                    "CompressionFormat": "SNAPPY"
                }
            ],
            MaxCapacity=5,          # Max 5 nodes (cost control)
            MaxRetries=1,
            Timeout=60,             # 60 min max
            Tags={
                "Team": "Analytics",
                "JobType": "RecipeJob",
                "Owner": "Sarah"
            }
        )
        print(f"  ✅ Recipe Job created: {RECIPE_JOB_NAME}")
        print(f"     Output: s3://{BUCKET_NAME}/cleaned/customers/ (Parquet + Snappy)")
    except databrew_client.exceptions.ConflictException:
        print(f"  ℹ️  Recipe Job already exists: {RECIPE_JOB_NAME}")

    # Start the Recipe Job
    try:
        response = databrew_client.start_job_run(Name=RECIPE_JOB_NAME)
        run_id = response["RunId"]
        print(f"  ✅ Recipe Job started: Run ID = {run_id}")
    except databrew_client.exceptions.ConflictException:
        print(f"  ⚠️  Recipe Job already running")
        return None

    return monitor_job(databrew_client, RECIPE_JOB_NAME, run_id)


def monitor_job(databrew_client, job_name, run_id, poll_interval=15):
    """
    Poll job status until completion
    
    Job states:
    → STARTING: provisioning compute nodes
    → RUNNING: processing data
    → SUCCEEDED: completed successfully
    → FAILED: error occurred
    → STOPPED: manually cancelled
    → TIMEOUT: exceeded max duration
    """
    print(f"\n  ⏳ Monitoring job: {job_name} (Run: {run_id})")
    print(f"     Polling every {poll_interval} seconds...")

    start_time = time.time()

    while True:
        response = databrew_client.list_job_runs(Name=job_name, MaxResults=1)
        
        if not response.get("JobRuns"):
            print(f"     No job runs found")
            time.sleep(poll_interval)
            continue

        # Find our specific run
        job_run = None
        for run in response["JobRuns"]:
            if run.get("RunId") == run_id:
                job_run = run
                break

        if not job_run:
            # Try listing more results
            response = databrew_client.list_job_runs(Name=job_name, MaxResults=20)
            for run in response["JobRuns"]:
                if run.get("RunId") == run_id:
                    job_run = run
                    break

        if not job_run:
            print(f"     Waiting for job run to appear...")
            time.sleep(poll_interval)
            continue

        state = job_run.get("State", "UNKNOWN")
        elapsed = int(time.time() - start_time)

        if state == "SUCCEEDED":
            duration = job_run.get("ExecutionTime", 0)
            print(f"\n  ✅ JOB SUCCEEDED")
            print(f"     Duration: {duration} seconds")
            print(f"     Elapsed (including queue): {elapsed} seconds")

            # Show output details
            if "Outputs" in job_run:
                for output in job_run["Outputs"]:
                    loc = output.get("Location", {})
                    fmt = output.get("Format", "UNKNOWN")
                    print(f"     Output: s3://{loc.get('Bucket','')}/{loc.get('Key','')} ({fmt})")

            return True

        elif state == "FAILED":
            error = job_run.get("ErrorMessage", "Unknown error")
            print(f"\n  ❌ JOB FAILED")
            print(f"     Error: {error}")
            print(f"     Elapsed: {elapsed} seconds")

            # Common failure troubleshooting
            if "AccessDenied" in str(error):
                print(f"     → Fix: Check IAM role permissions for S3 access")
            elif "NoSuchBucket" in str(error):
                print(f"     → Fix: Verify bucket name exists")
            elif "NoSuchKey" in str(error):
                print(f"     → Fix: Verify source data exists at specified path")

            return False

        elif state in ["STOPPED", "TIMEOUT"]:
            print(f"\n  ⚠️  JOB {state}")
            print(f"     Elapsed: {elapsed} seconds")
            return False

        else:
            # STARTING or RUNNING
            print(f"     [{elapsed:>4}s] State: {state}")
            time.sleep(poll_interval)


def run_all_jobs():
    """Orchestrate: profile first, then recipe job"""
    databrew_client = boto3.client("databrew", region_name=REGION)
    role_arn = get_role_arn()

    print("=" * 70)
    print("🚀 RUNNING DATABREW JOBS")
    print("=" * 70)

    # Phase 1: Profile (understand the data)
    print("\n" + "─" * 40)
    print("📊 PHASE 1: DATA PROFILING")
    print("─" * 40)
    profile_success = run_profile_job(databrew_client)

    if profile_success:
        print(f"\n  📄 Profile results available at:")
        print(f"     s3://{BUCKET_NAME}/databrew-profiles/customers/")
        print(f"     → Sarah can review in DataBrew console")

    # Phase 2: Recipe Job (clean the data)
    print("\n" + "─" * 40)
    print("🧹 PHASE 2: DATA CLEANING (Recipe Job)")
    print("─" * 40)
    recipe_success = create_and_run_recipe_job(databrew_client, role_arn)

    if recipe_success:
        print(f"\n  📄 Cleaned data available at:")
        print(f"     s3://{BUCKET_NAME}/cleaned/customers/ (Parquet + Snappy)")
        print(f"     → Ready for Athena queries (Project 24)")
        print(f"     → Ready for Redshift COPY (Project 21)")

    # Summary
    print("\n" + "=" * 70)
    print("📊 JOB EXECUTION SUMMARY")
    print("=" * 70)
    print(f"  Profile Job: {'✅ SUCCEEDED' if profile_success else '❌ FAILED'}")
    print(f"  Recipe Job:  {'✅ SUCCEEDED' if recipe_success else '❌ FAILED'}")
    print("=" * 70)


if __name__ == "__main__":
    run_all_jobs()