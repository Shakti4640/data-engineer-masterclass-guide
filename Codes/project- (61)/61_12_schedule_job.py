# file: 61_12_schedule_job.py
# Run from: Account B (222222222222)
# Purpose: Schedule nightly run + provide manual trigger

import boto3
import time

REGION = "us-east-2"
JOB_NAME = "cross-account-mariadb-to-redshift"


def create_schedule():
    """
    Schedule Glue job to run nightly at 2 AM UTC
    
    CRON FORMAT IN GLUE:
    → cron(minutes hours day-of-month month day-of-week year)
    → cron(0 2 * * ? *) = every day at 02:00 UTC
    
    ALTERNATIVE: EventBridge rule → Glue StartJobRun
    → More flexible, supports cross-service triggers
    → Covered in Project 66
    """
    glue = boto3.client("glue", region_name=REGION)

    try:
        glue.create_trigger(
            Name="trigger-nightly-cross-account-etl",
            Type="SCHEDULED",
            Schedule="cron(0 2 * * ? *)",  # 2 AM UTC daily
            Actions=[
                {
                    "JobName": JOB_NAME,
                    "Arguments": {
                        "--target_schema": "quickcart_ops"
                    }
                }
            ],
            Description="Nightly 2AM: MariaDB (Account A) → Redshift (Account B)",
            StartOnCreation=True,
            Tags={
                "Environment": "Production",
                "Pipeline": "CrossAccountETL"
            }
        )
        print("✅ Schedule created: Every day at 2:00 AM UTC")
        print(f"   Trigger: trigger-nightly-cross-account-etl")
        print(f"   Job: {JOB_NAME}")

    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Trigger already exists")


def run_job_manually():
    """
    Trigger job immediately for testing
    """
    glue = boto3.client("glue", region_name=REGION)

    response = glue.start_job_run(
        JobName=JOB_NAME,
        Arguments={
            "--target_schema": "quickcart_ops"
        }
    )

    run_id = response["JobRunId"]
    print(f"🚀 Job started: {JOB_NAME}")
    print(f"   Run ID: {run_id}")
    print(f"   Monitoring: CloudWatch → /aws-glue/jobs/output")

    return run_id


def monitor_job_run(run_id):
    """
    Poll job status until completion
    
    GLUE JOB STATES:
    → STARTING: allocating resources, creating ENIs
    → RUNNING: executing script
    → STOPPING: cleaning up
    → SUCCEEDED: completed without errors
    → FAILED: exception raised
    → TIMEOUT: exceeded Timeout setting
    → ERROR: infrastructure failure
    """
    glue = boto3.client("glue", region_name=REGION)

    print(f"\n⏳ Monitoring job run: {run_id}")
    terminal_states = ["SUCCEEDED", "FAILED", "TIMEOUT", "ERROR", "STOPPED"]

    while True:
        response = glue.get_job_run(
            JobName=JOB_NAME,
            RunId=run_id
        )

        run = response["JobRun"]
        state = run["JobRunState"]
        duration = run.get("ExecutionTime", 0)

        print(f"   State: {state} | Duration: {duration}s", end="\r")

        if state in terminal_states:
            print(f"\n\n{'='*50}")
            print(f"   Final State: {state}")
            print(f"   Duration: {duration} seconds")

            if state == "SUCCEEDED":
                print(f"   ✅ ETL completed successfully")
            else:
                error_msg = run.get("ErrorMessage", "No error message")
                print(f"   ❌ Error: {error_msg}")

            # Print DPU usage
            dpu_seconds = run.get("DPUSeconds", 0)
            cost_estimate = dpu_seconds * 0.44 / 3600  # $0.44/DPU-hour
            print(f"   DPU-Seconds: {dpu_seconds}")
            print(f"   Estimated Cost: ${cost_estimate:.4f}")
            print(f"{'='*50}")
            break

        time.sleep(30)  # Poll every 30 seconds

    return state


if __name__ == "__main__":
    print("=" * 60)
    print("📅 SCHEDULING & RUNNING CROSS-ACCOUNT ETL")
    print("=" * 60)

    # Create nightly schedule
    create_schedule()
    print()

    # Run immediately for testing
    run_id = run_job_manually()
    final_state = monitor_job_run(run_id)