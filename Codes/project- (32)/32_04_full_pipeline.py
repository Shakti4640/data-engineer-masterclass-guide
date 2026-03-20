# file: 32_04_full_pipeline.py
# Purpose: Orchestrate BOTH jobs: MariaDB→S3 (Project 31) + S3→Redshift (Project 32)
# This is a PREVIEW of what Step Functions (Project 55) will automate

import boto3
import time
from datetime import datetime

AWS_REGION = "us-east-2"
glue = boto3.client("glue", region_name=AWS_REGION)

# Job names from Project 31 and Project 32
JOB_EXTRACT = "quickcart-mariadb-to-s3-parquet"     # Project 31
JOB_LOAD = "quickcart-s3-to-redshift"                # Project 32

TABLES = "orders,customers,products"


def wait_for_job(job_name, run_id):
    """Wait for Glue job to complete — returns final state"""
    terminal = {"SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR"}

    while True:
        response = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = response["JobRun"]["JobRunState"]
        duration = response["JobRun"].get("ExecutionTime", 0)

        ts = datetime.now().strftime("%H:%M:%S")
        print(f"   [{ts}] {job_name}: {state} ({duration}s)")

        if state in terminal:
            return state
        time.sleep(15)


def run_full_pipeline():
    """
    Full pipeline:
    Step 1: MariaDB → S3 Parquet (Project 31 job)
    Step 2: S3 Parquet → Redshift (Project 32 job)
    
    Step 2 ONLY runs if Step 1 succeeds
    
    In production: use Step Functions (Project 55) for this
    This script demonstrates the logical flow
    """
    today = datetime.now().strftime("%Y-%m-%d")
    pipeline_start = datetime.now()

    print("=" * 70)
    print(f"🚀 FULL PIPELINE: MariaDB → S3 → Redshift")
    print(f"   Date: {today}")
    print(f"   Tables: {TABLES}")
    print("=" * 70)

    # ═══════════════════════════════════════════════════════════
    # STEP 1: Extract MariaDB → S3
    # ═══════════════════════════════════════════════════════════
    print(f"\n📤 STEP 1: MariaDB → S3 Parquet")
    print("-" * 50)

    response = glue.start_job_run(
        JobName=JOB_EXTRACT,
        Arguments={
            "--source_tables": TABLES,
            "--extraction_date": today
        }
    )
    extract_run_id = response["JobRunId"]
    print(f"   Run ID: {extract_run_id}")

    extract_state = wait_for_job(JOB_EXTRACT, extract_run_id)

    if extract_state != "SUCCEEDED":
        print(f"\n❌ PIPELINE ABORTED: Extract failed ({extract_state})")
        print(f"   → Fix extract job before proceeding")
        print(f"   → S3 → Redshift step SKIPPED (no fresh data)")
        return False

    print(f"   ✅ Extract complete")

    # ═══════════════════════════════════════════════════════════
    # STEP 2: Load S3 → Redshift
    # ═══════════════════════════════════════════════════════════
    print(f"\n📥 STEP 2: S3 Parquet → Redshift")
    print("-" * 50)

    response = glue.start_job_run(
        JobName=JOB_LOAD,
        Arguments={
            "--target_tables": TABLES,
            "--extraction_date": today
        }
    )
    load_run_id = response["JobRunId"]
    print(f"   Run ID: {load_run_id}")

    load_state = wait_for_job(JOB_LOAD, load_run_id)

    if load_state != "SUCCEEDED":
        print(f"\n⚠️  PIPELINE PARTIAL: Load failed ({load_state})")
        print(f"   → S3 data is safe (Step 1 succeeded)")
        print(f"   → Redshift data NOT updated")
        print(f"   → Fix load job and re-run Step 2 only")
        return False

    print(f"   ✅ Load complete")

    # ═══════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════
    total_time = (datetime.now() - pipeline_start).total_seconds()

    print("\n" + "=" * 70)
    print(f"📊 PIPELINE COMPLETE")
    print(f"   Step 1 (Extract): ✅ MariaDB → S3 Parquet")
    print(f"   Step 2 (Load):    ✅ S3 Parquet → Redshift")
    print(f"   Total time:       {total_time:.0f}s ({total_time/60:.1f} min)")
    print(f"   Tables:           {TABLES}")
    print(f"   Date:             {today}")
    print("=" * 70)

    return True


if __name__ == "__main__":
    success = run_full_pipeline()
    exit(0 if success else 1)