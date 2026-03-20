# file: 34_03_full_pipeline_with_silver.py
# Purpose: Orchestrate complete pipeline including silver layer
# Extends Project 32's pipeline with silver layer transformation
# Preview of Step Functions orchestration (Project 55)

import boto3
import time
from datetime import datetime

AWS_REGION = "us-east-2"
glue = boto3.client("glue", region_name=AWS_REGION)

# Job names from Projects 31, 32, 34
JOB_EXTRACT = "quickcart-mariadb-to-s3-parquet"       # Project 31
JOB_SILVER = "quickcart-silver-layer-transforms"        # Project 34
JOB_LOAD_REDSHIFT = "quickcart-s3-to-redshift"          # Project 32

TABLES = "orders,customers,products"


def wait_for_job(job_name, run_id):
    """Wait for Glue job to complete"""
    terminal = {"SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR"}
    while True:
        response = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = response["JobRun"]["JobRunState"]
        duration = response["JobRun"].get("ExecutionTime", 0)
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"   [{ts}] {job_name}: {state} ({duration}s)")
        if state in terminal:
            return state
        time.sleep(20)


def run_full_pipeline():
    """
    Complete 3-stage pipeline:
    
    Stage 1: MariaDB → S3 Bronze (Project 31)
    Stage 2: S3 Bronze → S3 Silver (Project 34) ← NEW
    Stage 3: S3 Silver → Redshift (Project 32) ← Modified source
    
    ARCHITECTURE:
    ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
    │ MariaDB  │────►│ S3       │────►│ S3       │────►│ Redshift │
    │          │     │ (Bronze) │     │ (Silver) │     │          │
    │ Raw OLTP │     │ Raw      │     │ Clean    │     │ Serving  │
    │          │     │ Extract  │     │ Joined   │     │ Layer    │
    │          │     │          │     │ Aggregated│    │          │
    └──────────┘     └──────────┘     └──────────┘     └──────────┘
      Stage 1          Stage 1          Stage 2          Stage 3
      (Proj 31)        Output          (Proj 34)        (Proj 32)
    
    WHY SILVER LAYER MATTERS:
    → Bronze has dirty, duplicated, un-joined data
    → Loading bronze directly to Redshift (Project 32) means:
      - Redshift does cleaning on every dashboard query
      - 45M rows joined on every query = slow dashboards
    → Silver pre-computes: clean + join + aggregate ONCE in Glue
    → Redshift loads pre-joined data → dashboards are instant
    """
    today = datetime.now().strftime("%Y-%m-%d")
    pipeline_start = datetime.now()

    print("=" * 70)
    print(f"🚀 FULL 3-STAGE PIPELINE: MariaDB → Bronze → Silver → Redshift")
    print(f"   Date: {today}")
    print("=" * 70)

    # ═══════════════════════════════════════════════════════════
    # STAGE 1: Extract MariaDB → S3 Bronze
    # ═══════════════════════════════════════════════════════════
    print(f"\n📤 STAGE 1: MariaDB → S3 Bronze (Project 31)")
    print("-" * 50)

    response = glue.start_job_run(
        JobName=JOB_EXTRACT,
        Arguments={
            "--source_tables": TABLES,
            "--extraction_date": today
        }
    )
    extract_state = wait_for_job(JOB_EXTRACT, response["JobRunId"])

    if extract_state != "SUCCEEDED":
        print(f"\n❌ PIPELINE ABORTED at Stage 1: {extract_state}")
        return False

    stage1_time = datetime.now()
    print(f"   ✅ Stage 1 complete ({(stage1_time - pipeline_start).seconds}s)")

    # ═══════════════════════════════════════════════════════════
    # STAGE 2: Transform Bronze → Silver (NEW — Project 34)
    # ═══════════════════════════════════════════════════════════
    print(f"\n🔄 STAGE 2: S3 Bronze → S3 Silver (Project 34)")
    print("-" * 50)

    response = glue.start_job_run(
        JobName=JOB_SILVER,
        Arguments={
            "--extraction_date": today
        }
    )
    silver_state = wait_for_job(JOB_SILVER, response["JobRunId"])

    if silver_state != "SUCCEEDED":
        print(f"\n❌ PIPELINE ABORTED at Stage 2: {silver_state}")
        print(f"   → Bronze data is safe (Stage 1 succeeded)")
        print(f"   → Fix silver transforms and re-run Stage 2")
        return False

    stage2_time = datetime.now()
    print(f"   ✅ Stage 2 complete ({(stage2_time - stage1_time).seconds}s)")

    # ═══════════════════════════════════════════════════════════
    # STAGE 3: Load Silver → Redshift
    # ═══════════════════════════════════════════════════════════
    print(f"\n📥 STAGE 3: S3 Silver → Redshift (Project 32)")
    print("-" * 50)

    # IMPORTANT: Modified to read from SILVER, not bronze
    # Source path changed: bronze/mariadb/ → silver/
    response = glue.start_job_run(
        JobName=JOB_LOAD_REDSHIFT,
        Arguments={
            "--source_s3_path": f"s3://quickcart-datalake-prod/silver/",
            "--target_tables": "order_details,daily_revenue_summary,customer_rfm",
            "--extraction_date": today
        }
    )
    redshift_state = wait_for_job(JOB_LOAD_REDSHIFT, response["JobRunId"])

    if redshift_state != "SUCCEEDED":
        print(f"\n⚠️  PIPELINE PARTIAL: Stage 3 failed ({redshift_state})")
        print(f"   → Bronze + Silver data safe")
        print(f"   → Fix Redshift load and re-run Stage 3 only")
        return False

    stage3_time = datetime.now()
    print(f"   ✅ Stage 3 complete ({(stage3_time - stage2_time).seconds}s)")

    # ═══════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════
    total_time = (datetime.now() - pipeline_start).total_seconds()

    print("\n" + "=" * 70)
    print(f"📊 PIPELINE COMPLETE — 3 STAGES")
    print(f"   Stage 1 (Extract):   ✅ MariaDB → Bronze  ({(stage1_time - pipeline_start).seconds}s)")
    print(f"   Stage 2 (Transform): ✅ Bronze → Silver   ({(stage2_time - stage1_time).seconds}s)")
    print(f"   Stage 3 (Load):      ✅ Silver → Redshift ({(stage3_time - stage2_time).seconds}s)")
    print(f"   Total time: {total_time:.0f}s ({total_time/60:.1f} min)")
    print("=" * 70)

    return True


if __name__ == "__main__":
    success = run_full_pipeline()
    exit(0 if success else 1)