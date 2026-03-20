# file: 33_05_watermark_health_check.py
# Purpose: Check health of all watermarks across all jobs
# Run from: Scheduled Lambda, cron, or manual check

import boto3
import json
from datetime import datetime, timedelta

AWS_REGION = "us-east-2"
S3_BUCKET = "quickcart-datalake-prod"
DDB_TABLE_NAME = "glue-job-watermarks"

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
glue_client = boto3.client("glue", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


def check_dynamodb_watermarks():
    """
    Scan all watermarks in DynamoDB and check for staleness
    
    STALE WATERMARK: watermark hasn't moved in > 24 hours
    → Means: job isn't running, or source has no new data
    → Either way: investigate
    """
    print("\n📊 DYNAMODB WATERMARK HEALTH CHECK")
    print("-" * 70)

    table = dynamodb.Table(DDB_TABLE_NAME)
    response = table.scan()

    now = datetime.now()
    stale_threshold = timedelta(hours=26)  # Allow 2-hour buffer beyond 24h

    stale_count = 0
    healthy_count = 0

    if not response.get("Items"):
        print("   (no watermarks found)")
        return

    print(f"   {'Job Name':<30} {'Table':<15} {'Watermark':<22} {'Updated':<22} {'Status'}")
    print(f"   {'-'*30} {'-'*15} {'-'*22} {'-'*22} {'-'*10}")

    for item in response["Items"]:
        job_name = item["job_name"]
        source_table = item["source_table"]
        watermark_value = item.get("watermark_value", "N/A")
        updated_at_str = item.get("updated_at", "")

        # Check staleness
        try:
            updated_at = datetime.fromisoformat(updated_at_str)
            age = now - updated_at
            is_stale = age > stale_threshold
        except (ValueError, TypeError):
            is_stale = True
            age = timedelta(days=999)

        status = "❌ STALE" if is_stale else "✅ OK"
        if is_stale:
            stale_count += 1
        else:
            healthy_count += 1

        print(
            f"   {job_name:<30} {source_table:<15} "
            f"{watermark_value:<22} {updated_at_str[:19]:<22} {status}"
        )

    print(f"\n   Summary: {healthy_count} healthy, {stale_count} stale")

    if stale_count > 0:
        print(f"   ⚠️  {stale_count} watermarks are stale (>26h since last update)")
        print(f"   → Check if Glue jobs are running successfully")
        print(f"   → Check if source databases have new data")


def check_glue_bookmarks():
    """
    Check Glue Job Bookmark states for bookmark-enabled jobs
    """
    print("\n📊 GLUE JOB BOOKMARK HEALTH CHECK")
    print("-" * 70)

    # List jobs that have bookmarks enabled
    bookmark_jobs = ["quickcart-csv-to-parquet"]  # Add your bookmarked jobs

    for job_name in bookmark_jobs:
        try:
            # Get bookmark state
            bookmark = glue_client.get_job_bookmark(JobName=job_name)
            entry = bookmark.get("JobBookmarkEntry", {})

            run_id = entry.get("RunId", "N/A")
            attempt = entry.get("Attempt", 0)
            version = entry.get("Version", 0)

            print(f"   Job: {job_name}")
            print(f"      Last Run ID: {run_id}")
            print(f"      Attempt: {attempt}")
            print(f"      Version: {version}")

            # Get last run status
            runs = glue_client.get_job_runs(
                JobName=job_name, MaxResults=1
            )
            if runs["JobRuns"]:
                last_run = runs["JobRuns"][0]
                print(f"      Last Run State: {last_run['JobRunState']}")
                print(f"      Last Run Time: {last_run.get('CompletedOn', 'Running')}")
                print(f"      DPU-seconds: {last_run.get('DPUSeconds', 'N/A')}")
            print()

        except glue_client.exceptions.EntityNotFoundException:
            print(f"   Job: {job_name} — NOT FOUND")
        except Exception as e:
            print(f"   Job: {job_name} — Error: {e}")


def check_s3_watermarks():
    """Check S3-based watermark files"""
    print("\n📊 S3 WATERMARK HEALTH CHECK")
    print("-" * 70)

    prefix = "state/watermarks/"
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=prefix
    )

    if "Contents" not in response:
        print("   (no S3 watermarks found)")
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith("_README.json"):
            continue

        try:
            data_response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            data = json.loads(data_response["Body"].read().decode("utf-8"))

            print(f"   File: {key}")
            print(f"      Watermark: {data.get('watermark_value', 'N/A')}")
            print(f"      Updated: {data.get('updated_at', 'N/A')}")
            print(f"      Rows Processed: {data.get('rows_processed', 'N/A')}")
            print()
        except Exception as e:
            print(f"   File: {key} — Error reading: {e}")


def generate_cost_comparison():
    """
    Show before/after cost comparison with incremental processing
    """
    print("\n💰 COST IMPACT OF INCREMENTAL PROCESSING")
    print("-" * 70)

    # Get recent job runs
    jobs_to_check = [
        "quickcart-csv-to-parquet",
        "quickcart-mariadb-to-s3-parquet",
        "quickcart-s3-to-redshift"
    ]

    total_dpu_seconds = 0
    total_runs = 0

    for job_name in jobs_to_check:
        try:
            runs = glue_client.get_job_runs(
                JobName=job_name,
                MaxResults=30  # Last 30 runs
            )

            if runs["JobRuns"]:
                job_dpu = sum(
                    r.get("DPUSeconds", 0)
                    for r in runs["JobRuns"]
                    if r["JobRunState"] == "SUCCEEDED"
                )
                job_runs = len([
                    r for r in runs["JobRuns"]
                    if r["JobRunState"] == "SUCCEEDED"
                ])

                avg_dpu = job_dpu / max(job_runs, 1)
                cost_per_run = round(avg_dpu * 0.44 / 3600, 4)

                print(f"   {job_name}:")
                print(f"      Runs (last 30): {job_runs}")
                print(f"      Avg DPU-sec/run: {avg_dpu:.0f}")
                print(f"      Avg cost/run: ${cost_per_run:.4f}")

                total_dpu_seconds += job_dpu
                total_runs += job_runs

        except Exception as e:
            print(f"   {job_name}: {e}")

    if total_runs > 0:
        total_cost = round(total_dpu_seconds * 0.44 / 3600, 2)
        avg_cost = round(total_cost / total_runs, 4)

        print(f"\n   TOTALS (last 30 days estimate):")
        print(f"      Total DPU-seconds: {total_dpu_seconds:,}")
        print(f"      Total cost: ${total_cost}")
        print(f"      Avg per run: ${avg_cost}")
        print(f"      Monthly (30 runs/job × 3 jobs): ~${total_cost:.2f}")

        # Estimate savings
        # Without bookmark: assume 10x more DPU-seconds
        no_bookmark_cost = total_cost * 10
        savings = no_bookmark_cost - total_cost
        print(f"\n   ESTIMATED SAVINGS vs full reprocess:")
        print(f"      Without incremental: ~${no_bookmark_cost:.2f}/month")
        print(f"      With incremental:    ~${total_cost:.2f}/month")
        print(f"      Monthly savings:     ~${savings:.2f}")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 70)
    print("🏥 INCREMENTAL PROCESSING HEALTH CHECK")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    check_dynamodb_watermarks()
    check_glue_bookmarks()
    check_s3_watermarks()
    generate_cost_comparison()

    print("\n" + "=" * 70)
    print("🏁 HEALTH CHECK COMPLETE")
    print("=" * 70)