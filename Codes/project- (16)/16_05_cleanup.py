# file: 16_05_cleanup.py
# Purpose: Drop tables and database when no longer needed
# IMPORTANT: EXTERNAL tables — dropping only removes metadata, NOT S3 data

import boto3
import time

REGION = "us-east-2"
ATHENA_OUTPUT = "s3://quickcart-athena-results/"
DATABASE = "quickcart_raw"


def run_ddl(athena_client, query):
    """Run DDL (free — no data scanned)"""
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    query_id = response["QueryExecutionId"]

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            print(f"   ✅ {query[:70]}...")
            return True
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            print(f"   ❌ {state}: {reason}")
            return False
        time.sleep(1)


def cleanup():
    athena_client = boto3.client("athena", region_name=REGION)

    print("🧹 Cleaning up Athena resources...")
    print("   NOTE: EXTERNAL TABLE drops remove METADATA only")
    print("   NOTE: S3 source files remain UNTOUCHED")
    print("   NOTE: DDL operations are FREE (no data scanned)\n")

    # ── DROP TABLES FIRST (must drop before database) ──
    tables = ["orders", "customers", "products"]
    for table in tables:
        run_ddl(
            athena_client,
            f"DROP TABLE IF EXISTS {DATABASE}.{table}"
        )

    # ── DROP DATABASE ──
    # CASCADE drops any remaining tables inside
    # RESTRICT (default) would fail if tables still exist
    run_ddl(
        athena_client,
        f"DROP DATABASE IF EXISTS {DATABASE} CASCADE"
    )

    print("\n✅ All catalog metadata removed")
    print("✅ S3 files untouched — still at s3://quickcart-raw-data-prod/")

    # ── VERIFY S3 FILES STILL EXIST ──
    s3_client = boto3.client("s3", region_name=REGION)
    try:
        response = s3_client.list_objects_v2(
            Bucket="quickcart-raw-data-prod",
            Prefix="daily_exports/",
            MaxKeys=5
        )
        count = response.get("KeyCount", 0)
        print(f"   → Verified: {count} files still in S3 (first 5 checked)")
        if count > 0:
            for obj in response["Contents"]:
                print(f"      📄 {obj['Key']} ({obj['Size']:,} bytes)")
    except Exception as e:
        print(f"   ⚠️  Could not verify S3: {e}")


def cleanup_workgroup():
    """Delete the workgroup created in Step 4"""
    athena_client = boto3.client("athena", region_name=REGION)

    workgroup_name = "quickcart-analysts"

    try:
        # Must set RecursiveDeleteOption=True to delete even if queries exist
        athena_client.delete_work_group(
            WorkGroup=workgroup_name,
            RecursiveDeleteOption=True
        )
        print(f"✅ Workgroup '{workgroup_name}' deleted")
        print("   → Query history for this workgroup is also removed")
    except athena_client.exceptions.InvalidRequestException as e:
        if "not found" in str(e).lower():
            print(f"ℹ️  Workgroup '{workgroup_name}' does not exist")
        else:
            raise


def cleanup_athena_results():
    """
    Delete accumulated result files from Athena output bucket
    
    WHY THIS MATTERS:
    → Every query creates 2 files: result.csv + result.csv.metadata
    → Even with lifecycle policy, you may want immediate cleanup
    → This deletes ALL objects in the results bucket
    """
    s3_client = boto3.client("s3", region_name=REGION)
    bucket = "quickcart-athena-results"

    print(f"\n🧹 Cleaning Athena result files from {bucket}...")

    paginator = s3_client.get_paginator("list_objects_v2")
    total_deleted = 0

    for page in paginator.paginate(Bucket=bucket):
        if "Contents" not in page:
            break

        objects_to_delete = [
            {"Key": obj["Key"]} for obj in page["Contents"]
        ]

        if objects_to_delete:
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects_to_delete}
            )
            total_deleted += len(objects_to_delete)

    print(f"   ✅ Deleted {total_deleted} result files")


if __name__ == "__main__":
    print("=" * 70)
    print("ATHENA PROJECT 16 — FULL CLEANUP")
    print("=" * 70)

    # Order matters:
    # 1. Drop tables and database (catalog metadata)
    cleanup()

    # 2. Delete workgroup
    print()
    cleanup_workgroup()

    # 3. Clean result files (optional)
    cleanup_athena_results()

    print("\n" + "=" * 70)
    print("🎯 CLEANUP COMPLETE")
    print("   → Glue Catalog: database + tables removed")
    print("   → Workgroup: deleted")
    print("   → Result files: cleared")
    print("   → Source S3 data: PRESERVED (untouched)")
    print("=" * 70)