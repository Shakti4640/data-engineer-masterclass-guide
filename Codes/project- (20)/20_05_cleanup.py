# file: 20_05_cleanup.py
# Purpose: Remove exported files from S3 and Athena table

import boto3
import time
from datetime import datetime

REGION = "us-east-2"
EXPORT_BUCKET = "quickcart-raw-data-prod"
EXPORT_PREFIX = "mariadb_exports"
ATHENA_OUTPUT = "s3://quickcart-athena-results/"


def delete_export_files():
    """Delete all exported files from S3"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print("STEP 1: Deleting exported files from S3")
    print("=" * 70)

    paginator = s3_client.get_paginator("list_objects_v2")
    total_deleted = 0

    for page in paginator.paginate(Bucket=EXPORT_BUCKET, Prefix=f"{EXPORT_PREFIX}/"):
        objects = page.get("Contents", [])
        if not objects:
            continue

        delete_list = [{"Key": obj["Key"]} for obj in objects]

        s3_client.delete_objects(
            Bucket=EXPORT_BUCKET,
            Delete={"Objects": delete_list}
        )
        total_deleted += len(delete_list)

        for obj in objects:
            print(f"   🗑️  {obj['Key']}")

    print(f"\n   ✅ Deleted {total_deleted} exported files")


def drop_athena_table():
    """Drop the Athena table created for export verification"""
    athena_client = boto3.client("athena", region_name=REGION)

    print("\n" + "=" * 70)
    print("STEP 2: Dropping Athena verification table")
    print("=" * 70)

    response = athena_client.start_query_execution(
        QueryString="DROP TABLE IF EXISTS quickcart_raw.exported_orders",
        QueryExecutionContext={"Database": "quickcart_raw"},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )

    query_id = response["QueryExecutionId"]

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            print("   ✅ Table 'exported_orders' dropped")
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            print(f"   ⚠️  {state}: {reason}")
            break
        time.sleep(1)


def cleanup_temp_files():
    """Remove any leftover temp files"""
    import os
    import shutil

    temp_dir = "/tmp/mariadb_exports"
    log_dir = "/tmp/quickcart_logs"

    for directory in [temp_dir, log_dir]:
        if os.path.exists(directory):
            shutil.rmtree(directory)
            print(f"   🗑️  Removed: {directory}")
        else:
            print(f"   ℹ️  Not found: {directory}")


if __name__ == "__main__":
    print("🧹 PROJECT 20 — CLEANUP")
    print("=" * 70)

    delete_export_files()
    drop_athena_table()

    print("\n" + "=" * 70)
    print("STEP 3: Cleaning up temp files")
    print("=" * 70)
    cleanup_temp_files()

    print("\n" + "=" * 70)
    print("🎯 CLEANUP COMPLETE")
    print("   → S3 export files: deleted")
    print("   → Athena table: dropped")
    print("   → Temp files: removed")
    print("   → MariaDB data: UNTOUCHED (use 19_05_cleanup.py for that)")
    print("   → S3 source files: UNTOUCHED (Project 1 data preserved)")
    print("=" * 70)