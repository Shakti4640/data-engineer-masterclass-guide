# file: 33_06_reset_and_reprocess.py
# Purpose: Reset bookmarks/watermarks for reprocessing scenarios
# Use when: Schema changed, bug fixed, data correction needed

import boto3
import json
import sys

AWS_REGION = "us-east-2"
DDB_TABLE_NAME = "glue-job-watermarks"
S3_BUCKET = "quickcart-datalake-prod"

glue = boto3.client("glue", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


def reset_glue_bookmark(job_name):
    """
    Reset Glue Job Bookmark — next run processes ALL files
    
    WHEN TO USE:
    → Schema changed (new columns added to source)
    → Bug fix (previous processing was incorrect)
    → Data correction (source data was retroactively fixed)
    → Testing (want to verify full pipeline)
    
    WARNING:
    → Next run will process EVERYTHING from scratch
    → For 365 days of files: could take hours
    → Consider: move old files to archive, reset, then only new prefix
    """
    print(f"\n🔄 Resetting Glue bookmark for: {job_name}")

    try:
        glue.reset_job_bookmark(JobName=job_name)
        print(f"   ✅ Bookmark reset — next run processes ALL files")
    except glue.exceptions.EntityNotFoundException:
        print(f"   ❌ Job not found: {job_name}")
    except Exception as e:
        print(f"   ❌ Error: {e}")


def reset_dynamodb_watermark(job_name, source_table=None):
    """
    Reset DynamoDB watermark — next run does full extract
    
    If source_table specified: reset only that table
    If source_table is None: reset ALL tables for this job
    """
    print(f"\n🔄 Resetting DynamoDB watermark: {job_name}")
    table = dynamodb.Table(DDB_TABLE_NAME)

    if source_table:
        # Reset specific table
        table.delete_item(
            Key={
                "job_name": job_name,
                "source_table": source_table
            }
        )
        print(f"   ✅ Watermark reset for {job_name}/{source_table}")
    else:
        # Reset all tables for this job
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("job_name").eq(job_name)
        )
        for item in response.get("Items", []):
            table.delete_item(
                Key={
                    "job_name": item["job_name"],
                    "source_table": item["source_table"]
                }
            )
            print(f"   ✅ Watermark reset for {job_name}/{item['source_table']}")


def reset_s3_watermark(job_name, source_table=None):
    """Reset S3-based watermark files"""
    print(f"\n🔄 Resetting S3 watermark: {job_name}")

    if source_table:
        key = f"state/watermarks/{job_name}/{source_table}.json"
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
        print(f"   ✅ Deleted: s3://{S3_BUCKET}/{key}")
    else:
        prefix = f"state/watermarks/{job_name}/"
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        if "Contents" in response:
            for obj in response["Contents"]:
                s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
                print(f"   ✅ Deleted: {obj['Key']}")


def set_watermark_to_specific_date(job_name, source_table, target_date):
    """
    Set watermark to a specific date — reprocess from that date forward
    
    EXAMPLE USE CASE:
    → Data from Jan 10-15 was corrupt
    → Fix source data
    → Set watermark to Jan 9 → next run reprocesses Jan 10 onward
    → No need to reprocess everything from the beginning
    """
    print(f"\n📌 Setting watermark to specific date:")
    print(f"   Job: {job_name}")
    print(f"   Table: {source_table}")
    print(f"   Target date: {target_date}")

    table = dynamodb.Table(DDB_TABLE_NAME)
    table.put_item(
        Item={
            "job_name": job_name,
            "source_table": source_table,
            "watermark_value": f"{target_date} 00:00:00",
            "updated_at": f"MANUAL_RESET_{datetime.now().isoformat()}",
            "rows_processed": 0,
            "job_run_id": "MANUAL_RESET"
        }
    )
    print(f"   ✅ Watermark set to: {target_date} 00:00:00")
    print(f"   → Next run will process all rows after {target_date}")


from datetime import datetime

# ═══════════════════════════════════════════════════════════════════
# COMMAND LINE INTERFACE
# ═══════════════════════════════════════════════════════════════════
def print_usage():
    print("""
USAGE:
    python 33_06_reset_and_reprocess.py <action> <job_name> [options]

ACTIONS:
    reset-bookmark  <job_name>
        → Reset Glue Job Bookmark (processes all files next run)

    reset-watermark <job_name> [source_table]
        → Reset DynamoDB watermark (full extract next run)
        → If source_table omitted: resets ALL tables for the job

    reset-s3        <job_name> [source_table]
        → Reset S3 watermark file

    set-date        <job_name> <source_table> <YYYY-MM-DD>
        → Set watermark to specific date (reprocess from that date)

    reset-all       <job_name>
        → Reset BOTH bookmark AND watermark (nuclear option)

EXAMPLES:
    python 33_06_reset_and_reprocess.py reset-bookmark quickcart-csv-to-parquet
    python 33_06_reset_and_reprocess.py reset-watermark quickcart-mariadb-incr orders
    python 33_06_reset_and_reprocess.py set-date quickcart-mariadb-incr orders 2025-01-10
    python 33_06_reset_and_reprocess.py reset-all quickcart-mariadb-incr
    """)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print_usage()
        sys.exit(1)

    action = sys.argv[1]
    job_name = sys.argv[2]

    print("=" * 60)
    print(f"🔧 BOOKMARK/WATERMARK MANAGEMENT")
    print(f"   Action: {action}")
    print(f"   Job: {job_name}")
    print("=" * 60)

    if action == "reset-bookmark":
        reset_glue_bookmark(job_name)

    elif action == "reset-watermark":
        source_table = sys.argv[3] if len(sys.argv) > 3 else None
        reset_dynamodb_watermark(job_name, source_table)

    elif action == "reset-s3":
        source_table = sys.argv[3] if len(sys.argv) > 3 else None
        reset_s3_watermark(job_name, source_table)

    elif action == "set-date":
        if len(sys.argv) < 5:
            print("ERROR: set-date requires <source_table> and <YYYY-MM-DD>")
            sys.exit(1)
        source_table = sys.argv[3]
        target_date = sys.argv[4]
        set_watermark_to_specific_date(job_name, source_table, target_date)

    elif action == "reset-all":
        reset_glue_bookmark(job_name)
        reset_dynamodb_watermark(job_name)
        reset_s3_watermark(job_name)
        print(f"\n🔥 ALL state reset for: {job_name}")
        print(f"   Next run will process EVERYTHING from scratch")

    else:
        print(f"Unknown action: {action}")
        print_usage()
        sys.exit(1)

    print("\n🏁 DONE")