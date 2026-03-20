# file: 20_glacier_restore.py
# Purpose: Demonstrate how to restore objects from Glacier classes
# DEPENDS ON: Understanding of storage classes from Steps 1-3
# CRITICAL: This is the most misunderstood Glacier concept

import boto3
import time
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def check_restore_status(s3_client, bucket, key):
    """
    Check if a Glacier object is being restored or already restored
    
    RESTORE STATES:
    → No Restore header: never requested restore
    → Restore: ongoing-request="true": restore in progress (wait)
    → Restore: ongoing-request="false", expiry-date="...": restored & accessible
    """
    response = s3_client.head_object(Bucket=bucket, Key=key)
    storage_class = response.get("StorageClass", "STANDARD")
    restore_status = response.get("Restore", None)

    print(f"   Storage Class: {storage_class}")
    print(f"   Restore Status: {restore_status if restore_status else 'No restore requested'}")

    if restore_status:
        if 'ongoing-request="true"' in restore_status:
            return "IN_PROGRESS"
        elif 'ongoing-request="false"' in restore_status:
            return "COMPLETED"
    return "NOT_STARTED"


def initiate_glacier_restore(bucket, key, retrieval_tier="Standard", restore_days=7):
    """
    Initiate restore of a Glacier object
    
    RETRIEVAL TIERS (Glacier Flexible):
    → Expedited:  1-5 minutes    ($0.03/GB + $10/1000 requests)
    → Standard:   3-5 hours      ($0.01/GB + $0.05/1000 requests)  
    → Bulk:       5-12 hours     ($0.0025/GB + $0.025/1000 requests)
    
    RETRIEVAL TIERS (Glacier Deep Archive):
    → Standard:   12 hours       ($0.02/GB + $0.10/1000 requests)
    → Bulk:       48 hours       ($0.0025/GB + $0.025/1000 requests)
    → NO expedited option for Deep Archive
    
    WHAT HAPPENS DURING RESTORE:
    1. You call restore_object()
    2. S3 reads data from archive storage (tape/cold HDD)
    3. Creates TEMPORARY copy in S3 Standard tier
    4. Temporary copy accessible for restore_days
    5. After restore_days, temporary copy auto-deleted
    6. Original archive copy REMAINS in Glacier (unchanged)
    7. You are charged for BOTH: Glacier storage + temporary Standard storage
    
    PARAMETERS:
    → restore_days: how many days the temporary copy stays available
    → retrieval_tier: speed/cost tradeoff for the restore
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print(f"\n🔄 INITIATING GLACIER RESTORE")
    print(f"   Key: {key}")
    print(f"   Retrieval Tier: {retrieval_tier}")
    print(f"   Restore Duration: {restore_days} days")

    # --- CHECK CURRENT STATE ---
    print("\n   Current state:")
    status = check_restore_status(s3_client, bucket, key)

    if status == "COMPLETED":
        print("   ✅ Object already restored — ready to download")
        return True

    if status == "IN_PROGRESS":
        print("   ⏳ Restore already in progress — wait for completion")
        return False

    # --- INITIATE RESTORE ---
    try:
        s3_client.restore_object(
            Bucket=bucket,
            Key=key,
            RestoreRequest={
                "Days": restore_days,
                "GlacierJobParameters": {
                    "Tier": retrieval_tier
                    # Valid values: "Expedited", "Standard", "Bulk"
                }
            }
        )
        print(f"   ✅ Restore initiated successfully")
        print(f"   ⏳ Estimated wait time:")

        wait_times = {
            "Expedited": "1-5 minutes (Glacier Flexible only)",
            "Standard": "3-5 hours (Glacier Flexible) / 12 hours (Deep Archive)",
            "Bulk": "5-12 hours (Glacier Flexible) / 48 hours (Deep Archive)"
        }
        print(f"      {wait_times.get(retrieval_tier, 'Unknown')}")
        return True

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "RestoreAlreadyInProgress":
            print("   ⏳ Restore already in progress")
            return False

        elif error_code == "InvalidObjectState":
            print("   ℹ️  Object is NOT in Glacier — no restore needed")
            print("   → Object is directly accessible via GetObject")
            return True

        elif error_code == "GlacierExpeditedRetrievalNotAvailable":
            print("   ❌ Expedited retrieval not available")
            print("   → Either: capacity unavailable or Deep Archive (no expedited)")
            print("   → Fallback: use 'Standard' tier instead")
            return False

        else:
            print(f"   ❌ Restore failed: {e}")
            return False


def poll_restore_status(bucket, key, poll_interval_seconds=60, max_polls=10):
    """
    Poll restore status until complete or timeout
    
    IN PRODUCTION: use S3 Event Notification + SNS/SQS instead of polling
    → S3 emits s3:ObjectRestore:Completed event when restore finishes
    → Much more efficient than polling
    → Covered in Project 17 (S3 Event Notifications)
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print(f"\n⏳ POLLING RESTORE STATUS (every {poll_interval_seconds}s, max {max_polls} polls)")
    print("-" * 50)

    for i in range(max_polls):
        status = check_restore_status(s3_client, bucket, key)

        if status == "COMPLETED":
            print(f"\n   ✅ RESTORE COMPLETE — object ready for download")
            return True

        elif status == "IN_PROGRESS":
            print(f"   Poll {i + 1}/{max_polls}: still restoring...")
            if i < max_polls - 1:
                time.sleep(poll_interval_seconds)

        elif status == "NOT_STARTED":
            print("   ❌ No restore in progress — initiate restore first")
            return False

    print(f"\n   ⏰ Timeout — restore still in progress after {max_polls} polls")
    print("   → For Glacier Flexible: may take 3-5 hours")
    print("   → For Deep Archive: may take 12-48 hours")
    print("   → Recommendation: use S3 Event Notification instead of polling")
    return False


def download_restored_object(bucket, key, local_path):
    """
    Download a restored Glacier object
    
    IMPORTANT: This only works AFTER restore is complete
    → If restore not initiated: InvalidObjectState error
    → If restore in progress: InvalidObjectState error
    → If restore expired: InvalidObjectState error
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print(f"\n📥 DOWNLOADING RESTORED OBJECT")
    print(f"   Key: {key}")
    print(f"   Destination: {local_path}")

    # Verify restore status first
    status = check_restore_status(s3_client, bucket, key)

    if status != "COMPLETED":
        print(f"   ❌ Cannot download — restore status: {status}")
        print("   → Must wait for restore to complete")
        return False

    try:
        s3_client.download_file(bucket, key, local_path)
        import os
        size_kb = os.path.getsize(local_path) / 1024
        print(f"   ✅ Downloaded: {size_kb:.1f} KB")
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] == "InvalidObjectState":
            print("   ❌ Object not yet restored or restore expired")
            print("   → Initiate a new restore request")
        else:
            print(f"   ❌ Download failed: {e}")
        return False


def copy_restored_to_standard(bucket, key, permanent_key=None):
    """
    PERMANENTLY move a restored Glacier object back to Standard
    
    WHY:
    → Restore creates TEMPORARY copy (expires after N days)
    → If you need the file permanently accessible → copy to Standard
    → Original Glacier copy remains (can delete separately if desired)
    
    WHEN TO USE:
    → Data was archived but now needs regular access again
    → Compliance hold lifted, data needed for active analysis
    → One-off: retrieve specific historical file for investigation
    """
    s3_client = boto3.client("s3", region_name=REGION)

    if permanent_key is None:
        permanent_key = key  # Overwrite in place

    print(f"\n📋 COPYING RESTORED OBJECT TO STANDARD TIER")
    print(f"   Source (Glacier): {key}")
    print(f"   Destination (Standard): {permanent_key}")

    status = check_restore_status(s3_client, bucket, key)
    if status != "COMPLETED":
        print(f"   ❌ Object not restored — status: {status}")
        return False

    try:
        s3_client.copy_object(
            Bucket=bucket,
            Key=permanent_key,
            CopySource={"Bucket": bucket, "Key": key},
            StorageClass="STANDARD",
            MetadataDirective="COPY"
        )
        print(f"   ✅ Copied to STANDARD tier")
        print(f"   → Object now permanently accessible via normal GET")
        print(f"   → Original Glacier version still exists (if versioning on)")
        return True

    except ClientError as e:
        print(f"   ❌ Copy failed: {e}")
        return False


def bulk_restore_prefix(bucket, prefix, retrieval_tier="Bulk", restore_days=7):
    """
    Restore ALL Glacier objects under a prefix
    
    USE CASE: 
    → "We need all of last year's orders for an audit"
    → Prefix: daily_exports/orders/ with date range
    → Bulk tier: cheapest, 5-12 hours
    """
    s3_client = boto3.client("s3", region_name=REGION)
    paginator = s3_client.get_paginator("list_objects_v2")

    print(f"\n🔄 BULK RESTORE: s3://{bucket}/{prefix}")
    print(f"   Tier: {retrieval_tier}")
    print(f"   Duration: {restore_days} days")
    print("-" * 60)

    glacier_classes = {"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"}
    initiated = 0
    skipped = 0
    already_restored = 0
    errors = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            storage_class = obj.get("StorageClass", "STANDARD")

            if storage_class not in glacier_classes:
                skipped += 1
                continue

            key = obj["Key"]

            try:
                # Check if already restored
                head = s3_client.head_object(Bucket=bucket, Key=key)
                restore_header = head.get("Restore", "")

                if 'ongoing-request="false"' in restore_header:
                    already_restored += 1
                    continue

                if 'ongoing-request="true"' in restore_header:
                    already_restored += 1
                    continue

                # Initiate restore
                s3_client.restore_object(
                    Bucket=bucket,
                    Key=key,
                    RestoreRequest={
                        "Days": restore_days,
                        "GlacierJobParameters": {
                            "Tier": retrieval_tier
                        }
                    }
                )
                initiated += 1
                print(f"   ✅ Initiated: {key}")

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "RestoreAlreadyInProgress":
                    already_restored += 1
                elif error_code == "InvalidObjectState":
                    skipped += 1
                else:
                    errors += 1
                    print(f"   ❌ Error on {key}: {e}")

    print(f"\n📊 BULK RESTORE SUMMARY:")
    print(f"   ✅ Initiated: {initiated}")
    print(f"   ⏭️  Skipped (not Glacier): {skipped}")
    print(f"   ♻️  Already restored/in-progress: {already_restored}")
    print(f"   ❌ Errors: {errors}")
    return initiated


if __name__ == "__main__":
    print("=" * 65)
    print("🧊 GLACIER RESTORE OPERATIONS")
    print("=" * 65)

    print("\n📋 GLACIER RESTORE CHEAT SHEET:")
    print("-" * 65)
    print(f"  {'Class':<22} {'Tier':<12} {'Time':<15} {'Cost/GB':<10}")
    print("-" * 65)
    print(f"  {'Glacier IR':<22} {'N/A':<12} {'Instant':<15} {'$0.030':<10}")
    print(f"  {'Glacier Flexible':<22} {'Expedited':<12} {'1-5 min':<15} {'$0.030':<10}")
    print(f"  {'Glacier Flexible':<22} {'Standard':<12} {'3-5 hours':<15} {'$0.010':<10}")
    print(f"  {'Glacier Flexible':<22} {'Bulk':<12} {'5-12 hours':<15} {'$0.003':<10}")
    print(f"  {'Deep Archive':<22} {'Standard':<12} {'12 hours':<15} {'$0.020':<10}")
    print(f"  {'Deep Archive':<22} {'Bulk':<12} {'48 hours':<15} {'$0.003':<10}")
    print("-" * 65)

    print("\n💡 KEY POINTS:")
    print("  → Glacier IR: NO restore needed — instant access like Standard")
    print("  → Glacier Flexible: MUST restore before reading")
    print("  → Deep Archive: MUST restore, NO expedited option")
    print("  → Restore creates TEMPORARY copy — set Days generously")
    print("  → To make permanent: copy restored object to STANDARD class")
    print("  → In production: use S3 Event Notification, not polling")

    # Demonstrate restore workflow
    print("\n🔄 RESTORE WORKFLOW:")
    print("  Step 1: initiate_glacier_restore(bucket, key, tier, days)")
    print("  Step 2: poll_restore_status(bucket, key)  OR  wait for S3 event")
    print("  Step 3: download_restored_object(bucket, key, local_path)")
    print("  Step 4: (optional) copy_restored_to_standard(bucket, key)")