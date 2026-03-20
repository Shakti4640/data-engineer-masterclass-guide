# file: 19_manual_storage_class_change.py
# Purpose: Understand how to manually change storage class via copy
# DEPENDS ON: Project 1 (files exist in S3)
# USE WHEN: need to move a specific object outside lifecycle rules

import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def check_storage_class(s3_client, bucket, key):
    """Check current storage class of an object"""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        storage_class = response.get("StorageClass", "STANDARD")
        size = response["ContentLength"]
        print(f"   Key: {key}")
        print(f"   Storage Class: {storage_class}")
        print(f"   Size: {size:,} bytes")
        return storage_class
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"   ❌ Object not found: {key}")
        else:
            raise
        return None


def change_storage_class(bucket, key, target_class):
    """
    Change storage class by copying object to itself with new class
    
    HOW THIS WORKS INTERNALLY:
    → S3 does a server-side copy (no data leaves AWS)
    → Creates a new version with the target storage class
    → Old version becomes non-current (if versioning enabled)
    → This is exactly what lifecycle rules do automatically
    
    VALID TARGET CLASSES:
    → STANDARD, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING,
    → GLACIER_IR, GLACIER, DEEP_ARCHIVE
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print(f"\n🔄 Changing storage class for: {key}")
    print(f"   Target class: {target_class}")

    # Check current class
    print("\n   BEFORE:")
    current_class = check_storage_class(s3_client, bucket, key)

    if current_class == target_class:
        print(f"   ℹ️  Already in {target_class} — no change needed")
        return

    try:
        # Server-side copy to same location with new storage class
        s3_client.copy_object(
            Bucket=bucket,
            Key=key,
            CopySource={"Bucket": bucket, "Key": key},
            StorageClass=target_class,
            MetadataDirective="COPY"  # Preserve existing metadata
        )

        print(f"\n   AFTER:")
        check_storage_class(s3_client, bucket, key)
        print(f"   ✅ Storage class changed to {target_class}")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "InvalidObjectState":
            print(f"   ❌ Cannot copy — object in Glacier/Deep Archive must be restored first")
        else:
            print(f"   ❌ Failed: {e}")


def demonstrate_upload_with_storage_class(bucket, key, storage_class):
    """
    Upload directly to a specific storage class
    → Useful for data you KNOW will be cold from day 1
    → Example: historical backfill data, compliance archives
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print(f"\n📤 Uploading directly to {storage_class}")
    print(f"   Key: {key}")

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=b"archive,data,sample\n1,2,3\n",
        ContentType="text/csv",
        StorageClass=storage_class,
        ServerSideEncryption="AES256"
    )

    check_storage_class(s3_client, bucket, key)
    print(f"   ✅ Uploaded directly to {storage_class}")


if __name__ == "__main__":
    from datetime import datetime

    print("=" * 60)
    print("🔄 MANUAL STORAGE CLASS OPERATIONS")
    print("=" * 60)

    today = datetime.now().strftime("%Y%m%d")
    test_key = f"daily_exports/products/products_{today}.csv"

    # Check current class
    s3_client = boto3.client("s3", region_name=REGION)
    print("\n📋 Current state:")
    check_storage_class(s3_client, BUCKET_NAME, test_key)

    # Demonstrate direct upload to S3-IA
    archive_key = "archive_tests/historical_data.csv"
    demonstrate_upload_with_storage_class(
        BUCKET_NAME, archive_key, "STANDARD_IA"
    )

    print("\n💡 In production, use lifecycle rules (automatic)")
    print("   Manual changes are for one-off migrations or testing")