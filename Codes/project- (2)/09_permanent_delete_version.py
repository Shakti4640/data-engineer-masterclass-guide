# file: 09_permanent_delete_version.py
# Purpose: Understand the DANGEROUS delete — version-targeted
# WARNING: This is what MFA Delete protects against
# DEPENDS ON: Understanding from Step 3

import boto3

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def permanent_delete_version(bucket, key, version_id):
    """
    ⚠️ DANGEROUS: Permanently deletes a SPECIFIC version
    
    THIS IS IRREVERSIBLE:
    → The bytes for this version are removed from all AZs
    → No recovery possible (unless cross-region replication exists)
    → This is what MFA Delete prevents unauthorized users from doing
    
    WHEN IS THIS USED:
    → Cleaning up old versions to save cost (lifecycle does this automatically)
    → Removing PII data that must be deleted (GDPR right to erasure)
    → Cleaning up after testing
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print(f"⚠️  PERMANENTLY DELETING VERSION")
    print(f"   Key: {key}")
    print(f"   VersionId: {version_id}")
    print(f"   THIS CANNOT BE UNDONE")

    # Safety confirmation
    confirm = input("   Type 'DELETE' to confirm: ")
    if confirm != "DELETE":
        print("   ❌ Aborted")
        return False

    s3_client.delete_object(
        Bucket=bucket,
        Key=key,
        VersionId=version_id   # THIS makes it permanent
    )

    print(f"   ✅ Version {version_id} permanently deleted")
    print(f"   → These bytes no longer exist anywhere in S3")
    print(f"   → Other versions of this key are unaffected")
    return True


def cleanup_all_versions(bucket, prefix):
    """
    Nuclear option: delete ALL versions and delete markers under a prefix
    Used for: cleaning up test data, decommissioning a dataset
    
    ⚠️ THIS PERMANENTLY DESTROYS ALL DATA UNDER THE PREFIX
    """
    s3_client = boto3.client("s3", region_name=REGION)
    paginator = s3_client.get_paginator("list_object_versions")

    objects_to_delete = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        # Collect all versions
        for v in page.get("Versions", []):
            objects_to_delete.append({
                "Key": v["Key"],
                "VersionId": v["VersionId"]
            })
        # Collect all delete markers
        for dm in page.get("DeleteMarkers", []):
            objects_to_delete.append({
                "Key": dm["Key"],
                "VersionId": dm["VersionId"]
            })

    if not objects_to_delete:
        print("ℹ️  No objects found to delete")
        return 0

    print(f"⚠️  About to permanently delete {len(objects_to_delete)} object versions")
    confirm = input("   Type 'DESTROY' to confirm: ")
    if confirm != "DESTROY":
        print("   ❌ Aborted")
        return 0

    # S3 delete_objects supports max 1000 per call
    deleted_count = 0
    for i in range(0, len(objects_to_delete), 1000):
        batch = objects_to_delete[i:i + 1000]
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": batch,
                "Quiet": True
            }
        )
        errors = response.get("Errors", [])
        deleted_count += len(batch) - len(errors)
        if errors:
            for err in errors:
                print(f"   ❌ Failed: {err['Key']} — {err['Message']}")

    print(f"   ✅ Permanently deleted {deleted_count} object versions")
    return deleted_count


if __name__ == "__main__":
    print("=" * 60)
    print("⚠️  VERSION-LEVEL DELETE OPERATIONS")
    print("   These operations are IRREVERSIBLE")
    print("   For educational understanding only")
    print("=" * 60)
    print("\n   Use cleanup_all_versions() to clean test data")
    print("   Use permanent_delete_version() for single version")