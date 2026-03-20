# file: 06_enable_versioning.py
# Purpose: Enable versioning on the bucket created in Project 1
# DEPENDS ON: Project 1 — bucket must already exist

import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"   # From Project 1
REGION = "us-east-2"                       # From Project 1


def check_versioning_status(s3_client, bucket_name):
    """
    Check current versioning status
    
    POSSIBLE RESPONSES:
    → {} (empty) → versioning was never enabled
    → {'Status': 'Enabled'} → versioning active
    → {'Status': 'Suspended'} → versioning paused (old versions still exist)
    """
    response = s3_client.get_bucket_versioning(Bucket=bucket_name)
    status = response.get("Status", "Never Enabled")
    mfa_delete = response.get("MFADelete", "Disabled")
    return status, mfa_delete


def enable_versioning(bucket_name):
    s3_client = boto3.client("s3", region_name=REGION)

    # --- CHECK CURRENT STATUS ---
    before_status, mfa_status = check_versioning_status(s3_client, bucket_name)
    print(f"📋 Current versioning status: {before_status}")
    print(f"📋 MFA Delete status: {mfa_status}")

    if before_status == "Enabled":
        print("ℹ️  Versioning already enabled — no action needed")
        return True

    # --- ENABLE VERSIONING ---
    try:
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={
                "Status": "Enabled"
                # MFADelete can only be set by ROOT account
                # Cannot be set here via IAM user/role
            }
        )
        print("✅ Versioning ENABLED")

        # --- VERIFY ---
        after_status, _ = check_versioning_status(s3_client, bucket_name)
        print(f"📋 Verified status: {after_status}")

        if after_status == "Enabled":
            print("✅ Verification passed")
            return True
        else:
            print("❌ Verification failed — status not Enabled")
            return False

    except ClientError as e:
        print(f"❌ Failed to enable versioning: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("🔒 ENABLING S3 VERSIONING")
    print(f"   Bucket: {BUCKET_NAME}")
    print("=" * 60)
    enable_versioning(BUCKET_NAME)

    print("\n⚠️  IMPORTANT NOTES:")
    print("   → Objects uploaded BEFORE now have versionId = 'null'")
    print("   → All NEW uploads will get unique version IDs")
    print("   → Once enabled, versioning can only be SUSPENDED, never fully disabled")
    print("   → Run lifecycle rules to control version cost (next step)")