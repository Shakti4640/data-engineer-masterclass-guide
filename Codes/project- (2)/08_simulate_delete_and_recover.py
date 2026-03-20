# file: 08_simulate_delete_and_recover.py
# Purpose: Prove that versioning works by simulating intern's mistake
# DEPENDS ON: Project 1 (upload), Project 2 Steps 1-2 (versioning + lifecycle)

import boto3
import time
from datetime import datetime

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"

# We'll test with one specific file
TEST_PREFIX = "daily_exports/orders/"


def list_object_versions(s3_client, bucket, prefix):
    """
    List ALL versions (including delete markers) for a prefix
    
    CRITICAL DISTINCTION:
    → list_objects_v2: shows only CURRENT (latest) versions
    → list_object_versions: shows ALL versions + delete markers
    """
    print(f"\n📋 All versions under: s3://{bucket}/{prefix}")
    print("-" * 85)
    print(f"  {'Key':<40} {'VersionId':<25} {'IsLatest':<10} {'Type':<15}")
    print("-" * 85)

    response = s3_client.list_object_versions(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=20
    )

    versions = []

    # Regular versions
    for v in response.get("Versions", []):
        print(f"  {v['Key']:<40} {v['VersionId'][:22]:<25} "
              f"{str(v['IsLatest']):<10} {'OBJECT':<15}")
        versions.append(v)

    # Delete markers
    for dm in response.get("DeleteMarkers", []):
        print(f"  {dm['Key']:<40} {dm['VersionId'][:22]:<25} "
              f"{str(dm['IsLatest']):<10} {'DELETE MARKER':<15}")
        versions.append(dm)

    if not versions:
        print("  (no versions found)")

    print("-" * 85)
    return versions


def upload_test_file(s3_client, bucket):
    """Upload a test file so we have something to delete"""
    today = datetime.now().strftime("%Y%m%d")
    key = f"{TEST_PREFIX}test_recovery_{today}.csv"
    content = "order_id,amount,status\nORD-001,99.99,completed\nORD-002,49.50,shipped"

    print(f"\n📤 Uploading test file: {key}")
    response = s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="text/csv"
    )
    version_id = response.get("VersionId", "null")
    print(f"   ✅ Uploaded — VersionId: {version_id}")
    return key, version_id


def upload_second_version(s3_client, bucket, key):
    """Upload a second version of same file to show version stacking"""
    content = "order_id,amount,status\nORD-001,99.99,completed\nORD-002,49.50,shipped\nORD-003,75.00,pending"

    print(f"\n📤 Uploading SECOND version of: {key}")
    response = s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="text/csv"
    )
    version_id = response.get("VersionId", "null")
    print(f"   ✅ Uploaded v2 — VersionId: {version_id}")
    return version_id


def simulate_accidental_delete(s3_client, bucket, key):
    """
    Simulate intern's DELETE — this is what 'aws s3 rm' does internally
    
    WITHOUT version ID → S3 inserts a DELETE MARKER
    → Object appears deleted to normal GET
    → But all previous versions STILL EXIST
    """
    print(f"\n🗑️  SIMULATING ACCIDENTAL DELETE: {key}")
    print("   (This is what 'aws s3 rm' does)")

    response = s3_client.delete_object(
        Bucket=bucket,
        Key=key
        # NOTE: No VersionId parameter → simple delete → delete marker
    )

    delete_marker = response.get("DeleteMarker", False)
    version_id = response.get("VersionId", "null")
    print(f"   Delete Marker Created: {delete_marker}")
    print(f"   Delete Marker VersionId: {version_id}")

    # --- PROVE IT LOOKS DELETED ---
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        print("   ❌ File still accessible (unexpected)")
    except s3_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print("   ✅ File appears DELETED (404) — as intern would see")
        else:
            raise

    return version_id


def recover_deleted_file_method1(s3_client, bucket, key, delete_marker_version_id):
    """
    RECOVERY METHOD 1: Delete the delete marker
    → Removes the tombstone
    → Previous version becomes "latest" again
    → Object reappears in normal GET
    """
    print(f"\n🔧 RECOVERY METHOD 1: Remove delete marker")
    print(f"   Deleting delete marker: {delete_marker_version_id}")

    s3_client.delete_object(
        Bucket=bucket,
        Key=key,
        VersionId=delete_marker_version_id
        # THIS deletes the delete marker itself — NOT the object
    )

    # --- VERIFY RECOVERY ---
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        version_id = response.get("VersionId", "null")
        print(f"   ✅ FILE RECOVERED!")
        print(f"   Current VersionId: {version_id}")
        print(f"   Content preview: {content[:80]}...")
        return True
    except Exception as e:
        print(f"   ❌ Recovery failed: {e}")
        return False


def recover_specific_version_method2(s3_client, bucket, key, target_version_id):
    """
    RECOVERY METHOD 2: Copy a specific old version to the same key
    → Useful when you want to restore a SPECIFIC version, not just latest
    → Creates a new version with the old data
    """
    print(f"\n🔧 RECOVERY METHOD 2: Copy specific version to same key")
    print(f"   Restoring version: {target_version_id}")

    s3_client.copy_object(
        Bucket=bucket,
        Key=key,
        CopySource={
            "Bucket": bucket,
            "Key": key,
            "VersionId": target_version_id
        }
    )

    # --- VERIFY ---
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    new_version_id = response.get("VersionId", "null")
    print(f"   ✅ VERSION RESTORED as new version: {new_version_id}")
    print(f"   Content preview: {content[:80]}...")
    return True


def bulk_recover_prefix(s3_client, bucket, prefix):
    """
    BULK RECOVERY: Recover all files under a prefix
    → This is what you'd run after intern's 'aws s3 rm --recursive'
    → Finds all delete markers and removes them
    """
    print(f"\n🔧 BULK RECOVERY: Removing all delete markers under {prefix}")

    paginator = s3_client.get_paginator("list_object_versions")
    recovered = 0
    failed = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        delete_markers = page.get("DeleteMarkers", [])

        for dm in delete_markers:
            if dm["IsLatest"]:  # Only remove latest delete markers
                try:
                    s3_client.delete_object(
                        Bucket=bucket,
                        Key=dm["Key"],
                        VersionId=dm["VersionId"]
                    )
                    print(f"   ✅ Recovered: {dm['Key']}")
                    recovered += 1
                except Exception as e:
                    print(f"   ❌ Failed: {dm['Key']} — {e}")
                    failed += 1

    print(f"\n📊 Bulk Recovery Summary:")
    print(f"   ✅ Recovered: {recovered} files")
    print(f"   ❌ Failed: {failed} files")
    return recovered


def run_full_simulation():
    """Complete disaster simulation and recovery drill"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print("🎯 DISASTER SIMULATION & RECOVERY DRILL")
    print("=" * 70)

    # --- PHASE 1: UPLOAD TEST FILE ---
    print("\n" + "=" * 70)
    print("📌 PHASE 1: Upload test file (2 versions)")
    print("=" * 70)
    key, v1_id = upload_test_file(s3_client, BUCKET_NAME)
    v2_id = upload_second_version(s3_client, BUCKET_NAME, key)

    print("\n--- State after uploads ---")
    list_object_versions(s3_client, BUCKET_NAME, key)

    # --- PHASE 2: SIMULATE DISASTER ---
    print("\n" + "=" * 70)
    print("💥 PHASE 2: Simulate accidental deletion (intern's mistake)")
    print("=" * 70)
    dm_version_id = simulate_accidental_delete(s3_client, BUCKET_NAME, key)

    print("\n--- State after deletion ---")
    list_object_versions(s3_client, BUCKET_NAME, key)

    # --- PHASE 3: RECOVER ---
    print("\n" + "=" * 70)
    print("🔧 PHASE 3: Recover the file")
    print("=" * 70)
    recover_deleted_file_method1(s3_client, BUCKET_NAME, key, dm_version_id)

    print("\n--- State after recovery ---")
    list_object_versions(s3_client, BUCKET_NAME, key)

    # --- PHASE 4: DEMONSTRATE METHOD 2 ---
    print("\n" + "=" * 70)
    print("🔧 PHASE 4: Recover a SPECIFIC old version (v1)")
    print("=" * 70)
    recover_specific_version_method2(s3_client, BUCKET_NAME, key, v1_id)

    print("\n--- Final state ---")
    list_object_versions(s3_client, BUCKET_NAME, key)

    # --- SUMMARY ---
    print("\n" + "=" * 70)
    print("✅ DRILL COMPLETE — KEY LEARNINGS:")
    print("=" * 70)
    print("  1. Simple DELETE → adds delete marker (object looks gone)")
    print("  2. All previous versions STILL EXIST in S3")
    print("  3. Recovery Method 1: Delete the delete marker → latest reappears")
    print("  4. Recovery Method 2: Copy specific version → restores exact data")
    print("  5. Bulk recovery: iterate delete markers under prefix, remove each")
    print("  6. WITHOUT versioning: all this would be IMPOSSIBLE")


if __name__ == "__main__":
    run_full_simulation()