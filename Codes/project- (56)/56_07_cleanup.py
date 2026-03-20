# file: 56_07_cleanup.py
# Purpose: Remove all cross-account IAM roles and policies

import boto3
import json

REGION = "us-east-2"

# Account A resources
CROSS_ACCOUNT_ROLE_NAME = "CrossAccount-S3Read-FromAnalytics"
BUCKET_NAME = "quickcart-data-prod"

# Account B resources
ACCOUNT_B_ROLES = [
    "QuickCart-Analytics-GlueRole",
    "QuickCart-Analytics-AthenaRole"
]


def cleanup_all():
    """Remove all cross-account IAM resources"""
    iam_client = boto3.client("iam")
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print("🧹 CLEANING UP PROJECT 56 — CROSS-ACCOUNT RESOURCES")
    print("=" * 70)

    # --- Step 1: Remove bucket policy ---
    print("\n📌 Step 1: Removing cross-account bucket policy...")
    try:
        s3_client.delete_bucket_policy(Bucket=BUCKET_NAME)
        print(f"  ✅ Bucket policy removed: {BUCKET_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 2: Delete Account A's cross-account role ---
    print(f"\n📌 Step 2: Deleting Account A cross-account role...")
    try:
        # Delete inline policies first
        policies = iam_client.list_role_policies(RoleName=CROSS_ACCOUNT_ROLE_NAME)
        for policy_name in policies.get("PolicyNames", []):
            iam_client.delete_role_policy(
                RoleName=CROSS_ACCOUNT_ROLE_NAME,
                PolicyName=policy_name
            )
            print(f"  ✅ Deleted policy: {policy_name}")

        iam_client.delete_role(RoleName=CROSS_ACCOUNT_ROLE_NAME)
        print(f"  ✅ Deleted role: {CROSS_ACCOUNT_ROLE_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 3: Delete Account B's roles ---
    print(f"\n📌 Step 3: Deleting Account B roles...")
    for role_name in ACCOUNT_B_ROLES:
        try:
            policies = iam_client.list_role_policies(RoleName=role_name)
            for policy_name in policies.get("PolicyNames", []):
                iam_client.delete_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name
                )
                print(f"  ✅ Deleted policy: {role_name}/{policy_name}")

            iam_client.delete_role(RoleName=role_name)
            print(f"  ✅ Deleted role: {role_name}")
        except Exception as e:
            print(f"  ⚠️  {role_name}: {e}")

    # --- Step 4: Clean up config file ---
    print(f"\n📌 Step 4: Cleaning up config files...")
    import os
    try:
        os.remove("/tmp/cross_account_config.json")
        print(f"  ✅ Removed /tmp/cross_account_config.json")
    except Exception:
        pass

    print("\n" + "=" * 70)
    print("🎉 ALL PROJECT 56 RESOURCES CLEANED UP")
    print("=" * 70)
    print(f"\n  ⚠️  NOTE: In a real multi-account setup:")
    print(f"  → Run Account A cleanup IN Account A")
    print(f"  → Run Account B cleanup IN Account B")
    print(f"  → This script assumes single-account simulation")


if __name__ == "__main__":
    cleanup_all()