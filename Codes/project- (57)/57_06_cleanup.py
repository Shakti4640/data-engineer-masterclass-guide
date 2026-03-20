# file: 57_06_cleanup.py
# Purpose: Remove Catalog Resource Policy and cross-account Catalog permissions

import boto3
import json

REGION = "us-east-2"


def cleanup_all():
    """Remove Catalog Resource Policy and Account B's Catalog permissions"""
    glue_client = boto3.client("glue", region_name=REGION)
    iam_client = boto3.client("iam")

    print("=" * 70)
    print("🧹 CLEANING UP PROJECT 57 — CROSS-ACCOUNT CATALOG")
    print("=" * 70)

    # --- Step 1: Remove Catalog Resource Policy (Account A) ---
    print("\n📌 Step 1: Removing Catalog Resource Policy...")
    try:
        # Get current policy hash (required for deletion)
        response = glue_client.get_resource_policy()
        policy_hash = response.get("PolicyHash", "")

        if policy_hash:
            glue_client.delete_resource_policy(
                PolicyHashCondition=policy_hash
            )
            print(f"  ✅ Catalog Resource Policy deleted")
        else:
            print(f"  ℹ️  No Catalog Resource Policy found")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"  ℹ️  No Catalog Resource Policy exists")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 2: Remove Account B's Catalog permissions ---
    print("\n📌 Step 2: Removing Account B's Catalog IAM policies...")
    roles = [
        "QuickCart-Analytics-GlueRole",
        "QuickCart-Analytics-AthenaRole"
    ]

    for role_name in roles:
        try:
            iam_client.delete_role_policy(
                RoleName=role_name,
                PolicyName="CrossAccountCatalogRead"
            )
            print(f"  ✅ Removed CrossAccountCatalogRead from {role_name}")
        except iam_client.exceptions.NoSuchEntityException:
            print(f"  ℹ️  Policy not found on {role_name}")
        except Exception as e:
            print(f"  ⚠️  {role_name}: {e}")

    # --- Step 3: Clean config file ---
    print("\n📌 Step 3: Cleaning config files...")
    import os
    try:
        os.remove("/tmp/catalog_sharing_config.json")
        print(f"  ✅ Removed config file")
    except Exception:
        pass

    # --- Step 4: Clean up saved scripts ---
    try:
        os.remove("/tmp/cross_account_glue_etl.py")
        print(f"  ✅ Removed Glue ETL script")
    except Exception:
        pass

    print("\n" + "=" * 70)
    print("🎉 ALL PROJECT 57 RESOURCES CLEANED UP")
    print("=" * 70)
    print(f"\n  ⚠️  NOTE: Account B's base roles (from Project 56) NOT deleted")
    print(f"  → Those are cleaned up in Project 56's cleanup script")


if __name__ == "__main__":
    cleanup_all()