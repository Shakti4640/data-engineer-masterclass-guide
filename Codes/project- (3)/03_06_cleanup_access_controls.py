# file: 15_cleanup_access_controls.py
# Purpose: Remove test users, policies, and optionally groups
# DEPENDS ON: Steps 1-5 (everything created in this project)
# USE WHEN: cleaning up after testing or decommissioning

import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

TEST_USERS = [
    "testuser-alice-eng",
    "testuser-bob-fin",
    "testuser-carol-mkt",
    "testuser-dave-intern",
]

GROUPS = [
    "QuickCart-DataEngineering",
    "QuickCart-Finance",
    "QuickCart-Marketing",
    "QuickCart-Interns",
]

POLICIES = [
    "QuickCart-S3-DataEngineering-Policy",
    "QuickCart-S3-Finance-ReadOnly-Policy",
    "QuickCart-S3-Marketing-ReadOnly-Policy",
    "QuickCart-S3-Interns-ReadOnly-Policy",
]


def delete_test_users(iam_client):
    """
    Delete test users — MUST follow this order:
    1. Delete access keys (user cannot be deleted with active keys)
    2. Remove from groups (user cannot be deleted while in groups)
    3. Detach inline policies (if any)
    4. Delete user
    
    WHY THIS ORDER MATTERS:
    → IAM enforces referential integrity
    → Cannot delete a user that has attached resources
    → Must clean up ALL dependencies first
    """
    print("\n🗑️  DELETING TEST USERS")
    print("-" * 50)

    for username in TEST_USERS:
        print(f"\n  Deleting: {username}")

        # --- Step A: Delete access keys ---
        try:
            keys_response = iam_client.list_access_keys(UserName=username)
            for key_meta in keys_response["AccessKeyMetadata"]:
                iam_client.delete_access_key(
                    UserName=username,
                    AccessKeyId=key_meta["AccessKeyId"]
                )
                print(f"    ✅ Deleted access key: {key_meta['AccessKeyId']}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                print(f"    ℹ️  User not found — skipping")
                continue
            else:
                raise

        # --- Step B: Remove from all groups ---
        try:
            groups_response = iam_client.list_groups_for_user(UserName=username)
            for group in groups_response["Groups"]:
                iam_client.remove_user_from_group(
                    GroupName=group["GroupName"],
                    UserName=username
                )
                print(f"    ✅ Removed from group: {group['GroupName']}")
        except ClientError:
            pass

        # --- Step C: Detach managed policies ---
        try:
            policies_response = iam_client.list_attached_user_policies(
                UserName=username
            )
            for pol in policies_response["AttachedPolicies"]:
                iam_client.detach_user_policy(
                    UserName=username,
                    PolicyArn=pol["PolicyArn"]
                )
                print(f"    ✅ Detached policy: {pol['PolicyName']}")
        except ClientError:
            pass

        # --- Step D: Delete inline policies ---
        try:
            inline_response = iam_client.list_user_policies(UserName=username)
            for policy_name in inline_response["PolicyNames"]:
                iam_client.delete_user_policy(
                    UserName=username,
                    PolicyName=policy_name
                )
                print(f"    ✅ Deleted inline policy: {policy_name}")
        except ClientError:
            pass

        # --- Step E: Delete the user ---
        try:
            iam_client.delete_user(UserName=username)
            print(f"    ✅ User deleted: {username}")
        except ClientError as e:
            print(f"    ❌ Failed to delete user: {e}")


def detach_and_delete_policies(iam_client):
    """
    Delete managed policies — MUST follow this order:
    1. Detach from all groups/users/roles
    2. Delete non-default policy versions
    3. Delete the policy itself
    
    WHY:
    → Cannot delete a policy that is attached to any entity
    → Cannot delete a policy with non-default versions
    """
    print("\n🗑️  DELETING IAM POLICIES")
    print("-" * 50)

    for policy_name in POLICIES:
        policy_arn = f"arn:aws:iam::{ACCOUNT_ID}:policy/{policy_name}"
        print(f"\n  Deleting: {policy_name}")

        try:
            # --- Step A: Detach from all groups ---
            response = iam_client.list_entities_for_policy(PolicyArn=policy_arn)
            for group in response.get("PolicyGroups", []):
                iam_client.detach_group_policy(
                    GroupName=group["GroupName"],
                    PolicyArn=policy_arn
                )
                print(f"    ✅ Detached from group: {group['GroupName']}")

            for user in response.get("PolicyUsers", []):
                iam_client.detach_user_policy(
                    UserName=user["UserName"],
                    PolicyArn=policy_arn
                )
                print(f"    ✅ Detached from user: {user['UserName']}")

            for role in response.get("PolicyRoles", []):
                iam_client.detach_role_policy(
                    RoleName=role["RoleName"],
                    PolicyArn=policy_arn
                )
                print(f"    ✅ Detached from role: {role['RoleName']}")

            # --- Step B: Delete non-default versions ---
            versions_response = iam_client.list_policy_versions(
                PolicyArn=policy_arn
            )
            for version in versions_response["Versions"]:
                if not version["IsDefaultVersion"]:
                    iam_client.delete_policy_version(
                        PolicyArn=policy_arn,
                        VersionId=version["VersionId"]
                    )
                    print(f"    ✅ Deleted version: {version['VersionId']}")

            # --- Step C: Delete the policy ---
            iam_client.delete_policy(PolicyArn=policy_arn)
            print(f"    ✅ Policy deleted: {policy_name}")

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                print(f"    ℹ️  Policy not found — skipping")
            else:
                print(f"    ❌ Failed: {e}")


def delete_groups(iam_client):
    """
    Delete IAM groups
    → Groups must be EMPTY (no users) and have NO attached policies
    → Steps above should have handled both prerequisites
    """
    print("\n🗑️  DELETING IAM GROUPS")
    print("-" * 50)

    for group_name in GROUPS:
        try:
            # Verify group is empty
            response = iam_client.get_group(GroupName=group_name)
            if response["Users"]:
                print(f"  ⚠️  Group {group_name} still has users — removing them")
                for user in response["Users"]:
                    iam_client.remove_user_from_group(
                        GroupName=group_name,
                        UserName=user["UserName"]
                    )

            # Verify no attached policies
            policies_response = iam_client.list_attached_group_policies(
                GroupName=group_name
            )
            for pol in policies_response["AttachedPolicies"]:
                iam_client.detach_group_policy(
                    GroupName=group_name,
                    PolicyArn=pol["PolicyArn"]
                )

            # Delete group
            iam_client.delete_group(GroupName=group_name)
            print(f"  ✅ Group deleted: {group_name}")

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                print(f"  ℹ️  Group not found: {group_name}")
            else:
                print(f"  ❌ Failed: {e}")


def remove_bucket_policy():
    """Remove the DENY bucket policy"""
    s3_client = boto3.client("s3")

    print("\n🗑️  REMOVING BUCKET POLICY")
    print("-" * 50)

    try:
        s3_client.delete_bucket_policy(Bucket=BUCKET_NAME)
        print(f"  ✅ Bucket policy removed from: {BUCKET_NAME}")
    except ClientError as e:
        print(f"  ❌ Failed: {e}")


def run_cleanup(delete_groups_flag=False, delete_bucket_policy_flag=False):
    """
    Master cleanup function
    
    PARAMETERS:
    → delete_groups_flag: set True to also delete the IAM groups
    → delete_bucket_policy_flag: set True to also remove bucket policy
    
    DEFAULT: only deletes test users and policies
    → Groups and bucket policy often remain for production use
    """
    iam_client = boto3.client("iam")

    print("=" * 60)
    print("🧹 CLEANUP — REMOVING TEST RESOURCES")
    print("=" * 60)

    # Order matters: users first, then policies, then groups
    delete_test_users(iam_client)
    detach_and_delete_policies(iam_client)

    if delete_groups_flag:
        delete_groups(iam_client)
    else:
        print("\nℹ️  Groups retained (pass delete_groups_flag=True to remove)")

    if delete_bucket_policy_flag:
        remove_bucket_policy()
    else:
        print("ℹ️  Bucket policy retained (pass delete_bucket_policy_flag=True to remove)")

    print("\n" + "=" * 60)
    print("✅ CLEANUP COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    # Safety confirmation
    print("⚠️  This will delete ALL test users and policies from Project 3")
    confirm = input("Type 'CLEANUP' to proceed: ")

    if confirm == "CLEANUP":
        run_cleanup(
            delete_groups_flag=False,         # Keep groups
            delete_bucket_policy_flag=False    # Keep bucket policy
        )
    else:
        print("❌ Aborted")