# file: 13_create_test_users.py
# Purpose: Create test users in each group to verify access controls
# DEPENDS ON: Steps 1-3

import boto3
import json
from botocore.exceptions import ClientError

# Test users mapped to groups
TEST_USERS = {
    "testuser-alice-eng": "QuickCart-DataEngineering",
    "testuser-bob-fin": "QuickCart-Finance",
    "testuser-carol-mkt": "QuickCart-Marketing",
    "testuser-dave-intern": "QuickCart-Interns",
}


def create_test_users():
    iam_client = boto3.client("iam")

    for username, group_name in TEST_USERS.items():
        # --- CREATE USER ---
        try:
            iam_client.create_user(UserName=username)
            print(f"✅ Created user: {username}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                print(f"ℹ️  User exists: {username}")
            else:
                raise

        # --- ADD TO GROUP ---
        try:
            iam_client.add_user_to_group(
                UserName=username,
                GroupName=group_name
            )
            print(f"   → Added to group: {group_name}")
        except ClientError as e:
            print(f"   ❌ Failed to add to group: {e}")

        # --- CREATE ACCESS KEY (for testing only) ---
        try:
            response = iam_client.create_access_key(UserName=username)
            key = response["AccessKey"]
            print(f"   → Access Key ID: {key['AccessKeyId']}")
            print(f"   → Secret Key: {key['SecretAccessKey'][:8]}... (SAVE THIS)")
            print(f"   ⚠️  Store these securely — shown only once")
        except ClientError as e:
            if "LimitExceeded" in str(e):
                print(f"   ℹ️  Max access keys reached for {username}")
            else:
                raise

        print()


def list_group_memberships():
    iam_client = boto3.client("iam")

    print("\n📋 GROUP MEMBERSHIP SUMMARY:")
    print("-" * 50)

    for group_name in set(TEST_USERS.values()):
        response = iam_client.get_group(GroupName=group_name)
        users = [u["UserName"] for u in response["Users"]]
        print(f"  {group_name}:")
        for u in users:
            print(f"    → {u}")
    print("-" * 50)


if __name__ == "__main__":
    print("=" * 60)
    print("👤 CREATING TEST USERS")
    print("=" * 60)
    create_test_users()
    list_group_memberships()

    print("\n🧪 NEXT: Run 14_test_access_controls.py to verify permissions")