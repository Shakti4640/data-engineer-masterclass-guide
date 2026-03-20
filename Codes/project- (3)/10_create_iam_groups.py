# file: 10_create_iam_groups.py
# Purpose: Create team-based IAM groups
# DEPENDS ON: Project 1 (bucket exists), Project 2 (versioning enabled)

import boto3
import json
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# --- GROUP DEFINITIONS ---
GROUPS = [
    {
        "name": "QuickCart-DataEngineering",
        "description": "Full access to all S3 prefixes — pipeline builders"
    },
    {
        "name": "QuickCart-Finance",
        "description": "Read-only access to daily_exports/ and finance_reports/"
    },
    {
        "name": "QuickCart-Marketing",
        "description": "Read-only access to daily_exports/ and marketing_data/"
    },
    {
        "name": "QuickCart-Interns",
        "description": "Read-only access to daily_exports/ only"
    }
]


def create_groups():
    iam_client = boto3.client("iam")

    for group in GROUPS:
        try:
            iam_client.create_group(GroupName=group["name"])
            print(f"✅ Created group: {group['name']}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                print(f"ℹ️  Group already exists: {group['name']}")
            else:
                raise

    print(f"\n📋 All groups created. Next: attach policies to each group.")


if __name__ == "__main__":
    print("=" * 60)
    print("👥 CREATING IAM GROUPS")
    print("=" * 60)
    create_groups()