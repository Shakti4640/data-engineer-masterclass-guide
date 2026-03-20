# file: 11_create_iam_policies.py
# Purpose: Create team-specific S3 access policies and attach to groups
# DEPENDS ON: Step 1 (groups exist)

import boto3
import json
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
BUCKET_ARN = f"arn:aws:s3:::{BUCKET_NAME}"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]


def create_data_engineering_policy():
    """
    Data Engineering: FULL access to the entire bucket
    → Can read, write, delete any prefix
    → Can manage versioning and lifecycle (from Projects 1 & 2)
    → This is the pipeline service team
    """
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowListBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket",
                    "s3:ListBucketVersions",
                    "s3:GetBucketLocation"
                ],
                "Resource": BUCKET_ARN
                # No Condition → can list ALL prefixes
            },
            {
                "Sid": "AllowFullObjectAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:DeleteObjectVersion"
                ],
                "Resource": f"{BUCKET_ARN}/*"
            },
            {
                "Sid": "AllowBucketManagement",
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketVersioning",
                    "s3:GetLifecycleConfiguration",
                    "s3:GetEncryptionConfiguration"
                ],
                "Resource": BUCKET_ARN
            }
        ]
    }

    return "QuickCart-S3-DataEngineering-Policy", policy


def create_finance_policy():
    """
    Finance: READ-ONLY access to:
    → daily_exports/orders/    (order data for revenue analysis)
    → daily_exports/products/  (product pricing)
    → finance_reports/         (their own team's reports)
    
    CANNOT access:
    → daily_exports/customers/ (PII — marketing only)
    → marketing_data/          (not their domain)
    
    CANNOT:
    → Write, delete, or modify anything
    """
    allowed_prefixes = [
        "daily_exports/orders/*",
        "daily_exports/products/*",
        "finance_reports/*"
    ]

    # For ListBucket Condition — needs prefix WITHOUT trailing /*
    list_prefixes = [
        "daily_exports/orders/",
        "daily_exports/products/",
        "finance_reports/"
    ]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowListSpecificPrefixes",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": BUCKET_ARN,
                "Condition": {
                    "StringLike": {
                        "s3:prefix": list_prefixes
                    }
                }
            },
            {
                "Sid": "AllowListBucketRoot",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": BUCKET_ARN,
                "Condition": {
                    "StringEquals": {
                        "s3:prefix": [""],
                        "s3:delimiter": ["/"]
                    }
                }
                # This allows listing top-level "folders" only
                # User sees: daily_exports/, finance_reports/
                # But cannot drill into prefixes they don't have access to
            },
            {
                "Sid": "AllowReadObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectVersion"
                ],
                "Resource": [
                    f"{BUCKET_ARN}/{prefix}" for prefix in allowed_prefixes
                ]
            },
            {
                "Sid": "AllowGetBucketLocation",
                "Effect": "Allow",
                "Action": "s3:GetBucketLocation",
                "Resource": BUCKET_ARN
            }
        ]
    }

    return "QuickCart-S3-Finance-ReadOnly-Policy", policy


def create_marketing_policy():
    """
    Marketing: READ-ONLY access to:
    → daily_exports/customers/  (customer analytics)
    → daily_exports/products/   (product catalog)
    → marketing_data/           (their own team's data)
    
    CANNOT access:
    → daily_exports/orders/     (revenue data — finance only)
    → finance_reports/          (not their domain)
    """
    allowed_prefixes = [
        "daily_exports/customers/*",
        "daily_exports/products/*",
        "marketing_data/*"
    ]

    list_prefixes = [
        "daily_exports/customers/",
        "daily_exports/products/",
        "marketing_data/"
    ]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowListSpecificPrefixes",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": BUCKET_ARN,
                "Condition": {
                    "StringLike": {
                        "s3:prefix": list_prefixes
                    }
                }
            },
            {
                "Sid": "AllowListBucketRoot",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": BUCKET_ARN,
                "Condition": {
                    "StringEquals": {
                        "s3:prefix": [""],
                        "s3:delimiter": ["/"]
                    }
                }
            },
            {
                "Sid": "AllowReadObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectVersion"
                ],
                "Resource": [
                    f"{BUCKET_ARN}/{prefix}" for prefix in allowed_prefixes
                ]
            },
            {
                "Sid": "AllowGetBucketLocation",
                "Effect": "Allow",
                "Action": "s3:GetBucketLocation",
                "Resource": BUCKET_ARN
            }
        ]
    }

    return "QuickCart-S3-Marketing-ReadOnly-Policy", policy


def create_intern_policy():
    """
    Interns: Most restricted access
    → READ-ONLY to daily_exports/ only (no finance or marketing data)
    → Cannot see finance_reports/ or marketing_data/ at all
    → Cannot write, delete, or modify anything
    """
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowListDailyExportsOnly",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": BUCKET_ARN,
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [
                            "daily_exports/*"
                        ]
                    }
                }
            },
            {
                "Sid": "AllowReadDailyExportsOnly",
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": f"{BUCKET_ARN}/daily_exports/*"
            },
            {
                "Sid": "AllowGetBucketLocation",
                "Effect": "Allow",
                "Action": "s3:GetBucketLocation",
                "Resource": BUCKET_ARN
            }
        ]
    }

    return "QuickCart-S3-Interns-ReadOnly-Policy", policy


def create_and_attach_policy(iam_client, policy_name, policy_document, group_name):
    """Create IAM managed policy and attach to group"""
    policy_arn = f"arn:aws:iam::{ACCOUNT_ID}:policy/{policy_name}"

    # --- CREATE POLICY ---
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description=f"S3 access policy for {group_name}"
        )
        policy_arn = response["Policy"]["Arn"]
        print(f"✅ Created policy: {policy_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"ℹ️  Policy exists: {policy_name} — updating...")
            # Get existing policy ARN
            policy_arn = f"arn:aws:iam::{ACCOUNT_ID}:policy/{policy_name}"
            # Create new version (max 5 versions per policy)
            try:
                iam_client.create_policy_version(
                    PolicyArn=policy_arn,
                    PolicyDocument=json.dumps(policy_document),
                    SetAsDefault=True
                )
                print(f"   ✅ Updated to new version")
            except ClientError:
                print(f"   ⚠️  Max versions reached — delete old versions first")
        else:
            raise

    # --- ATTACH TO GROUP ---
    try:
        iam_client.attach_group_policy(
            GroupName=group_name,
            PolicyArn=policy_arn
        )
        print(f"   ✅ Attached to group: {group_name}")
    except ClientError as e:
        print(f"   ❌ Failed to attach: {e}")

    return policy_arn


def main():
    iam_client = boto3.client("iam")

    # Map: (policy_creator_function, target_group)
    policy_group_map = [
        (create_data_engineering_policy, "QuickCart-DataEngineering"),
        (create_finance_policy, "QuickCart-Finance"),
        (create_marketing_policy, "QuickCart-Marketing"),
        (create_intern_policy, "QuickCart-Interns"),
    ]

    print("=" * 60)
    print("📜 CREATING & ATTACHING IAM POLICIES")
    print("=" * 60)

    for policy_func, group_name in policy_group_map:
        print(f"\n--- {group_name} ---")
        policy_name, policy_document = policy_func()

        # Display policy for review
        print(f"   Policy: {policy_name}")
        actions = set()
        for stmt in policy_document["Statement"]:
            action = stmt["Action"]
            if isinstance(action, list):
                actions.update(action)
            else:
                actions.add(action)
        print(f"   Actions: {', '.join(sorted(actions))}")

        create_and_attach_policy(
            iam_client, policy_name, policy_document, group_name
        )

    print("\n" + "=" * 60)
    print("✅ ALL POLICIES CREATED AND ATTACHED")
    print("=" * 60)


if __name__ == "__main__":
    main()