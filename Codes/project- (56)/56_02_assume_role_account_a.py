# file: 56_02_assume_role_account_a.py
# Purpose: Create cross-account IAM role in Account A (data owner)
# RUN THIS IN ACCOUNT A

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_A_ID =[REDACTED:BANK_ACCOUNT_NUMBER]11"
ACCOUNT_B_ID =[REDACTED:BANK_ACCOUNT_NUMBER]22"

CROSS_ACCOUNT_ROLE_NAME = "CrossAccount-S3Read-FromAnalytics"
BUCKET_NAME = "quickcart-data-prod"
EXTERNAL_ID = "quickcart-analytics-2025-xK9mP2"  # Shared secret


def create_cross_account_role_in_account_a():
    """
    Create IAM role in Account A that Account B can ASSUME
    
    THIS ROLE:
    → Lives in Account A
    → Has permissions to read Account A's S3 bucket
    → Trusts Account B's specific role to assume it
    → Requires External ID (confused deputy prevention)
    → Session duration: 4 hours (enough for long ETL jobs)
    """
    iam_client = boto3.client("iam")

    print("=" * 70)
    print("🔧 PATTERN 2: ASSUME ROLE — ACCOUNT A SETUP")
    print(f"   Creating cross-account role in Account A ({ACCOUNT_A_ID})")
    print(f"   Trusting Account B ({ACCOUNT_B_ID})")
    print("=" * 70)

    # --- TRUST POLICY ---
    # WHO can assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAccountBAnalyticsRole",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{ACCOUNT_B_ID}:role/QuickCart-Analytics-GlueRole"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        # External ID: prevents confused deputy attack
                        "sts:ExternalId": EXTERNAL_ID
                    }
                    # Optional: restrict to Organization
                    # "StringEquals": {
                    #     "aws:PrincipalOrgID": "o-1234567890"
                    # }
                }
            },
            {
                # Also allow Account B's Athena role
                "Sid": "AllowAccountBAthenaRole",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{ACCOUNT_B_ID}:role/QuickCart-Analytics-AthenaRole"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": EXTERNAL_ID
                    }
                }
            }
        ]
    }

    # --- PERMISSION POLICY ---
    # WHAT the assumed role can do
    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ReadAllowedPrefixes",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectVersion"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}/daily_exports/*",
                    f"arn:aws:s3:::{BUCKET_NAME}/cleaned/*",
                    f"arn:aws:s3:::{BUCKET_NAME}/gold/*"
                ]
            },
            {
                "Sid": "S3ListAllowedPrefixes",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}",
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [
                            "daily_exports/*",
                            "cleaned/*",
                            "gold/*"
                        ]
                    }
                }
            },
            {
                "Sid": "DenySensitivePrefixes",
                "Effect": "Deny",
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}/internal/*",
                    f"arn:aws:s3:::{BUCKET_NAME}/credentials/*",
                    f"arn:aws:s3:::{BUCKET_NAME}/pii-raw/*"
                ]
            }
        ]
    }

    # --- CREATE ROLE ---
    try:
        iam_client.create_role(
            RoleName=CROSS_ACCOUNT_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description=(
                f"Cross-account role allowing Account B ({ACCOUNT_B_ID}) "
                f"to read S3 data in Account A"
            ),
            MaxSessionDuration=14400,   # 4 hours max session
            Tags=[
                {"Key": "CrossAccount", "Value": "true"},
                {"Key": "SourceAccount", "Value": ACCOUNT_B_ID},
                {"Key": "Purpose", "Value": "S3-ReadOnly"},
                {"Key": "ManagedBy", "Value": "Security-Team"},
                {"Key": "ExternalIdRequired", "Value": "true"}
            ]
        )
        print(f"\n✅ Cross-account role created: {CROSS_ACCOUNT_ROLE_NAME}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        # Update trust policy
        iam_client.update_assume_role_policy(
            RoleName=CROSS_ACCOUNT_ROLE_NAME,
            PolicyDocument=json.dumps(trust_policy)
        )
        print(f"\nℹ️  Role exists — trust policy updated: {CROSS_ACCOUNT_ROLE_NAME}")

    # Attach permission policy
    iam_client.put_role_policy(
        RoleName=CROSS_ACCOUNT_ROLE_NAME,
        PolicyName="S3ReadOnlyAccess",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"✅ Permission policy attached")

    # Get role ARN
    role = iam_client.get_role(RoleName=CROSS_ACCOUNT_ROLE_NAME)
    role_arn = role["Role"]["Arn"]

    print(f"\n{'─' * 50}")
    print(f"📋 ROLE DETAILS:")
    print(f"  Role ARN:     {role_arn}")
    print(f"  External ID:  {EXTERNAL_ID}")
    print(f"  Max Session:  4 hours")
    print(f"  Allowed S3:   daily_exports/*, cleaned/*, gold/*")
    print(f"  Denied S3:    internal/*, credentials/*, pii-raw/*")

    print(f"\n{'─' * 50}")
    print(f"📌 SHARE WITH ACCOUNT B TEAM (securely):")
    print(f"  Role ARN:    {role_arn}")
    print(f"  External ID: {EXTERNAL_ID}")
    print(f"  (Share External ID via secure channel — NOT email/Slack)")
    print(f"{'─' * 50}")

    # Save for Account B scripts
    config = {
        "cross_account_role_arn": role_arn,
        "external_id": EXTERNAL_ID,
        "bucket_name": BUCKET_NAME,
        "account_a_id": ACCOUNT_A_ID,
        "account_b_id": ACCOUNT_B_ID,
        "allowed_prefixes": ["daily_exports/", "cleaned/", "gold/"],
        "denied_prefixes": ["internal/", "credentials/", "pii-raw/"]
    }
    with open("/tmp/cross_account_config.json", "w") as f:
        json.dump(config, f, indent=2)
    print(f"\n💾 Config saved to /tmp/cross_account_config.json")

    return role_arn


if __name__ == "__main__":
    create_cross_account_role_in_account_a()