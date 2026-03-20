# file: 56_01_bucket_policy_pattern.py
# Purpose: Set up cross-account S3 access via bucket policy (Pattern 1)
# RUN THIS IN ACCOUNT A (or simulate with different bucket/role)

import boto3
import json

REGION = "us-east-2"

# Account IDs (replace with your actual account IDs)
ACCOUNT_A_ID [REDACTED:BANK_ACCOUNT_NUMBER]111"   # Production Data account
ACCOUNT_B_ID [REDACTED:BANK_ACCOUNT_NUMBER]222"   # Analytics account

# Resource names
BUCKET_NAME = "quickcart-data-prod"
ACCOUNT_B_ROLE_NAME = "QuickCart-Analytics-GlueRole"
ACCOUNT_B_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_B_ID}:role/{ACCOUNT_B_ROLE_NAME}"


def setup_bucket_policy_cross_account():
    """
    PATTERN 1: Grant cross-account access via S3 bucket policy
    
    This runs in ACCOUNT A — grants Account B's role permission to read
    
    WHAT THIS DOES:
    → Account A's bucket policy says:
      "Allow Account B's GlueRole to GetObject and ListBucket
       but ONLY on daily_exports/ and cleaned/ prefixes"
    
    STILL NEEDED IN ACCOUNT B:
    → Account B's GlueRole must have an IAM policy allowing
      s3:GetObject on Account A's bucket
    → Both sides must allow (cross-account AND logic)
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print("🔧 PATTERN 1: BUCKET POLICY CROSS-ACCOUNT ACCESS")
    print(f"   Account A (data owner): {ACCOUNT_A_ID}")
    print(f"   Account B (consumer):   {ACCOUNT_B_ID}")
    print("=" * 70)

    # --- Build bucket policy ---
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAccountBListBucket",
                "Effect": "Allow",
                "Principal": {
                    "AWS": ACCOUNT_B_ROLE_ARN
                },
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}",
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [
                            "daily_exports/*",
                            "cleaned/*"
                        ]
                    }
                }
            },
            {
                "Sid": "AllowAccountBGetObject",
                "Effect": "Allow",
                "Principal": {
                    "AWS": ACCOUNT_B_ROLE_ARN
                },
                "Action": "s3:GetObject",
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}/daily_exports/*",
                    f"arn:aws:s3:::{BUCKET_NAME}/cleaned/*"
                ]
            },
            {
                "Sid": "DenyAccountBInternalAccess",
                "Effect": "Deny",
                "Principal": {
                    "AWS": ACCOUNT_B_ROLE_ARN
                },
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}/internal/*",
                    f"arn:aws:s3:::{BUCKET_NAME}/credentials/*"
                ]
            }
        ]
    }

    # --- Apply bucket policy ---
    try:
        s3_client.put_bucket_policy(
            Bucket=BUCKET_NAME,
            Policy=json.dumps(bucket_policy)
        )
        print(f"\n✅ Bucket policy applied to: {BUCKET_NAME}")
    except Exception as e:
        print(f"\n⚠️  Could not apply bucket policy: {e}")
        print(f"   (Expected if bucket doesn't exist — this is a template)")

    # --- Display policy ---
    print(f"\n📋 BUCKET POLICY:")
    print(json.dumps(bucket_policy, indent=2))

    # --- Explain what Account B needs ---
    print(f"\n{'─' * 50}")
    print(f"📌 ACCOUNT B MUST ALSO ADD (identity policy on GlueRole):")
    print(f"{'─' * 50}")

    account_b_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowReadAccountABucket",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/daily_exports/*",
                    f"arn:aws:s3:::{BUCKET_NAME}/cleaned/*"
                ]
            }
        ]
    }
    print(json.dumps(account_b_policy, indent=2))

    print(f"\n✅ PATTERN 1 COMPLETE")
    print(f"   Account B's GlueRole can now:")
    print(f"   ✅ List objects in daily_exports/ and cleaned/")
    print(f"   ✅ Read objects in daily_exports/ and cleaned/")
    print(f"   ❌ Cannot access internal/ or credentials/")
    print(f"   ❌ Cannot write or delete any objects")

    return bucket_policy


if __name__ == "__main__":
    setup_bucket_policy_cross_account()