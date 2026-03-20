# file: 12_create_bucket_policy.py
# Purpose: Add DENY rules as defense-in-depth safety net
# DEPENDS ON: Project 1 (bucket exists), Step 2 (IAM groups exist)

import boto3
import json
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
BUCKET_ARN = f"arn:aws:s3:::{BUCKET_NAME}"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
REGION = "us-east-2"


def create_bucket_policy():
    """
    Bucket Policy Strategy:
    → IAM policies handle ALLOW (positive permissions)
    → Bucket policy handles DENY (safety net)
    → Even if IAM is misconfigured, bucket policy DENY blocks unauthorized access
    
    WHY NOT USE NotPrincipal:
    → NotPrincipal is tricky with assumed roles and session contexts
    → aws:PrincipalArn condition is more predictable
    → Industry best practice: use Condition instead of NotPrincipal
    """
    
    # ARNs that ARE allowed to access finance data
    finance_allowed_arns = [
        f"arn:aws:iam::{ACCOUNT_ID}:group/QuickCart-DataEngineering",
        f"arn:aws:iam::{ACCOUNT_ID}:group/QuickCart-Finance",
        f"arn:aws:iam::{ACCOUNT_ID}:root"
    ]

    # ARNs that ARE allowed to delete anything
    delete_allowed_arns = [
        f"arn:aws:iam::{ACCOUNT_ID}:group/QuickCart-DataEngineering",
        f"arn:aws:iam::{ACCOUNT_ID}:root"
    ]

    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            # ──────────────────────────────────────────────────────
            # RULE 1: Deny finance_reports/ access to non-authorized
            # ──────────────────────────────────────────────────────
            {
                "Sid": "DenyFinanceReportsToUnauthorized",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    f"{BUCKET_ARN}/finance_reports",
                    f"{BUCKET_ARN}/finance_reports/*"
                ],
                "Condition": {
                    "StringNotLike": {
                        "aws:PrincipalArn": [
                            f"arn:aws:iam::{ACCOUNT_ID}:user/alice",
                            f"arn:aws:iam::{ACCOUNT_ID}:user/bob",
                            f"arn:aws:iam::{ACCOUNT_ID}:role/DataPipelineRole",
                            f"arn:aws:iam::{ACCOUNT_ID}:root"
                        ]
                    }
                }
            },

            # ──────────────────────────────────────────────────────
            # RULE 2: Deny DELETE actions to non-engineering users
            # ──────────────────────────────────────────────────────
            {
                "Sid": "DenyDeleteToNonEngineering",
                "Effect": "Deny",
                "Principal": "*",
                "Action": [
                    "s3:DeleteObject",
                    "s3:DeleteObjectVersion"
                ],
                "Resource": f"{BUCKET_ARN}/*",
                "Condition": {
                    "StringNotLike": {
                        "aws:PrincipalArn": [
                            f"arn:aws:iam::{ACCOUNT_ID}:user/alice",
                            f"arn:aws:iam::{ACCOUNT_ID}:role/DataPipelineRole",
                            f"arn:aws:iam::{ACCOUNT_ID}:root"
                        ]
                    }
                }
            },

            # ──────────────────────────────────────────────────────
            # RULE 3: Enforce SSL/TLS for all requests
            # ──────────────────────────────────────────────────────
            {
                "Sid": "DenyInsecureTransport",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    BUCKET_ARN,
                    f"{BUCKET_ARN}/*"
                ],
                "Condition": {
                    "Bool": {
                        "aws:SecureTransport": "false"
                    }
                }
            },

            # ──────────────────────────────────────────────────────
            # RULE 4: Enforce encryption on all uploads
            # ──────────────────────────────────────────────────────
            {
                "Sid": "DenyUnencryptedUploads",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": f"{BUCKET_ARN}/*",
                "Condition": {
                    "StringNotEquals": {
                        "s3:x-amz-server-side-encryption": "AES256"
                    }
                }
            }
        ]
    }

    return bucket_policy


def apply_bucket_policy(bucket_name, policy):
    s3_client = boto3.client("s3", region_name=REGION)

    policy_json = json.dumps(policy, indent=2)
    policy_size = len(policy_json.encode("utf-8"))

    print(f"📜 Bucket Policy Size: {policy_size} bytes (max: 20,480)")

    if policy_size > 20480:
        print("❌ Policy exceeds 20KB limit — must simplify")
        return False

    try:
        s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=policy_json
        )
        print("✅ Bucket policy applied successfully")
        return True

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "MalformedPolicy":
            print(f"❌ Malformed policy: {e.response['Error']['Message']}")
            print("   → Check ARN formats, Principal values, Action names")
        else:
            print(f"❌ Failed: {e}")
        return False


def verify_bucket_policy(bucket_name):
    s3_client = boto3.client("s3", region_name=REGION)

    try:
        response = s3_client.get_bucket_policy(Bucket=bucket_name)
        policy = json.loads(response["Policy"])

        print(f"\n📋 Active Bucket Policy for '{bucket_name}':")
        print("-" * 60)

        for stmt in policy["Statement"]:
            sid = stmt.get("Sid", "unnamed")
            effect = stmt["Effect"]
            actions = stmt["Action"]
            if isinstance(actions, str):
                actions = [actions]

            print(f"\n  📌 {sid}")
            print(f"     Effect: {effect}")
            print(f"     Actions: {', '.join(actions[:3])}{'...' if len(actions) > 3 else ''}")

            if "Resource" in stmt:
                resources = stmt["Resource"]
                if isinstance(resources, str):
                    resources = [resources]
                for r in resources:
                    # Show just the meaningful part
                    short_r = r.replace(f"arn:aws:s3:::{bucket_name}", "BUCKET")
                    print(f"     Resource: {short_r}")

            if "Condition" in stmt:
                for cond_op, cond_map in stmt["Condition"].items():
                    for cond_key, cond_val in cond_map.items():
                        print(f"     Condition: {cond_op}({cond_key})")

        print("\n" + "-" * 60)

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
            print("ℹ️  No bucket policy set")
        else:
            raise


if __name__ == "__main__":
    print("=" * 60)
    print("🛡️  APPLYING BUCKET POLICY (DENY SAFETY NET)")
    print(f"   Bucket: {BUCKET_NAME}")
    print("=" * 60)

    policy = create_bucket_policy()

    # Preview
    print("\n📋 Policy Preview:")
    print(json.dumps(policy, indent=2)[:500] + "\n...")

    apply_bucket_policy(BUCKET_NAME, policy)
    verify_bucket_policy(BUCKET_NAME)

    print("\n🛡️  DEFENSE-IN-DEPTH SUMMARY:")
    print("   Layer 1: IAM policies → ALLOW team-specific access")
    print("   Layer 2: Bucket policy → DENY unauthorized access")
    print("   Layer 3: Versioning → Recover if something slips through (Project 2)")
    print("   Layer 4: Block Public Access → No accidental internet exposure (Project 1)")
    print("   Layer 5: SSL enforcement → No unencrypted data in transit")