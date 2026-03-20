# file: 56_04_account_b_iam_setup.py
# Purpose: Create IAM role in Account B with permission to AssumeRole into Account A
# RUN THIS IN ACCOUNT B

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/cross_account_config.json", "r") as f:
        return json.load(f)


def create_account_b_roles():
    """
    Create roles in Account B that CAN assume Account A's cross-account role
    
    TWO ROLES:
    1. GlueRole: for Glue ETL jobs that need Account A's S3 data
    2. AthenaRole: for Athena queries on Account A's data
    
    Each role needs:
    → sts:AssumeRole permission on Account A's cross-account role ARN
    → Plus their own service permissions (glue:*, athena:*, etc.)
    """
    config = load_config()
    iam_client = boto3.client("iam")

    print("=" * 70)
    print("🔧 ACCOUNT B: CREATING IAM ROLES FOR CROSS-ACCOUNT ACCESS")
    print("=" * 70)

    roles_to_create = [
        {
            "role_name": "QuickCart-Analytics-GlueRole",
            "service": "glue.amazonaws.com",
            "description": "Glue ETL role that can assume Account A's S3 read role",
            "additional_policies": {
                "GlueServicePermissions": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "GlueBasicPermissions",
                            "Effect": "Allow",
                            "Action": [
                                "glue:*",
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:ListBucket",
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents"
                            ],
                            "Resource": "*"
                        }
                    ]
                }
            }
        },
        {
            "role_name": "QuickCart-Analytics-AthenaRole",
            "service": "athena.amazonaws.com",
            "description": "Athena role for querying Account A's data via cross-account",
            "additional_policies": {
                "AthenaServicePermissions": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AthenaPermissions",
                            "Effect": "Allow",
                            "Action": [
                                "athena:*",
                                "glue:GetTable",
                                "glue:GetDatabase",
                                "glue:GetPartitions",
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:ListBucket"
                            ],
                            "Resource": "*"
                        }
                    ]
                }
            }
        }
    ]

    for role_config in roles_to_create:
        role_name = role_config["role_name"]
        service = role_config["service"]

        print(f"\n📌 Creating: {role_name}")

        # Trust policy: allow the AWS service to assume this role
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": service},
                "Action": "sts:AssumeRole"
            }]
        }

        try:
            iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=role_config["description"],
                Tags=[
                    {"Key": "CrossAccountAccess", "Value": "true"},
                    {"Key": "TargetAccount", "Value": config["account_a_id"]},
                    {"Key": "Team", "Value": "Analytics"}
                ]
            )
            print(f"  ✅ Role created: {role_name}")
        except iam_client.exceptions.EntityAlreadyExistsException:
            print(f"  ℹ️  Role already exists: {role_name}")

        # --- CRITICAL: AssumeRole permission on Account A's cross-account role ---
        cross_account_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAssumeRoleInAccountA",
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Resource": config["cross_account_role_arn"]
                    # LEAST PRIVILEGE: only this specific role ARN
                    # NOT: "arn:aws:iam::111111111111:role/*" (too broad!)
                }
            ]
        }

        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="CrossAccountAssumeRole",
            PolicyDocument=json.dumps(cross_account_policy)
        )
        print(f"  ✅ Cross-account AssumeRole policy attached")
        print(f"     Target: {config['cross_account_role_arn']}")

        # --- Additional service-specific policies ---
        for policy_name, policy_doc in role_config["additional_policies"].items():
            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_doc)
            )
            print(f"  ✅ Service policy attached: {policy_name}")

        # Get role ARN
        role = iam_client.get_role(RoleName=role_name)
        print(f"  ARN: {role['Role']['Arn']}")

    # --- ALSO: Bucket policy pattern needs identity policy ---
    # For Pattern 1 (bucket policy), Account B's roles also need
    # direct S3 permissions on Account A's bucket
    print(f"\n{'─' * 50}")
    print(f"📌 Adding Pattern 1 (bucket policy) identity permissions...")

    bucket_policy_identity = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DirectS3AccessAccountABucket",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{config['bucket_name']}",
                    f"arn:aws:s3:::{config['bucket_name']}/daily_exports/*",
                    f"arn:aws:s3:::{config['bucket_name']}/cleaned/*",
                    f"arn:aws:s3:::{config['bucket_name']}/gold/*"
                ]
            }
        ]
    }

    for role_config in roles_to_create:
        iam_client.put_role_policy(
            RoleName=role_config["role_name"],
            PolicyName="DirectS3CrossAccountRead",
            PolicyDocument=json.dumps(bucket_policy_identity)
        )
        print(f"  ✅ Direct S3 policy attached to: {role_config['role_name']}")

    # --- SUMMARY ---
    print(f"\n{'=' * 70}")
    print(f"📊 ACCOUNT B IAM SETUP COMPLETE")
    print(f"{'=' * 70}")
    print(f"")
    print(f"  PATTERN 1 (Bucket Policy) — Each role can:")
    print(f"  → Directly access Account A's S3 via bucket policy grant")
    print(f"  → Requires: bucket policy in A + identity policy in B")
    print(f"")
    print(f"  PATTERN 2 (AssumeRole) — Each role can:")
    print(f"  → Call sts:AssumeRole on Account A's cross-account role")
    print(f"  → Get temporary credentials")
    print(f"  → Use those credentials for S3 access")
    print(f"  → Requires: trust policy in A + sts:AssumeRole in B")
    print(f"")
    print(f"  BOTH PATTERNS AVAILABLE — choose per use case:")
    print(f"  → Pattern 1: simpler for Athena/Glue native S3 access")
    print(f"  → Pattern 2: more secure for custom code, Lambda, scripts")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    create_account_b_roles()