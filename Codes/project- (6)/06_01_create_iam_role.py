# file: 06_01_create_iam_role.py
# Purpose: Create IAM Role + Instance Profile for EC2-to-S3 access

import boto3
import json
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
ROLE_NAME = "QuickCart-EC2-S3ReadWrite-Prod"
INSTANCE_PROFILE_NAME = "QuickCart-EC2-S3ReadWrite-Prod-Profile"
POLICY_NAME = "QuickCart-S3-ScopedAccess"
BUCKET_NAME = "quickcart-raw-data-prod"


def create_role_and_profile():
    iam_client = boto3.client("iam")

    # ================================================================
    # STEP 1: Define TRUST POLICY (WHO can assume this role)
    # ================================================================
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    # ONLY EC2 service can assume this role
                    # Not any IAM user, not Lambda, not Glue — ONLY EC2
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # ================================================================
    # STEP 2: Define PERMISSION POLICY (WHAT the role can do)
    # ================================================================
    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                # --- BUCKET-LEVEL operations ---
                # ListBucket operates on the BUCKET ARN (no /*)
                "Sid": "AllowListBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket"
                ],
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}"
                # WHY separate: ListBucket = bucket-level permission
                # GetObject/PutObject = object-level permission
                # Different ARN patterns → must be separate statements
            },
            {
                # --- OBJECT-LEVEL operations ---
                # Get/Put operate on OBJECT ARNs (with /*)
                "Sid": "AllowReadWriteObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}/*"
                # /* means ALL objects in the bucket
                # Could restrict further:
                # f"arn:aws:s3:::{BUCKET_NAME}/daily_exports/*"
                # → only allows access to daily_exports prefix
            },
            {
                # --- HEAD operations (needed by verify script) ---
                "Sid": "AllowHeadObject",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObjectAttributes"
                ],
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}/*"
            }
        ]
    }
    # NOTE: We deliberately DO NOT include:
    # - s3:DeleteObject (upload script doesn't need delete)
    # - s3:PutBucketPolicy (infrastructure, not application)
    # - s3:* (never use wildcards in production)

    # ================================================================
    # STEP 3: Create the IAM Role
    # ================================================================
    try:
        create_role_response = iam_client.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows EC2 instances to read/write QuickCart S3 bucket",
            MaxSessionDuration=3600,  # 1 hour max per STS session
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Team", "Value": "Data-Engineering"},
                {"Key": "ManagedBy", "Value": "boto3-script"}
            ]
        )
        role_arn = create_role_response["Role"]["Arn"]
        print(f"✅ IAM Role created: {ROLE_NAME}")
        print(f"   ARN: {role_arn}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"ℹ️  Role '{ROLE_NAME}' already exists — skipping creation")
            role_arn = iam_client.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
        else:
            raise

    # ================================================================
    # STEP 4: Attach INLINE permission policy to the role
    # ================================================================
    # WHY INLINE vs MANAGED?
    # → Inline: embedded in the role, deleted when role is deleted
    #   → Good for: role-specific, tightly coupled permissions
    # → Managed: standalone policy, can attach to multiple roles
    #   → Good for: shared policies (e.g., "ReadOnlyS3" used by 10 roles)
    # → Here: inline is appropriate (specific to this one role)

    iam_client.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName=POLICY_NAME,
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"✅ Permission policy '{POLICY_NAME}' attached to role")

    # ================================================================
    # STEP 5: Create Instance Profile
    # ================================================================
    try:
        iam_client.create_instance_profile(
            InstanceProfileName=INSTANCE_PROFILE_NAME,
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Team", "Value": "Data-Engineering"}
            ]
        )
        print(f"✅ Instance Profile created: {INSTANCE_PROFILE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"ℹ️  Instance Profile already exists — skipping")
        else:
            raise

    # ================================================================
    # STEP 6: Add Role to Instance Profile
    # ================================================================
    try:
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=INSTANCE_PROFILE_NAME,
            RoleName=ROLE_NAME
        )
        print(f"✅ Role added to Instance Profile")
    except ClientError as e:
        if e.response["Error"]["Code"] == "LimitExceeded":
            print(f"ℹ️  Role already in Instance Profile — skipping")
        else:
            raise

    print(f"\n🎯 SUMMARY:")
    print(f"   Role ARN:              {role_arn}")
    print(f"   Instance Profile Name: {INSTANCE_PROFILE_NAME}")
    print(f"   Bucket Access:         s3://{BUCKET_NAME}/*")
    print(f"   Permissions:           List, Get, Put (NO Delete)")
    print(f"\n   Next step: Attach this Instance Profile to your EC2 instance")

    return role_arn


if __name__ == "__main__":
    create_role_and_profile()