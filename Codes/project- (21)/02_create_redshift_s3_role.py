# file: 02_create_redshift_s3_role.py
# Creates IAM Role that allows Redshift to read from S3
# Must be run ONCE, then attach role to Redshift cluster

import boto3
import json

IAM_ROLE_NAME = "RedshiftS3ReadRole"
S3_BUCKET = "quickcart-raw-data-prod"


def create_redshift_s3_role():
    iam_client = boto3.client("iam")

    # --- TRUST POLICY ---
    # Allows Redshift service to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # --- PERMISSION POLICY ---
    # Grants read access to specific S3 bucket
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",       # Read file content
                    "s3:ListBucket"       # List files in bucket
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET}",      # Bucket itself (for LIST)
                    f"arn:aws:s3:::{S3_BUCKET}/*"      # Objects inside (for GET)
                ]
            }
        ]
    }

    try:
        # Create the role
        role_response = iam_client.create_role(
            RoleName=IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Redshift to read from QuickCart S3 bucket",
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Service", "Value": "Redshift"}
            ]
        )
        role_arn = role_response["Role"]["Arn"]
        print(f"✅ Role created: {role_arn}")

        # Attach inline policy
        iam_client.put_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyName="S3ReadAccess",
            PolicyDocument=json.dumps(s3_policy)
        )
        print(f"✅ S3 read policy attached")

        print(f"\n📋 NEXT STEP:")
        print(f"   Attach this role to your Redshift cluster:")
        print(f"   Console → Redshift → Clusters → your-cluster → Properties")
        print(f"   → Manage IAM roles → Add: {IAM_ROLE_NAME}")
        print(f"\n   Or via CLI:")
        print(f"   aws redshift modify-cluster-iam-roles \\")
        print(f"     --cluster-identifier quickcart-warehouse \\")
        print(f"     --add-iam-roles {role_arn}")

        return role_arn

    except iam_client.exceptions.EntityAlreadyExistsException:
        role = iam_client.get_role(RoleName=IAM_ROLE_NAME)
        role_arn = role["Role"]["Arn"]
        print(f"ℹ️  Role already exists: {role_arn}")
        return role_arn


if __name__ == "__main__":
    arn = create_redshift_s3_role()
    print(f"\n🔑 Role ARN (use in COPY command): {arn}")