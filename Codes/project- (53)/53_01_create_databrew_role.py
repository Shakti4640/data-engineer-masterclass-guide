# file: 53_01_create_databrew_role.py
# Purpose: Create IAM role that DataBrew uses to access S3

import boto3
import json
import time

REGION = "us-east-2"
ROLE_NAME = "QuickCart-DataBrew-Role"
SOURCE_BUCKET = "quickcart-raw-data-prod"
OUTPUT_BUCKET = "quickcart-raw-data-prod"     # Same bucket, different prefix


def create_databrew_role():
    """
    Create IAM role for DataBrew with:
    → S3 read on source prefix
    → S3 write on output prefix
    → CloudWatch Logs for execution logging
    → Glue Catalog read (if using Catalog as source)
    """
    iam_client = boto3.client("iam")

    # Trust policy: DataBrew service can assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "databrew.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # Permission policy
    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ReadSource",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{SOURCE_BUCKET}",
                    f"arn:aws:s3:::{SOURCE_BUCKET}/daily_exports/*"
                ]
            },
            {
                "Sid": "S3WriteOutput",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{OUTPUT_BUCKET}",
                    f"arn:aws:s3:::{OUTPUT_BUCKET}/cleaned/*",
                    f"arn:aws:s3:::{OUTPUT_BUCKET}/databrew-profiles/*"
                ]
            },
            {
                "Sid": "GlueCatalogRead",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabases",
                    "glue:GetDatabase",
                    "glue:GetTables",
                    "glue:GetTable",
                    "glue:GetPartitions"
                ],
                "Resource": "*"
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:log-group:/aws-glue-databrew/*"
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Glue DataBrew to access S3 and Glue Catalog",
            Tags=[
                {"Key": "Team", "Value": "Analytics"},
                {"Key": "Owner", "Value": "Sarah"}
            ]
        )
        print(f"✅ IAM Role created: {ROLE_NAME}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  IAM Role already exists: {ROLE_NAME}")

    iam_client.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="DataBrewS3Access",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"✅ Permission policy attached")

    # Get role ARN
    role = iam_client.get_role(RoleName=ROLE_NAME)
    role_arn = role["Role"]["Arn"]
    print(f"✅ Role ARN: {role_arn}")

    time.sleep(10)  # IAM propagation
    return role_arn


if __name__ == "__main__":
    create_databrew_role()