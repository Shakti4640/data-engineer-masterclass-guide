# file: 01_create_crawler_role.py
# Creates IAM Role that allows Glue Crawler to:
# - Read S3 files (to discover schema)
# - Write to Glue Catalog (to create tables)
# - Write to CloudWatch Logs (for logging)

import boto3
import json
import time

IAM_ROLE_NAME = "GlueCrawlerRole"
BUCKET_NAME = "quickcart-raw-data-prod"


def create_crawler_role():
    iam_client = boto3.client("iam")

    # --- TRUST POLICY ---
    # Allows Glue service to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # --- PERMISSION POLICY ---
    crawler_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ReadAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/*"
                ]
            },
            {
                "Sid": "GlueCatalogAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:CreateDatabase",
                    "glue:GetTable",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetPartition",
                    "glue:CreatePartition",
                    "glue:BatchCreatePartition",
                    "glue:UpdatePartition",
                    "glue:BatchGetPartition"
                ],
                "Resource": [
                    "arn:aws:glue:*:*:catalog",
                    "arn:aws:glue:*:*:database/quickcart_datalake",
                    "arn:aws:glue:*:*:table/quickcart_datalake/*"
                ]
            },
            {
                "Sid": "CloudWatchLogging",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:/aws-glue/*"
            }
        ]
    }

    try:
        # Create role
        role_response = iam_client.create_role(
            RoleName=IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Glue Crawlers to read S3 and write to Glue Catalog",
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Service", "Value": "Glue-Crawler"}
            ]
        )
        role_arn = role_response["Role"]["Arn"]
        print(f"✅ Role created: {role_arn}")

        # Attach inline policy
        iam_client.put_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyName="CrawlerPermissions",
            PolicyDocument=json.dumps(crawler_policy)
        )
        print(f"✅ Crawler permissions attached")

        # Also attach AWS managed policy for Glue service
        iam_client.attach_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        )
        print(f"✅ AWSGlueServiceRole managed policy attached")

        # IAM role propagation takes a few seconds
        print(f"⏳ Waiting 10 seconds for IAM role propagation...")
        time.sleep(10)

        return role_arn

    except iam_client.exceptions.EntityAlreadyExistsException:
        role = iam_client.get_role(RoleName=IAM_ROLE_NAME)
        role_arn = role["Role"]["Arn"]
        print(f"ℹ️  Role already exists: {role_arn}")
        return role_arn


if __name__ == "__main__":
    arn = create_crawler_role()
    print(f"\n🔑 Role ARN (use in crawler config): {arn}")