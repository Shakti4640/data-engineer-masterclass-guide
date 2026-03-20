# file: 01_create_glue_etl_role.py
# Creates IAM Role for Glue ETL jobs
# Needs: S3 read (source) + S3 write (target) + Glue Catalog + CloudWatch

import boto3
import json
import time

IAM_ROLE_NAME = "GlueETLJobRole"
BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def create_glue_etl_role():
    iam_client = boto3.client("iam")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    etl_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ReadSource",
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:ListBucket"],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/daily_exports/*"
                ]
            },
            {
                "Sid": "S3WriteTarget",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}/silver/*"
                ]
            },
            {
                "Sid": "S3ScriptAccess",
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}/glue-scripts/*"
                ]
            },
            {
                "Sid": "GlueCatalogAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
                    "glue:CreateTable", "glue:UpdateTable",
                    "glue:GetPartition", "glue:GetPartitions",
                    "glue:CreatePartition", "glue:BatchCreatePartition",
                    "glue:UpdatePartition"
                ],
                "Resource": [
                    "arn:aws:glue:*:*:catalog",
                    "arn:aws:glue:*:*:database/quickcart_datalake",
                    "arn:aws:glue:*:*:table/quickcart_datalake/*"
                ]
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup", "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:/aws-glue/*"
            },
            {
                "Sid": "CloudWatchMetrics",
                "Effect": "Allow",
                "Action": ["cloudwatch:PutMetricData"],
                "Resource": "*",
                "Condition": {
                    "StringEquals": {"cloudwatch:namespace": "Glue"}
                }
            }
        ]
    }

    try:
        role_response = iam_client.create_role(
            RoleName=IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Glue ETL jobs to read CSV from S3 and write Parquet"
        )
        role_arn = role_response["Role"]["Arn"]
        print(f"✅ Role created: {role_arn}")

        iam_client.put_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyName="GlueETLPermissions",
            PolicyDocument=json.dumps(etl_policy)
        )

        iam_client.attach_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        )
        print(f"✅ Policies attached")

        print(f"⏳ Waiting for IAM propagation...")
        time.sleep(10)
        return role_arn

    except iam_client.exceptions.EntityAlreadyExistsException:
        role = iam_client.get_role(RoleName=IAM_ROLE_NAME)
        print(f"ℹ️  Role exists: {role['Role']['Arn']}")
        return role["Role"]["Arn"]


if __name__ == "__main__":
    create_glue_etl_role()