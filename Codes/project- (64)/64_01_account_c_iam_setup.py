# file: 64_01_account_c_iam_setup.py
# Run from: Account C (333333333333) — Marketing
# Purpose: Create IAM role that Account B's Lambda can assume
# Builds on: Project 56 (cross-account IAM)

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID = [REDACTED:BANK_ACCOUNT_NUMBER]2"  # Analytics account (connector runs here)
ACCOUNT_C_ID = "333333333333"  # Marketing account (data lives here)


def create_cross_account_read_role():
    """
    Create role in Account C that Account B's Lambda can assume
    
    TRUST: Account B's specific Lambda execution role
    PERMISSIONS: 
    → DynamoDB: Read-only on campaign_events table
    → S3: Read-only on marketing-logs bucket
    → Glue: Read-only on marketing_data catalog
    
    PRINCIPLE OF LEAST PRIVILEGE:
    → Not all of Account B — just the specific Lambda role
    → Not all DynamoDB — just the specific table
    → Not all S3 — just the specific bucket and prefix
    """
    iam = boto3.client("iam")

    role_name = "athena-federation-cross-account-read"

    # Trust policy: only Account B's specific Lambda role can assume
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{ACCOUNT_B_ID}:role/AthenaConnectorLambdaRole"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": "athena-federation-2025"
                    }
                }
            }
        ]
    }

    # Permissions policy: read-only access to marketing data
    permissions_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DynamoDBReadCampaignEvents",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:DescribeTable"
                ],
                "Resource": [
                    f"arn:aws:dynamodb:{REGION}:{ACCOUNT_C_ID}:table/campaign_events",
                    f"arn:aws:dynamodb:{REGION}:{ACCOUNT_C_ID}:table/campaign_events/index/*"
                ]
            },
            {
                "Sid": "S3ReadMarketingLogs",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::quickcart-marketing-logs",
                    "arn:aws:s3:::quickcart-marketing-logs/*"
                ]
            },
            {
                "Sid": "GlueCatalogRead",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions"
                ],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{ACCOUNT_C_ID}:catalog",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_C_ID}:database/marketing_data",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_C_ID}:table/marketing_data/*"
                ]
            }
        ]
    }

    # Create role
    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Account B Athena connectors to read marketing data",
            MaxSessionDuration=3600,  # 1 hour max session
            Tags=[
                {"Key": "Purpose", "Value": "AthenaFederation"},
                {"Key": "TrustedAccount", "Value": ACCOUNT_B_ID},
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print(f"✅ Role created: {role_name}")
    except iam.exceptions.EntityAlreadyExistsException:
        iam.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=json.dumps(trust_policy)
        )
        print(f"ℹ️  Role exists — trust policy updated")

    # Attach permissions
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="MarketingDataReadOnly",
        PolicyDocument=json.dumps(permissions_policy)
    )
    print(f"✅ Permissions policy attached")

    role_arn = f"arn:aws:iam::{ACCOUNT_C_ID}:role/{role_name}"
    print(f"\n📋 Role ARN (share with Account B):")
    print(f"   {role_arn}")

    return role_arn


if __name__ == "__main__":
    create_cross_account_read_role()