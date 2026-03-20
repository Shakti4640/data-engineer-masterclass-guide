# file: 05_workgroup_iam_policy.py
# Creates IAM policies that restrict analysts to their team's workgroup
# Prevents finance team from running queries in engineering workgroup

import boto3
import json

IAM_POLICY_PREFIX = "AthenaWorkgroupAccess"
REGION = "us-east-2"
ACCOUNT_ID[REDACTED:BANK_ACCOUNT_NUMBER]9012"
DATABASE_NAME = "quickcart_datalake"
BUCKET_NAME = "quickcart-raw-data-prod"


def create_workgroup_policy(team_name, workgroup_name):
    """
    Create IAM policy restricting user to specific Athena workgroup
    
    POLICY GRANTS:
    → Athena: query execution ONLY in specified workgroup
    → Glue: read-only access to catalog (see tables, columns)
    → S3: read data files + write query results to team prefix
    
    POLICY DENIES:
    → Running queries in any other workgroup
    → Writing to S3 prefixes outside team's result location
    → Modifying Glue catalog (no CREATE/DROP tables)
    """
    iam_client = boto3.client("iam")

    policy_name = f"{IAM_POLICY_PREFIX}-{team_name}"

    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AthenaQueryInWorkgroupOnly",
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution",
                    "athena:StopQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:ListQueryExecutions",
                    "athena:BatchGetQueryExecution",
                    "athena:GetWorkGroup",
                    "athena:ListNamedQueries",
                    "athena:GetNamedQuery",
                    "athena:BatchGetNamedQuery"
                ],
                "Resource": [
                    f"arn:aws:athena:{REGION}:{ACCOUNT_ID}:workgroup/{workgroup_name}"
                ]
            },
            {
                "Sid": "GlueCatalogReadOnly",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition"
                ],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/{DATABASE_NAME}",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/{DATABASE_NAME}/*"
                ]
            },
            {
                "Sid": "S3ReadDataFiles",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/daily_exports/*",
                    f"arn:aws:s3:::{BUCKET_NAME}/exports/*"
                ]
            },
            {
                "Sid": "S3WriteQueryResults",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:AbortMultipartUpload"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}/athena-results/{team_name}/*"
                ]
            },
            {
                "Sid": "DenyOtherWorkgroups",
                "Effect": "Deny",
                "Action": [
                    "athena:StartQueryExecution"
                ],
                "NotResource": [
                    f"arn:aws:athena:{REGION}:{ACCOUNT_ID}:workgroup/{workgroup_name}"
                ]
            }
        ]
    }

    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description=f"Restricts Athena access to {workgroup_name} workgroup",
            Tags=[
                {"Key": "Team", "Value": team_name},
                {"Key": "Service", "Value": "Athena"}
            ]
        )
        policy_arn = response["Policy"]["Arn"]
        print(f"✅ Policy created: {policy_name}")
        print(f"   ARN: {policy_arn}")
        print(f"   Workgroup: {workgroup_name}")
        return policy_arn

    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  Policy already exists: {policy_name}")
        return f"arn:aws:iam::{ACCOUNT_ID}:policy/{policy_name}"


def create_all_policies():
    """Create IAM policies for all teams"""
    print("=" * 60)
    print("🔐 CREATING WORKGROUP IAM POLICIES")
    print("=" * 60)

    teams = [
        {"name": "finance", "workgroup": "finance_team"},
        {"name": "marketing", "workgroup": "marketing_team"},
        {"name": "product", "workgroup": "product_team"},
        {"name": "engineering", "workgroup": "data_engineering"}
    ]

    for team in teams:
        create_workgroup_policy(team["name"], team["workgroup"])
        print()

    print("=" * 60)
    print("✅ All policies created")
    print("\n💡 Attach policies to IAM users/groups:")
    print("   aws iam attach-user-policy \\")
    print("     --user-name analyst_john \\")
    print(f"     --policy-arn arn:aws:iam::{ACCOUNT_ID}:policy/{IAM_POLICY_PREFIX}-finance")


if __name__ == "__main__":
    create_all_policies()