# file: 44_03_create_zone_iam_policies.py
# Purpose: Create IAM policies that restrict access per data lake zone
# Builds on Project 3 (S3 bucket policies) and Project 7 (IAM deep dive)

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
DATALAKE_BUCKET = "quickcart-datalake-prod"
BUCKET_ARN = f"arn:aws:s3:::{DATALAKE_BUCKET}"


def create_zone_policies():
    """
    Create IAM policies for each team role.
    
    PRINCIPLE: Least privilege per zone.
    → BI Analyst: Read Gold only
    → ML Engineer: Read Silver + Gold
    → Analytics Engineer: Read Silver, Write Gold
    → Data Engineer: Read/Write all zones
    → Glue ETL Role: Read Bronze, Write Silver/Gold
    """
    iam_client = boto3.client("iam")

    policies = {
        # ━━━ BI ANALYST: Gold Read-Only ━━━
        "quickcart-datalake-bi-analyst": {
            "description": "BI Analyst — read Gold zone only",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "ListBucket",
                        "Effect": "Allow",
                        "Action": "s3:ListBucket",
                        "Resource": BUCKET_ARN,
                        "Condition": {
                            "StringLike": {"s3:prefix": ["gold/*"]}
                        }
                    },
                    {
                        "Sid": "ReadGold",
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                        "Resource": f"{BUCKET_ARN}/gold/*"
                    },
                    {
                        "Sid": "AthenaGoldDatabase",
                        "Effect": "Allow",
                        "Action": [
                            "glue:GetDatabase", "glue:GetTable",
                            "glue:GetTables", "glue:GetPartitions",
                            "glue:GetPartition"
                        ],
                        "Resource": [
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/quickcart_gold",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/quickcart_gold/*"
                        ]
                    },
                    {
                        "Sid": "AthenaQueryExecution",
                        "Effect": "Allow",
                        "Action": ["athena:StartQueryExecution", "athena:GetQueryExecution",
                                   "athena:GetQueryResults", "athena:StopQueryExecution"],
                        "Resource": f"arn:aws:athena:{REGION}:{ACCOUNT_ID}:workgroup/bi-analysts"
                    },
                    {
                        "Sid": "AthenaResultsWrite",
                        "Effect": "Allow",
                        "Action": "s3:PutObject",
                        "Resource": f"{BUCKET_ARN}/_system/athena-results/*"
                    }
                ]
            }
        },

        # ━━━ ML ENGINEER: Silver + Gold Read ━━━
        "quickcart-datalake-ml-engineer": {
            "description": "ML Engineer — read Silver + Gold zones",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "ListBucket",
                        "Effect": "Allow",
                        "Action": "s3:ListBucket",
                        "Resource": BUCKET_ARN,
                        "Condition": {
                            "StringLike": {"s3:prefix": ["silver/*", "gold/*"]}
                        }
                    },
                    {
                        "Sid": "ReadSilverGold",
                        "Effect": "Allow",
                        "Action": ["s3:GetObject"],
                        "Resource": [
                            f"{BUCKET_ARN}/silver/*",
                            f"{BUCKET_ARN}/gold/*"
                        ]
                    },
                    {
                        "Sid": "GlueCatalogRead",
                        "Effect": "Allow",
                        "Action": [
                            "glue:GetDatabase", "glue:GetTable",
                            "glue:GetTables", "glue:GetPartitions",
                            "glue:GetPartition"
                        ],
                        "Resource": [
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/quickcart_silver",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/quickcart_gold",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/quickcart_silver/*",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/quickcart_gold/*"
                        ]
                    }
                ]
            }
        },

        # ━━━ GLUE ETL ROLE: Read Bronze, Write Silver/Gold ━━━
        "quickcart-datalake-glue-etl": {
            "description": "Glue ETL jobs — read Bronze, write Silver/Gold, manage system",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "ReadBronze",
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:ListBucket"],
                        "Resource": [
                            BUCKET_ARN,
                            f"{BUCKET_ARN}/bronze/*"
                        ]
                    },
                    {
                        "Sid": "WriteSilverGold",
                        "Effect": "Allow",
                        "Action": [
                            "s3:PutObject", "s3:DeleteObject",
                            "s3:GetObject", "s3:ListBucket"
                        ],
                        "Resource": [
                            f"{BUCKET_ARN}/silver/*",
                            f"{BUCKET_ARN}/gold/*"
                        ]
                    },
                    {
                        "Sid": "SystemAccess",
                        "Effect": "Allow",
                        "Action": [
                            "s3:PutObject", "s3:GetObject",
                            "s3:DeleteObject", "s3:ListBucket"
                        ],
                        "Resource": [
                            f"{BUCKET_ARN}/_system/*"
                        ]
                    },
                    {
                        "Sid": "GlueCatalogFullAccess",
                        "Effect": "Allow",
                        "Action": ["glue:*"],
                        "Resource": [
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/quickcart_*",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/quickcart_*/*",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:crawler/quickcart-*",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:job/quickcart-*"
                        ]
                    }
                ]
            }
        },

        # ━━━ DATA ENGINEER: Full Access All Zones ━━━
        "quickcart-datalake-data-engineer": {
            "description": "Data Engineer — full read/write across all zones",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "FullS3Access",
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                            "s3:ListBucket", "s3:GetBucketLocation",
                            "s3:GetObjectVersion", "s3:ListBucketVersions"
                        ],
                        "Resource": [
                            BUCKET_ARN,
                            f"{BUCKET_ARN}/*"
                        ]
                    },
                    {
                        "Sid": "FullGlueCatalog",
                        "Effect": "Allow",
                        "Action": ["glue:*"],
                        "Resource": [
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/quickcart_*",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/quickcart_*/*",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:crawler/quickcart-*",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:job/quickcart-*"
                        ]
                    },
                    {
                        "Sid": "AthenaFullAccess",
                        "Effect": "Allow",
                        "Action": ["athena:*"],
                        "Resource": f"arn:aws:athena:{REGION}:{ACCOUNT_ID}:workgroup/*"
                    }
                ]
            }
        },

        # ━━━ ANALYTICS ENGINEER: Read Silver, Write Gold ━━━
        "quickcart-datalake-analytics-engineer": {
            "description": "Analytics Engineer — read Silver, write Gold (dbt models)",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "ListBucket",
                        "Effect": "Allow",
                        "Action": "s3:ListBucket",
                        "Resource": BUCKET_ARN,
                        "Condition": {
                            "StringLike": {
                                "s3:prefix": ["silver/*", "gold/*", "_system/athena-results/*"]
                            }
                        }
                    },
                    {
                        "Sid": "ReadSilver",
                        "Effect": "Allow",
                        "Action": ["s3:GetObject"],
                        "Resource": f"{BUCKET_ARN}/silver/*"
                    },
                    {
                        "Sid": "WriteGold",
                        "Effect": "Allow",
                        "Action": ["s3:PutObject", "s3:DeleteObject", "s3:GetObject"],
                        "Resource": f"{BUCKET_ARN}/gold/*"
                    },
                    {
                        "Sid": "AthenaResults",
                        "Effect": "Allow",
                        "Action": ["s3:PutObject", "s3:GetObject"],
                        "Resource": f"{BUCKET_ARN}/_system/athena-results/*"
                    },
                    {
                        "Sid": "GlueCatalogSilverRead",
                        "Effect": "Allow",
                        "Action": [
                            "glue:GetDatabase", "glue:GetTable",
                            "glue:GetTables", "glue:GetPartitions",
                            "glue:GetPartition"
                        ],
                        "Resource": [
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/quickcart_silver",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/quickcart_silver/*"
                        ]
                    },
                    {
                        "Sid": "GlueCatalogGoldReadWrite",
                        "Effect": "Allow",
                        "Action": [
                            "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
                            "glue:GetPartitions", "glue:GetPartition",
                            "glue:CreateTable", "glue:UpdateTable",
                            "glue:CreatePartition", "glue:BatchCreatePartition",
                            "glue:UpdatePartition"
                        ],
                        "Resource": [
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/quickcart_gold",
                            f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/quickcart_gold/*"
                        ]
                    }
                ]
            }
        }
    }

    # Create all policies
    for policy_name, policy_config in policies.items():
        try:
            response = iam_client.create_policy(
                PolicyName=policy_name,
                Description=policy_config["description"],
                PolicyDocument=json.dumps(policy_config["policy"]),
                Tags=[
                    {"Key": "Environment", "Value": "Production"},
                    {"Key": "Project", "Value": "QuickCart-DataLake"}
                ]
            )
            print(f"✅ Policy created: {policy_name}")
            print(f"   ARN: {response['Policy']['Arn']}")
        except iam_client.exceptions.EntityAlreadyExistsException:
            print(f"ℹ️  Already exists: {policy_name}")

    # Print access matrix
    print(f"\n📋 ACCESS MATRIX:")
    print(f"{'─'*75}")
    print(f"{'Role':<35} {'Bronze':<10} {'Silver':<10} {'Gold':<10} {'_system':<10}")
    print(f"{'─'*75}")
    print(f"{'Data Engineer':<35} {'R/W':<10} {'R/W':<10} {'R/W':<10} {'R/W':<10}")
    print(f"{'Analytics Engineer':<35} {'—':<10} {'R':<10} {'R/W':<10} {'R(athena)':<10}")
    print(f"{'ML Engineer':<35} {'—':<10} {'R':<10} {'R':<10} {'—':<10}")
    print(f"{'BI Analyst':<35} {'—':<10} {'—':<10} {'R':<10} {'R(athena)':<10}")
    print(f"{'Glue ETL Role':<35} {'R':<10} {'R/W':<10} {'R/W':<10} {'R/W':<10}")
    print(f"{'─'*75}")


if __name__ == "__main__":
    create_zone_policies()