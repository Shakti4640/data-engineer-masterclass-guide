# file: lineage/01_setup_lineage_store.py
# Purpose: Create DynamoDB table + S3 bucket for lineage storage

import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"
LINEAGE_TABLE = "DataLineage"
LINEAGE_BUCKET = "quickcart-lineage-archive-prod"


def create_lineage_dynamodb_table():
    """
    DynamoDB single-table design for lineage graph
    
    Adjacency list pattern:
    → PK: NODE#{node_id} or JOB_RUN#{run_id}
    → SK: META | UPSTREAM#{source_id} | DOWNSTREAM#{target_id} | EDGE#{edge_id}
    
    GSI1 — query by account + service:
    → PK: account_id
    → SK: service#database#table#column
    
    GSI2 — query by job name (find all lineage from a specific job):
    → PK: job_name
    → SK: created_at
    """
    dynamodb = boto3.client("dynamodb", region_name=REGION)

    try:
        dynamodb.create_table(
            TableName=LINEAGE_TABLE,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "account_id", "AttributeType": "S"},
                {"AttributeName": "asset_path", "AttributeType": "S"},
                {"AttributeName": "job_name", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"}
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1-AccountAsset",
                    "KeySchema": [
                        {"AttributeName": "account_id", "KeyType": "HASH"},
                        {"AttributeName": "asset_path", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                },
                {
                    "IndexName": "GSI2-JobLineage",
                    "KeySchema": [
                        {"AttributeName": "job_name", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "Purpose", "Value": "DataLineage"},
                {"Key": "Team", "Value": "DataPlatform"}
            ]
        )
        print(f"✅ DynamoDB table '{LINEAGE_TABLE}' created")

        # Wait for table to become active
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=LINEAGE_TABLE)
        print(f"✅ Table is ACTIVE")

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"ℹ️  Table '{LINEAGE_TABLE}' already exists")
        else:
            raise


def create_lineage_archive_bucket():
    """
    S3 bucket for historical lineage events
    → Partitioned by date for Athena queryability
    → Lifecycle: IA after 90 days, Glacier after 365 days (Project 4 pattern)
    """
    s3 = boto3.client("s3", region_name=REGION)

    try:
        s3.create_bucket(
            Bucket=LINEAGE_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        print(f"✅ S3 bucket '{LINEAGE_BUCKET}' created")

        # Lifecycle — per Project 4 pattern
        s3.put_bucket_lifecycle_configuration(
            Bucket=LINEAGE_BUCKET,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "ArchiveOldLineage",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "events/"},
                        "Transitions": [
                            {"Days": 90, "StorageClass": "STANDARD_IA"},
                            {"Days": 365, "StorageClass": "GLACIER"}
                        ],
                        "Expiration": {"Days": 2555}  # 7 years for SOX
                    }
                ]
            }
        )
        print(f"✅ Lifecycle policy applied (IA→90d, Glacier→365d, Expire→7yr)")

    except ClientError as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            print(f"ℹ️  Bucket '{LINEAGE_BUCKET}' already exists")
        else:
            raise


if __name__ == "__main__":
    create_lineage_dynamodb_table()
    create_lineage_archive_bucket()
    print("\n🎯 Lineage storage infrastructure ready")