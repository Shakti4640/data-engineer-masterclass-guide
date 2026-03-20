# file: 66_02_create_state_tracker.py
# Run from: Account B
# Purpose: DynamoDB table to track which sources completed for each batch

import boto3

REGION = "us-east-2"


def create_state_table():
    """
    DynamoDB table: pipeline_batch_state
    
    PK: batch_date (e.g., "2025-01-15")
    SK: source_name (e.g., "partner_feed")
    
    Attributes:
    → status: pending | processing | completed | failed
    → started_at: when processing began
    → completed_at: when processing finished
    → file_key: S3 key that triggered this source
    → execution_arn: Step Function execution ARN
    → row_count: rows processed
    """
    dynamodb = boto3.client("dynamodb", region_name=REGION)

    try:
        dynamodb.create_table(
            TableName="pipeline_batch_state",
            KeySchema=[
                {"AttributeName": "batch_date", "KeyType": "HASH"},
                {"AttributeName": "source_name", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "batch_date", "AttributeType": "S"},
                {"AttributeName": "source_name", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "Purpose", "Value": "PipelineOrchestration"},
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print("✅ DynamoDB table created: pipeline_batch_state")
    except dynamodb.exceptions.ResourceInUseException:
        print("ℹ️  Table already exists")

    # Enable TTL to auto-delete old records after 90 days
    try:
        dynamodb.update_time_to_live(
            TableName="pipeline_batch_state",
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "ttl"
            }
        )
        print("✅ TTL enabled (90-day auto-cleanup)")
    except Exception:
        print("ℹ️  TTL already configured")


if __name__ == "__main__":
    create_state_table()