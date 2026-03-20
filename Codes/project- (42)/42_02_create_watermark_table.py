# file: 42_02_create_watermark_table.py
# Purpose: Create DynamoDB table to store CDC watermarks

import boto3
from datetime import datetime

REGION = "us-east-2"
TABLE_NAME = "quickcart_cdc_watermarks"


def create_watermark_table():
    """
    Create DynamoDB table for CDC watermark tracking.
    
    Schema:
    → Partition Key: entity_name (String)
    → No Sort Key (one watermark per entity)
    
    WHY DynamoDB:
    → Strongly consistent reads (no stale watermark)
    → Atomic writes (no race conditions)
    → Serverless (no EC2, no maintenance)
    → Cost: $0.00 for this volume
    """
    dynamodb = boto3.client("dynamodb", region_name=REGION)

    try:
        dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "entity_name", "KeyType": "HASH"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "entity_name", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Project", "Value": "QuickCart-CDC"}
            ]
        )
        print(f"✅ DynamoDB table created: {TABLE_NAME}")

        # Wait for table to become active
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=TABLE_NAME)
        print(f"   ✅ Table is ACTIVE")

    except dynamodb.exceptions.ResourceInUseException:
        print(f"ℹ️  Table already exists: {TABLE_NAME}")

    return TABLE_NAME


def initialize_watermarks():
    """
    Set initial watermarks for each entity.
    
    First run strategy:
    → Set watermark to epoch (1970-01-01) for initial full load
    → OR set to current timestamp to skip history and start fresh
    
    QuickCart decision: set to 1 hour ago
    → Captures recent changes for first run
    → Avoids full 5.2M row initial load
    → (Assumes historical data was already loaded via Project 21 COPY)
    """
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    entities = ["orders", "customers", "products"]

    # Initial watermark: 1 hour ago (assumes historical data already in Redshift)
    # Change to "1970-01-01T00:00:00" for full initial load
    initial_watermark = "1970-01-01T00:00:00"

    for entity in entities:
        # Conditional put: only if item doesn't exist (don't overwrite existing)
        try:
            table.put_item(
                Item={
                    "entity_name": entity,
                    "last_watermark": initial_watermark,
                    "last_run_at": "never",
                    "rows_extracted": 0,
                    "status": "INITIALIZED",
                    "initialized_at": datetime.utcnow().isoformat()
                },
                ConditionExpression="attribute_not_exists(entity_name)"
            )
            print(f"✅ Initialized watermark: {entity} → {initial_watermark}")
        except Exception as e:
            if "ConditionalCheckFailedException" in str(e):
                # Read existing
                response = table.get_item(Key={"entity_name": entity})
                existing = response.get("Item", {})
                print(f"ℹ️  {entity}: watermark already set → {existing.get('last_watermark', 'N/A')}")
            else:
                raise


def read_watermark(entity_name):
    """Read current watermark for an entity — used by extraction script"""
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    response = table.get_item(
        Key={"entity_name": entity_name},
        ConsistentRead=True
    )

    if "Item" not in response:
        raise ValueError(f"No watermark found for entity: {entity_name}")

    item = response["Item"]
    print(f"📋 Watermark for '{entity_name}':")
    print(f"   last_watermark: {item['last_watermark']}")
    print(f"   last_run_at: {item['last_run_at']}")
    print(f"   rows_extracted: {item['rows_extracted']}")
    print(f"   status: {item['status']}")

    return item["last_watermark"]


if __name__ == "__main__":
    create_watermark_table()
    initialize_watermarks()

    print(f"\n--- Current Watermarks ---")
    for entity in ["orders", "customers", "products"]:
        read_watermark(entity)
        print()