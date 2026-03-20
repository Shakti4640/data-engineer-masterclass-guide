# file: 45_01_schema_registry.py
# Purpose: Create and manage the schema registry for all entities
# Stores baseline schemas, versions, and alias mappings

import boto3
import json
from datetime import datetime
from decimal import Decimal

REGION = "us-east-2"
REGISTRY_TABLE = "quickcart_schema_registry"


def create_registry_table():
    """Create DynamoDB table for schema registry"""
    dynamodb = boto3.client("dynamodb", region_name=REGION)

    try:
        dynamodb.create_table(
            TableName=REGISTRY_TABLE,
            KeySchema=[
                {"AttributeName": "entity_name", "KeyType": "HASH"},
                {"AttributeName": "version", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "entity_name", "AttributeType": "S"},
                {"AttributeName": "version", "AttributeType": "N"}
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "Project", "Value": "QuickCart-SchemaEvolution"}
            ]
        )
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=REGISTRY_TABLE)
        print(f"✅ Schema registry table created: {REGISTRY_TABLE}")
    except dynamodb.exceptions.ResourceInUseException:
        print(f"ℹ️  Registry table already exists: {REGISTRY_TABLE}")


def register_schema(entity_name, columns, alias_map=None, is_active=True):
    """
    Register a new schema version for an entity.
    
    columns: list of dicts, each with:
    → name: column name
    → type: data type string (e.g., "string", "decimal(10,2)")
    → nullable: True/False
    → required_for_silver: True/False (if True, missing = pipeline FAIL)
    → pii: True/False (if True, must be masked in Silver)
    
    alias_map: dict mapping old column names to new ones
    → {"status": "order_status"} means "status" was renamed to "order_status"
    """
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(REGISTRY_TABLE)

    # Get next version number
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("entity_name").eq(entity_name),
        ScanIndexForward=False,
        Limit=1
    )

    if response["Items"]:
        next_version = int(response["Items"][0]["version"]) + 1
        # Deactivate previous version
        prev_version = int(response["Items"][0]["version"])
        table.update_item(
            Key={"entity_name": entity_name, "version": prev_version},
            UpdateExpression="SET is_active = :f",
            ExpressionAttributeValues={":f": False}
        )
    else:
        next_version = 1

    # Store new version
    item = {
        "entity_name": entity_name,
        "version": next_version,
        "columns": columns,
        "alias_map": alias_map or {},
        "is_active": is_active,
        "created_at": datetime.utcnow().isoformat(),
        "created_by": "schema_registry_setup"
    }

    table.put_item(Item=json.loads(json.dumps(item), parse_float=Decimal))

    print(f"✅ Registered: {entity_name} v{next_version} ({len(columns)} columns)")
    return next_version


def get_active_schema(entity_name):
    """Get the currently active schema for an entity"""
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(REGISTRY_TABLE)

    response = table.query(
        KeyConditionExpression=(
            boto3.dynamodb.conditions.Key("entity_name").eq(entity_name)
        ),
        FilterExpression=boto3.dynamodb.conditions.Attr("is_active").eq(True),
        ScanIndexForward=False,
        Limit=1
    )

    if not response["Items"]:
        raise ValueError(f"No active schema found for: {entity_name}")

    item = response["Items"][0]
    return {
        "entity_name": item["entity_name"],
        "version": int(item["version"]),
        "columns": item["columns"],
        "alias_map": item.get("alias_map", {}),
        "created_at": item["created_at"]
    }


def initialize_all_schemas():
    """Register baseline schemas for all QuickCart entities"""

    # ━━━ ORDERS ━━━
    register_schema("orders", columns=[
        {"name": "order_id", "type": "string", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "customer_id", "type": "string", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "product_id", "type": "string", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "quantity", "type": "integer", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "unit_price", "type": "decimal(10,2)", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "total_amount", "type": "decimal(12,2)", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "status", "type": "string", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "order_date", "type": "date", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "created_at", "type": "timestamp", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "updated_at", "type": "timestamp", "nullable": False, "required_for_silver": True, "pii": False}
    ])

    # ━━━ CUSTOMERS ━━━
    register_schema("customers", columns=[
        {"name": "customer_id", "type": "string", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "name", "type": "string", "nullable": False, "required_for_silver": True, "pii": True},
        {"name": "email", "type": "string", "nullable": True, "required_for_silver": False, "pii": True},
        {"name": "city", "type": "string", "nullable": True, "required_for_silver": False, "pii": False},
        {"name": "state", "type": "string", "nullable": True, "required_for_silver": False, "pii": False},
        {"name": "tier", "type": "string", "nullable": True, "required_for_silver": False, "pii": False},
        {"name": "signup_date", "type": "date", "nullable": True, "required_for_silver": False, "pii": False},
        {"name": "created_at", "type": "timestamp", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "updated_at", "type": "timestamp", "nullable": False, "required_for_silver": True, "pii": False}
    ])

    # ━━━ PRODUCTS ━━━
    register_schema("products", columns=[
        {"name": "product_id", "type": "string", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "name", "type": "string", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "category", "type": "string", "nullable": True, "required_for_silver": False, "pii": False},
        {"name": "price", "type": "decimal(10,2)", "nullable": True, "required_for_silver": False, "pii": False},
        {"name": "stock_quantity", "type": "integer", "nullable": True, "required_for_silver": False, "pii": False},
        {"name": "is_active", "type": "boolean", "nullable": True, "required_for_silver": False, "pii": False},
        {"name": "created_at", "type": "timestamp", "nullable": False, "required_for_silver": True, "pii": False},
        {"name": "updated_at", "type": "timestamp", "nullable": False, "required_for_silver": True, "pii": False}
    ])


def display_all_schemas():
    """Show current active schemas for all entities"""
    for entity in ["orders", "customers", "products"]:
        try:
            schema = get_active_schema(entity)
            print(f"\n📋 {entity} (v{schema['version']}, created: {schema['created_at'][:10]})")
            print(f"   Aliases: {schema['alias_map'] if schema['alias_map'] else 'None'}")
            print(f"   {'Column':<20} {'Type':<18} {'Nullable':<10} {'Required':<10} {'PII'}")
            print(f"   {'─'*70}")
            for col in schema["columns"]:
                print(f"   {col['name']:<20} {col['type']:<18} {str(col['nullable']):<10} "
                      f"{str(col['required_for_silver']):<10} {col.get('pii', False)}")
        except ValueError as e:
            print(f"   ❌ {e}")


if __name__ == "__main__":
    create_registry_table()
    initialize_all_schemas()
    display_all_schemas()