# file: 71_03_governance_account_setup.py
# Account G (444444444444): Central Governance / Platform Account
# Creates: SNS topic for data product events, DynamoDB registry, subscription management

import boto3
import json
from datetime import datetime

# --- CONFIGURATION ---
GOVERNANCE_ACCOUNT_ID = [REDACTED:BANK_ACCOUNT_NUMBER]4"
REGION = "us-east-2"

# All domain accounts in the mesh
MESH_ACCOUNTS = {
    "orders": {
        "account_id": [REDACTED:BANK_ACCOUNT_NUMBER]1",
        "role": "producer"
    },
    "customers": {
        "account_id": [REDACTED:BANK_ACCOUNT_NUMBER]2",
        "role": "producer"
    },
    "analytics": {
        "account_id": "333333333333",
        "role": "consumer"
    }
}

# Consumer SQS queues that subscribe to data product events
CONSUMER_SQS_ARNS = [
    "arn:aws:sqs:us-east-2:333333333333:mesh-analytics-data-events",
    "arn:aws:sqs:us-east-2:222222222222:mesh-customers-data-events"
]

# Producer accounts allowed to publish events
PRODUCER_ACCOUNT_IDS = ["111111111111", "222222222222"]


def create_data_product_sns_topic(sns_client):
    """
    Central SNS topic for ALL data product lifecycle events
    
    Event types:
    → data_product.published   (new data available)
    → data_product.deprecated  (product being retired)
    → data_product.quality_failed (quality check failed)
    → data_product.schema_changed (breaking change warning)
    
    Cross-account pattern (Project 59):
    → Producers PUBLISH to this topic
    → Consumer SQS queues SUBSCRIBE
    → Topic policy allows both directions
    """
    topic_name = "data-product-events"

    response = sns_client.create_topic(
        Name=topic_name,
        Attributes={
            "DisplayName": "Data Mesh — Product Events"
        },
        Tags=[
            {"Key": "MeshComponent", "Value": "event-bus"},
            {"Key": "ManagedBy", "Value": "platform-team"}
        ]
    )
    topic_arn = response["TopicArn"]
    print(f"✅ SNS topic created: {topic_name}")
    print(f"   ARN: {topic_arn}")

    # Topic Policy: allow producers to publish + consumer SQS to subscribe
    topic_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowProducerPublish",
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        f"arn:aws:iam::{acct_id}:root"
                        for acct_id in PRODUCER_ACCOUNT_IDS
                    ]
                },
                "Action": "sns:Publish",
                "Resource": topic_arn
            },
            {
                "Sid": "AllowConsumerSQSSubscription",
                "Effect": "Allow",
                "Principal": {
                    "Service": "sqs.amazonaws.com"
                },
                "Action": "sns:Subscribe",
                "Resource": topic_arn
            },
            {
                "Sid": "AllowGovernanceFullAccess",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{GOVERNANCE_ACCOUNT_ID}:root"
                },
                "Action": "sns:*",
                "Resource": topic_arn
            }
        ]
    }

    sns_client.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(topic_policy)
    )
    print(f"✅ Topic policy set: {len(PRODUCER_ACCOUNT_IDS)} producers, SQS subscription allowed")

    return topic_arn


def subscribe_consumer_queues(sns_client, topic_arn):
    """
    Subscribe each consumer's SQS queue to the central SNS topic
    
    Message filtering (Project 51 pattern):
    → Each consumer can filter by domain/event_type
    → Analytics subscribes to ALL events
    → Customer domain subscribes only to orders.published events
    """
    for queue_arn in CONSUMER_SQS_ARNS:
        # Determine domain from queue name
        # Format: mesh-{domain}-data-events
        parts = queue_arn.split(":")[-1]  # mesh-analytics-data-events

        response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=queue_arn,
            Attributes={
                "RawMessageDelivery": "true"  # Send raw JSON, not SNS wrapper
            },
            ReturnSubscriptionArn=True
        )
        sub_arn = response["SubscriptionArn"]
        print(f"✅ Subscribed: {queue_arn.split(':')[-1]} → {sub_arn}")

    # Add filter policy for customer domain (only orders events)
    # Analytics gets everything (no filter)
    customer_queue_arn = "arn:aws:sqs:us-east-2:222222222222:mesh-customers-data-events"
    if customer_queue_arn in CONSUMER_SQS_ARNS:
        # Find the subscription ARN for this queue
        subs = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
        for sub in subs.get("Subscriptions", []):
            if sub["Endpoint"] == customer_queue_arn:
                sns_client.set_subscription_attributes(
                    SubscriptionArn=sub["SubscriptionArn"],
                    AttributeName="FilterPolicy",
                    AttributeValue=json.dumps({
                        "source_domain": ["orders"],
                        "event_type": ["data_product.published"]
                    })
                )
                print(f"✅ Filter policy set for customer queue: orders.published only")
                break


def create_data_product_registry(dynamodb_client):
    """
    DynamoDB table: Central registry of ALL data products across the mesh
    
    This is the "catalog of catalogs" — single place to discover
    what data products exist, who owns them, where they live
    
    Schema:
    → PK: domain#product_name (e.g., "orders#daily_orders")
    → SK: version (e.g., "v2")
    → Attributes: s3_path, glue_database, glue_table, owner_email,
                  sla_hours, quality_score, last_published, status
    """
    table_name = "mesh-data-product-registry"

    try:
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "domain_product", "KeyType": "HASH"},
                {"AttributeName": "version", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "domain_product", "AttributeType": "S"},
                {"AttributeName": "version", "AttributeType": "S"},
                {"AttributeName": "domain", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"}
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "domain-index",
                    "KeySchema": [
                        {"AttributeName": "domain", "KeyType": "HASH"},
                        {"AttributeName": "status", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "MeshComponent", "Value": "product-registry"},
                {"Key": "ManagedBy", "Value": "platform-team"}
            ]
        )
        print(f"✅ DynamoDB registry table created: {table_name}")

        # Wait for table to be active
        waiter = dynamodb_client.get_waiter("table_exists")
        waiter.wait(TableName=table_name)
        print(f"✅ Table is ACTIVE")

    except dynamodb_client.exceptions.ResourceInUseException:
        print(f"ℹ️  Registry table already exists: {table_name}")

    return table_name


def seed_registry(dynamodb_resource, table_name):
    """Register initial data products from each domain"""
    table = dynamodb_resource.Table(table_name)

    products = [
        {
            "domain_product": "orders#daily_orders",
            "version": "v2",
            "domain": "orders",
            "product_name": "daily_orders",
            "description": "Daily aggregated order data with customer and product references",
            "owner_account": "[REDACTED:BANK_ACCOUNT_NUMBER]",
            "owner_email": "orders-team@quickcart.com",
            "s3_location": "s3://orders-data-prod/gold/daily_orders/",
            "glue_database": "orders_gold",
            "glue_table": "daily_orders",
            "format": "parquet/snappy",
            "partition_keys": "year,month,day",
            "sla_freshness_hours": 6,
            "quality_threshold_pct": 99,
            "last_quality_score": 99.7,
            "last_published": "2025-01-15T06:00:00Z",
            "status": "active",
            "consumers": ["analytics", "customers"],
            "schema_version": "2.1",
            "row_count_approx": 5000000,
            "size_gb_approx": 2.3,
            "created_at": "2024-06-01T00:00:00Z",
            "updated_at": datetime.utcnow().isoformat() + "Z"
        },
        {
            "domain_product": "orders#daily_refunds",
            "version": "v1",
            "domain": "orders",
            "product_name": "daily_refunds",
            "description": "Daily refund transactions with reason codes",
            "owner_account": "[REDACTED:BANK_ACCOUNT_NUMBER]",
            "owner_email": "orders-team@quickcart.com",
            "s3_location": "s3://orders-data-prod/gold/daily_refunds/",
            "glue_database": "orders_gold",
            "glue_table": "daily_refunds",
            "format": "parquet/snappy",
            "partition_keys": "year,month,day",
            "sla_freshness_hours": 12,
            "quality_threshold_pct": 98,
            "last_quality_score": 99.1,
            "last_published": "2025-01-15T08:00:00Z",
            "status": "active",
            "consumers": ["analytics"],
            "schema_version": "1.0",
            "row_count_approx": 200000,
            "size_gb_approx": 0.4,
            "created_at": "2024-09-15T00:00:00Z",
            "updated_at": datetime.utcnow().isoformat() + "Z"
        },
        {
            "domain_product": "customers#customer_segments",
            "version": "v3",
            "domain": "customers",
            "product_name": "customer_segments",
            "description": "Customer segmentation with RFM scores and lifetime value",
            "owner_account": "[REDACTED:BANK_ACCOUNT_NUMBER]",
            "owner_email": "customer-team@quickcart.com",
            "s3_location": "s3://customers-data-prod/gold/customer_segments/",
            "glue_database": "customers_gold",
            "glue_table": "customer_segments",
            "format": "parquet/snappy",
            "partition_keys": "year,month",
            "sla_freshness_hours": 24,
            "quality_threshold_pct": 99,
            "last_quality_score": 99.9,
            "last_published": "2025-01-15T04:00:00Z",
            "status": "active",
            "consumers": ["analytics", "orders"],
            "schema_version": "3.2",
            "row_count_approx": 1200000,
            "size_gb_approx": 0.8,
            "created_at": "2024-03-01T00:00:00Z",
            "updated_at": datetime.utcnow().isoformat() + "Z"
        }
    ]

    with table.batch_writer() as batch:
        for product in products:
            batch.put_item(Item=product)

    print(f"✅ Registered {len(products)} data products in registry")
    for p in products:
        print(f"   → {p['domain_product']} ({p['version']}): {p['status']}")


def create_mesh_monitoring_dashboard():
    """
    CloudWatch dashboard showing mesh health across all accounts
    → Data product freshness
    → Pipeline success rates
    → Cross-account access patterns
    → SQS queue depths
    """
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    dashboard_body = {
        "widgets": [
            {
                "type": "text",
                "x": 0, "y": 0, "width": 24, "height": 2,
                "properties": {
                    "markdown": "# 🔗 QuickCart Data Mesh — Governance Dashboard\n"
                                "Central monitoring for all data product domains"
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 2, "width": 12, "height": 6,
                "properties": {
                    "title": "SNS: Data Product Events Published",
                    "metrics": [
                        ["AWS/SNS", "NumberOfMessagesPublished",
                         "TopicName", "data-product-events",
                         {"stat": "Sum", "period": 3600}]
                    ],
                    "view": "timeSeries",
                    "region": REGION
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 2, "width": 12, "height": 6,
                "properties": {
                    "title": "SQS: Consumer Queue Depths",
                    "metrics": [
                        ["AWS/SQS", "ApproximateNumberOfMessagesVisible",
                         "QueueName", "mesh-analytics-data-events",
                         {"stat": "Maximum", "period": 300}]
                    ],
                    "view": "timeSeries",
                    "region": REGION
                }
            }
        ]
    }

    cw_client.put_dashboard(
        DashboardName="DataMesh-Governance",
        DashboardBody=json.dumps(dashboard_body)
    )
    print("✅ CloudWatch governance dashboard created: DataMesh-Governance")


def main():
    print("=" * 70)
    print(f"🏗️  SETTING UP GOVERNANCE ACCOUNT: Central Platform")
    print(f"   Account: {GOVERNANCE_ACCOUNT_ID}")
    print("=" * 70)

    sns_client = boto3.client("sns", region_name=REGION)
    dynamodb_client = boto3.client("dynamodb", region_name=REGION)
    dynamodb_resource = boto3.resource("dynamodb", region_name=REGION)

    # Step 1: Central SNS Topic
    print("\n--- Step 1: Central SNS Topic ---")
    topic_arn = create_data_product_sns_topic(sns_client)

    # Step 2: Subscribe Consumer Queues
    print("\n--- Step 2: Consumer Subscriptions ---")
    subscribe_consumer_queues(sns_client, topic_arn)

    # Step 3: Data Product Registry
    print("\n--- Step 3: Data Product Registry ---")
    table_name = create_data_product_registry(dynamodb_client)

    # Step 4: Seed Registry
    print("\n--- Step 4: Seed Initial Products ---")
    seed_registry(dynamodb_resource, table_name)

    # Step 5: Monitoring Dashboard
    print("\n--- Step 5: Monitoring Dashboard ---")
    create_mesh_monitoring_dashboard()

    print("\n" + "=" * 70)
    print("✅ GOVERNANCE ACCOUNT SETUP COMPLETE")
    print(f"   SNS Topic: {topic_arn}")
    print(f"   Registry:  {table_name}")
    print("=" * 70)


if __name__ == "__main__":
    main()