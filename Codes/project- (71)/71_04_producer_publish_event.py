# file: 71_04_producer_publish_event.py
# Runs in Account A (111111111111) AFTER Glue ETL completes
# Publishes "data product available" event to Governance SNS
# Updates DynamoDB registry via cross-account role

import boto3
import json
from datetime import datetime

# --- CONFIGURATION ---
PRODUCER_ACCOUNT_ID = "111111111111"
GOVERNANCE_ACCOUNT_ID =[REDACTED:BANK_ACCOUNT_NUMBER]44"
REGION = "us-east-2"
DOMAIN = "orders"
PRODUCT_NAME = "daily_orders"
GOVERNANCE_SNS_TOPIC_ARN = (
    f"arn:aws:sns:{REGION}:{GOVERNANCE_ACCOUNT_ID}:data-product-events"
)


def publish_data_product_event(
    domain,
    product_name,
    version,
    s3_location,
    partition_values,
    row_count,
    quality_score,
    status="published"
):
    """
    Publish structured event to Governance SNS topic
    
    Event schema (standardized across all domains):
    → source_domain: who published
    → event_type: what happened
    → product: what data product
    → partition: which specific partition is new
    → quality: data quality score
    → timestamp: when it was published
    
    Cross-account publish (Project 59 pattern):
    → Producer's IAM role needs sns:Publish on Governance topic ARN
    → Governance topic policy allows this account to publish
    """
    sns_client = boto3.client("sns", region_name=REGION)

    event_payload = {
        "event_id": f"{domain}-{product_name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "event_type": f"data_product.{status}",
        "source_domain": domain,
        "source_account": PRODUCER_ACCOUNT_ID,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "data_product": {
            "name": product_name,
            "version": version,
            "s3_location": s3_location,
            "format": "parquet/snappy",
            "partition": partition_values,
            "row_count": row_count,
            "size_bytes": None  # Could calculate from S3
        },
        "quality": {
            "score": quality_score,
            "threshold": 99.0,
            "passed": quality_score >= 99.0,
            "checks_run": [
                "completeness",
                "uniqueness",
                "freshness",
                "referential_integrity"
            ]
        },
        "sla": {
            "freshness_hours": 6,
            "met": True
        },
        "contact": {
            "team": "orders-domain-team",
            "email": "orders-team@quickcart.com",
            "slack": "#orders-data"
        }
    }

    # SNS Message Attributes enable filtering (Project 51 pattern)
    # Consumers can filter by domain, event_type, quality status
    message_attributes = {
        "source_domain": {
            "DataType": "String",
            "StringValue": domain
        },
        "event_type": {
            "DataType": "String",
            "StringValue": f"data_product.{status}"
        },
        "product_name": {
            "DataType": "String",
            "StringValue": product_name
        },
        "quality_passed": {
            "DataType": "String",
            "StringValue": str(quality_score >= 99.0).lower()
        }
    }

    try:
        response = sns_client.publish(
            TopicArn=GOVERNANCE_SNS_TOPIC_ARN,
            Message=json.dumps(event_payload, indent=2),
            Subject=f"[DataMesh] {domain}.{product_name} {status}",
            MessageAttributes=message_attributes
        )
        message_id = response["MessageId"]
        print(f"✅ Event published to Governance SNS")
        print(f"   MessageId: {message_id}")
        print(f"   Event: {domain}.{product_name} → {status}")
        print(f"   Quality: {quality_score}% ({'✅ PASSED' if quality_score >= 99 else '❌ FAILED'})")
        return message_id

    except Exception as e:
        print(f"❌ Failed to publish event: {e}")
        # In production: retry with exponential backoff
        # Or write to local DLQ and retry later
        raise


def update_registry_cross_account(
    domain,
    product_name,
    version,
    quality_score,
    row_count
):
    """
    Update the central DynamoDB registry in Governance account
    
    Cross-account DynamoDB access:
    → Producer assumes a role in Governance account
    → That role has dynamodb:UpdateItem on the registry table
    → STS temporary credentials used
    """
    sts_client = boto3.client("sts", region_name=REGION)

    # Assume cross-account role in Governance account
    governance_role_arn = (
        f"arn:aws:iam::{GOVERNANCE_ACCOUNT_ID}:role/"
        f"MeshGovernance-RegistryWriter"
    )

    try:
        assumed = sts_client.assume_role(
            RoleArn=governance_role_arn,
            RoleSessionName=f"{domain}-registry-update",
            ExternalId="mesh-registry-2025",
            DurationSeconds=900  # 15 minutes is enough
        )
        creds = assumed["Credentials"]

        # Create DynamoDB client with cross-account credentials
        dynamodb = boto3.resource(
            "dynamodb",
            region_name=REGION,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"]
        )

        table = dynamodb.Table("mesh-data-product-registry")
        table.update_item(
            Key={
                "domain_product": f"{domain}#{product_name}",
                "version": version
            },
            UpdateExpression=(
                "SET last_published = :ts, "
                "last_quality_score = :qs, "
                "row_count_approx = :rc, "
                "updated_at = :ua, "
                "#st = :status"
            ),
            ExpressionAttributeNames={
                "#st": "status"  # 'status' is a reserved word
            },
            ExpressionAttributeValues={
                ":ts": datetime.utcnow().isoformat() + "Z",
                ":qs": str(quality_score),
                ":rc": row_count,
                ":ua": datetime.utcnow().isoformat() + "Z",
                ":status": "active"
            }
        )
        print(f"✅ Registry updated: {domain}#{product_name} v{version}")

    except Exception as e:
        print(f"⚠️  Registry update failed (non-critical): {e}")
        # Registry update failure should NOT block the pipeline
        # Log and alert, but don't fail the publish


def simulate_post_etl_publish():
    """
    This function runs AFTER Glue ETL completes successfully
    
    Typical trigger:
    → Glue Workflow completion → Lambda → this script
    → OR: Step Functions state → this script
    → OR: Glue job's last lines call this
    """
    today = datetime.utcnow()
    partition = {
        "year": today.strftime("%Y"),
        "month": today.strftime("%m"),
        "day": today.strftime("%d")
    }

    print("=" * 70)
    print(f"📢 PUBLISHING DATA PRODUCT: {DOMAIN}.{PRODUCT_NAME}")
    print(f"   Partition: {partition}")
    print("=" * 70)

    # Step 1: Publish SNS event
    print("\n--- Step 1: Publish Event ---")
    publish_data_product_event(
        domain=DOMAIN,
        product_name=PRODUCT_NAME,
        version="v2",
        s3_location=f"s3://orders-data-prod/gold/daily_orders/year={partition['year']}/month={partition['month']}/day={partition['day']}/",
        partition_values=partition,
        row_count=198542,
        quality_score=99.7,
        status="published"
    )

    # Step 2: Update central registry
    print("\n--- Step 2: Update Registry ---")
    update_registry_cross_account(
        domain=DOMAIN,
        product_name=PRODUCT_NAME,
        version="v2",
        quality_score=99.7,
        row_count=198542
    )

    print("\n" + "=" * 70)
    print("✅ DATA PRODUCT PUBLISH COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    simulate_post_etl_publish()