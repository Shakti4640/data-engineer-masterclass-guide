# file: 01_create_queues.py
# Purpose: Create Standard + FIFO queues with Dead Letter Queues
# Run once during infrastructure setup

import boto3
import json
import time
from botocore.exceptions import ClientError

REGION = "us-east-2"
ACCOUNT_ID = [REDACTED:BANK_ACCOUNT_NUMBER]2"


def create_queue(sqs_client, queue_name, attributes, tags):
    """Create SQS queue with given attributes"""
    try:
        response = sqs_client.create_queue(
            QueueName=queue_name,
            Attributes=attributes,
            tags=tags
        )
        queue_url = response["QueueUrl"]
        print(f"✅ Queue created: {queue_name}")
        print(f"   URL: {queue_url}")
        return queue_url
    except ClientError as e:
        if "QueueAlreadyExists" in str(e):
            # Get existing queue URL
            response = sqs_client.get_queue_url(QueueName=queue_name)
            queue_url = response["QueueUrl"]
            print(f"ℹ️  Queue already exists: {queue_name}")
            print(f"   URL: {queue_url}")
            return queue_url
        raise


def get_queue_arn(sqs_client, queue_url):
    """Get ARN from queue URL"""
    response = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )
    return response["Attributes"]["QueueArn"]


def create_all_queues():
    """Create the complete queue infrastructure"""
    print("=" * 60)
    print("📦 CREATING SQS QUEUE INFRASTRUCTURE")
    print("=" * 60)
    
    sqs_client = boto3.client("sqs", region_name=REGION)
    
    common_tags = {
        "Environment": "Production",
        "Team": "Data-Engineering",
        "Project": "QuickCart"
    }
    
    queues = {}
    
    # ──────────────────────────────────────────────────
    # 1. STANDARD QUEUE: Dead Letter Queue (create FIRST)
    # ──────────────────────────────────────────────────
    print(f"\n📋 Step 1: Standard DLQ...")
    dlq_std_url = create_queue(
        sqs_client,
        queue_name="quickcart-file-processing-dlq",
        attributes={
            "MessageRetentionPeriod": str(14 * 86400),   # 14 days (maximum)
            "VisibilityTimeout": "300",                    # 5 minutes
            "ReceiveMessageWaitTimeSeconds": "20",         # Long polling
            "SqsManagedSseEnabled": "true"                 # Encryption at rest
        },
        tags={**common_tags, "QueueType": "DLQ-Standard"}
    )
    dlq_std_arn = get_queue_arn(sqs_client, dlq_std_url)
    queues["file_processing_dlq"] = {"url": dlq_std_url, "arn": dlq_std_arn}
    
    # ──────────────────────────────────────────────────
    # 2. STANDARD QUEUE: Main file processing queue
    # ──────────────────────────────────────────────────
    print(f"\n📋 Step 2: Standard main queue...")
    main_std_url = create_queue(
        sqs_client,
        queue_name="quickcart-file-processing",
        attributes={
            "MessageRetentionPeriod": str(7 * 86400),     # 7 days
            "VisibilityTimeout": "300",                     # 5 min (processing ~30-60s)
            "ReceiveMessageWaitTimeSeconds": "20",          # Long polling
            "SqsManagedSseEnabled": "true",
            
            # Dead Letter Queue configuration
            "RedrivePolicy": json.dumps({
                "deadLetterTargetArn": dlq_std_arn,
                "maxReceiveCount": 3                        # 3 failures → DLQ
            })
        },
        tags={**common_tags, "QueueType": "Standard"}
    )
    main_std_arn = get_queue_arn(sqs_client, main_std_url)
    queues["file_processing"] = {"url": main_std_url, "arn": main_std_arn}
    
    # ──────────────────────────────────────────────────
    # 3. FIFO QUEUE: Dead Letter Queue (create FIRST)
    # FIFO DLQ must also be FIFO
    # ──────────────────────────────────────────────────
    print(f"\n📋 Step 3: FIFO DLQ...")
    dlq_fifo_url = create_queue(
        sqs_client,
        queue_name="quickcart-order-events-dlq.fifo",     # Must end with .fifo
        attributes={
            "FifoQueue": "true",
            "MessageRetentionPeriod": str(14 * 86400),
            "VisibilityTimeout": "300",
            "ReceiveMessageWaitTimeSeconds": "20",
            "SqsManagedSseEnabled": "true",
            "ContentBasedDeduplication": "true"
        },
        tags={**common_tags, "QueueType": "DLQ-FIFO"}
    )
    dlq_fifo_arn = get_queue_arn(sqs_client, dlq_fifo_url)
    queues["order_events_dlq"] = {"url": dlq_fifo_url, "arn": dlq_fifo_arn}
    
    # ──────────────────────────────────────────────────
    # 4. FIFO QUEUE: Main order events queue
    # ──────────────────────────────────────────────────
    print(f"\n📋 Step 4: FIFO main queue...")
    main_fifo_url = create_queue(
        sqs_client,
        queue_name="quickcart-order-events.fifo",
        attributes={
            "FifoQueue": "true",
            "MessageRetentionPeriod": str(7 * 86400),
            "VisibilityTimeout": "300",
            "ReceiveMessageWaitTimeSeconds": "20",
            "SqsManagedSseEnabled": "true",
            "ContentBasedDeduplication": "true",           # Dedup by message body hash
            
            "RedrivePolicy": json.dumps({
                "deadLetterTargetArn": dlq_fifo_arn,
                "maxReceiveCount": 3
            })
        },
        tags={**common_tags, "QueueType": "FIFO"}
    )
    main_fifo_arn = get_queue_arn(sqs_client, main_fifo_url)
    queues["order_events"] = {"url": main_fifo_url, "arn": main_fifo_arn}
    
    # Save configuration
    config = {
        "region": REGION,
        "queues": queues
    }
    with open("sqs_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"✅ ALL QUEUES CREATED")
    print(f"{'='*60}")
    for name, info in queues.items():
        print(f"\n   📦 {name}:")
        print(f"      URL: {info['url']}")
        print(f"      ARN: {info['arn']}")
    
    print(f"\n📁 Config saved to sqs_config.json")
    return queues


if __name__ == "__main__":
    create_all_queues()