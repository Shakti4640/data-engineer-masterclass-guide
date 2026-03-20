# file: 03_send_test_messages.py
# Sends simulated S3 event messages to SQS for testing
# Use this to verify worker processes messages correctly

import boto3
import json
from datetime import datetime

REGION = "us-east-2"
QUEUE_NAME = "quickcart-file-processing-queue"
BUCKET_NAME = "quickcart-raw-data-prod"


def send_simulated_s3_event(queue_url, key, size=1024000):
    """Send a fake S3 event message to SQS"""
    sqs_client = boto3.client("sqs", region_name=REGION)

    # Simulate S3 event notification JSON structure
    s3_event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "eventTime": datetime.utcnow().isoformat() + "Z",
                "s3": {
                    "bucket": {
                        "name": BUCKET_NAME,
                        "arn": f"arn:aws:s3:::{BUCKET_NAME}"
                    },
                    "object": {
                        "key": key,
                        "size": size,
                        "eTag": "abc123def456"
                    }
                }
            }
        ]
    }

    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(s3_event),
        MessageAttributes={
            "Source": {
                "DataType": "String",
                "StringValue": "test-script"
            }
        }
    )

    message_id = response["MessageId"]
    print(f"✅ Sent: {key} (MessageId: {message_id})")
    return message_id


def send_test_batch():
    """Send multiple test messages"""
    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]

    print("=" * 60)
    print("📤 SENDING TEST MESSAGES TO SQS")
    print(f"   Queue: {QUEUE_NAME}")
    print("=" * 60)

    test_files = [
        ("incoming/shipfast/updates_20250130_001.csv", 524288),
        ("incoming/shipfast/updates_20250130_002.csv", 1048576),
        ("incoming/shipfast/updates_20250130_003.csv", 262144),
        ("daily_exports/orders/orders_20250131.csv", 52428800),
        ("incoming/partner_b/inventory_20250130.csv", 2097152)
    ]

    for key, size in test_files:
        send_simulated_s3_event(queue_url, key, size)

    print(f"\n📊 Sent {len(test_files)} test messages")
    print(f"   Start the worker: python 02_sqs_worker.py")

    # Check queue depth
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessagesVisible"]
    )
    depth = attrs["Attributes"]["ApproximateNumberOfMessagesVisible"]
    print(f"   Queue depth: ~{depth} messages")


if __name__ == "__main__":
    send_test_batch()