# file: 50_05_test_dlq.py
# Purpose: Test the DLQ setup by simulating message failures

import boto3
import json
import time
from datetime import datetime

REGION = "us-east-2"
SOURCE_QUEUE = "quickcart-file-process-q"
DLQ_NAME = "quickcart-file-process-q-dlq"


def send_test_messages(count=5):
    """Send test messages that will intentionally fail processing"""
    sqs_client = boto3.client("sqs", region_name=REGION)
    source_url = sqs_client.get_queue_url(QueueName=SOURCE_QUEUE)["QueueUrl"]

    print(f"{'='*60}")
    print(f"🧪 DLQ TEST — Sending {count} test messages")
    print(f"{'='*60}")

    for i in range(1, count + 1):
        msg_body = json.dumps({
            "test_id": f"dlq-test-{datetime.now().strftime('%Y%m%d%H%M%S')}-{i}",
            "action": "process_file",
            "file_key": f"bronze/test/INTENTIONALLY_MISSING_FILE_{i}.csv",
            "timestamp": datetime.utcnow().isoformat(),
            "test_type": "dlq_validation"
        })

        sqs_client.send_message(
            QueueUrl=source_url,
            MessageBody=msg_body,
            MessageAttributes={
                "TestType": {
                    "DataType": "String",
                    "StringValue": "dlq_test"
                },
                "ExpectedOutcome": {
                    "DataType": "String",
                    "StringValue": "should_fail_and_go_to_dlq"
                }
            }
        )
        print(f"   📤 Sent test message {i}: INTENTIONALLY_MISSING_FILE_{i}.csv")

    print(f"\n   ✅ {count} test messages sent to {SOURCE_QUEUE}")
    print(f"   ⏳ Consumer will attempt to process → fail → retry → DLQ")
    print(f"   Expected: after maxReceiveCount retries, messages appear in DLQ")


def simulate_consumer_failures():
    """
    Simulate a consumer that always fails — messages should end up in DLQ.
    
    This mimics what happens when:
    → File doesn't exist in S3
    → Glue job quota exceeded
    → Network timeout
    """
    sqs_client = boto3.client("sqs", region_name=REGION)
    source_url = sqs_client.get_queue_url(QueueName=SOURCE_QUEUE)["QueueUrl"]

    print(f"\n{'='*60}")
    print(f"🧪 SIMULATING CONSUMER FAILURES")
    print(f"{'='*60}")

    # Get maxReceiveCount from redrive policy
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=source_url,
        AttributeNames=["RedrivePolicy"]
    )["Attributes"]

    redrive = json.loads(attrs.get("RedrivePolicy", "{}"))
    max_receive = int(redrive.get("maxReceiveCount", 3))
    print(f"   maxReceiveCount: {max_receive}")
    print(f"   Will receive each message {max_receive} times, then DLQ")

    # Get visibility timeout
    vis_attrs = sqs_client.get_queue_attributes(
        QueueUrl=source_url,
        AttributeNames=["VisibilityTimeout"]
    )["Attributes"]
    vis_timeout = int(vis_attrs.get("VisibilityTimeout", 30))

    # Process messages — intentionally fail (don't delete)
    for attempt in range(max_receive + 1):
        print(f"\n   ── Attempt {attempt + 1} ──")

        response = sqs_client.receive_message(
            QueueUrl=source_url,
            MaxNumberOfMessages=10,
            AttributeNames=["ApproximateReceiveCount"],
            WaitTimeSeconds=5,
            VisibilityTimeout=5  # Short timeout for testing
        )

        messages = response.get("Messages", [])

        if not messages:
            print(f"   No messages available (may have moved to DLQ)")
            break

        for msg in messages:
            receive_count = msg.get("Attributes", {}).get("ApproximateReceiveCount", "?")
            body = json.loads(msg["Body"])
            test_id = body.get("test_id", "unknown")

            print(f"   Received: {test_id} (receive count: {receive_count})")
            print(f"   ❌ Simulating FAILURE — not deleting message")
            # NOT calling delete_message → message will return to queue

        # Wait for visibility timeout to expire
        if attempt < max_receive:
            print(f"   ⏳ Waiting {6}s for visibility timeout...")
            time.sleep(6)

    # Check DLQ
    print(f"\n   ⏳ Waiting 10s for DLQ move...")
    time.sleep(10)

    dlq_url = sqs_client.get_queue_url(QueueName=DLQ_NAME)["QueueUrl"]
    dlq_attrs = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]

    dlq_count = int(dlq_attrs.get("ApproximateNumberOfMessages", 0))

    print(f"\n{'='*60}")
    if dlq_count > 0:
        print(f"✅ DLQ TEST PASSED — {dlq_count} messages in DLQ")
        print(f"   Messages were quarantined after {max_receive} failed attempts")
    else:
        print(f"⚠️  DLQ may need more time — check again in 60 seconds")
        print(f"   Or: messages may still be in source queue (retry in progress)")
    print(f"{'='*60}")


def cleanup_test_messages():
    """Remove test messages from DLQ after validation"""
    sqs_client = boto3.client("sqs", region_name=REGION)
    dlq_url = sqs_client.get_queue_url(QueueName=DLQ_NAME)["QueueUrl"]

    print(f"\n🧹 Cleaning up test messages from DLQ...")

    cleaned = 0
    while True:
        response = sqs_client.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2
        )

        messages = response.get("Messages", [])
        if not messages:
            break

        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                if body.get("test_type") == "dlq_validation":
                    sqs_client.delete_message(
                        QueueUrl=dlq_url,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    cleaned += 1
            except (json.JSONDecodeError, KeyError):
                pass

    print(f"   ✅ Cleaned {cleaned} test messages from DLQ")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python 50_05_test_dlq.py send          # Send test messages")
        print("  python 50_05_test_dlq.py simulate       # Simulate consumer failures")
        print("  python 50_05_test_dlq.py full_test      # Send + simulate + verify")
        print("  python 50_05_test_dlq.py cleanup        # Remove test messages from DLQ")
        sys.exit(0)

    action = sys.argv[1]

    if action == "send":
        send_test_messages(5)

    elif action == "simulate":
        simulate_consumer_failures()

    elif action == "full_test":
        send_test_messages(3)
        time.sleep(5)
        simulate_consumer_failures()
        cleanup_test_messages()

    elif action == "cleanup":
        cleanup_test_messages()