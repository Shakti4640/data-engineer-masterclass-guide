# file: 59_04_test_cross_account_delivery.py
# Purpose: Test cross-account SNS → SQS delivery end-to-end

import boto3
import json
import time
import uuid
from datetime import datetime

REGION = "us-east-2"


def load_config():
    with open("/tmp/cross_account_sns_config.json", "r") as f:
        return json.load(f)


def publish_test_events():
    """
    Publish test events from Account A's SNS topic
    and verify they arrive in Account B's SQS queues
    """
    config = load_config()
    sns_client = boto3.client("sns", region_name=REGION)
    sqs_client = boto3.client("sqs", region_name=REGION)
    topic_arn = config["topic_arn"]

    print("=" * 70)
    print("🧪 CROSS-ACCOUNT SNS → SQS DELIVERY TEST")
    print("=" * 70)

    # Define test events
    test_events = [
        {
            "name": "ETL Completed (should → pipeline queue)",
            "message": {
                "event_type": "etl_completed",
                "pipeline": "nightly-etl",
                "tables_updated": ["orders_gold", "customers_silver"],
                "rows_processed": 245000,
                "duration_seconds": 1542,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            },
            "attributes": {
                "event_type": {"DataType": "String", "StringValue": "etl_completed"},
                "severity": {"DataType": "String", "StringValue": "info"},
                "source_system": {"DataType": "String", "StringValue": "glue"}
            },
            "expected_queue": "acct-a-pipeline-events-queue"
        },
        {
            "name": "DQ Passed (should → pipeline queue)",
            "message": {
                "event_type": "dq_passed",
                "quality_score": 94.5,
                "checks_passed": 4,
                "checks_total": 4,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            },
            "attributes": {
                "event_type": {"DataType": "String", "StringValue": "dq_passed"},
                "severity": {"DataType": "String", "StringValue": "info"}
            },
            "expected_queue": "acct-a-pipeline-events-queue"
        },
        {
            "name": "Pipeline Failed (should → alert queue)",
            "message": {
                "event_type": "pipeline_failed",
                "pipeline": "nightly-etl",
                "failed_step": "RunGlueETL",
                "error": "OutOfMemoryError: Java heap space",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            },
            "attributes": {
                "event_type": {"DataType": "String", "StringValue": "pipeline_failed"},
                "severity": {"DataType": "String", "StringValue": "critical"},
                "source_system": {"DataType": "String", "StringValue": "step_functions"}
            },
            "expected_queue": "acct-a-alert-events-queue"
        },
        {
            "name": "Pipeline Started (should → NEITHER queue — filtered out)",
            "message": {
                "event_type": "pipeline_started",
                "pipeline": "nightly-etl",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            },
            "attributes": {
                "event_type": {"DataType": "String", "StringValue": "pipeline_started"},
                "severity": {"DataType": "String", "StringValue": "info"}
            },
            "expected_queue": "NONE (filtered out)"
        }
    ]

    # Publish all test events
    print(f"\n📤 PUBLISHING {len(test_events)} TEST EVENTS...")
    for i, event in enumerate(test_events, 1):
        msg_id = str(uuid.uuid4())
        event["message"]["message_id"] = msg_id

        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(event["message"]),
            MessageAttributes=event["attributes"]
        )

        print(f"\n  Event {i}: {event['name']}")
        print(f"    SNS MessageId: {response['MessageId']}")
        print(f"    Expected in: {event['expected_queue']}")

    # Wait for delivery
    print(f"\n⏳ Waiting 5 seconds for cross-account delivery...")
    time.sleep(5)

    # Check each queue
    print(f"\n{'=' * 70}")
    print(f"📥 CHECKING QUEUE CONTENTS")
    print(f"{'=' * 70}")

    results = {}

    for queue in config["queues"]:
        queue_name = queue["queue_name"]
        queue_url = queue["queue_url"]

        print(f"\n📬 Queue: {queue_name}")
        print(f"   Filter: {json.dumps(queue['filter_policy'])}")

        messages = []
        # Drain the queue
        while True:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=3,
                MessageAttributeNames=["All"]
            )

            if "Messages" not in response:
                break

            for msg in response["Messages"]:
                body = json.loads(msg["Body"])
                messages.append(body)

                # Delete after reading
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=msg["ReceiptHandle"]
                )

        event_types = [m.get("event_type", "unknown") for m in messages]
        results[queue_name] = {
            "count": len(messages),
            "event_types": event_types
        }

        print(f"   Received: {len(messages)} messages")
        print(f"   Event types: {event_types}")

        for msg in messages:
            print(f"   → {msg.get('event_type', '?')}: {msg.get('message_id', '?')[:12]}...")

    # Verify expected results
    print(f"\n{'=' * 70}")
    print(f"📊 DELIVERY VERIFICATION")
    print(f"{'=' * 70}")

    expected = {
        "acct-a-pipeline-events-queue": {
            "expected_types": ["etl_completed", "dq_passed"],
            "expected_count": 2
        },
        "acct-a-alert-events-queue": {
            "expected_types": ["pipeline_failed"],
            "expected_count": 1
        }
    }

    all_passed = True
    for queue_name, exp in expected.items():
        actual = results.get(queue_name, {"count": 0, "event_types": []})

        count_match = actual["count"] == exp["expected_count"]
        types_match = sorted(actual["event_types"]) == sorted(exp["expected_types"])

        if count_match and types_match:
            print(f"  ✅ {queue_name}: PASS")
            print(f"     Expected: {exp['expected_count']} events {exp['expected_types']}")
            print(f"     Actual:   {actual['count']} events {actual['event_types']}")
        else:
            print(f"  ❌ {queue_name}: FAIL")
            print(f"     Expected: {exp['expected_count']} events {exp['expected_types']}")
            print(f"     Actual:   {actual['count']} events {actual['event_types']}")
            all_passed = False

    # Verify pipeline_started was filtered out
    total_received = sum(r["count"] for r in results.values())
    print(f"\n  📊 Total messages received across all queues: {total_received}")
    print(f"     Total published: {len(test_events)}")
    print(f"     Filtered out: {len(test_events) - total_received}")
    print(f"     (pipeline_started should be filtered out → {'✅' if total_received == 3 else '❌'})")

    print(f"\n{'=' * 70}")
    if all_passed and total_received == 3:
        print(f"🎉 ALL TESTS PASSED — Cross-account SNS → SQS working correctly!")
    else:
        print(f"💥 SOME TESTS FAILED — Check policies and filter configurations")
    print(f"{'=' * 70}")

    return all_passed


if __name__ == "__main__":
    publish_test_events()