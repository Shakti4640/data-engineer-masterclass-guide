# file: 51_03_verify_filtered_routing.py
# Purpose: Read from each queue and verify correct message routing

import boto3
import json
import time

REGION = "us-east-2"


def load_infrastructure():
    with open("/tmp/filtered_infra.json", "r") as f:
        return json.load(f)


def drain_queue(sqs_client, queue_url, queue_name, max_wait_seconds=10):
    """
    Read ALL messages from a queue (drain it)
    
    Returns list of messages with their attributes
    Uses long polling (from Project 27) for efficiency
    """
    messages_received = []
    start_time = time.time()

    print(f"\n📥 Draining: {queue_name}")
    print(f"   Queue URL: {queue_url}")

    while (time.time() - start_time) < max_wait_seconds:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,     # Max per call
            WaitTimeSeconds=3,          # Short poll for verification
            MessageAttributeNames=["All"],  # Get SNS-forwarded attributes
            AttributeNames=["All"]          # Get SQS system attributes
        )

        if "Messages" not in response:
            break  # Queue is empty

        for msg in response["Messages"]:
            # Parse message body
            body = json.loads(msg["Body"])

            # Extract forwarded MessageAttributes
            # With RawMessageDelivery=true: attributes come as SQS MessageAttributes
            msg_attrs = {}
            if "MessageAttributes" in msg:
                for attr_name, attr_val in msg["MessageAttributes"].items():
                    msg_attrs[attr_name] = attr_val.get("StringValue", "N/A")

            messages_received.append({
                "event_name": body.get("event_name", "unknown"),
                "event_type": body.get("event_type", "unknown"),
                "severity": body.get("severity", "unknown"),
                "message_id": body.get("message_id", "unknown"),
                "attributes": msg_attrs
            })

            # Delete message after reading (acknowledge)
            # From Project 27: always delete after successful processing
            sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg["ReceiptHandle"]
            )

    return messages_received


def verify_routing():
    """
    Drain all queues and verify each received correct messages
    """
    infra = load_infrastructure()
    sqs_client = boto3.client("sqs", region_name=REGION)

    # Allow time for SNS to deliver to all queues
    print("⏳ Waiting 5 seconds for SNS delivery propagation...")
    time.sleep(5)

    print("=" * 70)
    print("🔍 VERIFYING FILTERED MESSAGE ROUTING")
    print("=" * 70)

    # --- EXPECTED RESULTS ---
    expected = {
        "quickcart-etl-queue": {
            "expected_events": ["file_landed", "glue_job_failed"],
            "expected_count": 2
        },
        "quickcart-order-queue": {
            "expected_events": ["order_placed", "order_shipped"],
            "expected_count": 2
        },
        "quickcart-alert-queue": {
            "expected_events": [
                "pipeline_failed", "dq_check_failed", "schema_drift_detected"
            ],
            "expected_count": 3
        },
        "quickcart-audit-queue": {
            "expected_events": [
                "file_landed", "glue_job_failed",
                "order_placed", "order_shipped",
                "pipeline_failed", "dq_check_failed", "schema_drift_detected"
            ],
            "expected_count": 7
        }
    }

    all_passed = True

    for queue_info in infra["queues"]:
        queue_name = queue_info["queue_name"]
        queue_url = queue_info["queue_url"]

        # Drain queue
        messages = drain_queue(sqs_client, queue_url, queue_name)

        # Verify
        exp = expected[queue_name]
        actual_events = [m["event_name"] for m in messages]
        actual_count = len(messages)

        print(f"\n  📊 Results for {queue_name}:")
        print(f"     Expected count: {exp['expected_count']}")
        print(f"     Actual count:   {actual_count}")
        print(f"     Expected events: {exp['expected_events']}")
        print(f"     Actual events:   {actual_events}")

        # Check count
        count_match = actual_count == exp["expected_count"]

        # Check events (order may differ)
        events_match = sorted(actual_events) == sorted(exp["expected_events"])

        if count_match and events_match:
            print(f"     ✅ PASS — correct messages routed")
        else:
            print(f"     ❌ FAIL — routing mismatch!")
            if not count_match:
                print(f"        Count mismatch: expected {exp['expected_count']}, got {actual_count}")
            if not events_match:
                missing = set(exp["expected_events"]) - set(actual_events)
                extra = set(actual_events) - set(exp["expected_events"])
                if missing:
                    print(f"        Missing events: {missing}")
                if extra:
                    print(f"        Unexpected events: {extra}")
            all_passed = False

    # --- FINAL VERDICT ---
    print("\n" + "=" * 70)
    if all_passed:
        print("🎉 ALL QUEUES PASSED — FILTER ROUTING IS CORRECT")
    else:
        print("💥 SOME QUEUES FAILED — CHECK FILTER POLICIES")
    print("=" * 70)

    return all_passed


if __name__ == "__main__":
    verify_routing()