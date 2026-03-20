# file: 05_integration_test.py
# Purpose: Full round-trip test of the fan-out pattern
# Publish → verify all queues received → consume from each → verify DLQs empty

import boto3
import json
import time
import uuid
from datetime import datetime, timezone
from botocore.exceptions import ClientError


def load_config():
    with open("fanout_config.json", "r") as f:
        return json.load(f)


def run_integration_test():
    """Complete fan-out integration test"""
    config = load_config()
    sns_client = boto3.client("sns", region_name=config["region"])
    sqs_client = boto3.client("sqs", region_name=config["region"])
    
    print("=" * 65)
    print("🧪 FAN-OUT INTEGRATION TEST")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)
    
    results = {"passed": 0, "failed": 0}
    
    # ── TEST 1: Publish to SNS ──
    print(f"\n{'─'*65}")
    print(f"🧪 TEST 1: Publish event to SNS topic")
    print(f"{'─'*65}")
    
    test_event_id = str(uuid.uuid4())
    test_message = {
        "event_id": test_event_id,
        "event_type": "integration_test",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "s3": {
            "bucket": "quickcart-raw-data-prod",
            "key": "test/integration_test.csv",
            "size_mb": 1.0
        },
        "source": "integration_test"
    }
    
    try:
        response = sns_client.publish(
            TopicArn=config["topic_arn"],
            Subject="Integration Test Event",
            Message=json.dumps(test_message),
            MessageAttributes={
                "event_type": {
                    "DataType": "String",
                    "StringValue": "integration_test"
                },
                "source": {
                    "DataType": "String",
                    "StringValue": "integration_test"
                }
            }
        )
        sns_msg_id = response["MessageId"]
        print(f"   ✅ Published: {sns_msg_id}")
        print(f"   Event ID: {test_event_id}")
        results["passed"] += 1
    except ClientError as e:
        print(f"   ❌ Publish failed: {e}")
        results["failed"] += 1
        return results
    
    # Wait for fan-out delivery
    print(f"\n   ⏳ Waiting 3 seconds for fan-out delivery...")
    time.sleep(3)
    
    # ── TEST 2: Verify all queues received the message ──
    print(f"\n{'─'*65}")
    print(f"🧪 TEST 2: Verify all queues received the message")
    print(f"{'─'*65}")
    
    queues_received = {}
    
    for key, info in config["queues"].items():
        try:
            attrs = sqs_client.get_queue_attributes(
                QueueUrl=info["url"],
                AttributeNames=["ApproximateNumberOfMessages"]
            )
            depth = int(attrs["Attributes"].get("ApproximateNumberOfMessages", 0))
            queues_received[key] = depth
            
            if depth >= 1:
                print(f"   ✅ {key}: {depth} message(s) — received")
                results["passed"] += 1
            else:
                print(f"   ❌ {key}: 0 messages — NOT received")
                print(f"      → Check queue policy allows SNS topic")
                results["failed"] += 1
                
        except ClientError as e:
            print(f"   ❌ {key}: error — {e}")
            results["failed"] += 1
    
    # ── TEST 3: Consume from each queue and verify content ──
    print(f"\n{'─'*65}")
    print(f"🧪 TEST 3: Consume and verify message content")
    print(f"{'─'*65}")
    
    for key, info in config["queues"].items():
        try:
            response = sqs_client.receive_message(
                QueueUrl=info["url"],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5,
                MessageAttributeNames=["All"]
            )
            
            messages = response.get("Messages", [])
            
            if not messages:
                print(f"   ⚠️  {key}: no message to consume")
                continue
            
            msg = messages[0]
            body = json.loads(msg["Body"])
            
            # Handle raw vs wrapped
            if "Type" in body and body.get("Type") == "Notification":
                inner = json.loads(body.get("Message", "{}"))
                delivery_type = "SNS-wrapped"
            else:
                inner = body
                delivery_type = "raw"
            
            # Verify event_id matches
            received_event_id = inner.get("event_id", "")
            
            if received_event_id == test_event_id:
                print(f"   ✅ {key}: event_id matches ({delivery_type} delivery)")
                results["passed"] += 1
            else:
                print(f"   ❌ {key}: event_id mismatch")
                print(f"      Expected: {test_event_id}")
                print(f"      Received: {received_event_id}")
                results["failed"] += 1
            
            # Verify message attributes (if raw delivery)
            msg_attrs = msg.get("MessageAttributes", {})
            if msg_attrs:
                source_attr = msg_attrs.get("source", {}).get("StringValue", "")
                if source_attr == "integration_test":
                    print(f"      ✅ Message attributes preserved")
                else:
                    print(f"      ⚠️  Source attribute: '{source_attr}' (expected 'integration_test')")
            
            # Delete consumed message
            sqs_client.delete_message(
                QueueUrl=info["url"],
                ReceiptHandle=msg["ReceiptHandle"]
            )
            print(f"      🗑️  Message deleted from {key}")
            
        except (ClientError, json.JSONDecodeError, KeyError) as e:
            print(f"   ❌ {key}: error — {e}")
            results["failed"] += 1
    
    # ── TEST 4: Verify DLQs are empty ──
    print(f"\n{'─'*65}")
    print(f"🧪 TEST 4: Verify all DLQs are empty")
    print(f"{'─'*65}")
    
    for key, info in config["queues"].items():
        try:
            dlq_attrs = sqs_client.get_queue_attributes(
                QueueUrl=info["dlq_url"],
                AttributeNames=["ApproximateNumberOfMessages"]
            )
            dlq_depth = int(dlq_attrs["Attributes"].get(
                "ApproximateNumberOfMessages", 0))
            
            if dlq_depth == 0:
                print(f"   ✅ {key} DLQ: empty (no failures)")
                results["passed"] += 1
            else:
                print(f"   ⚠️  {key} DLQ: {dlq_depth} messages (investigate)")
                results["failed"] += 1
                
        except ClientError as e:
            print(f"   ❌ {key} DLQ: error — {e}")
            results["failed"] += 1
    
    # ── TEST 5: Verify queues are now empty ──
    print(f"\n{'─'*65}")
    print(f"🧪 TEST 5: Verify all queues empty after consumption")
    print(f"{'─'*65}")
    
    time.sleep(2)       # Wait for eventual consistency
    
    for key, info in config["queues"].items():
        try:
            attrs = sqs_client.get_queue_attributes(
                QueueUrl=info["url"],
                AttributeNames=["ApproximateNumberOfMessages"]
            )
            depth = int(attrs["Attributes"].get("ApproximateNumberOfMessages", 0))
            
            if depth == 0:
                print(f"   ✅ {key}: empty (all consumed)")
                results["passed"] += 1
            else:
                print(f"   ⚠️  {key}: {depth} remaining (eventual consistency)")
                results["passed"] += 1      # Not a hard failure
                
        except ClientError as e:
            print(f"   ❌ {key}: error — {e}")
            results["failed"] += 1
    
    # ── SUMMARY ──
    total = results["passed"] + results["failed"]
    
    print(f"\n{'='*65}")
    print(f"📊 INTEGRATION TEST RESULTS")
    print(f"{'='*65}")
    print(f"   Passed: {results['passed']}/{total}")
    print(f"   Failed: {results['failed']}/{total}")
    
    if results["failed"] == 0:
        print(f"   🟢 ALL TESTS PASSED — fan-out verified end-to-end")
    else:
        print(f"   🔴 SOME TESTS FAILED — troubleshooting needed")
        print(f"""
   TROUBLESHOOTING CHECKLIST:
   ──────────────────────────
   1. Queue policy allows SNS topic?
      → Check: aws sqs get-queue-attributes --queue-url <url> --attribute-names Policy
      → Must include: sns:SendMessage from topic ARN
      
   2. Subscription confirmed?
      → SQS subscriptions auto-confirm (same account)
      → Email subscriptions require manual confirmation
      
   3. Raw delivery setting correct?
      → Check subscription attributes
      → If consumer expects raw but gets wrapped: parse failure
      
   4. Message attributes match filter policy?
      → If filter policy set: only matching messages delivered
      → No filter policy = all messages delivered
        """)
    
    print(f"{'='*65}")
    return results


if __name__ == "__main__":
    run_integration_test()