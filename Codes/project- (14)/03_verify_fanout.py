# file: 03_verify_fanout.py
# Purpose: Verify all fan-out queues received the published messages
# Critical for debugging fan-out issues

import boto3
import json
import time
from botocore.exceptions import ClientError


def load_config():
    with open("fanout_config.json", "r") as f:
        return json.load(f)


def verify_fanout():
    """Check message counts across all fan-out queues"""
    config = load_config()
    sqs_client = boto3.client("sqs", region_name=config["region"])
    
    print("=" * 65)
    print("🔍 FAN-OUT DELIVERY VERIFICATION")
    print("=" * 65)
    
    print(f"\n   Topic: {config['topic_name']}")
    
    queue_depths = {}
    all_match = True
    
    for key, info in config["queues"].items():
        try:
            response = sqs_client.get_queue_attributes(
                QueueUrl=info["url"],
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible"
                ]
            )
            
            visible = int(response["Attributes"].get(
                "ApproximateNumberOfMessages", 0))
            in_flight = int(response["Attributes"].get(
                "ApproximateNumberOfMessagesNotVisible", 0))
            total = visible + in_flight
            
            queue_depths[key] = visible
            
            print(f"\n   📦 {key}:")
            print(f"      Visible:   {visible}")
            print(f"      In-flight: {in_flight}")
            print(f"      Total:     {total}")
            print(f"      Purpose:   {info['description']}")
            
        except ClientError as e:
            print(f"\n   📦 {key}: ❌ Error: {e}")
            all_match = False
    
    # Check if all queues have same depth
    depths = list(queue_depths.values())
    if depths and all(d == depths[0] for d in depths):
        print(f"\n   ✅ ALL QUEUES HAVE SAME DEPTH: {depths[0]} messages")
        print(f"   → Fan-out is working correctly")
    elif depths:
        print(f"\n   ⚠️  QUEUE DEPTHS DIFFER:")
        for key, depth in queue_depths.items():
            marker = "✅" if depth == max(depths) else "⚠️"
            print(f"      {marker} {key}: {depth}")
        print(f"\n   Possible causes:")
        print(f"   → Some consumers already processed their messages")
        print(f"   → Queue policy issue (check permissions)")
        print(f"   → Subscription filter blocking some messages")
    
    # Check DLQs
    print(f"\n   🔍 DLQ Status:")
    for key, info in config["queues"].items():
        try:
            dlq_response = sqs_client.get_queue_attributes(
                QueueUrl=info["dlq_url"],
                AttributeNames=["ApproximateNumberOfMessages"]
            )
            dlq_depth = int(dlq_response["Attributes"].get(
                "ApproximateNumberOfMessages", 0))
            
            if dlq_depth > 0:
                print(f"      🔴 {key} DLQ: {dlq_depth} poison messages!")
            else:
                print(f"      🟢 {key} DLQ: empty")
                
        except ClientError:
            print(f"      ⚠️  {key} DLQ: unable to check")
    
    return all_match


def peek_all_queues(max_per_queue=1):
    """Peek at one message from each queue to compare content"""
    config = load_config()
    sqs_client = boto3.client("sqs", region_name=config["region"])
    
    print(f"\n{'='*65}")
    print(f"🔍 PEEKING AT MESSAGE CONTENT ACROSS QUEUES")
    print(f"{'='*65}")
    
    for key, info in config["queues"].items():
        try:
            response = sqs_client.receive_message(
                QueueUrl=info["url"],
                MaxNumberOfMessages=max_per_queue,
                WaitTimeSeconds=3,
                VisibilityTimeout=5,         # Short — just peeking
                MessageAttributeNames=["All"]
            )
            
            messages = response.get("Messages", [])
            
            print(f"\n   📦 {key}:")
            
            if not messages:
                print(f"      (empty)")
                continue
            
            msg = messages[0]
            body = msg["Body"]
            
            # Try to parse as JSON
            try:
                parsed = json.loads(body)
                # Check if it's SNS-wrapped or raw
                if "Type" in parsed and parsed.get("Type") == "Notification":
                    print(f"      Delivery: SNS-wrapped (not raw)")
                    inner = json.loads(parsed.get("Message", "{}"))
                    print(f"      Inner message: {json.dumps(inner)[:200]}")
                else:
                    print(f"      Delivery: Raw")
                    print(f"      Message: {json.dumps(parsed)[:200]}")
            except json.JSONDecodeError:
                print(f"      Message (text): {body[:200]}")
            
            # Show attributes
            attrs = msg.get("MessageAttributes", {})
            if attrs:
                attr_summary = {k: v.get("StringValue", "") for k, v in attrs.items()}
                print(f"      Attributes: {json.dumps(attr_summary)}")
            
        except ClientError as e:
            print(f"\n   📦 {key}: ❌ {e}")


def consume_and_verify(max_messages=5):
    """Consume messages from all queues and verify content matches"""
    config = load_config()
    sqs_client = boto3.client("sqs", region_name=config["region"])
    
    print(f"\n{'='*65}")
    print(f"🧪 CONSUMING & COMPARING MESSAGES ACROSS QUEUES")
    print(f"{'='*65}")
    
    # Collect messages from each queue
    all_messages = {}
    
    for key, info in config["queues"].items():
        messages = []
        
        response = sqs_client.receive_message(
            QueueUrl=info["url"],
            MaxNumberOfMessages=min(max_messages, 10),
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"]
        )
        
        for msg in response.get("Messages", []):
            body = msg["Body"]
            try:
                parsed = json.loads(body)
                # Handle raw vs wrapped
                if "Type" in parsed and parsed.get("Type") == "Notification":
                    inner = json.loads(parsed.get("Message", "{}"))
                else:
                    inner = parsed
                
                event_id = inner.get("event_id", "unknown")
                messages.append({
                    "event_id": event_id,
                    "receipt_handle": msg["ReceiptHandle"]
                })
            except (json.JSONDecodeError, KeyError):
                messages.append({
                    "event_id": "parse_error",
                    "receipt_handle": msg["ReceiptHandle"]
                })
        
        all_messages[key] = messages
        print(f"\n   📦 {key}: {len(messages)} messages")
        for m in messages:
            print(f"      → event_id: {m['event_id'][:20]}...")
    
    # Compare event IDs across queues
    print(f"\n   🔍 Cross-queue comparison:")
    
    # Get all event IDs from first queue
    if all_messages:
        first_key = list(all_messages.keys())[0]
        first_ids = set(m["event_id"] for m in all_messages[first_key])
        
        all_consistent = True
        for key, msgs in all_messages.items():
            ids = set(m["event_id"] for m in msgs)
            if ids == first_ids:
                print(f"      ✅ {key}: matches ({len(ids)} events)")
            else:
                missing = first_ids - ids
                extra = ids - first_ids
                print(f"      ⚠️  {key}: differs")
                if missing:
                    print(f"         Missing: {missing}")
                if extra:
                    print(f"         Extra: {extra}")
                all_consistent = False
        
        if all_consistent:
            print(f"\n   ✅ ALL QUEUES RECEIVED IDENTICAL EVENTS — fan-out verified!")
        else:
            print(f"\n   ⚠️  INCONSISTENCY DETECTED — investigate queue policies")
    
    # Clean up — delete consumed messages
    print(f"\n   🗑️  Cleaning up consumed messages...")
    for key, msgs in all_messages.items():
        if msgs:
            entries = [
                {"Id": str(i), "ReceiptHandle": m["receipt_handle"]}
                for i, m in enumerate(msgs)
            ]
            # Batch delete (max 10 per call)
            for i in range(0, len(entries), 10):
                batch = entries[i:i+10]
                sqs_client.delete_message_batch(
                    QueueUrl=config["queues"][key]["url"],
                    Entries=batch
                )
            print(f"      {key}: {len(msgs)} messages deleted")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "peek":
        verify_fanout()
        peek_all_queues()
    elif len(sys.argv) > 1 and sys.argv[1] == "consume":
        consume_and_verify()
    else:
        verify_fanout()
        print(f"\nTip: Run 'python3 03_verify_fanout.py peek' to see message content")
        print(f"     Run 'python3 03_verify_fanout.py consume' to consume and compare")