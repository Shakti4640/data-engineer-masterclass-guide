# file: 50_02_dlq_inspector.py
# Purpose: Inspect DLQ messages without consuming them
# View failed message details for investigation

import boto3
import json
from datetime import datetime, timezone

REGION = "us-east-2"

# All DLQs to monitor
DLQ_NAMES = [
    "quickcart-file-process-q-dlq",
    "quickcart-notification-q-dlq",
    "quickcart-etl-trigger-q-dlq"
]


def inspect_all_dlqs():
    """Check all DLQs for messages and display details"""
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("╔" + "═" * 68 + "╗")
    print("║" + "  🔍 DLQ INSPECTION REPORT".center(68) + "║")
    print("║" + f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    total_messages = 0

    for dlq_name in DLQ_NAMES:
        try:
            dlq_url = sqs_client.get_queue_url(QueueName=dlq_name)["QueueUrl"]
        except Exception:
            print(f"\n   ⚠️  Queue not found: {dlq_name}")
            continue

        # Get queue attributes
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
                "CreatedTimestamp",
                "MessageRetentionPeriod"
            ]
        )["Attributes"]

        visible = int(attrs.get("ApproximateNumberOfMessages", 0))
        not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        retention_days = int(attrs.get("MessageRetentionPeriod", 0)) // 86400
        total_in_queue = visible + not_visible

        total_messages += total_in_queue

        status = "✅" if total_in_queue == 0 else "❌"

        print(f"\n{'─'*60}")
        print(f"{status} DLQ: {dlq_name}")
        print(f"{'─'*60}")
        print(f"   Messages visible: {visible}")
        print(f"   Messages in-flight: {not_visible}")
        print(f"   Retention: {retention_days} days")

        if visible == 0:
            print(f"   ✅ No failed messages — all clear")
            continue

        # Peek at messages (receive but DON'T delete)
        print(f"\n   📋 Failed messages ({min(visible, 10)} shown):")

        messages = sqs_client.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=10,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            VisibilityTimeout=0,  # Immediately visible again (peek only)
            WaitTimeSeconds=0
        ).get("Messages", [])

        for i, msg in enumerate(messages, 1):
            msg_id = msg["MessageId"]
            body = msg["Body"]
            attrs = msg.get("Attributes", {})

            receive_count = attrs.get("ApproximateReceiveCount", "?")
            sent_ts = attrs.get("SentTimestamp", "0")
            first_receive_ts = attrs.get("ApproximateFirstReceiveTimestamp", "0")

            # Calculate age
            if sent_ts != "0":
                sent_time = datetime.fromtimestamp(int(sent_ts) / 1000, tz=timezone.utc)
                age = datetime.now(timezone.utc) - sent_time
                age_str = f"{age.total_seconds() / 3600:.1f} hours"
            else:
                age_str = "unknown"

            print(f"\n   ── Message {i} ──")
            print(f"   ID: {msg_id}")
            print(f"   Age: {age_str}")
            print(f"   Receive count: {receive_count}")
            print(f"   Sent: {sent_time.strftime('%Y-%m-%d %H:%M:%S') if sent_ts != '0' else 'unknown'}")

            # Parse body
            try:
                body_json = json.loads(body)
                # Check if it's an SNS wrapper
                if "Message" in body_json and "TopicArn" in body_json:
                    # SNS-wrapped message — extract inner message
                    inner = json.loads(body_json["Message"])
                    print(f"   Source: SNS ({body_json.get('TopicArn', 'unknown')})")
                    print(f"   Inner message: {json.dumps(inner, indent=2)[:200]}")
                else:
                    print(f"   Body: {json.dumps(body_json, indent=2)[:200]}")
            except json.JSONDecodeError:
                print(f"   Body (raw): {body[:200]}")

            # Custom message attributes
            msg_attrs = msg.get("MessageAttributes", {})
            if msg_attrs:
                print(f"   Custom attributes:")
                for key, val in msg_attrs.items():
                    print(f"      {key}: {val.get('StringValue', val.get('BinaryValue', '?'))}")

    # Summary
    print(f"\n{'='*60}")
    if total_messages == 0:
        print(f"✅ ALL DLQs EMPTY — no failed messages")
    else:
        print(f"❌ TOTAL FAILED MESSAGES ACROSS ALL DLQs: {total_messages}")
        print(f"   Action: investigate root causes, then redrive or delete")
    print(f"{'='*60}")

    return total_messages


if __name__ == "__main__":
    inspect_all_dlqs()