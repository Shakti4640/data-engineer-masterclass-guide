# file: 04_monitor_queue.py
# Monitor queue health: depth, age, DLQ, processing rate
# Integrates with SNS alerts (Project 12)

import boto3
from datetime import datetime

REGION = "us-east-2"
QUEUE_NAME = "quickcart-file-processing-queue"
DLQ_NAME = "quickcart-file-processing-dlq"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:123456789012:data-pipeline-alerts"


def get_queue_metrics(sqs_client, queue_name):
    """Get current queue metrics"""
    queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"
        ]
    )["Attributes"]

    return {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
        "in_flight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
    }


def monitor_queues():
    """Display queue health dashboard"""
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("=" * 60)
    print(f"📊 SQS QUEUE MONITOR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Main queue
    main = get_queue_metrics(sqs_client, QUEUE_NAME)
    print(f"\n   📬 Main Queue: {QUEUE_NAME}")
    print(f"      Visible (waiting):    {main['visible']}")
    print(f"      In-flight (processing): {main['in_flight']}")
    print(f"      Delayed:              {main['delayed']}")

    # DLQ
    dlq = get_queue_metrics(sqs_client, DLQ_NAME)
    print(f"\n   💀 Dead Letter Queue: {DLQ_NAME}")
    print(f"      Messages:             {dlq['visible']}")

    # Health assessment
    print(f"\n   🏥 HEALTH CHECK:")
    alerts = []

    if main["visible"] > 100:
        print(f"      ⚠️  HIGH BACKLOG: {main['visible']} messages waiting")
        alerts.append(f"Queue backlog: {main['visible']} messages")
    elif main["visible"] > 0:
        print(f"      ℹ️  {main['visible']} messages waiting (normal)")
    else:
        print(f"      ✅ Queue empty — worker is caught up")

    if dlq["visible"] > 0:
        print(f"      ❌ DLQ HAS {dlq['visible']} FAILED MESSAGE(S)")
        alerts.append(f"DLQ contains {dlq['visible']} failed messages")
    else:
        print(f"      ✅ DLQ empty — no failures")

    # Send alerts if needed
    if alerts:
        try:
            sns_client = boto3.client("sns", region_name=REGION)
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"⚠️ SQS Queue Alert — {len(alerts)} issue(s)",
                Message="\n".join([
                    "SQS QUEUE HEALTH ALERT",
                    f"Time: {datetime.now().isoformat()}",
                    "",
                    "Issues:",
                    *[f"• {a}" for a in alerts],
                    "",
                    f"Main queue depth: {main['visible']}",
                    f"In-flight: {main['in_flight']}",
                    f"DLQ depth: {dlq['visible']}"
                ])
            )
            print(f"\n   📧 Alert sent to SNS")
        except Exception as e:
            print(f"\n   ⚠️  Could not send alert: {e}")

    print("=" * 60)


if __name__ == "__main__":
    monitor_queues()