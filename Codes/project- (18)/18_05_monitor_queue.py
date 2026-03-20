# file: 18_05_monitor_queue.py
# Purpose: Monitor SQS queue health — messages waiting, in-flight, age
# Used for operational visibility and debugging

import boto3
from datetime import datetime, timedelta

REGION = "us-east-2"
QUEUE_NAME = "quickcart-file-processing-queue"


def get_queue_url():
    sqs_client = boto3.client("sqs", region_name=REGION)
    return sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]


def show_queue_dashboard():
    """Comprehensive queue health dashboard"""
    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = get_queue_url()

    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["All"]
    )["Attributes"]

    available = int(attrs["ApproximateNumberOfMessages"])
    in_flight = int(attrs["ApproximateNumberOfMessagesNotVisible"])
    delayed = int(attrs["ApproximateNumberOfMessagesDelayed"])
    total = available + in_flight + delayed

    vis_timeout = int(attrs["VisibilityTimeout"])
    retention = int(attrs["MessageRetentionPeriod"])
    wait_time = int(attrs["ReceiveMessageWaitTimeSeconds"])
    created = datetime.fromtimestamp(int(attrs["CreatedTimestamp"]))
    modified = datetime.fromtimestamp(int(attrs["LastModifiedTimestamp"]))

    print("=" * 70)
    print(f"📊 SQS QUEUE DASHBOARD: {QUEUE_NAME}")
    print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # ── MESSAGE COUNTS ──
    print(f"\n   📬 MESSAGE STATUS:")
    print(f"   ┌──────────────────────────┬─────────┐")
    print(f"   │ Available (waiting)      │ {available:>7} │")
    print(f"   │ In-Flight (processing)   │ {in_flight:>7} │")
    print(f"   │ Delayed                  │ {delayed:>7} │")
    print(f"   │ TOTAL                    │ {total:>7} │")
    print(f"   └──────────────────────────┴─────────┘")

    # ── HEALTH INDICATORS ──
    print(f"\n   🏥 HEALTH INDICATORS:")
    if available == 0 and in_flight == 0:
        print(f"   ✅ Queue is empty — all messages processed")
    elif available > 0 and in_flight == 0:
        print(f"   ⚠️  {available} messages waiting — no consumer active!")
        print(f"   → Start worker: python 18_04_continuous_worker.py")
    elif available > 100:
        print(f"   🔴 {available} messages backlog — consumer too slow")
        print(f"   → Consider: multiple workers (Project 52)")
    elif in_flight > 0:
        print(f"   🟢 {in_flight} message(s) being processed now")

    # ── QUEUE CONFIGURATION ──
    print(f"\n   ⚙️  QUEUE CONFIGURATION:")
    print(f"   ┌──────────────────────────┬──────────────────────┐")
    print(f"   │ Visibility Timeout       │ {vis_timeout}s ({vis_timeout//60}min){' ':>8}│")
    print(f"   │ Message Retention        │ {retention}s ({retention//86400}days){' ':>5}│")
    print(f"   │ Long Poll Wait Time      │ {wait_time}s{' ':>18}│")
    print(f"   │ Created                  │ {created.strftime('%Y-%m-%d %H:%M')}{'':>6}│")
    print(f"   │ Last Modified            │ {modified.strftime('%Y-%m-%d %H:%M')}{'':>6}│")
    print(f"   └──────────────────────────┴──────────────────────┘")

    return available


def show_cloudwatch_metrics():
    """Show SQS CloudWatch metrics for last 24 hours"""
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    print(f"\n   📈 CLOUDWATCH METRICS (last 24 hours):")

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)

    metrics = [
        ("NumberOfMessagesSent", "Messages Sent (by S3)"),
        ("NumberOfMessagesReceived", "Messages Received (by worker)"),
        ("NumberOfMessagesDeleted", "Messages Deleted (processed)"),
        ("ApproximateAgeOfOldestMessage", "Oldest Message Age (seconds)"),
        ("ApproximateNumberOfMessagesVisible", "Avg Visible Messages"),
    ]

    for metric_name, label in metrics:
        stat = "Maximum" if "Age" in metric_name else "Sum"

        response = cloudwatch.get_metric_statistics(
            Namespace="AWS/SQS",
            MetricName=metric_name,
            Dimensions=[
                {"Name": "QueueName", "Value": QUEUE_NAME}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,
            Statistics=[stat]
        )

        datapoints = response.get("Datapoints", [])
        if datapoints:
            value = datapoints[0][stat]
            if "Age" in metric_name and value > 0:
                age_str = f"{value:.0f}s ({value/3600:.1f}hr)"
                print(f"   → {label}: {age_str}")
            else:
                print(f"   → {label}: {int(value)}")
        else:
            print(f"   → {label}: (no data)")


def peek_at_messages():
    """
    Peek at messages without processing them
    
    HOW: Receive with short visibility timeout → message reappears quickly
    USEFUL FOR: Debugging message content without consuming
    """
    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = get_queue_url()

    print(f"\n   👀 PEEKING AT MESSAGES (non-destructive):")

    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=3,
        WaitTimeSeconds=1,
        VisibilityTimeout=5,  # Only hide for 5 seconds → reappears quickly
        AttributeNames=["All"]
    )

    messages = response.get("Messages", [])

    if not messages:
        print(f"   (no messages to peek at)")
        return

    for i, msg in enumerate(messages):
        print(f"\n   ── Message {i+1} ──")
        print(f"   ID:           {msg['MessageId'][:30]}...")
        receive_count = msg["Attributes"].get("ApproximateReceiveCount", "?")
        print(f"   ReceiveCount: {receive_count}")

        # Parse body (first 200 chars)
        body = msg["Body"]
        try:
            parsed = json.loads(body)
            if "Records" in parsed:
                key = parsed["Records"][0]["s3"]["object"]["key"]
                print(f"   S3 Key:       {key}")
            elif parsed.get("Event") == "s3:TestEvent":
                print(f"   Type:         S3 Test Event")
            else:
                print(f"   Body:         {body[:100]}...")
        except Exception:
            print(f"   Body:         {body[:100]}...")

        print(f"   (will reappear in 5 seconds — not consumed)")


if __name__ == "__main__":
    import json

    show_queue_dashboard()
    show_cloudwatch_metrics()
    peek_at_messages()

    print("\n" + "=" * 70)
    print("🎯 MONITORING TIPS:")
    print("   → Run this periodically to check queue health")
    print("   → If 'Available' growing: worker is down or too slow")
    print("   → If 'Oldest Message Age' > 1 hour: investigate backlog")
    print("   → Set CloudWatch Alarm on queue depth (Project 54)")
    print("=" * 70)