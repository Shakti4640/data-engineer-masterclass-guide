# file: 17_05_monitor_notifications.py
# Purpose: View all active S3 event notifications and SNS subscription status
# Useful for debugging and operational visibility

import boto3
import json
from datetime import datetime, timedelta

REGION = "us-east-2"
SOURCE_BUCKET = "quickcart-raw-data-prod"
TOPIC_NAME = "quickcart-s3-file-alerts"


def show_bucket_notifications():
    """Display all event notification configurations on the bucket"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print(f"S3 BUCKET NOTIFICATIONS: {SOURCE_BUCKET}")
    print("=" * 70)

    config = s3_client.get_bucket_notification_configuration(
        Bucket=SOURCE_BUCKET
    )

    # ── SNS NOTIFICATIONS ──
    topic_configs = config.get("TopicConfigurations", [])
    print(f"\n   📢 SNS Topic Notifications: {len(topic_configs)}")
    for tc in topic_configs:
        print(f"\n      ID:      {tc.get('Id', 'N/A')}")
        print(f"      Topic:   {tc['TopicArn']}")
        print(f"      Events:  {', '.join(tc['Events'])}")
        filters = tc.get("Filter", {}).get("Key", {}).get("FilterRules", [])
        for f in filters:
            print(f"      Filter:  {f['Name']} = '{f['Value']}'")

    # ── SQS NOTIFICATIONS ──
    queue_configs = config.get("QueueConfigurations", [])
    print(f"\n   📬 SQS Queue Notifications: {len(queue_configs)}")
    for qc in queue_configs:
        print(f"\n      ID:      {qc.get('Id', 'N/A')}")
        print(f"      Queue:   {qc['QueueArn']}")
        print(f"      Events:  {', '.join(qc['Events'])}")
        filters = qc.get("Filter", {}).get("Key", {}).get("FilterRules", [])
        for f in filters:
            print(f"      Filter:  {f['Name']} = '{f['Value']}'")

    # ── LAMBDA NOTIFICATIONS ──
    lambda_configs = config.get("LambdaFunctionConfigurations", [])
    print(f"\n   ⚡ Lambda Function Notifications: {len(lambda_configs)}")
    for lc in lambda_configs:
        print(f"\n      ID:       {lc.get('Id', 'N/A')}")
        print(f"      Lambda:   {lc['LambdaFunctionArn']}")
        print(f"      Events:   {', '.join(lc['Events'])}")

    # ── EVENTBRIDGE ──
    eb_config = config.get("EventBridgeConfiguration", {})
    if eb_config:
        print(f"\n   🌉 EventBridge: ENABLED")
    else:
        print(f"\n   🌉 EventBridge: not configured")

    total = len(topic_configs) + len(queue_configs) + len(lambda_configs)
    print(f"\n   TOTAL: {total} notification rules active")


def show_sns_subscriptions():
    """Show all subscriptions for our SNS topic"""
    sns_client = boto3.client("sns", region_name=REGION)
    sts_client = boto3.client("sts", region_name=REGION)
    account_id = sts_client.get_caller_identity()["Account"]
    topic_arn = f"arn:aws:sns:{REGION}:{account_id}:{TOPIC_NAME}"

    print("\n" + "=" * 70)
    print(f"SNS TOPIC SUBSCRIPTIONS: {TOPIC_NAME}")
    print("=" * 70)

    response = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)

    subscriptions = response.get("Subscriptions", [])
    print(f"\n   Total subscriptions: {len(subscriptions)}")

    for sub in subscriptions:
        sub_arn = sub["SubscriptionArn"]
        protocol = sub["Protocol"]
        endpoint = sub["Endpoint"]

        # Determine status
        if sub_arn == "PendingConfirmation":
            status = "⏳ PENDING CONFIRMATION"
        elif sub_arn.startswith("arn:"):
            status = "✅ CONFIRMED"
        else:
            status = "❓ UNKNOWN"

        print(f"\n      Protocol:     {protocol}")
        print(f"      Endpoint:     {endpoint}")
        print(f"      Status:       {status}")
        print(f"      ARN:          {sub_arn}")


def show_recent_sns_metrics():
    """Show CloudWatch metrics for SNS topic — last 24 hours"""
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    print("\n" + "=" * 70)
    print(f"SNS CLOUDWATCH METRICS (last 24 hours): {TOPIC_NAME}")
    print("=" * 70)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)

    metrics_to_check = [
        ("NumberOfMessagesPublished", "Messages Published (by S3)"),
        ("NumberOfNotificationsDelivered", "Notifications Delivered (to subscribers)"),
        ("NumberOfNotificationsFailed", "Notifications Failed"),
    ]

    for metric_name, label in metrics_to_check:
        response = cloudwatch.get_metric_statistics(
            Namespace="AWS/SNS",
            MetricName=metric_name,
            Dimensions=[
                {"Name": "TopicName", "Value": TOPIC_NAME}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,  # 24 hours as single data point
            Statistics=["Sum"]
        )

        datapoints = response.get("Datapoints", [])
        if datapoints:
            total = int(sum(dp["Sum"] for dp in datapoints))
            print(f"   {label}: {total}")
        else:
            print(f"   {label}: 0 (or no data yet)")

    print(f"\n   ℹ️  Metrics may take 1-5 minutes to appear after events")


if __name__ == "__main__":
    show_bucket_notifications()
    show_sns_subscriptions()
    show_recent_sns_metrics()