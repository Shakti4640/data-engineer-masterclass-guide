# file: 51_08_cleanup.py
# Purpose: Tear down all infrastructure created in this project

import boto3
import json

REGION = "us-east-2"


def cleanup_all():
    """Remove all SNS topics, SQS queues, and CloudWatch alarms"""
    sns_client = boto3.client("sns", region_name=REGION)
    sqs_client = boto3.client("sqs", region_name=REGION)
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    print("=" * 70)
    print("🧹 CLEANING UP PROJECT 51 INFRASTRUCTURE")
    print("=" * 70)

    # Load infrastructure config
    try:
        with open("/tmp/filtered_infra.json", "r") as f:
            infra = json.load(f)
    except FileNotFoundError:
        print("⚠️  No infrastructure config found. Manual cleanup needed.")
        return

    # --- Delete subscriptions first ---
    print("\n📌 Removing subscriptions...")
    topic_arn = infra["topic_arn"]
    subscriptions = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
    for sub in subscriptions.get("Subscriptions", []):
        if sub["SubscriptionArn"] != "PendingConfirmation":
            sns_client.unsubscribe(SubscriptionArn=sub["SubscriptionArn"])
            print(f"  ✅ Unsubscribed: {sub['Endpoint'].split(':')[-1]}")

    # --- Delete SQS queues ---
    print("\n📌 Deleting SQS queues...")
    for q in infra["queues"]:
        try:
            sqs_client.delete_queue(QueueUrl=q["queue_url"])
            print(f"  ✅ Deleted: {q['queue_name']}")
        except Exception as e:
            print(f"  ⚠️  Could not delete {q['queue_name']}: {e}")

    # --- Delete SNS topic ---
    print("\n📌 Deleting SNS topic...")
    sns_client.delete_topic(TopicArn=topic_arn)
    print(f"  ✅ Deleted: {topic_arn.split(':')[-1]}")

    # --- Delete CloudWatch alarms ---
    print("\n📌 Deleting CloudWatch alarms...")
    alarm_names = [
        "SNS-FilteredOut-NoAttributes-High",
        "SNS-DeliveryFailures-High"
    ]
    try:
        cw_client.delete_alarms(AlarmNames=alarm_names)
        for name in alarm_names:
            print(f"  ✅ Deleted alarm: {name}")
    except Exception as e:
        print(f"  ⚠️  Could not delete alarms: {e}")

    # --- Delete alert topic ---
    print("\n📌 Deleting alert topic...")
    try:
        # Find and delete the filter-alerts topic
        topics = sns_client.list_topics()
        for topic in topics.get("Topics", []):
            if "quickcart-filter-alerts" in topic["TopicArn"]:
                sns_client.delete_topic(TopicArn=topic["TopicArn"])
                print(f"  ✅ Deleted: quickcart-filter-alerts")
    except Exception as e:
        print(f"  ⚠️  Could not delete alert topic: {e}")

    print("\n" + "=" * 70)
    print("🎉 ALL PROJECT 51 RESOURCES CLEANED UP")
    print("=" * 70)


if __name__ == "__main__":
    cleanup_all()