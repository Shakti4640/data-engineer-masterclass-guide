# file: 17_06_cleanup.py
# Purpose: Remove S3 event notification and optionally delete SNS topic
# CAREFUL: This removes the notification — files will upload silently again

import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"
SOURCE_BUCKET = "quickcart-raw-data-prod"
TOPIC_NAME = "quickcart-s3-file-alerts"


def remove_s3_notification():
    """
    Remove our specific notification from the bucket
    PRESERVES other notifications (SQS, Lambda, etc.)
    
    WHY NOT JUST PUT EMPTY CONFIG:
    → Empty config deletes ALL notifications on the bucket
    → Other teams may have their own notifications
    → We must surgically remove only ours
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print("STEP 1: Removing S3 → SNS event notification")
    print("=" * 70)

    # Get current config
    config = s3_client.get_bucket_notification_configuration(
        Bucket=SOURCE_BUCKET
    )
    config.pop("ResponseMetadata", None)

    # Remove our specific notification by ID
    original_count = len(config.get("TopicConfigurations", []))
    config["TopicConfigurations"] = [
        tc for tc in config.get("TopicConfigurations", [])
        if tc.get("Id") != "daily-csv-upload-alert"
    ]
    new_count = len(config.get("TopicConfigurations", []))

    if original_count == new_count:
        print("   ℹ️  Notification 'daily-csv-upload-alert' not found")
        print("   → Already removed or never existed")
        return

    # Remove empty lists
    config = {k: v for k, v in config.items() if v}

    # If no notifications left, must pass empty config
    if not config:
        config = {}

    s3_client.put_bucket_notification_configuration(
        Bucket=SOURCE_BUCKET,
        NotificationConfiguration=config
    )

    print(f"   ✅ Notification 'daily-csv-upload-alert' removed")
    print(f"   → Remaining SNS notifications: {new_count}")
    print(f"   → Other notification types: preserved")


def delete_sns_topic():
    """
    Delete SNS topic and all its subscriptions
    
    NOTE: Deleting a topic automatically removes all subscriptions
    No need to unsubscribe individually
    """
    sns_client = boto3.client("sns", region_name=REGION)
    sts_client = boto3.client("sts", region_name=REGION)
    account_id = sts_client.get_caller_identity()["Account"]
    topic_arn = f"arn:aws:sns:{REGION}:{account_id}:{TOPIC_NAME}"

    print("\n" + "=" * 70)
    print("STEP 2: Deleting SNS Topic")
    print("=" * 70)

    # Show subscriptions that will be removed
    try:
        subs = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
        sub_count = len(subs.get("Subscriptions", []))
        print(f"   Subscriptions to be removed: {sub_count}")
        for sub in subs.get("Subscriptions", []):
            print(f"      → {sub['Protocol']}: {sub['Endpoint']}")
    except ClientError:
        print("   Topic may already be deleted")
        return

    # Delete topic (auto-removes all subscriptions)
    try:
        sns_client.delete_topic(TopicArn=topic_arn)
        print(f"\n   ✅ Topic deleted: {topic_arn}")
        print(f"   ✅ All {sub_count} subscriptions auto-removed")
    except ClientError as e:
        if "NotFound" in str(e):
            print(f"   ℹ️  Topic already deleted")
        else:
            raise


def verify_cleanup():
    """Confirm notifications are removed"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("\n" + "=" * 70)
    print("VERIFICATION: Current bucket notification state")
    print("=" * 70)

    config = s3_client.get_bucket_notification_configuration(
        Bucket=SOURCE_BUCKET
    )
    config.pop("ResponseMetadata", None)

    topic_count = len(config.get("TopicConfigurations", []))
    queue_count = len(config.get("QueueConfigurations", []))
    lambda_count = len(config.get("LambdaFunctionConfigurations", []))

    total = topic_count + queue_count + lambda_count

    if total == 0:
        print("   ✅ No event notifications on bucket")
    else:
        print(f"   SNS: {topic_count} | SQS: {queue_count} | Lambda: {lambda_count}")
        print("   → Other notifications preserved (not ours)")


if __name__ == "__main__":
    print("🧹 PROJECT 17 — CLEANUP")
    print("=" * 70)

    # Step 1: Remove S3 notification (safe — preserves others)
    remove_s3_notification()

    # Step 2: Delete SNS topic (removes all subscriptions)
    delete_sns_topic()

    # Verify
    verify_cleanup()

    print("\n" + "=" * 70)
    print("🎯 CLEANUP COMPLETE")
    print("   → S3 notification rule: removed")
    print("   → SNS topic + subscriptions: deleted")
    print("   → S3 source files: UNTOUCHED")
    print("   → Other bucket notifications: PRESERVED")
    print("=" * 70)