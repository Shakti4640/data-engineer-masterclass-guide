# file: 18_06_cleanup.py
# Purpose: Remove S3→SQS notification and delete SQS queue

import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"
SOURCE_BUCKET = "quickcart-raw-data-prod"
QUEUE_NAME = "quickcart-file-processing-queue"


def remove_s3_sqs_notification():
    """Remove our SQS notification from S3 bucket (preserves others)"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print("STEP 1: Removing S3 → SQS event notification")
    print("=" * 70)

    config = s3_client.get_bucket_notification_configuration(Bucket=SOURCE_BUCKET)
    config.pop("ResponseMetadata", None)

    original_count = len(config.get("QueueConfigurations", []))

    # Remove our specific notification by ID
    config["QueueConfigurations"] = [
        qc for qc in config.get("QueueConfigurations", [])
        if qc.get("Id") != "daily-csv-sqs-processing"
    ]

    new_count = len(config.get("QueueConfigurations", []))

    if original_count == new_count:
        print("   ℹ️  Notification 'daily-csv-sqs-processing' not found")
        return

    # Remove empty lists
    config = {k: v for k, v in config.items() if v}
    if not config:
        config = {}

    s3_client.put_bucket_notification_configuration(
        Bucket=SOURCE_BUCKET,
        NotificationConfiguration=config
    )
    print(f"   ✅ SQS notification removed")
    print(f"   → Remaining queue notifications: {new_count}")


def purge_and_delete_queue():
    """Purge all messages then delete the queue"""
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("\n" + "=" * 70)
    print("STEP 2: Purging and deleting SQS queue")
    print("=" * 70)

    try:
        queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    except ClientError as e:
        if "NonExistentQueue" in str(e) or "QueueDoesNotExist" in str(e):
            print(f"   ℹ️  Queue '{QUEUE_NAME}' does not exist")
            return
        raise

    # Check message count before purge
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    msg_count = int(attrs["ApproximateNumberOfMessages"])

    if msg_count > 0:
        print(f"   ⚠️  {msg_count} messages in queue — purging...")
        try:
            sqs_client.purge_queue(QueueUrl=queue_url)
            print(f"   ✅ Queue purged (may take up to 60 seconds)")
        except ClientError as e:
            if "PurgeQueueInProgress" in str(e):
                print(f"   ℹ️  Purge already in progress")
            else:
                raise
    else:
        print(f"   ✅ Queue already empty")

    # Delete queue
    sqs_client.delete_queue(QueueUrl=queue_url)
    print(f"   ✅ Queue '{QUEUE_NAME}' deleted")
    print(f"   ⚠️  Queue name unavailable for 60 seconds (AWS restriction)")


def verify_cleanup():
    """Confirm everything is cleaned up"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("\n" + "=" * 70)
    print("VERIFICATION")
    print("=" * 70)

    config = s3_client.get_bucket_notification_configuration(Bucket=SOURCE_BUCKET)
    queue_count = len(config.get("QueueConfigurations", []))
    topic_count = len(config.get("TopicConfigurations", []))
    lambda_count = len(config.get("LambdaFunctionConfigurations", []))
    total = queue_count + topic_count + lambda_count

    print(f"   S3 notifications remaining: {total}")
    print(f"     SNS: {topic_count} | SQS: {queue_count} | Lambda: {lambda_count}")

    if total == 0:
        print(f"   ✅ No event notifications on bucket")
    else:
        print(f"   → Other notifications preserved")


if __name__ == "__main__":
    print("🧹 PROJECT 18 — CLEANUP")
    print("=" * 70)

    remove_s3_sqs_notification()
    purge_and_delete_queue()
    verify_cleanup()

    print("\n" + "=" * 70)
    print("🎯 CLEANUP COMPLETE")
    print("   → S3 → SQS notification: removed")
    print("   → SQS queue: purged and deleted")
    print("   → S3 source files: UNTOUCHED")
    print("   → Other bucket notifications: PRESERVED")
    print("=" * 70)