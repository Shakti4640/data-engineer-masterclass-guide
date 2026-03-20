# file: 01_create_sqs_queue.py
# Creates main processing queue + dead letter queue
# Configures: long polling, visibility timeout, DLQ redrive policy

import boto3
import json

REGION = "us-east-2"
QUEUE_NAME = "quickcart-file-processing-queue"
DLQ_NAME = "quickcart-file-processing-dlq"


def create_queues():
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("=" * 60)
    print("📬 CREATING SQS QUEUES")
    print("=" * 60)

    # --- STEP 1: CREATE DEAD LETTER QUEUE FIRST ---
    # DLQ must exist before main queue references it
    try:
        dlq_response = sqs_client.create_queue(
            QueueName=DLQ_NAME,
            Attributes={
                "MessageRetentionPeriod": "1209600",  # 14 days (maximum)
                "ReceiveMessageWaitTimeSeconds": "20", # Long polling
                "VisibilityTimeout": "300"
            },
            tags={
                "Environment": "Production",
                "Purpose": "Dead-Letter-Queue",
                "Team": "Data-Engineering"
            }
        )
        dlq_url = dlq_response["QueueUrl"]
        print(f"✅ DLQ created: {dlq_url}")
    except Exception as e:
        if "QueueAlreadyExists" in str(e):
            dlq_url = sqs_client.get_queue_url(QueueName=DLQ_NAME)["QueueUrl"]
            print(f"ℹ️  DLQ already exists: {dlq_url}")
        else:
            raise

    # Get DLQ ARN (needed for redrive policy)
    dlq_attrs = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["QueueArn"]
    )
    dlq_arn = dlq_attrs["Attributes"]["QueueArn"]

    # --- STEP 2: CREATE MAIN QUEUE WITH REDRIVE POLICY ---
    redrive_policy = {
        "deadLetterTargetArn": dlq_arn,
        "maxReceiveCount": "3"    # After 3 failures → move to DLQ
    }

    try:
        queue_response = sqs_client.create_queue(
            QueueName=QUEUE_NAME,
            Attributes={
                "VisibilityTimeout": "300",            # 5 minutes
                "MessageRetentionPeriod": "345600",    # 4 days
                "ReceiveMessageWaitTimeSeconds": "20",  # Long polling (queue-level default)
                "RedrivePolicy": json.dumps(redrive_policy)
            },
            tags={
                "Environment": "Production",
                "Purpose": "File-Processing",
                "Team": "Data-Engineering"
            }
        )
        queue_url = queue_response["QueueUrl"]
        print(f"✅ Main queue created: {queue_url}")
    except Exception as e:
        if "QueueAlreadyExists" in str(e):
            queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
            print(f"ℹ️  Main queue already exists: {queue_url}")
        else:
            raise

    # --- STEP 3: VERIFY CONFIGURATION ---
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["All"]
    )["Attributes"]

    print(f"\n📋 Queue Configuration:")
    print(f"   Queue URL:          {queue_url}")
    print(f"   Queue ARN:          {attrs['QueueArn']}")
    print(f"   Visibility Timeout: {attrs['VisibilityTimeout']}s")
    print(f"   Retention:          {int(attrs['MessageRetentionPeriod'])//86400} days")
    print(f"   Long Poll Wait:     {attrs['ReceiveMessageWaitTimeSeconds']}s")
    print(f"   DLQ:                {DLQ_NAME}")
    print(f"   Max Receive Count:  3 (then → DLQ)")

    # --- STEP 4: SET QUEUE POLICY FOR S3 EVENTS ---
    # Allow S3 to send messages to this queue (for Project 18 integration)
    queue_arn = attrs["QueueArn"]
    account_id = queue_arn.split(":")[4]
    bucket_name = "quickcart-raw-data-prod"

    queue_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowS3Events",
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "SQS:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:::{bucket_name}"
                    }
                }
            }
        ]
    }

    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "Policy": json.dumps(queue_policy)
        }
    )
    print(f"\n✅ Queue policy set — S3 can send events to this queue")

    return queue_url, dlq_url


if __name__ == "__main__":
    queue_url, dlq_url = create_queues()
    print(f"\n🔑 Queue URL (use in worker): {queue_url}")
    print(f"🔑 DLQ URL (monitor for failures): {dlq_url}")