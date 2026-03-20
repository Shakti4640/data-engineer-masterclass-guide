# file: 01_create_sns_topic.py
# Creates SNS topic for S3 file arrival events
# Configures: S3 permission, email subscription, SQS subscription

import boto3
import json
import time

REGION = "us-east-2"
TOPIC_NAME = "quickcart-incoming-file-events"
BUCKET_NAME = "quickcart-raw-data-prod"
QUEUE_NAME = "quickcart-file-processing-queue"  # From Project 27
EMAIL = "data-team@quickcart.com"


def create_topic_with_subscriptions():
    sns_client = boto3.client("sns", region_name=REGION)
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("=" * 60)
    print("📢 CREATING SNS TOPIC FOR FILE EVENTS")
    print("=" * 60)

    # --- STEP 1: CREATE TOPIC ---
    response = sns_client.create_topic(
        Name=TOPIC_NAME,
        Tags=[
            {"Key": "Environment", "Value": "Production"},
            {"Key": "Purpose", "Value": "S3-File-Events"}
        ]
    )
    topic_arn = response["TopicArn"]
    print(f"✅ Topic created: {topic_arn}")

    # --- STEP 2: SET TOPIC POLICY (allow S3 to publish) ---
    account_id = topic_arn.split(":")[4]

    topic_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowS3Publish",
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "SNS:Publish",
                "Resource": topic_arn,
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:::{BUCKET_NAME}"
                    }
                }
            }
        ]
    }

    sns_client.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(topic_policy)
    )
    print(f"✅ Topic policy set — S3 can publish events")

    # --- STEP 3: SUBSCRIBE SQS QUEUE ---
    queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    queue_attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )
    queue_arn = queue_attrs["Attributes"]["QueueArn"]

    # Update SQS policy to allow SNS to send messages
    sqs_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowSNSMessages",
                "Effect": "Allow",
                "Principal": {"Service": "sns.amazonaws.com"},
                "Action": "SQS:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnEquals": {"aws:SourceArn": topic_arn}
                }
            },
            {
                "Sid": "AllowS3DirectMessages",
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "SQS:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:::{BUCKET_NAME}"
                    }
                }
            }
        ]
    }

    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(sqs_policy)}
    )

    # Subscribe SQS to SNS
    sqs_sub = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={
            "RawMessageDelivery": "false"  # Keep SNS envelope (we handle unwrapping)
        }
    )
    print(f"✅ SQS subscribed: {QUEUE_NAME}")

    # --- STEP 4: SUBSCRIBE EMAIL ---
    email_sub = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=EMAIL
    )
    print(f"📧 Email subscription pending confirmation: {EMAIL}")
    print(f"   → Check inbox and click 'Confirm subscription' link")

    print(f"\n📋 TOPIC SUMMARY:")
    print(f"   Topic ARN: {topic_arn}")
    print(f"   S3 → SNS: ✅ allowed")
    print(f"   SNS → SQS: ✅ subscribed")
    print(f"   SNS → Email: ⏳ pending confirmation")

    return topic_arn


if __name__ == "__main__":
    create_topic_with_subscriptions()