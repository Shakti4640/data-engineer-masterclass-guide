# file: 17_01_create_sns_topic.py
# Purpose: Create SNS Topic that S3 is ALLOWED to publish to
# Dependency: SNS concept from Project 12, S3 bucket from Project 1

import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"
TOPIC_NAME = "quickcart-s3-file-alerts"
SOURCE_BUCKET = "quickcart-raw-data-prod"


def get_account_id():
    """Fetch current AWS account ID dynamically via STS"""
    sts_client = boto3.client("sts", region_name=REGION)
    return sts_client.get_caller_identity()["Account"]


def create_sns_topic_with_s3_permission():
    sns_client = boto3.client("sns", region_name=REGION)
    account_id = get_account_id()

    # ── STEP 1: CREATE TOPIC ──
    print("=" * 70)
    print("STEP 1: Creating SNS Topic")
    print("=" * 70)

    response = sns_client.create_topic(
        Name=TOPIC_NAME,
        Tags=[
            {"Key": "Purpose", "Value": "S3-File-Arrival-Alerts"},
            {"Key": "SourceBucket", "Value": SOURCE_BUCKET},
            {"Key": "Team", "Value": "Data-Engineering"}
        ]
    )
    topic_arn = response["TopicArn"]
    print(f"   ✅ Topic created: {topic_arn}")

    # ── STEP 2: SET TOPIC POLICY (CRITICAL!) ──
    # This policy ALLOWS S3 to publish messages to this topic
    # Without this: put_bucket_notification_configuration will FAIL
    #
    # WHY S3 NEEDS EXPLICIT PERMISSION:
    # → S3 is a separate service — it calls SNS Publish API
    # → SNS denies by default unless policy explicitly allows
    # → The "Principal" is the S3 service, not a user/role
    # → Condition restricts to specific bucket ARN (security best practice)
    print("\n" + "=" * 70)
    print("STEP 2: Setting Topic Policy (allow S3 to publish)")
    print("=" * 70)

    topic_policy = {
        "Version": "2012-10-17",
        "Id": "S3PublishPolicy",
        "Statement": [
            {
                "Sid": "AllowS3ToPublish",
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": "SNS:Publish",
                "Resource": topic_arn,
                "Condition": {
                    "ArnLike": {
                        # Only allow events from OUR specific bucket
                        # Without this condition: ANY S3 bucket in ANY
                        # account could publish to our topic
                        "aws:SourceArn": f"arn:aws:s3:::{SOURCE_BUCKET}"
                    },
                    "StringEquals": {
                        # Only allow from our account
                        "aws:SourceAccount": account_id
                    }
                }
            },
            {
                "Sid": "AllowAccountToManageTopic",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:root"
                },
                "Action": [
                    "SNS:Subscribe",
                    "SNS:Publish",
                    "SNS:GetTopicAttributes",
                    "SNS:SetTopicAttributes",
                    "SNS:DeleteTopic"
                ],
                "Resource": topic_arn
            }
        ]
    }

    sns_client.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(topic_policy)
    )
    print("   ✅ Topic Policy set — S3 can now publish to this topic")
    print(f"   → Restricted to bucket: {SOURCE_BUCKET}")
    print(f"   → Restricted to account: {account_id}")

    return topic_arn


def subscribe_email(topic_arn, email_address):
    """
    Subscribe an email address to the topic
    
    IMPORTANT:
    → Subscription stays "PendingConfirmation" until email link is clicked
    → No messages delivered until confirmed
    → Check spam folder — AWS confirmation emails often land there
    """
    sns_client = boto3.client("sns", region_name=REGION)

    print("\n" + "=" * 70)
    print(f"STEP 3: Subscribing email: {email_address}")
    print("=" * 70)

    response = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=email_address,
        ReturnSubscriptionArn=True
    )

    sub_arn = response["SubscriptionArn"]

    if sub_arn == "pending confirmation":
        print(f"   ⏳ Subscription PENDING — check email inbox")
        print(f"   → Email sent to: {email_address}")
        print(f"   → Subject: 'AWS Notification - Subscription Confirmation'")
        print(f"   → Click 'Confirm subscription' link in email")
        print(f"   → Check SPAM folder if not in inbox")
        print(f"   → No alerts delivered until confirmed!")
    else:
        print(f"   ✅ Subscription confirmed: {sub_arn}")

    return sub_arn


def verify_topic_setup(topic_arn):
    """Verify topic exists and policy is correct"""
    sns_client = boto3.client("sns", region_name=REGION)

    print("\n" + "=" * 70)
    print("VERIFICATION: Topic configuration")
    print("=" * 70)

    attrs = sns_client.get_topic_attributes(TopicArn=topic_arn)
    attributes = attrs["Attributes"]

    print(f"   Topic ARN:           {attributes['TopicArn']}")
    print(f"   Subscriptions Total: {attributes['SubscriptionsConfirmed']} confirmed, "
          f"{attributes['SubscriptionsPending']} pending")
    print(f"   Display Name:        {attributes.get('DisplayName', '(none)')}")

    # Parse and verify policy
    policy = json.loads(attributes["Policy"])
    for stmt in policy["Statement"]:
        if stmt.get("Sid") == "AllowS3ToPublish":
            print(f"   S3 Publish Allowed:  ✅ Yes")
            condition = stmt.get("Condition", {})
            source_arn = condition.get("ArnLike", {}).get("aws:SourceArn", "N/A")
            print(f"   Restricted to:       {source_arn}")
            break
    else:
        print(f"   S3 Publish Allowed:  ❌ No — Topic Policy missing!")


if __name__ == "__main__":
    topic_arn = create_sns_topic_with_s3_permission()

    # Subscribe an email (change to your email)
    email = "data-team@example.com"
    subscribe_email(topic_arn, email)

    # Verify setup
    verify_topic_setup(topic_arn)

    print(f"\n🎯 Topic ARN (save this): {topic_arn}")
    print(f"   → Confirm email subscription before proceeding")
    print(f"   → Then run 17_02_configure_s3_events.py")