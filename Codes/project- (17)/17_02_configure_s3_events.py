# file: 17_02_configure_s3_events.py
# Purpose: Configure S3 bucket to send events to SNS on file upload
# Dependency: SNS topic from 17_01, S3 bucket from Project 1

import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"
SOURCE_BUCKET = "quickcart-raw-data-prod"
TOPIC_NAME = "quickcart-s3-file-alerts"


def get_topic_arn():
    """Construct or look up the SNS Topic ARN"""
    sts_client = boto3.client("sts", region_name=REGION)
    account_id = sts_client.get_caller_identity()["Account"]
    return f"arn:aws:sns:{REGION}:{account_id}:{TOPIC_NAME}"


def get_existing_notification_config(s3_client, bucket):
    """
    GET existing notification config before modifying
    
    WHY THIS IS CRITICAL:
    → put_bucket_notification_configuration is a FULL REPLACE
    → If bucket already has Lambda trigger for another prefix
    → And you PUT only your SNS config
    → The Lambda trigger is SILENTLY DELETED
    → Always: GET → MERGE → PUT
    """
    try:
        response = s3_client.get_bucket_notification_configuration(
            Bucket=bucket
        )
        # Remove ResponseMetadata (not part of config)
        response.pop("ResponseMetadata", None)
        return response
    except ClientError:
        return {}


def configure_s3_event_notification():
    s3_client = boto3.client("s3", region_name=REGION)
    topic_arn = get_topic_arn()

    print("=" * 70)
    print("STEP 1: Fetching existing notification configuration")
    print("=" * 70)

    existing_config = get_existing_notification_config(s3_client, SOURCE_BUCKET)

    if existing_config:
        existing_topics = existing_config.get("TopicConfigurations", [])
        existing_queues = existing_config.get("QueueConfigurations", [])
        existing_lambdas = existing_config.get("LambdaFunctionConfigurations", [])
        print(f"   Existing SNS notifications:    {len(existing_topics)}")
        print(f"   Existing SQS notifications:    {len(existing_queues)}")
        print(f"   Existing Lambda notifications: {len(existing_lambdas)}")
    else:
        existing_config = {}
        print("   No existing notifications found")

    # ── STEP 2: BUILD NEW NOTIFICATION ──
    print("\n" + "=" * 70)
    print("STEP 2: Building S3 → SNS event notification")
    print("=" * 70)

    new_topic_notification = {
        # Unique ID for this notification rule
        # Used in: event JSON ("configurationId" field)
        # Used for: identifying which rule fired
        "Id": "daily-csv-upload-alert",

        # Target SNS Topic ARN
        "TopicArn": topic_arn,

        # Which events trigger this notification
        # Using wildcard to catch ALL creation methods:
        # → PutObject (small files, boto3 default for <8MB)
        # → CompleteMultipartUpload (large files, boto3 auto for >8MB)
        # → CopyObject (S3 copy operations)
        # → PostObject (form-based uploads)
        "Events": [
            "s3:ObjectCreated:*"
        ],

        # Filters: only match files in daily_exports/ ending with .csv
        "Filter": {
            "Key": {
                "FilterRules": [
                    {
                        "Name": "prefix",
                        "Value": "daily_exports/"
                    },
                    {
                        "Name": "suffix",
                        "Value": ".csv"
                    }
                ]
            }
        }
    }

    print(f"   Event:  s3:ObjectCreated:*")
    print(f"   Prefix: daily_exports/")
    print(f"   Suffix: .csv")
    print(f"   Target: {topic_arn}")

    # ── STEP 3: MERGE WITH EXISTING CONFIG ──
    # SAFE MERGE: preserve existing notifications, add ours
    print("\n" + "=" * 70)
    print("STEP 3: Merging with existing configuration (safe update)")
    print("=" * 70)

    # Get existing topic configs, remove any with same ID (update scenario)
    topic_configs = existing_config.get("TopicConfigurations", [])
    topic_configs = [
        tc for tc in topic_configs
        if tc.get("Id") != "daily-csv-upload-alert"
    ]
    # Add our new notification
    topic_configs.append(new_topic_notification)

    # Build complete config preserving ALL existing notifications
    full_config = {
        "TopicConfigurations": topic_configs,
        "QueueConfigurations": existing_config.get("QueueConfigurations", []),
        "LambdaFunctionConfigurations": existing_config.get(
            "LambdaFunctionConfigurations", []
        )
    }

    # Remove empty lists (S3 API doesn't like empty arrays)
    full_config = {k: v for k, v in full_config.items() if v}

    print(f"   Total SNS notifications:    {len(full_config.get('TopicConfigurations', []))}")
    print(f"   Total SQS notifications:    {len(full_config.get('QueueConfigurations', []))}")
    print(f"   Total Lambda notifications: {len(full_config.get('LambdaFunctionConfigurations', []))}")

    # ── STEP 4: APPLY CONFIGURATION ──
    print("\n" + "=" * 70)
    print("STEP 4: Applying notification configuration to S3 bucket")
    print("=" * 70)

    try:
        s3_client.put_bucket_notification_configuration(
            Bucket=SOURCE_BUCKET,
            NotificationConfiguration=full_config
        )
        print(f"   ✅ Notification configured on '{SOURCE_BUCKET}'")
        print(f"   → Any .csv file created under daily_exports/ will trigger SNS")

    except ClientError as e:
        error_msg = str(e)
        if "Unable to validate" in error_msg:
            print(f"   ❌ VALIDATION FAILED")
            print(f"   → Most likely cause: SNS Topic Policy doesn't allow S3")
            print(f"   → Run 17_01_create_sns_topic.py first")
            print(f"   → Verify Topic Policy has: Principal: s3.amazonaws.com")
            print(f"   → Verify Topic ARN: {topic_arn}")
            return False
        else:
            raise

    # ── STEP 5: VERIFY CONFIGURATION ──
    print("\n" + "=" * 70)
    print("STEP 5: Verifying applied configuration")
    print("=" * 70)

    verify_config = s3_client.get_bucket_notification_configuration(
        Bucket=SOURCE_BUCKET
    )

    for tc in verify_config.get("TopicConfigurations", []):
        print(f"   Notification ID: {tc.get('Id', 'N/A')}")
        print(f"   Topic ARN:       {tc['TopicArn']}")
        print(f"   Events:          {tc['Events']}")
        filters = tc.get("Filter", {}).get("Key", {}).get("FilterRules", [])
        for f in filters:
            print(f"   Filter {f['Name']:>6}:   {f['Value']}")
        print()

    return True


if __name__ == "__main__":
    success = configure_s3_event_notification()
    if success:
        print("🎯 S3 Event Notification is LIVE")
        print("   → Upload a CSV to daily_exports/ to trigger alert")
        print("   → Run 17_03_test_notification.py to test")
    else:
        print("❌ Setup failed — check SNS Topic Policy")