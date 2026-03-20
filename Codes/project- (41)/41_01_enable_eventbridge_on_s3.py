# file: 41_01_enable_eventbridge_on_s3.py
# Purpose: Enable S3 bucket to send events to EventBridge
# Prerequisite: Bucket exists (Project 1)

import boto3

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def enable_eventbridge_notifications(bucket_name):
    """
    Enable EventBridge notification on an S3 bucket.
    
    WHAT THIS DOES:
    → Every object-level event (Create, Delete, Restore, etc.)
      is sent to EventBridge default bus in the same region
    → You then use EventBridge RULES to filter which events
      you care about
    → This is a ONE-TIME bucket configuration
    
    WHAT THIS DOES NOT DO:
    → Does NOT disable existing S3 Event Notifications (SNS/SQS/Lambda)
    → Both mechanisms coexist independently
    """
    s3_client = boto3.client("s3", region_name=REGION)

    # Get current notification configuration
    try:
        current_config = s3_client.get_bucket_notification_configuration(
            Bucket=bucket_name
        )
    except Exception as e:
        print(f"❌ Failed to get current config: {e}")
        return False

    # Remove ResponseMetadata (not part of the config)
    current_config.pop("ResponseMetadata", None)

    # Add EventBridge configuration
    current_config["EventBridgeConfiguration"] = {}

    # Apply updated configuration
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=current_config
    )

    print(f"✅ EventBridge notifications enabled on '{bucket_name}'")
    print(f"   All object-level events now flow to EventBridge default bus")
    print(f"   Region: {REGION}")

    # Verify
    verify = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
    if "EventBridgeConfiguration" in verify:
        print(f"   ✅ Verified: EventBridgeConfiguration present")
    else:
        print(f"   ⚠️  EventBridgeConfiguration NOT found after setting")

    return True


if __name__ == "__main__":
    enable_eventbridge_notifications(BUCKET_NAME)