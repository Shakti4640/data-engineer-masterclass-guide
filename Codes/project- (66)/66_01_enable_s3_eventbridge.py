# file: 66_01_enable_s3_eventbridge.py
# Run from: Account B (222222222222)
# Purpose: Enable EventBridge notifications on all source S3 buckets

import boto3

REGION = "us-east-2"


def enable_eventbridge_on_bucket(bucket_name):
    """
    Enable S3 → EventBridge event delivery
    
    CRITICAL: Without this, S3 events NEVER reach EventBridge
    → Default: S3 sends events to SNS/SQS/Lambda only
    → Must explicitly enable EventBridge delivery
    → This is the #1 forgotten step
    
    NOTE: This ADDS EventBridge config without removing existing
    notification configs (SNS/SQS/Lambda from previous projects)
    """
    s3 = boto3.client("s3", region_name=REGION)

    # Get existing notification configuration
    try:
        existing = s3.get_bucket_notification_configuration(Bucket=bucket_name)
    except Exception:
        existing = {}

    # Remove ResponseMetadata (not part of config)
    existing.pop("ResponseMetadata", None)

    # Add EventBridge configuration
    existing["EventBridgeConfiguration"] = {}

    s3.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=existing
    )
    print(f"✅ EventBridge enabled on: {bucket_name}")


if __name__ == "__main__":
    print("=" * 60)
    print("📡 ENABLING S3 → EVENTBRIDGE NOTIFICATIONS")
    print("=" * 60)

    buckets = [
        "quickcart-partner-feeds",
        "quickcart-payment-data",
        "quickcart-marketing-uploads",
        "quickcart-analytics-datalake"
    ]

    for bucket in buckets:
        try:
            enable_eventbridge_on_bucket(bucket)
        except Exception as e:
            print(f"⚠️ Failed for {bucket}: {str(e)[:100]}")