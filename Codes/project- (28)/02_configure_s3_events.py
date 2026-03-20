# file: 02_configure_s3_events.py
# Configures S3 bucket to send events to SNS when files arrive
# Triggers on: ObjectCreated in incoming/shipfast/ prefix with .csv suffix

import boto3
import json

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
TOPIC_NAME = "quickcart-incoming-file-events"


def configure_s3_notifications():
    s3_client = boto3.client("s3", region_name=REGION)
    sns_client = boto3.client("sns", region_name=REGION)

    # Get topic ARN
    topics = sns_client.list_topics()["Topics"]
    topic_arn = None
    for t in topics:
        if TOPIC_NAME in t["TopicArn"]:
            topic_arn = t["TopicArn"]
            break

    if not topic_arn:
        print(f"❌ Topic not found: {TOPIC_NAME}")
        print(f"   Run 01_create_sns_topic.py first")
        return

    print("=" * 60)
    print("🔔 CONFIGURING S3 EVENT NOTIFICATIONS")
    print("=" * 60)

    notification_config = {
        "TopicConfigurations": [
            {
                "Id": "ShipFastFileArrival",
                "TopicArn": topic_arn,
                "Events": [
                    "s3:ObjectCreated:*"     # Trigger on any object creation
                ],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "prefix",
                                "Value": "incoming/shipfast/"
                            },
                            {
                                "Name": "suffix",
                                "Value": ".csv"
                            }
                        ]
                    }
                }
            }
        ]
    }

    # NOTE: put_bucket_notification_configuration REPLACES all existing configs
    # Must include existing configs if any
    try:
        existing = s3_client.get_bucket_notification_configuration(Bucket=BUCKET_NAME)
        # Preserve existing configurations
        existing_topics = existing.get("TopicConfigurations", [])
        existing_queues = existing.get("QueueConfigurations", [])
        existing_lambdas = existing.get("LambdaFunctionConfigurations", [])

        # Remove old ShipFast config if exists
        existing_topics = [t for t in existing_topics if t.get("Id") != "ShipFastFileArrival"]

        # Merge
        notification_config["TopicConfigurations"] = (
            existing_topics + notification_config["TopicConfigurations"]
        )
        if existing_queues:
            notification_config["QueueConfigurations"] = existing_queues
        if existing_lambdas:
            notification_config["LambdaFunctionConfigurations"] = existing_lambdas

    except Exception:
        pass  # No existing config

    s3_client.put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration=notification_config
    )

    print(f"✅ S3 event notification configured:")
    print(f"   Bucket:  {BUCKET_NAME}")
    print(f"   Prefix:  incoming/shipfast/")
    print(f"   Suffix:  .csv")
    print(f"   Event:   s3:ObjectCreated:*")
    print(f"   Target:  {topic_arn}")
    print(f"\n💡 Test: upload a CSV to s3://{BUCKET_NAME}/incoming/shipfast/")
    print(f"   → Should trigger SNS → SQS → EC2 worker")


if __name__ == "__main__":
    configure_s3_notifications()