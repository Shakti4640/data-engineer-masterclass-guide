# file: 41_05_create_sns_topic.py
# Purpose: Create SNS topic for pipeline success/failure alerts
# Builds on Project 12 (SNS basics)

import boto3

REGION = "us-east-2"
TOPIC_NAME = "quickcart-pipeline-alerts"
ALERT_EMAIL = "data-team@quickcart.com"


def create_notification_topic():
    sns_client = boto3.client("sns", region_name=REGION)

    # Create topic (idempotent — returns existing ARN if already exists)
    response = sns_client.create_topic(
        Name=TOPIC_NAME,
        Tags=[
            {"Key": "Environment", "Value": "Production"},
            {"Key": "Project", "Value": "QuickCart-ETL"}
        ]
    )
    topic_arn = response["TopicArn"]
    print(f"✅ SNS Topic: {topic_arn}")

    # Subscribe email (requires confirmation click — see Project 12)
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=ALERT_EMAIL
    )
    print(f"   ✅ Subscribed: {ALERT_EMAIL}")
    print(f"   ⚠️  Check inbox and CONFIRM the subscription")

    return topic_arn


if __name__ == "__main__":
    create_notification_topic()