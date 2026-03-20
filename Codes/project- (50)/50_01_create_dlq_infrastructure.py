# file: 50_01_create_dlq_infrastructure.py
# Purpose: Create DLQs for all source queues, configure redrive policies,
#          set up CloudWatch alarms and SNS notifications

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-pipeline-alerts"

# Source queues that need DLQs
SOURCE_QUEUES = [
    {
        "name": "quickcart-file-process-q",
        "description": "Processes file arrival events from S3",
        "max_receive_count": 3,
        "visibility_timeout": 120,    # 2 minutes for processing
        "dlq_retention_days": 14
    },
    {
        "name": "quickcart-notification-q",
        "description": "Receives SNS notifications for fan-out",
        "max_receive_count": 5,       # More retries — usually transient
        "visibility_timeout": 60,
        "dlq_retention_days": 14
    },
    {
        "name": "quickcart-etl-trigger-q",
        "description": "Triggers ETL pipeline steps",
        "max_receive_count": 3,
        "visibility_timeout": 300,    # 5 minutes for ETL steps
        "dlq_retention_days": 14
    }
]


def create_dlq_for_queue(sqs_client, queue_config):
    """
    Create a DLQ for a source queue and configure the redrive policy.
    
    Steps:
    1. Create the DLQ (if not exists)
    2. Get/Create the source queue (if not exists)
    3. Set redrive policy on source queue pointing to DLQ
    4. Create CloudWatch alarm on DLQ
    """
    source_name = queue_config["name"]
    dlq_name = f"{source_name}-dlq"
    max_receive = queue_config["max_receive_count"]
    retention_days = queue_config["dlq_retention_days"]

    print(f"\n{'─'*60}")
    print(f"📋 Setting up DLQ for: {source_name}")
    print(f"{'─'*60}")

    # ━━━ CREATE DLQ ━━━
    try:
        dlq_response = sqs_client.create_queue(
            QueueName=dlq_name,
            Attributes={
                "MessageRetentionPeriod": str(retention_days * 86400),
                "VisibilityTimeout": "30",
                "ReceiveMessageWaitTimeSeconds": "0"
            },
            tags={
                "Environment": "Production",
                "Type": "DLQ",
                "SourceQueue": source_name,
                "Project": "QuickCart-DLQ"
            }
        )
        dlq_url = dlq_response["QueueUrl"]
        print(f"   ✅ DLQ created: {dlq_name}")
    except sqs_client.exceptions.QueueNameExists:
        dlq_url = sqs_client.get_queue_url(QueueName=dlq_name)["QueueUrl"]
        print(f"   ℹ️  DLQ exists: {dlq_name}")

    # Get DLQ ARN
    dlq_attrs = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["QueueArn"]
    )
    dlq_arn = dlq_attrs["Attributes"]["QueueArn"]

    # ━━━ CREATE/UPDATE SOURCE QUEUE ━━━
    try:
        source_response = sqs_client.create_queue(
            QueueName=source_name,
            Attributes={
                "VisibilityTimeout": str(queue_config["visibility_timeout"]),
                "MessageRetentionPeriod": str(4 * 86400),  # 4 days
                "ReceiveMessageWaitTimeSeconds": "20"       # Long polling
            },
            tags={
                "Environment": "Production",
                "Type": "SourceQueue",
                "DLQ": dlq_name,
                "Project": "QuickCart-Pipeline"
            }
        )
        source_url = source_response["QueueUrl"]
        print(f"   ✅ Source queue: {source_name}")
    except sqs_client.exceptions.QueueNameExists:
        source_url = sqs_client.get_queue_url(QueueName=source_name)["QueueUrl"]
        print(f"   ℹ️  Source queue exists: {source_name}")

    # ━━━ SET REDRIVE POLICY ━━━
    redrive_policy = {
        "deadLetterTargetArn": dlq_arn,
        "maxReceiveCount": str(max_receive)
    }

    sqs_client.set_queue_attributes(
        QueueUrl=source_url,
        Attributes={
            "RedrivePolicy": json.dumps(redrive_policy)
        }
    )
    print(f"   ✅ Redrive policy: maxReceiveCount={max_receive} → {dlq_name}")

    # ━━━ CREATE CLOUDWATCH ALARM ━━━
    create_dlq_alarm(dlq_name, dlq_arn, source_name)

    return {
        "source_url": source_url,
        "dlq_url": dlq_url,
        "dlq_arn": dlq_arn,
        "dlq_name": dlq_name
    }


def create_dlq_alarm(dlq_name, dlq_arn, source_name):
    """
    Create CloudWatch alarm that fires when DLQ has any messages.
    
    Alarm → SNS → Email notification
    "ALERT: Failed messages in quickcart-file-process-q-dlq"
    """
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    alarm_name = f"dlq-messages-{dlq_name}"

    cw_client.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=f"DLQ has messages — failed processing from {source_name}",
        Namespace="AWS/SQS",
        MetricName="ApproximateNumberOfMessagesVisible",
        Dimensions=[
            {"Name": "QueueName", "Value": dlq_name}
        ],
        Statistic="Maximum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=0,
        ComparisonOperator="GreaterThanThreshold",
        AlarmActions=[SNS_TOPIC_ARN],
        OKActions=[SNS_TOPIC_ARN],
        TreatMissingData="notBreaching",
        Tags=[
            {"Key": "Type", "Value": "DLQ-Alarm"},
            {"Key": "SourceQueue", "Value": source_name}
        ]
    )

    print(f"   ✅ CloudWatch alarm: {alarm_name} (threshold: > 0 messages)")

    # Also create alarm for message age (escalation)
    age_alarm_name = f"dlq-age-{dlq_name}"
    cw_client.put_metric_alarm(
        AlarmName=age_alarm_name,
        AlarmDescription=f"DLQ oldest message > 24 hours — investigate urgently",
        Namespace="AWS/SQS",
        MetricName="ApproximateAgeOfOldestMessage",
        Dimensions=[
            {"Name": "QueueName", "Value": dlq_name}
        ],
        Statistic="Maximum",
        Period=300,
        EvaluationPeriods=1,
        Threshold=86400,  # 24 hours in seconds
        ComparisonOperator="GreaterThanThreshold",
        AlarmActions=[SNS_TOPIC_ARN],
        TreatMissingData="notBreaching"
    )

    print(f"   ✅ Age alarm: {age_alarm_name} (threshold: > 24 hours)")


def setup_all_dlqs():
    """Create DLQs for all source queues"""
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("=" * 60)
    print("🔧 SETTING UP DEAD LETTER QUEUES")
    print("=" * 60)

    results = {}
    for queue_config in SOURCE_QUEUES:
        result = create_dlq_for_queue(sqs_client, queue_config)
        results[queue_config["name"]] = result

    # Summary
    print(f"\n{'='*60}")
    print(f"✅ DLQ SETUP COMPLETE")
    print(f"{'='*60}")
    for source, info in results.items():
        print(f"   {source}")
        print(f"   └── DLQ: {info['dlq_name']}")
    print(f"\n   CloudWatch alarms: {len(results) * 2} (message count + age)")
    print(f"   SNS alerts: → {SNS_TOPIC_ARN}")

    return results


if __name__ == "__main__":
    setup_all_dlqs()