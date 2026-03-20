# file: 01_create_fanout.py
# Purpose: Create SNS topic + multiple SQS queues + wire them together
# This is the COMPLETE fan-out setup in one script

import boto3
import json
import time
from botocore.exceptions import ClientError

REGION = "us-east-2"
ACCOUNT_ID [REDACTED:BANK_ACCOUNT_NUMBER]012"

# Fan-out configuration
TOPIC_NAME = "quickcart-file-events"

FANOUT_QUEUES = {
    "etl_processing": {
        "queue_name": "quickcart-etl-processing",
        "dlq_name": "quickcart-etl-processing-dlq",
        "visibility_timeout": 300,        # 5 min (ETL takes 1-3 min)
        "retention_days": 7,
        "max_receive_count": 3,
        "raw_delivery": True,             # Consumer wants clean JSON
        "description": "ETL pipeline loads file into Redshift"
    },
    "quality_check": {
        "queue_name": "quickcart-quality-check",
        "dlq_name": "quickcart-quality-check-dlq",
        "visibility_timeout": 60,         # 1 min (QA check takes ~10s)
        "retention_days": 3,
        "max_receive_count": 5,
        "raw_delivery": True,
        "description": "Data quality validation before ETL"
    },
    "audit_log": {
        "queue_name": "quickcart-audit-log",
        "dlq_name": "quickcart-audit-log-dlq",
        "visibility_timeout": 30,         # 30s (audit write takes ~5s)
        "retention_days": 14,             # Compliance: keep longer
        "max_receive_count": 3,
        "raw_delivery": True,
        "description": "Compliance audit logging"
    }
}

EMAIL_SUBSCRIBERS = [
    "data-team@quickcart.com"
]


def create_sns_topic(sns_client):
    """Create the fan-out SNS topic"""
    print(f"\n📋 Creating SNS topic: {TOPIC_NAME}")
    
    response = sns_client.create_topic(
        Name=TOPIC_NAME,
        Attributes={
            "DisplayName": "QuickCart File Events",
            "KmsMasterKeyId": "alias/aws/sns"
        },
        Tags=[
            {"Key": "Environment", "Value": "Production"},
            {"Key": "Pattern", "Value": "FanOut"},
            {"Key": "Project", "Value": "QuickCart"}
        ]
    )
    
    topic_arn = response["TopicArn"]
    print(f"   ✅ Topic ARN: {topic_arn}")
    return topic_arn


def create_queue_with_dlq(sqs_client, config):
    """Create SQS queue with its DLQ"""
    
    # Create DLQ first
    dlq_response = sqs_client.create_queue(
        QueueName=config["dlq_name"],
        Attributes={
            "MessageRetentionPeriod": str(14 * 86400),
            "VisibilityTimeout": "300",
            "ReceiveMessageWaitTimeSeconds": "20",
            "SqsManagedSseEnabled": "true"
        }
    )
    dlq_url = dlq_response["QueueUrl"]
    
    # Get DLQ ARN
    dlq_attrs = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["QueueArn"]
    )
    dlq_arn = dlq_attrs["Attributes"]["QueueArn"]
    
    print(f"   ✅ DLQ: {config['dlq_name']}")
    
    # Create main queue with DLQ redrive
    main_response = sqs_client.create_queue(
        QueueName=config["queue_name"],
        Attributes={
            "MessageRetentionPeriod": str(config["retention_days"] * 86400),
            "VisibilityTimeout": str(config["visibility_timeout"]),
            "ReceiveMessageWaitTimeSeconds": "20",
            "SqsManagedSseEnabled": "true",
            "RedrivePolicy": json.dumps({
                "deadLetterTargetArn": dlq_arn,
                "maxReceiveCount": config["max_receive_count"]
            })
        }
    )
    main_url = main_response["QueueUrl"]
    
    # Get main queue ARN
    main_attrs = sqs_client.get_queue_attributes(
        QueueUrl=main_url,
        AttributeNames=["QueueArn"]
    )
    main_arn = main_attrs["Attributes"]["QueueArn"]
    
    print(f"   ✅ Queue: {config['queue_name']}")
    
    return {
        "url": main_url,
        "arn": main_arn,
        "dlq_url": dlq_url,
        "dlq_arn": dlq_arn
    }


def set_queue_policy(sqs_client, queue_url, queue_arn, topic_arn):
    """
    Allow SNS topic to send messages to this SQS queue
    
    THIS IS THE CRITICAL STEP — without this, fan-out silently fails
    """
    policy = {
        "Version": "2012-10-17",
        "Id": f"{queue_arn}-sns-policy",
        "Statement": [
            {
                "Sid": "AllowSNSToSendMessage",
                "Effect": "Allow",
                "Principal": {
                    "Service": "sns.amazonaws.com"
                },
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnEquals": {
                        "aws:SourceArn": topic_arn
                    }
                }
            }
        ]
    }
    
    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "Policy": json.dumps(policy)
        }
    )
    print(f"   ✅ Queue policy set (allows SNS topic)")


def subscribe_queue_to_topic(sns_client, topic_arn, queue_arn, raw_delivery):
    """
    Subscribe SQS queue to SNS topic
    
    SQS subscriptions are AUTO-CONFIRMED (no email confirmation needed)
    """
    response = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={
            "RawMessageDelivery": str(raw_delivery).lower()
        },
        ReturnSubscriptionArn=True
    )
    
    sub_arn = response["SubscriptionArn"]
    raw_label = "raw" if raw_delivery else "wrapped"
    print(f"   ✅ Subscribed ({raw_label}): {sub_arn[:60]}...")
    return sub_arn


def subscribe_email_to_topic(sns_client, topic_arn, email):
    """Subscribe email to topic (requires confirmation)"""
    response = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=email,
        ReturnSubscriptionArn=True
    )
    
    sub_arn = response["SubscriptionArn"]
    if sub_arn == "pending confirmation":
        print(f"   ⏳ Email: {email} — PENDING CONFIRMATION (check inbox)")
    else:
        print(f"   ✅ Email: {email} — confirmed")
    
    return sub_arn


def run_fanout_setup():
    """Create complete fan-out infrastructure"""
    print("=" * 65)
    print("🔀 CREATING SNS → SQS FAN-OUT INFRASTRUCTURE")
    print("=" * 65)
    
    sns_client = boto3.client("sns", region_name=REGION)
    sqs_client = boto3.client("sqs", region_name=REGION)
    
    # Step 1: Create SNS topic
    topic_arn = create_sns_topic(sns_client)
    
    # Step 2: Create SQS queues + DLQs + policies + subscriptions
    queue_info = {}
    
    for key, config in FANOUT_QUEUES.items():
        print(f"\n📦 Setting up '{key}' ({config['description']})")
        
        # Create queue with DLQ
        info = create_queue_with_dlq(sqs_client, config)
        
        # Set queue policy to allow SNS
        set_queue_policy(sqs_client, info["url"], info["arn"], topic_arn)
        
        # Subscribe queue to topic
        sub_arn = subscribe_queue_to_topic(
            sns_client, topic_arn, info["arn"], config["raw_delivery"]
        )
        info["subscription_arn"] = sub_arn
        info["description"] = config["description"]
        
        queue_info[key] = info
    
    # Step 3: Subscribe email
    print(f"\n📧 Email subscribers:")
    for email in EMAIL_SUBSCRIBERS:
        subscribe_email_to_topic(sns_client, topic_arn, email)
    
    # Save configuration
    fanout_config = {
        "region": REGION,
        "topic_arn": topic_arn,
        "topic_name": TOPIC_NAME,
        "queues": queue_info,
        "email_subscribers": EMAIL_SUBSCRIBERS
    }
    
    with open("fanout_config.json", "w") as f:
        json.dump(fanout_config, f, indent=2)
    
    # Summary
    print(f"\n{'='*65}")
    print(f"✅ FAN-OUT INFRASTRUCTURE COMPLETE")
    print(f"{'='*65}")
    print(f"\n   SNS Topic: {topic_arn}")
    print(f"\n   Fan-out targets:")
    for key, info in queue_info.items():
        print(f"   ├── SQS: {key}")
        print(f"   │   URL: {info['url']}")
        print(f"   │   Purpose: {info['description']}")
    for email in EMAIL_SUBSCRIBERS:
        print(f"   └── Email: {email}")
    
    print(f"\n📁 Config saved to fanout_config.json")
    
    return fanout_config


if __name__ == "__main__":
    run_fanout_setup()