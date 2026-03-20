# file: 51_01_setup_filtered_infrastructure.py
# Purpose: Create SNS topic + 4 SQS queues + subscriptions with filter policies

import boto3
import json
import time

REGION = "us-east-2"
TOPIC_NAME = "quickcart-all-events"

# --- QUEUE DEFINITIONS WITH THEIR FILTER POLICIES ---
QUEUE_CONFIGS = [
    {
        "queue_name": "quickcart-etl-queue",
        "filter_policy": {
            "event_type": ["etl_event"]
        },
        "description": "Receives ONLY ETL pipeline events"
    },
    {
        "queue_name": "quickcart-order-queue",
        "filter_policy": {
            "event_type": ["order_event"]
        },
        "description": "Receives ONLY order lifecycle events"
    },
    {
        "queue_name": "quickcart-alert-queue",
        "filter_policy": {
            # OR logic: matches alert_event OR data_quality_event
            "event_type": ["alert_event", "data_quality_event"]
        },
        "description": "Receives alerts AND data quality failures"
    },
    {
        "queue_name": "quickcart-audit-queue",
        "filter_policy": None,   # No filter = receives EVERYTHING
        "description": "Compliance catch-all — receives ALL messages"
    }
]


def create_sns_topic(sns_client, topic_name):
    """Create SNS topic (idempotent — returns existing if exists)"""
    response = sns_client.create_topic(
        Name=topic_name,
        Tags=[
            {"Key": "Environment", "Value": "Production"},
            {"Key": "Team", "Value": "Data-Engineering"}
        ]
    )
    topic_arn = response["TopicArn"]
    print(f"✅ SNS Topic: {topic_arn}")
    return topic_arn


def create_sqs_queue(sqs_client, queue_name):
    """Create SQS queue with sensible defaults"""
    response = sqs_client.create_queue(
        QueueName=queue_name,
        Attributes={
            "VisibilityTimeout": "300",        # 5 min — enough for processing
            "MessageRetentionPeriod": "604800", # 7 days (from Project 13)
            "ReceiveMessageWaitTimeSeconds": "20"  # Long polling (from Project 27)
        },
        tags={
            "Environment": "Production",
            "Team": "Data-Engineering"
        }
    )
    queue_url = response["QueueUrl"]

    # Get queue ARN (needed for SNS subscription + policy)
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]

    print(f"  ✅ SQS Queue: {queue_name}")
    print(f"     ARN: {queue_arn}")
    return queue_url, queue_arn


def set_sqs_policy_for_sns(sqs_client, queue_url, queue_arn, topic_arn):
    """
    Allow SNS topic to send messages to this SQS queue
    
    WITHOUT this policy: SNS subscription succeeds but messages
    are SILENTLY dropped — one of the most common debugging nightmares
    (Referenced from Project 14 and Project 18)
    """
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowSNSToSendMessage",
                "Effect": "Allow",
                "Principal": {"Service": "sns.amazonaws.com"},
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
    print(f"     ✅ SQS policy set — SNS can deliver")


def subscribe_with_filter(sns_client, topic_arn, queue_arn, filter_policy):
    """
    Subscribe SQS queue to SNS topic WITH filter policy
    
    KEY DISTINCTION:
    → filter_policy = None → subscription receives ALL messages
    → filter_policy = {} → subscription receives NOTHING (empty filter matches nothing)
    → filter_policy = {"key": ["val"]} → subscription receives only matching
    """
    subscribe_args = {
        "TopicArn": topic_arn,
        "Protocol": "sqs",
        "Endpoint": queue_arn,
        "Attributes": {
            # Raw delivery: body goes directly to SQS (no SNS envelope)
            # MessageAttributes still forwarded as SQS attributes
            "RawMessageDelivery": "true"
        }
    }

    # Only set FilterPolicy if we WANT filtering
    # Omitting it entirely = receive everything
    if filter_policy is not None:
        subscribe_args["Attributes"]["FilterPolicy"] = json.dumps(filter_policy)
        subscribe_args["Attributes"]["FilterPolicyScope"] = "MessageAttributes"

    response = sns_client.subscribe(**subscribe_args)
    subscription_arn = response["SubscriptionArn"]

    if filter_policy:
        print(f"     ✅ Subscribed with filter: {json.dumps(filter_policy)}")
    else:
        print(f"     ✅ Subscribed WITHOUT filter (catch-all)")

    print(f"     Subscription ARN: {subscription_arn}")
    return subscription_arn


def setup_complete_infrastructure():
    """
    Orchestrate: create topic → create queues → set policies → subscribe with filters
    """
    sns_client = boto3.client("sns", region_name=REGION)
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("=" * 70)
    print("🚀 SETTING UP SNS FILTERED FAN-OUT INFRASTRUCTURE")
    print("=" * 70)

    # --- Step 1: Create SNS Topic ---
    print("\n📌 Step 1: Creating SNS Topic")
    topic_arn = create_sns_topic(sns_client, TOPIC_NAME)

    # --- Step 2: Create Queues + Subscribe with Filters ---
    infrastructure = {"topic_arn": topic_arn, "queues": []}

    for config in QUEUE_CONFIGS:
        print(f"\n📌 Setting up: {config['queue_name']}")
        print(f"   Purpose: {config['description']}")

        # Create queue
        queue_url, queue_arn = create_sqs_queue(
            sqs_client, config["queue_name"]
        )

        # Set SQS policy to allow SNS delivery
        set_sqs_policy_for_sns(sqs_client, queue_url, queue_arn, topic_arn)

        # Subscribe with filter
        subscription_arn = subscribe_with_filter(
            sns_client, topic_arn, queue_arn, config["filter_policy"]
        )

        infrastructure["queues"].append({
            "queue_name": config["queue_name"],
            "queue_url": queue_url,
            "queue_arn": queue_arn,
            "subscription_arn": subscription_arn,
            "filter_policy": config["filter_policy"]
        })

    # --- Summary ---
    print("\n" + "=" * 70)
    print("📊 INFRASTRUCTURE SUMMARY")
    print(f"   Topic: {topic_arn}")
    for q in infrastructure["queues"]:
        filter_str = json.dumps(q["filter_policy"]) if q["filter_policy"] else "NONE (catch-all)"
        print(f"   Queue: {q['queue_name']}")
        print(f"     Filter: {filter_str}")
    print("=" * 70)

    # Save for other scripts
    with open("/tmp/filtered_infra.json", "w") as f:
        json.dump(infrastructure, f, indent=2)
    print(f"\n💾 Infrastructure config saved to /tmp/filtered_infra.json")

    return infrastructure


if __name__ == "__main__":
    setup_complete_infrastructure()