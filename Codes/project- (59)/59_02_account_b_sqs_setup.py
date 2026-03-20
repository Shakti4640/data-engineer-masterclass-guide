# file: 59_02_account_b_sqs_setup.py
# Purpose: Create SQS queues in Account B with policies allowing Account A's SNS
# RUN THIS IN ACCOUNT B

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/cross_account_sns_config.json", "r") as f:
        return json.load(f)


def create_cross_account_sqs_queues():
    """
    Create SQS queues in Account B that can receive from Account A's SNS
    
    TWO QUEUES:
    1. Pipeline Events Queue: receives etl_completed events → triggers Account B's pipeline
    2. Alert Events Queue: receives pipeline_failed events → triggers Account B's monitoring
    """
    config = load_config()
    sqs_client = boto3.client("sqs", region_name=REGION)
    topic_arn = config["topic_arn"]

    print("=" * 70)
    print("🔧 ACCOUNT B: CREATING SQS QUEUES FOR CROSS-ACCOUNT SNS")
    print(f"   Source Topic: {topic_arn}")
    print("=" * 70)

    queues_to_create = [
        {
            "queue_name": "acct-a-pipeline-events-queue",
            "description": "Receives pipeline completion events from Account A",
            "filter_policy": {
                "event_type": ["etl_completed", "dq_passed"]
            },
            "dlq_name": "acct-a-pipeline-events-dlq"
        },
        {
            "queue_name": "acct-a-alert-events-queue",
            "description": "Receives pipeline failure alerts from Account A",
            "filter_policy": {
                "event_type": ["pipeline_failed"],
                "severity": ["critical"]
            },
            "dlq_name": "acct-a-alert-events-dlq"
        }
    ]

    created_queues = []

    for queue_config in queues_to_create:
        print(f"\n📌 Creating: {queue_config['queue_name']}")
        print(f"   Purpose: {queue_config['description']}")
        print(f"   Filter: {json.dumps(queue_config['filter_policy'])}")

        # --- Create DLQ first (from Project 50) ---
        dlq_response = sqs_client.create_queue(
            QueueName=queue_config["dlq_name"],
            Attributes={
                "MessageRetentionPeriod": "1209600"  # 14 days
            }
        )
        dlq_url = dlq_response["QueueUrl"]
        dlq_attrs = sqs_client.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )
        dlq_arn = dlq_attrs["Attributes"]["QueueArn"]
        print(f"   ✅ DLQ created: {queue_config['dlq_name']}")

        # --- Create main queue with DLQ redrive ---
        redrive_policy = {
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": "3"  # After 3 failures → DLQ
        }

        queue_response = sqs_client.create_queue(
            QueueName=queue_config["queue_name"],
            Attributes={
                "VisibilityTimeout": "300",
                "MessageRetentionPeriod": "604800",  # 7 days
                "ReceiveMessageWaitTimeSeconds": "20",  # Long polling
                "RedrivePolicy": json.dumps(redrive_policy)
            }
        )
        queue_url = queue_response["QueueUrl"]
        queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        queue_arn = queue_attrs["Attributes"]["QueueArn"]
        print(f"   ✅ Queue created: {queue_config['queue_name']}")
        print(f"   Queue ARN: {queue_arn}")

        # --- Set queue policy allowing Account A's SNS ---
        queue_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAccountASNSToDeliver",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "sns.amazonaws.com"
                    },
                    "Action": "sqs:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {
                        "ArnEquals": {
                            "aws:SourceArn": topic_arn
                            # CRITICAL: only allow THIS specific topic
                            # Without condition: any SNS topic could deliver
                        }
                    }
                }
            ]
        }

        sqs_client.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={
                "Policy": json.dumps(queue_policy)
            }
        )
        print(f"   ✅ Queue policy set — allows SNS delivery from Account A")
        print(f"   Policy condition: aws:SourceArn = {topic_arn}")

        created_queues.append({
            "queue_name": queue_config["queue_name"],
            "queue_url": queue_url,
            "queue_arn": queue_arn,
            "dlq_name": queue_config["dlq_name"],
            "dlq_url": dlq_url,
            "dlq_arn": dlq_arn,
            "filter_policy": queue_config["filter_policy"],
            "description": queue_config["description"]
        })

    # Save queue info
    config["queues"] = created_queues
    with open("/tmp/cross_account_sns_config.json", "w") as f:
        json.dump(config, f, indent=2)

    # Summary
    print(f"\n{'=' * 70}")
    print(f"📊 ACCOUNT B SQS QUEUES READY")
    print(f"{'=' * 70}")
    for q in created_queues:
        print(f"  📬 {q['queue_name']}")
        print(f"     Filter: {json.dumps(q['filter_policy'])}")
        print(f"     DLQ: {q['dlq_name']} (max 3 retries)")
    print(f"{'=' * 70}")

    return created_queues


if __name__ == "__main__":
    create_cross_account_sqs_queues()