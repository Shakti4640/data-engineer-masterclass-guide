# file: 59_01_account_a_topic_policy.py
# Purpose: Update Account A's SNS topic policy to allow Account B subscriptions
# RUN THIS IN ACCOUNT A

import boto3
import json

REGION = "us-east-2"
ACCOUNT_A_ID [REDACTED:BANK_ACCOUNT_NUMBER]111"
ACCOUNT_B_ID [REDACTED:BANK_ACCOUNT_NUMBER]222"
TOPIC_NAME = "quickcart-pipeline-events"


def update_topic_policy_for_cross_account():
    """
    Update Account A's SNS topic policy to:
    1. Allow Account B to SUBSCRIBE (create SQS subscriptions)
    2. Allow Account B's SQS queues to RECEIVE messages
    
    This is the PRODUCER-SIDE setup
    """
    sns_client = boto3.client("sns", region_name=REGION)

    print("=" * 70)
    print("🔧 ACCOUNT A: UPDATING SNS TOPIC POLICY FOR CROSS-ACCOUNT")
    print(f"   Topic: {TOPIC_NAME}")
    print(f"   Allowing Account B: {ACCOUNT_B_ID}")
    print("=" * 70)

    # Get or create topic
    topic_response = sns_client.create_topic(Name=TOPIC_NAME)
    topic_arn = topic_response["TopicArn"]
    print(f"\n  Topic ARN: {topic_arn}")

    # Build topic policy
    topic_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DefaultTopicPolicy",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{ACCOUNT_A_ID}:root"
                },
                "Action": [
                    "sns:Publish",
                    "sns:Subscribe",
                    "sns:SetTopicAttributes",
                    "sns:GetTopicAttributes",
                    "sns:ListSubscriptionsByTopic"
                ],
                "Resource": topic_arn
            },
            {
                "Sid": "AllowAccountBToSubscribe",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{ACCOUNT_B_ID}:root"
                },
                "Action": "sns:Subscribe",
                "Resource": topic_arn,
                "Condition": {
                    "StringEquals": {
                        "sns:Protocol": "sqs"
                        # ONLY allow SQS subscriptions
                        # Prevents Account B from adding email/HTTPS subscribers
                    }
                }
            },
            {
                "Sid": "AllowAccountBToReceive",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{ACCOUNT_B_ID}:root"
                },
                "Action": [
                    "sns:Receive"
                    # Allows SNS to deliver to Account B's SQS
                ],
                "Resource": topic_arn
            }
        ]
    }

    # Apply topic policy
    sns_client.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(topic_policy)
    )
    print(f"\n  ✅ Topic policy updated")

    # Verify
    attrs = sns_client.get_topic_attributes(TopicArn=topic_arn)
    current_policy = json.loads(attrs["Attributes"]["Policy"])
    print(f"\n  📋 CURRENT TOPIC POLICY:")
    print(json.dumps(current_policy, indent=2))

    print(f"\n  📌 Account B ({ACCOUNT_B_ID}) can now:")
    print(f"  ✅ Create SQS subscriptions on this topic")
    print(f"  ✅ Receive messages from this topic in their SQS queues")
    print(f"  ❌ Cannot create email/HTTPS subscriptions")
    print(f"  ❌ Cannot publish to this topic")
    print(f"  ❌ Cannot modify topic settings")

    # Save config
    config = {
        "topic_arn": topic_arn,
        "topic_name": TOPIC_NAME,
        "account_a_id": ACCOUNT_A_ID,
        "account_b_id": ACCOUNT_B_ID
    }
    with open("/tmp/cross_account_sns_config.json", "w") as f:
        json.dump(config, f, indent=2)
    print(f"\n  💾 Config saved to /tmp/cross_account_sns_config.json")

    return topic_arn


if __name__ == "__main__":
    update_topic_policy_for_cross_account()