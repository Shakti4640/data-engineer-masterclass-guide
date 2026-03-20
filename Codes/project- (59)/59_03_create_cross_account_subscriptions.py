# file: 59_03_create_cross_account_subscriptions.py
# Purpose: Subscribe Account B's SQS queues to Account A's SNS topic
# Can be run from EITHER account (we run from Account B)

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/cross_account_sns_config.json", "r") as f:
        return json.load(f)


def create_subscriptions():
    """
    Create cross-account SNS → SQS subscriptions
    
    Each subscription:
    → Links Account B's SQS queue to Account A's SNS topic
    → Includes filter policy (from Project 51)
    → Uses raw message delivery (from Project 14)
    → Auto-confirms (SQS queue policy allows delivery)
    """
    config = load_config()
    sns_client = boto3.client("sns", region_name=REGION)
    topic_arn = config["topic_arn"]

    print("=" * 70)
    print("🔗 CREATING CROSS-ACCOUNT SNS → SQS SUBSCRIPTIONS")
    print(f"   Topic (Account A): {topic_arn}")
    print("=" * 70)

    for queue in config["queues"]:
        print(f"\n📌 Subscribing: {queue['queue_name']}")
        print(f"   Queue ARN: {queue['queue_arn']}")
        print(f"   Filter: {json.dumps(queue['filter_policy'])}")

        try:
            response = sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol="sqs",
                Endpoint=queue["queue_arn"],
                Attributes={
                    "FilterPolicy": json.dumps(queue["filter_policy"]),
                    "FilterPolicyScope": "MessageAttributes",
                    "RawMessageDelivery": "true"
                },
                ReturnSubscriptionArn=True
            )

            subscription_arn = response["SubscriptionArn"]

            if subscription_arn == "pending confirmation":
                print(f"   ⚠️  Subscription PENDING CONFIRMATION")
                print(f"   → Check: SQS queue policy allows SNS delivery?")
                print(f"   → Check: SNS topic policy allows Account B to subscribe?")
                queue["subscription_arn"] = "PendingConfirmation"
            else:
                print(f"   ✅ Subscription CONFIRMED")
                print(f"   Subscription ARN: {subscription_arn}")
                queue["subscription_arn"] = subscription_arn

        except Exception as e:
            print(f"   ❌ Subscription FAILED: {e}")
            print(f"   → Check topic policy (Account A)")
            print(f"   → Check queue policy (Account B)")
            queue["subscription_arn"] = f"FAILED: {e}"

    # Save updated config
    with open("/tmp/cross_account_sns_config.json", "w") as f:
        json.dump(config, f, indent=2)

    # Verify all subscriptions on the topic
    print(f"\n{'─' * 50}")
    print(f"📋 ALL SUBSCRIPTIONS ON TOPIC:")
    print(f"{'─' * 50}")

    try:
        subs = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
        for sub in subs.get("Subscriptions", []):
            protocol = sub["Protocol"]
            endpoint = sub["Endpoint"]
            sub_arn = sub["SubscriptionArn"]

            # Identify cross-account
            is_cross = config["account_b_id"] in endpoint
            marker = "🌐 CROSS-ACCOUNT" if is_cross else "🏠 SAME-ACCOUNT"

            print(f"\n  {marker}")
            print(f"    Protocol: {protocol}")
            print(f"    Endpoint: {endpoint}")
            print(f"    Status: {sub_arn[:30]}...")
    except Exception as e:
        print(f"  ⚠️  Could not list subscriptions: {e}")

    print(f"\n{'=' * 70}")


if __name__ == "__main__":
    create_subscriptions()