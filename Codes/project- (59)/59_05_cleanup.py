# file: 59_05_cleanup.py
# Purpose: Remove all cross-account SNS/SQS resources

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/cross_account_sns_config.json", "r") as f:
        return json.load(f)


def cleanup_all():
    """Remove subscriptions, queues, and reset topic policy"""
    sns_client = boto3.client("sns", region_name=REGION)
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("=" * 70)
    print("🧹 CLEANING UP PROJECT 59 — CROSS-ACCOUNT SNS → SQS")
    print("=" * 70)

    try:
        config = load_config()
    except FileNotFoundError:
        print("⚠️  Config not found — manual cleanup needed")
        return

    # --- Step 1: Remove cross-account subscriptions ---
    print("\n📌 Step 1: Removing cross-account subscriptions...")
    try:
        subs = sns_client.list_subscriptions_by_topic(
            TopicArn=config["topic_arn"]
        )
        for sub in subs.get("Subscriptions", []):
            # Only remove cross-account subscriptions
            if config["account_b_id"] in sub.get("Endpoint", ""):
                if sub["SubscriptionArn"] != "PendingConfirmation":
                    sns_client.unsubscribe(
                        SubscriptionArn=sub["SubscriptionArn"]
                    )
                    print(f"  ✅ Unsubscribed: {sub['Endpoint'].split(':')[-1]}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 2: Delete Account B's SQS queues ---
    print("\n📌 Step 2: Deleting Account B's SQS queues...")
    for queue in config.get("queues", []):
        for url_key in ["queue_url", "dlq_url"]:
            url = queue.get(url_key, "")
            if url:
                try:
                    sqs_client.delete_queue(QueueUrl=url)
                    name = url.split("/")[-1]
                    print(f"  ✅ Deleted: {name}")
                except Exception as e:
                    print(f"  ⚠️  {e}")

    # --- Step 3: Reset topic policy (remove cross-account statements) ---
    print("\n📌 Step 3: Resetting topic policy (removing cross-account access)...")
    try:
        # Set minimal policy (owner-only)
        minimal_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "DefaultTopicPolicy",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": f"arn:aws:iam::{config['account_a_id']}:root"
                    },
                    "Action": "sns:*",
                    "Resource": config["topic_arn"]
                }
            ]
        }
        sns_client.set_topic_attributes(
            TopicArn=config["topic_arn"],
            AttributeName="Policy",
            AttributeValue=json.dumps(minimal_policy)
        )
        print(f"  ✅ Topic policy reset to owner-only")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 4: Clean config file ---
    print("\n📌 Step 4: Cleaning config files...")
    import os
    try:
        os.remove("/tmp/cross_account_sns_config.json")
        print(f"  ✅ Removed config file")
    except FileNotFoundError:
        pass

    print("\n" + "=" * 70)
    print("🎉 ALL PROJECT 59 RESOURCES CLEANED UP")
    print("=" * 70)
    print(f"\n  ⚠️  NOTE: Account A's SNS topic still exists (from Project 54)")
    print(f"  → Topic policy reset to owner-only (cross-account removed)")
    print(f"  → Same-account subscriptions NOT affected")
    print(f"  → Delete topic itself via Project 54 cleanup if needed")


if __name__ == "__main__":
    cleanup_all()