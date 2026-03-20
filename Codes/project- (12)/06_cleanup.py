# file: 06_cleanup.py
# Purpose: Delete topics and subscriptions (for dev/test environments)
# NEVER run in production without approval

import boto3
import json
import sys
from botocore.exceptions import ClientError

REGION = "us-east-2"


def cleanup_topics(force=False):
    """Delete all SNS topics created by this project"""
    
    if not force:
        print("⚠️  This will DELETE all QuickCart SNS topics and subscriptions!")
        print("   All subscribers will stop receiving notifications.")
        confirm = input("   Type 'DELETE' to confirm: ")
        if confirm != "DELETE":
            print("   Cancelled.")
            return
    
    try:
        with open("sns_config.json", "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        print("❌ sns_config.json not found — nothing to clean up")
        return
    
    sns_client = boto3.client("sns", region_name=config["region"])
    
    for topic_key, topic_arn in config["topics"].items():
        print(f"\n🗑️  Deleting topic: {topic_key}")
        
        # First list and remove all subscriptions
        try:
            response = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
            for sub in response.get("Subscriptions", []):
                sub_arn = sub["SubscriptionArn"]
                if sub_arn not in ("PendingConfirmation", "Deleted"):
                    sns_client.unsubscribe(SubscriptionArn=sub_arn)
                    print(f"   ✅ Unsubscribed: {sub['Protocol']}:{sub['Endpoint']}")
        except ClientError:
            pass
        
        # Delete the topic
        try:
            sns_client.delete_topic(TopicArn=topic_arn)
            print(f"   ✅ Topic deleted: {topic_arn}")
        except ClientError as e:
            print(f"   ❌ Failed: {e}")
    
    # Remove config file
    import os
    if os.path.exists("sns_config.json"):
        os.remove("sns_config.json")
        print(f"\n✅ sns_config.json removed")
    
    print(f"\n✅ CLEANUP COMPLETE — all topics and subscriptions removed")


if __name__ == "__main__":
    force = "--force" in sys.argv
    cleanup_topics(force=force)