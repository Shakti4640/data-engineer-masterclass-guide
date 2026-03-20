# file: 02_subscribe_endpoints.py
# Purpose: Add email, SMS, and SQS subscribers to topics
# Subscribers must CONFIRM before receiving messages

import boto3
import json
import time
from botocore.exceptions import ClientError

REGION = "us-east-2"

# Load topic ARNs from setup
def load_config():
    try:
        with open("sns_config.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ sns_config.json not found — run 01_create_topics.py first")
        exit(1)

# Subscriber definitions
SUBSCRIPTIONS = {
    "success": [
        {
            "protocol": "email",
            "endpoint": "data-team@quickcart.com",
            "description": "Data team — daily success notifications"
        },
        {
            "protocol": "email",
            "endpoint": "analyst@quickcart.com",
            "description": "Lead analyst — data freshness confirmation"
        }
    ],
    "failure": [
        {
            "protocol": "email",
            "endpoint": "data-team@quickcart.com",
            "description": "Data team — failure awareness"
        },
        {
            "protocol": "email",
            "endpoint": "oncall@quickcart.com",
            "description": "On-call engineer — must respond"
        },
        {
            "protocol": "sms",
            "endpoint": "+15551234567",
            "description": "On-call phone — critical failures only"
        }
    ]
}


def subscribe(sns_client, topic_arn, protocol, endpoint, description):
    """
    Subscribe an endpoint to a topic
    
    CONFIRMATION:
    → Email: subscriber receives email with confirmation link
    → SMS: auto-confirmed (opt-in via first message)
    → SQS: auto-confirmed (same account)
    → HTTP: must respond to SubscribeURL POST
    """
    try:
        response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol=protocol,
            Endpoint=endpoint,
            Attributes={
                # Filter policy: empty = receive ALL messages
                # Will be customized in Project 51
            },
            ReturnSubscriptionArn=True
        )
        
        sub_arn = response["SubscriptionArn"]
        
        if sub_arn == "pending confirmation":
            print(f"   ⏳ {protocol}:{endpoint} — PENDING CONFIRMATION")
            print(f"      → Check inbox/phone for confirmation link")
            print(f"      → Purpose: {description}")
        else:
            print(f"   ✅ {protocol}:{endpoint} — CONFIRMED")
            print(f"      → ARN: {sub_arn}")
            print(f"      → Purpose: {description}")
        
        return sub_arn
        
    except ClientError as e:
        print(f"   ❌ Failed: {protocol}:{endpoint} — {e}")
        return None


def list_subscriptions(sns_client, topic_arn, topic_name):
    """Show all subscriptions for a topic"""
    response = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
    
    subs = response.get("Subscriptions", [])
    
    print(f"\n   📋 Subscriptions for '{topic_name}':")
    if not subs:
        print(f"      (no subscriptions)")
        return
    
    for sub in subs:
        protocol = sub["Protocol"]
        endpoint = sub["Endpoint"]
        arn = sub["SubscriptionArn"]
        
        if arn == "PendingConfirmation":
            status = "⏳ PENDING"
        elif arn == "Deleted":
            status = "❌ DELETED"
        else:
            status = "✅ CONFIRMED"
        
        # Mask email/phone for security
        if protocol == "email":
            parts = endpoint.split("@")
            masked = f"{parts[0][:3]}***@{parts[1]}"
        elif protocol == "sms":
            masked = f"{endpoint[:4]}***{endpoint[-4:]}"
        else:
            masked = endpoint
        
        print(f"      {status} {protocol:<8} {masked}")


def run_subscriptions():
    """Create all subscriptions"""
    print("=" * 60)
    print("📬 SUBSCRIBING ENDPOINTS TO SNS TOPICS")
    print("=" * 60)
    
    config = load_config()
    sns_client = boto3.client("sns", region_name=config["region"])
    
    for topic_key, subscribers in SUBSCRIPTIONS.items():
        topic_arn = config["topics"][topic_key]
        print(f"\n📋 Topic: {topic_key} ({topic_arn})")
        
        for sub in subscribers:
            subscribe(
                sns_client=sns_client,
                topic_arn=topic_arn,
                protocol=sub["protocol"],
                endpoint=sub["endpoint"],
                description=sub["description"]
            )
    
    # List all subscriptions
    print(f"\n{'='*60}")
    print(f"📊 SUBSCRIPTION SUMMARY")
    print(f"{'='*60}")
    
    for topic_key, topic_arn in config["topics"].items():
        list_subscriptions(sns_client, topic_arn, topic_key)
    
    print(f"""
{'='*60}
⚠️  IMPORTANT NEXT STEPS
{'='*60}

1. CHECK EMAIL for confirmation links
   → Each email subscriber gets a confirmation email
   → Click "Confirm subscription" link
   → Unconfirmed subscriptions receive NOTHING

2. CHECK SPAM FOLDERS
   → SNS emails from: no-reply@sns.amazonaws.com
   → Some providers flag as spam initially

3. SMS SANDBOX (new accounts)
   → By default, can only send SMS to verified numbers
   → Add phone numbers in SNS Console → Mobile → Text messaging
   → Request production access for unrestricted SMS

4. TEST with: python3 03_publish_notification.py test
    """)


if __name__ == "__main__":
    run_subscriptions()