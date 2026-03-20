# file: 05_manage_subscriptions.py
# Purpose: List, add, remove SNS subscriptions
# Operations tool for managing notification recipients

import boto3
import json
import sys
from botocore.exceptions import ClientError

REGION = "us-east-2"


def load_config():
    with open("sns_config.json", "r") as f:
        return json.load(f)


def list_all_subscriptions():
    """Show all subscriptions across all topics"""
    config = load_config()
    sns_client = boto3.client("sns", region_name=config["region"])
    
    print("=" * 65)
    print("📋 ALL SNS SUBSCRIPTIONS")
    print("=" * 65)
    
    for topic_key, topic_arn in config["topics"].items():
        print(f"\n📌 Topic: {topic_key}")
        print(f"   ARN: {topic_arn}")
        
        response = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
        subs = response.get("Subscriptions", [])
        
        if not subs:
            print(f"   (no subscriptions)")
            continue
        
        print(f"   {'Status':<12} {'Protocol':<10} {'Endpoint':<40} {'ARN (last 20)'}")
        print(f"   {'-'*85}")
        
        for sub in subs:
            arn = sub["SubscriptionArn"]
            if arn == "PendingConfirmation":
                status = "⏳ PENDING"
                arn_short = "pending"
            elif arn == "Deleted":
                status = "❌ DELETED"
                arn_short = "deleted"
            else:
                status = "✅ ACTIVE"
                arn_short = arn[-20:]
            
            print(f"   {status:<12} {sub['Protocol']:<10} {sub['Endpoint']:<40} ...{arn_short}")


def add_subscriber(topic_key, protocol, endpoint):
    """Add new subscriber to a topic"""
    config = load_config()
    sns_client = boto3.client("sns", region_name=config["region"])
    
    if topic_key not in config["topics"]:
        print(f"❌ Unknown topic: {topic_key}. Available: {list(config['topics'].keys())}")
        return
    
    topic_arn = config["topics"][topic_key]
    
    try:
        response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol=protocol,
            Endpoint=endpoint,
            ReturnSubscriptionArn=True
        )
        
        sub_arn = response["SubscriptionArn"]
        if sub_arn == "pending confirmation":
            print(f"✅ Subscription created — CONFIRMATION REQUIRED")
            print(f"   Check {endpoint} for confirmation link")
        else:
            print(f"✅ Subscription confirmed: {sub_arn}")
            
    except ClientError as e:
        print(f"❌ Failed: {e}")


def remove_subscriber(subscription_arn):
    """Remove a subscription by ARN"""
    config = load_config()
    sns_client = boto3.client("sns", region_name=config["region"])
    
    try:
        sns_client.unsubscribe(SubscriptionArn=subscription_arn)
        print(f"✅ Unsubscribed: {subscription_arn}")
    except ClientError as e:
        print(f"❌ Failed: {e}")


def get_topic_attributes(topic_key):
    """Show detailed topic configuration"""
    config = load_config()
    sns_client = boto3.client("sns", region_name=config["region"])
    
    if topic_key not in config["topics"]:
        print(f"❌ Unknown topic: {topic_key}")
        return
    
    topic_arn = config["topics"][topic_key]
    
    response = sns_client.get_topic_attributes(TopicArn=topic_arn)
    attrs = response["Attributes"]
    
    print(f"\n📋 TOPIC DETAILS: {topic_key}")
    print(f"   {'─'*50}")
    print(f"   ARN:                    {attrs.get('TopicArn', 'N/A')}")
    print(f"   Display Name:           {attrs.get('DisplayName', 'N/A')}")
    print(f"   Owner:                  {attrs.get('Owner', 'N/A')}")
    print(f"   Subscriptions Confirmed:{attrs.get('SubscriptionsConfirmed', '0')}")
    print(f"   Subscriptions Pending:  {attrs.get('SubscriptionsPending', '0')}")
    print(f"   Subscriptions Deleted:  {attrs.get('SubscriptionsDeleted', '0')}")
    print(f"   Encryption:             {attrs.get('KmsMasterKeyId', 'None')}")
    
    # Parse and display policy
    policy_str = attrs.get("Policy", "{}")
    policy = json.loads(policy_str)
    statements = policy.get("Statement", [])
    print(f"   Policy Statements:      {len(statements)}")
    for stmt in statements:
        print(f"     → {stmt.get('Sid', 'unnamed')}: "
              f"{stmt.get('Effect', '?')} {', '.join(stmt.get('Action', []) if isinstance(stmt.get('Action'), list) else [stmt.get('Action', '?')])}")


def publish_test_message(topic_key):
    """Send a test message to verify delivery"""
    config = load_config()
    sns_client = boto3.client("sns", region_name=config["region"])
    
    if topic_key not in config["topics"]:
        print(f"❌ Unknown topic: {topic_key}")
        return
    
    topic_arn = config["topics"][topic_key]
    
    from datetime import datetime, timezone
    
    test_message = {
        "pipeline_name": "TEST_NOTIFICATION",
        "status": "TEST",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": "This is a test notification. No action required.",
        "sent_by": "05_manage_subscriptions.py"
    }
    
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Subject=f"🧪 TEST — QuickCart SNS Test ({topic_key})",
            Message=json.dumps(test_message, indent=2),
            MessageAttributes={
                "severity": {
                    "DataType": "String",
                    "StringValue": "test"
                },
                "pipeline": {
                    "DataType": "String",
                    "StringValue": "test"
                }
            }
        )
        
        print(f"✅ Test message published to '{topic_key}'")
        print(f"   MessageId: {response['MessageId']}")
        print(f"   Check subscriber inboxes for delivery")
        
    except ClientError as e:
        print(f"❌ Publish failed: {e}")


def show_delivery_stats():
    """Show CloudWatch metrics for SNS topics"""
    config = load_config()
    cw_client = boto3.client("cloudwatch", region_name=config["region"])
    
    from datetime import datetime, timedelta
    
    print(f"\n📊 SNS DELIVERY METRICS (Last 24 Hours)")
    print(f"   {'─'*55}")
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)
    
    for topic_key, topic_arn in config["topics"].items():
        topic_name = topic_arn.split(":")[-1]
        
        metrics = [
            ("NumberOfMessagesPublished", "Published"),
            ("NumberOfNotificationsDelivered", "Delivered"),
            ("NumberOfNotificationsFailed", "Failed")
        ]
        
        print(f"\n   📌 Topic: {topic_key}")
        
        for metric_name, label in metrics:
            try:
                response = cw_client.get_metric_statistics(
                    Namespace="AWS/SNS",
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "TopicName", "Value": topic_name}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,       # 24-hour aggregate
                    Statistics=["Sum"]
                )
                
                datapoints = response.get("Datapoints", [])
                total = sum(dp["Sum"] for dp in datapoints) if datapoints else 0
                
                status = "⚠️" if metric_name == "NumberOfNotificationsFailed" and total > 0 else "  "
                print(f"   {status} {label:<15}: {int(total)}")
                
            except ClientError:
                print(f"      {label:<15}: N/A")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python 05_manage_subscriptions.py list")
        print("  python 05_manage_subscriptions.py add <topic_key> <protocol> <endpoint>")
        print("  python 05_manage_subscriptions.py remove <subscription_arn>")
        print("  python 05_manage_subscriptions.py details <topic_key>")
        print("  python 05_manage_subscriptions.py test <topic_key>")
        print("  python 05_manage_subscriptions.py stats")
        print("")
        print("Examples:")
        print("  python 05_manage_subscriptions.py list")
        print("  python 05_manage_subscriptions.py add failure email newguy@quickcart.com")
        print("  python 05_manage_subscriptions.py add failure sms +15559876543")
        print("  python 05_manage_subscriptions.py test success")
        print("  python 05_manage_subscriptions.py details failure")
        sys.exit(0)
    
    action = sys.argv[1].lower()
    
    if action == "list":
        list_all_subscriptions()
    
    elif action == "add":
        if len(sys.argv) != 5:
            print("Usage: add <topic_key> <protocol> <endpoint>")
            sys.exit(1)
        add_subscriber(sys.argv[2], sys.argv[3], sys.argv[4])
    
    elif action == "remove":
        if len(sys.argv) != 3:
            print("Usage: remove <subscription_arn>")
            sys.exit(1)
        remove_subscriber(sys.argv[2])
    
    elif action == "details":
        if len(sys.argv) != 3:
            print("Usage: details <topic_key>")
            sys.exit(1)
        get_topic_attributes(sys.argv[2])
    
    elif action == "test":
        if len(sys.argv) != 3:
            print("Usage: test <topic_key>")
            sys.exit(1)
        publish_test_message(sys.argv[2])
    
    elif action == "stats":
        show_delivery_stats()
    
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)