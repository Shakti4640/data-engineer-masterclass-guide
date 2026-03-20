# file: 02_fanout_publisher.py
# Purpose: Publish events that fan out to all consumers
# ONE publish → ALL consumers receive simultaneously

import boto3
import json
import uuid
from datetime import datetime, timezone
from botocore.exceptions import ClientError


def load_config():
    with open("fanout_config.json", "r") as f:
        return json.load(f)


class FanOutPublisher:
    """
    Publishes file events to SNS topic
    SNS automatically fans out to all subscribed SQS queues + email
    
    Publisher knows ONLY the topic ARN — completely decoupled from consumers
    """
    
    def __init__(self, config_path="fanout_config.json"):
        config = load_config()
        self.sns_client = boto3.client("sns", region_name=config["region"])
        self.topic_arn = config["topic_arn"]
    
    def publish_file_event(self, s3_bucket, s3_key, file_size_mb=0,
                           source="unknown", event_type="file_arrived"):
        """
        Publish file arrival event
        
        This ONE call delivers to:
        → ETL processing queue
        → Quality check queue
        → Audit log queue
        → Email subscribers
        All simultaneously, all independently
        """
        message = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "s3": {
                "bucket": s3_bucket,
                "key": s3_key,
                "size_mb": file_size_mb
            },
            "source": source,
            "environment": "production"
        }
        
        subject = f"📁 File Event: {s3_key.split('/')[-1]}"
        
        try:
            response = self.sns_client.publish(
                TopicArn=self.topic_arn,
                Subject=subject[:100],
                Message=json.dumps(message),
                MessageAttributes={
                    "event_type": {
                        "DataType": "String",
                        "StringValue": event_type
                    },
                    "source": {
                        "DataType": "String",
                        "StringValue": source
                    },
                    "file_type": {
                        "DataType": "String",
                        "StringValue": s3_key.split(".")[-1] if "." in s3_key else "unknown"
                    },
                    "file_size_mb": {
                        "DataType": "Number",
                        "StringValue": str(file_size_mb)
                    }
                }
            )
            
            msg_id = response["MessageId"]
            print(f"   📤 Published: {msg_id}")
            print(f"      File: {s3_key}")
            print(f"      → Fanning out to ALL subscribers")
            return msg_id
            
        except ClientError as e:
            print(f"   ❌ Publish failed: {e}")
            return None


def run_publisher_demo():
    """Demonstrate fan-out publishing"""
    print("=" * 65)
    print("🔀 FAN-OUT PUBLISHER DEMONSTRATION")
    print("   One publish → multiple consumers")
    print("=" * 65)
    
    publisher = FanOutPublisher()
    
    # Simulate file arrivals
    files = [
        {
            "bucket": "quickcart-raw-data-prod",
            "key": "daily_exports/orders/orders_20250715.csv",
            "size_mb": 52.3,
            "source": "order_system"
        },
        {
            "bucket": "quickcart-raw-data-prod",
            "key": "daily_exports/customers/customers_20250715.csv",
            "size_mb": 10.1,
            "source": "order_system"
        },
        {
            "bucket": "quickcart-raw-data-prod",
            "key": "campaign_uploads/summer_promo_results.csv",
            "size_mb": 5.7,
            "source": "marketing"
        }
    ]
    
    print(f"\n📤 Publishing {len(files)} file events:")
    for f in files:
        print()
        publisher.publish_file_event(
            s3_bucket=f["bucket"],
            s3_key=f["key"],
            file_size_mb=f["size_mb"],
            source=f["source"]
        )
    
    print(f"\n{'='*65}")
    print(f"✅ {len(files)} events published")
    print(f"   Each event delivered to:")
    
    config = load_config()
    for key, info in config["queues"].items():
        print(f"   ├── SQS: {key} ({info['description']})")
    for email in config.get("email_subscribers", []):
        print(f"   └── Email: {email}")
    
    print(f"\n   Verify: python3 03_verify_fanout.py")


if __name__ == "__main__":
    run_publisher_demo()