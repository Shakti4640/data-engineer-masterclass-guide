# file: 02_producer.py
# Purpose: Demonstrate sending messages to Standard and FIFO queues
# Reusable patterns for all future projects

import boto3
import json
import uuid
import time
from datetime import datetime, timezone
from botocore.exceptions import ClientError


def load_config():
    with open("sqs_config.json", "r") as f:
        return json.load(f)


class QueueProducer:
    """
    Reusable SQS message producer
    
    USAGE:
        producer = QueueProducer()
        
        # Standard queue — file processing
        producer.send_file_event(
            s3_bucket="quickcart-raw-data-prod",
            s3_key="daily_exports/orders/orders_20250715.csv",
            file_size_mb=52.3
        )
        
        # FIFO queue — order events
        producer.send_order_event(
            order_id="ORD-20250715-000123",
            event_type="order_created",
            payload={"customer_id": "CUST-000001", "total": 299.99}
        )
    """
    
    def __init__(self, config_path="sqs_config.json"):
        config = load_config()
        self.sqs_client = boto3.client("sqs", region_name=config["region"])
        self.queues = config["queues"]
    
    def send_file_event(self, s3_bucket, s3_key, file_size_mb=0, source="unknown"):
        """
        Send file processing event to STANDARD queue
        
        WHY STANDARD (not FIFO):
        → File processing order doesn't matter
        → Need unlimited throughput for burst uploads
        → At-least-once is fine — consumer checks "already processed"
        """
        queue_url = self.queues["file_processing"]["url"]
        
        message_body = {
            "event_type": "file_arrived",
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
            "file_size_mb": file_size_mb,
            "source": source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message_id": str(uuid.uuid4())
        }
        
        try:
            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message_body),
                
                # Message attributes — metadata for filtering/routing
                MessageAttributes={
                    "event_type": {
                        "DataType": "String",
                        "StringValue": "file_arrived"
                    },
                    "source": {
                        "DataType": "String",
                        "StringValue": source
                    },
                    "file_size_mb": {
                        "DataType": "Number",
                        "StringValue": str(file_size_mb)
                    }
                },
                
                # Delay delivery by N seconds (0-900)
                # Useful for: "process this file 5 minutes after upload"
                DelaySeconds=0
            )
            
            msg_id = response["MessageId"]
            print(f"   📤 Sent to Standard queue: {msg_id}")
            print(f"      Key: {s3_key}")
            return msg_id
            
        except ClientError as e:
            print(f"   ❌ Send failed: {e}")
            return None
    
    def send_order_event(self, order_id, event_type, payload):
        """
        Send order event to FIFO queue
        
        WHY FIFO:
        → Order events MUST be processed in sequence
        → "created" before "paid" before "shipped"
        → MessageGroupId = order_id (events for same order = same group)
        → Events for DIFFERENT orders process in PARALLEL
        """
        queue_url = self.queues["order_events"]["url"]
        
        message_body = {
            "order_id": order_id,
            "event_type": event_type,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message_body),
                
                # FIFO-specific parameters
                MessageGroupId=order_id,
                # All events for same order → same group → processed in order
                # Different orders → different groups → parallel processing
                
                # Deduplication: using content-based (enabled on queue)
                # If same message sent within 5 minutes → silently ignored
                # Alternative: explicit MessageDeduplicationId
                
                MessageAttributes={
                    "event_type": {
                        "DataType": "String",
                        "StringValue": event_type
                    },
                    "order_id": {
                        "DataType": "String",
                        "StringValue": order_id
                    }
                }
            )
            
            msg_id = response["MessageId"]
            seq = response.get("SequenceNumber", "N/A")
            print(f"   📤 Sent to FIFO queue: {msg_id}")
            print(f"      Order: {order_id}, Event: {event_type}, Seq: {seq}")
            return msg_id
            
        except ClientError as e:
            print(f"   ❌ Send failed: {e}")
            return None
    
    def send_batch_file_events(self, file_events):
        """
        Send up to 10 messages in one API call (batch)
        
        WHY BATCH:
        → 1 API call for 10 messages vs 10 API calls
        → 10x fewer API calls = 10x lower cost
        → Slightly higher latency per message but better throughput
        """
        queue_url = self.queues["file_processing"]["url"]
        
        # SQS batch max: 10 messages per call
        entries = []
        for i, event in enumerate(file_events[:10]):
            entries.append({
                "Id": str(i),                  # Unique within batch
                "MessageBody": json.dumps({
                    "event_type": "file_arrived",
                    "s3_bucket": event["bucket"],
                    "s3_key": event["key"],
                    "file_size_mb": event.get("size_mb", 0),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }),
                "MessageAttributes": {
                    "event_type": {
                        "DataType": "String",
                        "StringValue": "file_arrived"
                    }
                }
            })
        
        try:
            response = self.sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            
            successful = response.get("Successful", [])
            failed = response.get("Failed", [])
            
            print(f"   📤 Batch sent: {len(successful)} succeeded, {len(failed)} failed")
            
            if failed:
                for f in failed:
                    print(f"      ❌ Failed: {f['Id']} — {f['Message']}")
            
            return len(successful)
            
        except ClientError as e:
            print(f"   ❌ Batch send failed: {e}")
            return 0


def run_producer_demo():
    """Demonstrate all producer patterns"""
    print("=" * 60)
    print("📤 SQS PRODUCER DEMONSTRATION")
    print("=" * 60)
    
    producer = QueueProducer()
    
    # --- Demo 1: Single file events to Standard queue ---
    print(f"\n📋 Demo 1: File processing events (Standard queue)")
    print("-" * 50)
    
    files = [
        ("daily_exports/orders/orders_20250715.csv", 52.3, "order_system"),
        ("daily_exports/customers/customers_20250715.csv", 10.1, "order_system"),
        ("campaign_uploads/summer_promo.csv", 5.7, "marketing"),
    ]
    
    for s3_key, size, source in files:
        producer.send_file_event(
            s3_bucket="quickcart-raw-data-prod",
            s3_key=s3_key,
            file_size_mb=size,
            source=source
        )
    
    # --- Demo 2: Order events to FIFO queue ---
    print(f"\n📋 Demo 2: Order lifecycle events (FIFO queue)")
    print("-" * 50)
    
    # Order 1: full lifecycle
    order1 = "ORD-20250715-000001"
    producer.send_order_event(order1, "order_created",
        {"customer_id": "CUST-000001", "total": 299.99})
    producer.send_order_event(order1, "payment_received",
        {"payment_method": "credit_card", "amount": 299.99})
    producer.send_order_event(order1, "order_shipped",
        {"tracking_number": "TRK-ABC-123"})
    
    # Order 2: different order (processes in parallel with order 1)
    order2 = "ORD-20250715-000002"
    producer.send_order_event(order2, "order_created",
        {"customer_id": "CUST-000002", "total": 149.50})
    producer.send_order_event(order2, "order_cancelled",
        {"reason": "customer_request"})
    
    # --- Demo 3: Batch send ---
    print(f"\n📋 Demo 3: Batch file events (10 files in 1 API call)")
    print("-" * 50)
    
    batch_files = [
        {"bucket": "quickcart-raw-data-prod", "key": f"batch/file_{i}.csv", "size_mb": 1.5}
        for i in range(10)
    ]
    producer.send_batch_file_events(batch_files)
    
    print(f"\n{'='*60}")
    print(f"✅ ALL MESSAGES SENT")
    print(f"{'='*60}")


if __name__ == "__main__":
    run_producer_demo()