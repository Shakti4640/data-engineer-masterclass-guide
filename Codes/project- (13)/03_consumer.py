# file: 03_consumer.py
# Purpose: Demonstrate proper SQS consumer patterns
# This is THE pattern used in Projects 27, 28, 30, 52

import boto3
import json
import time
import signal
import sys
from datetime import datetime
from botocore.exceptions import ClientError


def load_config():
    with open("sqs_config.json", "r") as f:
        return json.load(f)


class QueueConsumer:
    """
    Production-grade SQS consumer with:
    → Long polling (efficient, cost-saving)
    → Graceful shutdown (finish current message before exit)
    → Error handling (don't crash on bad messages)
    → Visibility timeout extension (for long processing)
    → Batch receive and delete (cost optimization)
    """
    
    def __init__(self, queue_key, process_func, max_messages=10, 
                 wait_time=20, config_path="sqs_config.json"):
        """
        Args:
            queue_key: key in sqs_config.json (e.g., "file_processing")
            process_func: function(message_body_dict) → bool (True=success)
            max_messages: messages per receive call (1-10)
            wait_time: long polling seconds (1-20)
        """
        config = load_config()
        self.sqs_client = boto3.client("sqs", region_name=config["region"])
        self.queue_url = config["queues"][queue_key]["url"]
        self.queue_name = queue_key
        self.process_func = process_func
        self.max_messages = min(max_messages, 10)       # Hard limit: 10
        self.wait_time = min(wait_time, 20)             # Hard limit: 20
        self.running = True
        self.messages_processed = 0
        self.messages_failed = 0
        
        # Graceful shutdown on Ctrl+C
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _shutdown(self, signum, frame):
        """Handle shutdown signal — finish current message then exit"""
        print(f"\n⚠️  Shutdown signal received — finishing current message...")
        self.running = False
    
    def _extend_visibility(self, receipt_handle, additional_seconds):
        """
        Extend visibility timeout for long-running processing
        
        WHY:
        → Default visibility: 300 seconds (5 minutes)
        → If processing takes 8 minutes: message becomes visible at 5 min
        → Another consumer picks it up → duplicate processing
        → Solution: call change_message_visibility to extend
        → Call periodically during long processing
        """
        try:
            self.sqs_client.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=additional_seconds
            )
            return True
        except ClientError:
            return False
    
    def poll_once(self):
        """
        Single poll cycle: receive → process → delete
        Returns number of messages processed
        """
        try:
            # ── RECEIVE ──
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.max_messages,
                WaitTimeSeconds=self.wait_time,          # LONG POLLING
                MessageAttributeNames=["All"],           # Include all attributes
                AttributeNames=["All"]                   # Include system attributes
            )
            
            messages = response.get("Messages", [])
            
            if not messages:
                return 0
            
            processed_count = 0
            delete_entries = []
            
            for msg in messages:
                msg_id = msg["MessageId"]
                receipt_handle = msg["ReceiptHandle"]
                receive_count = int(msg.get("Attributes", {}).get(
                    "ApproximateReceiveCount", "1"))
                
                # Parse message body
                try:
                    body = json.loads(msg["Body"])
                except json.JSONDecodeError:
                    body = {"raw": msg["Body"]}
                
                # Parse message attributes
                attrs = {}
                for attr_name, attr_val in msg.get("MessageAttributes", {}).items():
                    attrs[attr_name] = attr_val.get("StringValue", "")
                
                print(f"\n   📩 Received: {msg_id[:12]}... "
                      f"(attempt #{receive_count})")
                
                # ── PROCESS ──
                try:
                    success = self.process_func(body)
                    
                    if success:
                        # ── MARK FOR DELETION ──
                        delete_entries.append({
                            "Id": msg_id,
                            "ReceiptHandle": receipt_handle
                        })
                        self.messages_processed += 1
                        processed_count += 1
                        print(f"      ✅ Processed successfully")
                    else:
                        # Processing returned False — don't delete
                        # Message will become visible again after timeout
                        self.messages_failed += 1
                        print(f"      ⚠️  Processing returned False — will retry")
                        
                except Exception as e:
                    # Processing threw exception — don't delete
                    self.messages_failed += 1
                    print(f"      ❌ Processing error: {e}")
                    print(f"      → Message will retry after visibility timeout")
                    print(f"      → After {3} failures → moves to DLQ")
            
            # ── BATCH DELETE ──
            if delete_entries:
                # Delete in batches of 10 (API limit)
                for i in range(0, len(delete_entries), 10):
                    batch = delete_entries[i:i+10]
                    try:
                        response = self.sqs_client.delete_message_batch(
                            QueueUrl=self.queue_url,
                            Entries=batch
                        )
                        failed = response.get("Failed", [])
                        if failed:
                            print(f"      ⚠️  {len(failed)} deletes failed")
                    except ClientError as e:
                        print(f"      ❌ Batch delete error: {e}")
            
            return processed_count
            
        except ClientError as e:
            print(f"   ❌ Receive error: {e}")
            return 0
    
    def run(self, max_iterations=None):
        """
        Main consumer loop
        
        Args:
            max_iterations: None = run forever, N = run N poll cycles
        """
        print(f"\n{'='*60}")
        print(f"🔄 CONSUMER STARTED: {self.queue_name}")
        print(f"   Queue URL: {self.queue_url}")
        print(f"   Long polling: {self.wait_time}s")
        print(f"   Batch size: {self.max_messages}")
        print(f"   Press Ctrl+C to stop gracefully")
        print(f"{'='*60}")
        
        iteration = 0
        
        while self.running:
            if max_iterations and iteration >= max_iterations:
                break
            
            count = self.poll_once()
            iteration += 1
            
            if count == 0 and self.running:
                # No messages — long polling already waited 20 seconds
                # Just continue to next poll
                pass
        
        # Summary
        print(f"\n{'='*60}")
        print(f"📊 CONSUMER SUMMARY: {self.queue_name}")
        print(f"   Iterations:  {iteration}")
        print(f"   Processed:   {self.messages_processed}")
        print(f"   Failed:      {self.messages_failed}")
        print(f"{'='*60}")


# ═══════════════════════════════════════════════
# EXAMPLE PROCESSOR FUNCTIONS
# ═══════════════════════════════════════════════

def process_file_event(message_body):
    """
    Process a file arrival event
    Replace with actual file processing logic
    """
    s3_key = message_body.get("s3_key", "unknown")
    file_size = message_body.get("file_size_mb", 0)
    source = message_body.get("source", "unknown")
    
    print(f"      Processing: {s3_key} ({file_size} MB, source={source})")
    
    # Simulate processing time
    time.sleep(1)
    
    # Simulate occasional failure (10% chance)
    import random
    if random.random() < 0.1:
        raise ValueError(f"Simulated processing failure for {s3_key}")
    
    print(f"      → File processed successfully")
    return True


def process_order_event(message_body):
    """
    Process an order lifecycle event
    Must handle events in order per order_id (FIFO guarantees this)
    """
    order_id = message_body.get("order_id", "unknown")
    event_type = message_body.get("event_type", "unknown")
    payload = message_body.get("payload", {})
    
    print(f"      Order: {order_id}, Event: {event_type}")
    print(f"      Payload: {json.dumps(payload)[:100]}")
    
    # Simulate processing
    time.sleep(0.5)
    
    print(f"      → Event processed: {event_type}")
    return True


def run_consumer_demo():
    """Run consumer for demonstration"""
    print("=" * 60)
    print("📥 SQS CONSUMER DEMONSTRATION")
    print("=" * 60)
    
    if len(sys.argv) > 1:
        queue_type = sys.argv[1].lower()
    else:
        queue_type = "standard"
    
    if queue_type == "standard":
        print("\n🔄 Starting Standard queue consumer (file processing)...")
        consumer = QueueConsumer(
            queue_key="file_processing",
            process_func=process_file_event,
            max_messages=5,
            wait_time=20
        )
        # Run 3 iterations for demo (not forever)
        consumer.run(max_iterations=3)
        
    elif queue_type == "fifo":
        print("\n🔄 Starting FIFO queue consumer (order events)...")
        consumer = QueueConsumer(
            queue_key="order_events",
            process_func=process_order_event,
            max_messages=5,
            wait_time=20
        )
        consumer.run(max_iterations=3)
        
    elif queue_type == "forever":
        print("\n🔄 Starting FOREVER consumer (Ctrl+C to stop)...")
        consumer = QueueConsumer(
            queue_key="file_processing",
            process_func=process_file_event,
            max_messages=10,
            wait_time=20
        )
        consumer.run(max_iterations=None)      # Runs until Ctrl+C
    
    else:
        print(f"Usage: python 03_consumer.py [standard|fifo|forever]")


if __name__ == "__main__":
    run_consumer_demo()