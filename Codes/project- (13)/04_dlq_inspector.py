# file: 04_dlq_inspector.py
# Purpose: Inspect, analyze, and optionally reprocess DLQ messages
# Critical operational tool — run daily to check for poison messages

import boto3
import json
import sys
from datetime import datetime
from botocore.exceptions import ClientError


def load_config():
    with open("sqs_config.json", "r") as f:
        return json.load(f)


class DLQInspector:
    """
    Dead Letter Queue inspector and manager
    
    OPERATIONS:
    → peek: read messages WITHOUT deleting (for investigation)
    → count: check how many messages in DLQ
    → redrive: move messages BACK to main queue for reprocessing
    → purge: delete all DLQ messages (after investigation)
    """
    
    def __init__(self, dlq_key, main_queue_key, config_path="sqs_config.json"):
        config = load_config()
        self.sqs_client = boto3.client("sqs", region_name=config["region"])
        self.dlq_url = config["queues"][dlq_key]["url"]
        self.main_queue_url = config["queues"][main_queue_key]["url"]
        self.dlq_name = dlq_key
    
    def get_queue_stats(self):
        """Get message counts for DLQ"""
        response = self.sqs_client.get_queue_attributes(
            QueueUrl=self.dlq_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
                "CreatedTimestamp",
                "LastModifiedTimestamp",
                "MessageRetentionPeriod"
            ]
        )
        
        attrs = response["Attributes"]
        visible = int(attrs.get("ApproximateNumberOfMessages", 0))
        in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
        retention_days = int(attrs.get("MessageRetentionPeriod", 0)) // 86400
        
        print(f"\n📊 DLQ STATS: {self.dlq_name}")
        print(f"   {'─'*45}")
        print(f"   Messages visible:     {visible}")
        print(f"   Messages in-flight:   {in_flight}")
        print(f"   Messages delayed:     {delayed}")
        print(f"   Total:                {visible + in_flight + delayed}")
        print(f"   Retention:            {retention_days} days")
        
        if visible > 0:
            print(f"\n   ⚠️  {visible} POISON MESSAGES need investigation!")
        else:
            print(f"\n   ✅ DLQ is empty — no failures")
        
        return visible
    
    def peek_messages(self, max_messages=5):
        """
        Read DLQ messages WITHOUT deleting them
        
        HOW: receive with short visibility timeout
        → Message becomes visible again quickly
        → We DON'T delete it — just inspect
        → Use very short visibility so it returns quickly
        """
        print(f"\n🔍 PEEKING AT DLQ MESSAGES (max {max_messages}):")
        print(f"   {'─'*60}")
        
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.dlq_url,
                MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=5,               # Short wait for peek
                VisibilityTimeout=5,             # Very short — returns quickly
                MessageAttributeNames=["All"],
                AttributeNames=["All"]
            )
            
            messages = response.get("Messages", [])
            
            if not messages:
                print("   (no messages in DLQ)")
                return []
            
            for i, msg in enumerate(messages, 1):
                msg_id = msg["MessageId"]
                receive_count = msg.get("Attributes", {}).get(
                    "ApproximateReceiveCount", "?")
                sent_timestamp = msg.get("Attributes", {}).get(
                    "SentTimestamp", "0")
                first_received = msg.get("Attributes", {}).get(
                    "ApproximateFirstReceiveTimestamp", "0")
                
                # Parse timestamps
                if sent_timestamp != "0":
                    sent_time = datetime.fromtimestamp(
                        int(sent_timestamp) / 1000
                    ).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    sent_time = "unknown"
                
                # Parse body
                try:
                    body = json.loads(msg["Body"])
                    body_str = json.dumps(body, indent=6)
                except json.JSONDecodeError:
                    body_str = msg["Body"]
                
                # Parse attributes
                attrs = {}
                for attr_name, attr_val in msg.get("MessageAttributes", {}).items():
                    attrs[attr_name] = attr_val.get("StringValue", "")
                
                print(f"\n   📩 Message {i}/{len(messages)}:")
                print(f"      MessageId:     {msg_id}")
                print(f"      ReceiveCount:  {receive_count}")
                print(f"      Sent:          {sent_time}")
                print(f"      Attributes:    {json.dumps(attrs)}")
                print(f"      Body:")
                for line in body_str.split("\n"):
                    print(f"         {line}")
            
            return messages
            
        except ClientError as e:
            print(f"   ❌ Peek failed: {e}")
            return []
    
    def redrive_messages(self, max_messages=10):
        """
        Move messages from DLQ back to main queue for reprocessing
        
        WHEN TO USE:
        → Root cause has been FIXED (e.g., IAM permission restored)
        → Messages are valid — just failed due to transient issue
        → You want to reprocess without manually recreating messages
        
        HOW:
        → Read from DLQ
        → Send to main queue
        → Delete from DLQ
        → All in a loop
        """
        print(f"\n🔄 REDRIVING MESSAGES: DLQ → Main Queue")
        print(f"   DLQ: {self.dlq_url}")
        print(f"   Main: {self.main_queue_url}")
        print(f"   Max: {max_messages}")
        
        redriven = 0
        failed = 0
        
        while redriven + failed < max_messages:
            # Receive from DLQ
            response = self.sqs_client.receive_message(
                QueueUrl=self.dlq_url,
                MaxNumberOfMessages=min(10, max_messages - redriven - failed),
                WaitTimeSeconds=5,
                MessageAttributeNames=["All"]
            )
            
            messages = response.get("Messages", [])
            if not messages:
                break
            
            for msg in messages:
                try:
                    # Rebuild message attributes for send
                    send_attrs = {}
                    for attr_name, attr_val in msg.get("MessageAttributes", {}).items():
                        send_attrs[attr_name] = {
                            "DataType": attr_val["DataType"],
                            "StringValue": attr_val.get("StringValue", "")
                        }
                    
                    # Send to main queue
                    self.sqs_client.send_message(
                        QueueUrl=self.main_queue_url,
                        MessageBody=msg["Body"],
                        MessageAttributes=send_attrs
                    )
                    
                    # Delete from DLQ
                    self.sqs_client.delete_message(
                        QueueUrl=self.dlq_url,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    
                    redriven += 1
                    print(f"   ✅ Redriven: {msg['MessageId'][:12]}...")
                    
                except ClientError as e:
                    failed += 1
                    print(f"   ❌ Failed: {msg['MessageId'][:12]}... — {e}")
        
        print(f"\n   Summary: {redriven} redriven, {failed} failed")
        return redriven
    
    def purge_dlq(self):
        """
        Delete ALL messages from DLQ
        
        WHEN TO USE:
        → Messages investigated and deemed unrecoverable
        → Root cause fixed, don't need to reprocess old failures
        → DLQ cleanup after maintenance
        
        ⚠️  IRREVERSIBLE — messages are permanently deleted
        """
        print(f"\n🗑️  PURGING DLQ: {self.dlq_name}")
        
        confirm = input("   Type 'PURGE' to confirm: ")
        if confirm != "PURGE":
            print("   Cancelled.")
            return
        
        try:
            self.sqs_client.purge_queue(QueueUrl=self.dlq_url)
            print(f"   ✅ Purge initiated — messages will be deleted within 60 seconds")
            print(f"   ⚠️  Cannot purge again for 60 seconds (AWS limitation)")
        except ClientError as e:
            if "PurgeQueueInProgress" in str(e):
                print(f"   ⚠️  Purge already in progress — wait 60 seconds")
            else:
                print(f"   ❌ Purge failed: {e}")


def run_dlq_inspector():
    """Main entry point"""
    print("=" * 60)
    print("🔍 DEAD LETTER QUEUE INSPECTOR")
    print("=" * 60)
    
    if len(sys.argv) < 2:
        print(f"""
Usage:
  python 04_dlq_inspector.py stats [standard|fifo]
  python 04_dlq_inspector.py peek [standard|fifo] [count]
  python 04_dlq_inspector.py redrive [standard|fifo] [count]
  python 04_dlq_inspector.py purge [standard|fifo]

Examples:
  python 04_dlq_inspector.py stats standard
  python 04_dlq_inspector.py peek standard 5
  python 04_dlq_inspector.py redrive standard 10
        """)
        sys.exit(0)
    
    action = sys.argv[1].lower()
    queue_type = sys.argv[2].lower() if len(sys.argv) > 2 else "standard"
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    
    # Map queue type to config keys
    if queue_type == "standard":
        inspector = DLQInspector("file_processing_dlq", "file_processing")
    elif queue_type == "fifo":
        inspector = DLQInspector("order_events_dlq", "order_events")
    else:
        print(f"❌ Unknown queue type: {queue_type}. Use 'standard' or 'fifo'")
        sys.exit(1)
    
    if action == "stats":
        inspector.get_queue_stats()
    elif action == "peek":
        inspector.get_queue_stats()
        inspector.peek_messages(max_messages=count)
    elif action == "redrive":
        inspector.get_queue_stats()
        inspector.redrive_messages(max_messages=count)
    elif action == "purge":
        inspector.get_queue_stats()
        inspector.purge_dlq()
    else:
        print(f"❌ Unknown action: {action}")


if __name__ == "__main__":
    run_dlq_inspector()