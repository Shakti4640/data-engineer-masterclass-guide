# file: 05_queue_monitor.py
# Purpose: Monitor all queues — backlog, age, DLQ depth
# Run on schedule or ad-hoc for operational visibility

import boto3
import json
import time
from datetime import datetime
from botocore.exceptions import ClientError


def load_config():
    with open("sqs_config.json", "r") as f:
        return json.load(f)


def get_all_queue_metrics():
    """Get comprehensive metrics for all queues"""
    config = load_config()
    sqs_client = boto3.client("sqs", region_name=config["region"])
    
    print("=" * 75)
    print(f"📊 SQS QUEUE MONITORING DASHBOARD — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 75)
    
    all_healthy = True
    
    for queue_name, queue_info in config["queues"].items():
        queue_url = queue_info["url"]
        
        try:
            response = sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["All"]
            )
            attrs = response["Attributes"]
            
            visible = int(attrs.get("ApproximateNumberOfMessages", 0))
            in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
            delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
            total = visible + in_flight + delayed
            
            # Determine queue type
            is_fifo = attrs.get("FifoQueue", "false") == "true"
            is_dlq = "dlq" in queue_name.lower()
            
            # Health assessment
            if is_dlq and visible > 0:
                health = "🔴 POISON MESSAGES"
                all_healthy = False
            elif visible > 1000:
                health = "🟠 HIGH BACKLOG"
                all_healthy = False
            elif visible > 100:
                health = "🟡 MODERATE BACKLOG"
            else:
                health = "🟢 HEALTHY"
            
            # Display
            queue_type = "FIFO" if is_fifo else "Standard"
            dlq_tag = " [DLQ]" if is_dlq else ""
            
            print(f"\n   📦 {queue_name}{dlq_tag} ({queue_type})")
            print(f"   {'─'*55}")
            print(f"   Status:            {health}")
            print(f"   Messages visible:  {visible}")
            print(f"   Messages in-flight:{in_flight}")
            print(f"   Messages delayed:  {delayed}")
            print(f"   Total:             {total}")
            
            # Redrive policy
            redrive = attrs.get("RedrivePolicy")
            if redrive:
                policy = json.loads(redrive)
                max_receives = policy.get("maxReceiveCount", "?")
                dlq_arn = policy.get("deadLetterTargetArn", "?")
                dlq_short = dlq_arn.split(":")[-1] if ":" in dlq_arn else dlq_arn
                print(f"   DLQ:               {dlq_short} (after {max_receives} failures)")
            
            # Visibility timeout
            vis_timeout = int(attrs.get("VisibilityTimeout", 0))
            print(f"   Visibility timeout:{vis_timeout}s")
            
            # Retention
            retention = int(attrs.get("MessageRetentionPeriod", 0))
            retention_days = retention // 86400
            print(f"   Retention:         {retention_days} days")
            
        except ClientError as e:
            print(f"\n   📦 {queue_name}")
            print(f"   ❌ Error: {e}")
            all_healthy = False
    
    # Overall assessment
    print(f"\n{'='*75}")
    if all_healthy:
        print(f"✅ ALL QUEUES HEALTHY — no action needed")
    else:
        print(f"⚠️  ATTENTION NEEDED — check queues marked with 🔴 or 🟠")
    print(f"{'='*75}")
    
    return all_healthy


def monitor_continuous(interval_seconds=60, iterations=None):
    """
    Continuous monitoring loop
    
    Use for: real-time monitoring during deployments or incidents
    Press Ctrl+C to stop
    """
    import signal
    
    running = True
    
    def stop(sig, frame):
        nonlocal running
        running = False
    
    signal.signal(signal.SIGINT, stop)
    
    count = 0
    while running:
        if iterations and count >= iterations:
            break
        
        # Clear screen for dashboard effect
        print("\033[2J\033[H", end="")     # ANSI clear screen
        
        get_all_queue_metrics()
        
        print(f"\n   ⏰ Next refresh in {interval_seconds}s (Ctrl+C to stop)")
        
        count += 1
        
        # Wait but check running flag
        for _ in range(interval_seconds):
            if not running:
                break
            time.sleep(1)
    
    print("\n📊 Monitoring stopped")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "watch":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        monitor_continuous(interval_seconds=interval)
    else:
        get_all_queue_metrics()
        print(f"\nTip: Run 'python 05_queue_monitor.py watch 30' for continuous monitoring")