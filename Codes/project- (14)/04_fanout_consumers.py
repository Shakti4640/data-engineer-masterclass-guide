# file: 04_fanout_consumers.py
# Purpose: Independent consumers for each fan-out queue
# Each consumer has different processing logic
# Demonstrates consumer independence

import boto3
import json
import time
import sys
from datetime import datetime
from botocore.exceptions import ClientError


def load_config():
    with open("fanout_config.json", "r") as f:
        return json.load(f)


# ═══════════════════════════════════════════════
# CONSUMER 1: ETL Processor
# ═══════════════════════════════════════════════

def etl_processor(message_body):
    """
    Process file for ETL pipeline
    → Download from S3
    → Validate schema
    → Load into Redshift staging
    """
    s3_info = message_body.get("s3", {})
    s3_key = s3_info.get("key", "unknown")
    size_mb = s3_info.get("size_mb", 0)
    
    print(f"      🔄 ETL: Loading {s3_key} ({size_mb} MB)")
    print(f"         Step 1: Download from S3...")
    time.sleep(0.5)
    print(f"         Step 2: Validate schema...")
    time.sleep(0.3)
    print(f"         Step 3: COPY to Redshift staging...")
    time.sleep(1.0)
    print(f"         ✅ ETL complete for {s3_key.split('/')[-1]}")
    return True


# ═══════════════════════════════════════════════
# CONSUMER 2: Quality Checker
# ═══════════════════════════════════════════════

def quality_checker(message_body):
    """
    Validate data quality of arrived file
    → Check row count > 0
    → Check no completely null columns
    → Check schema matches expected
    """
    s3_info = message_body.get("s3", {})
    s3_key = s3_info.get("key", "unknown")
    
    print(f"      🔍 QA: Checking quality of {s3_key.split('/')[-1]}")
    time.sleep(0.3)
    
    # Simulate quality checks
    checks = {
        "row_count": {"status": "pass", "value": "12,500 rows"},
        "null_check": {"status": "pass", "value": "0 fully null columns"},
        "schema_match": {"status": "pass", "value": "8/8 columns match"}
    }
    
    for check_name, result in checks.items():
        status = "✅" if result["status"] == "pass" else "❌"
        print(f"         {status} {check_name}: {result['value']}")
    
    all_passed = all(c["status"] == "pass" for c in checks.values())
    
    if all_passed:
        print(f"         ✅ Quality check PASSED")
    else:
        print(f"         ❌ Quality check FAILED — blocking ETL")
    
    return all_passed


# ═══════════════════════════════════════════════
# CONSUMER 3: Audit Logger
# ═══════════════════════════════════════════════

def audit_logger(message_body):
    """
    Log file event to audit system
    → Record: who, what, when, where
    → Write to audit table (MariaDB from Project 9)
    """
    event_id = message_body.get("event_id", "unknown")
    s3_info = message_body.get("s3", {})
    source = message_body.get("source", "unknown")
    timestamp = message_body.get("timestamp", "unknown")
    
    print(f"      📝 AUDIT: Recording event {event_id[:12]}...")
    print(f"         File: {s3_info.get('key', 'unknown')}")
    print(f"         Source: {source}")
    print(f"         Time: {timestamp}")
    print(f"         Size: {s3_info.get('size_mb', 0)} MB")
    
    time.sleep(0.2)
    
    # In production: INSERT INTO audit.file_events ...
    print(f"         ✅ Audit record written")
    return True


# ═══════════════════════════════════════════════
# GENERIC CONSUMER RUNNER
# ═══════════════════════════════════════════════

def run_consumer(queue_key, process_func, max_iterations=3):
    """Run a consumer for a specific fan-out queue"""
    config = load_config()
    sqs_client = boto3.client("sqs", region_name=config["region"])
    queue_url = config["queues"][queue_key]["url"]
    
    print(f"\n{'─'*65}")
    print(f"🔄 CONSUMER: {queue_key}")
    print(f"   Queue: {queue_url}")
    print(f"   Processing function: {process_func.__name__}")
    print(f"{'─'*65}")
    
    processed = 0
    failed = 0
    
    for iteration in range(max_iterations):
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"]
        )
        
        messages = response.get("Messages", [])
        
        if not messages:
            print(f"\n   Iteration {iteration + 1}/{max_iterations}: no messages (queue empty)")
            continue
        
        print(f"\n   Iteration {iteration + 1}/{max_iterations}: {len(messages)} messages")
        
        delete_entries = []
        
        for msg in messages:
            msg_id = msg["MessageId"]
            
            # Parse body — handle both raw and SNS-wrapped
            try:
                body = json.loads(msg["Body"])
                
                # Check if SNS-wrapped
                if "Type" in body and body.get("Type") == "Notification":
                    # Unwrap SNS envelope
                    inner_body = json.loads(body.get("Message", "{}"))
                else:
                    # Raw delivery — body IS the message
                    inner_body = body
                    
            except json.JSONDecodeError:
                print(f"      ❌ Cannot parse message: {msg['Body'][:100]}")
                failed += 1
                continue
            
            # Process
            try:
                success = process_func(inner_body)
                
                if success:
                    delete_entries.append({
                        "Id": msg_id,
                        "ReceiptHandle": msg["ReceiptHandle"]
                    })
                    processed += 1
                else:
                    failed += 1
                    
            except Exception as e:
                print(f"      ❌ Processing error: {e}")
                failed += 1
        
        # Batch delete processed messages
        if delete_entries:
            for i in range(0, len(delete_entries), 10):
                batch = delete_entries[i:i+10]
                sqs_client.delete_message_batch(
                    QueueUrl=queue_url,
                    Entries=batch
                )
    
    print(f"\n   📊 Consumer '{queue_key}' summary:")
    print(f"      Processed: {processed}")
    print(f"      Failed:    {failed}")
    
    return processed, failed


def run_all_consumers_sequentially():
    """
    Run all 3 consumers sequentially for demonstration
    
    In PRODUCTION: each consumer runs as separate process/container
    → EC2 instance for ETL consumer
    → Lambda for quality checker
    → EC2 or Lambda for audit logger
    → All running SIMULTANEOUSLY and INDEPENDENTLY
    """
    print("=" * 65)
    print("🔀 FAN-OUT CONSUMERS — SEQUENTIAL DEMO")
    print("   (In production, these run in PARALLEL on separate instances)")
    print("=" * 65)
    
    results = {}
    
    # Consumer 1: ETL Processor
    p, f = run_consumer("etl_processing", etl_processor, max_iterations=2)
    results["etl_processing"] = {"processed": p, "failed": f}
    
    # Consumer 2: Quality Checker
    p, f = run_consumer("quality_check", quality_checker, max_iterations=2)
    results["quality_check"] = {"processed": p, "failed": f}
    
    # Consumer 3: Audit Logger
    p, f = run_consumer("audit_log", audit_logger, max_iterations=2)
    results["audit_log"] = {"processed": p, "failed": f}
    
    # Summary
    print(f"\n{'='*65}")
    print(f"📊 FAN-OUT CONSUMER SUMMARY")
    print(f"{'='*65}")
    print(f"\n   {'Consumer':<20} {'Processed':>10} {'Failed':>10} {'Status'}")
    print(f"   {'─'*55}")
    
    for key, stats in results.items():
        status = "🟢" if stats["failed"] == 0 else "🔴"
        print(f"   {key:<20} {stats['processed']:>10} {stats['failed']:>10} {status}")
    
    total_processed = sum(r["processed"] for r in results.values())
    total_failed = sum(r["failed"] for r in results.values())
    
    print(f"   {'─'*55}")
    print(f"   {'TOTAL':<20} {total_processed:>10} {total_failed:>10}")
    
    print(f"""
💡 KEY INSIGHT:
   Each consumer processed the SAME events independently.
   If ETL processed 3 events, quality checker also processed 3.
   If quality checker was slow, ETL was NOT affected.
   
   This is the power of fan-out:
   → ONE publish → MULTIPLE independent consumers
   → Each at their own pace
   → Each with their own error handling
   → Each with their own DLQ
    """)


def run_single_consumer():
    """Run a specific consumer (for production use)"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python 04_fanout_consumers.py all          # Run all sequentially (demo)")
        print("  python 04_fanout_consumers.py etl          # Run ETL consumer only")
        print("  python 04_fanout_consumers.py quality      # Run quality checker only")
        print("  python 04_fanout_consumers.py audit        # Run audit logger only")
        sys.exit(0)
    
    target = sys.argv[1].lower()
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    
    if target == "all":
        run_all_consumers_sequentially()
    elif target == "etl":
        run_consumer("etl_processing", etl_processor, max_iterations=iterations)
    elif target == "quality":
        run_consumer("quality_check", quality_checker, max_iterations=iterations)
    elif target == "audit":
        run_consumer("audit_log", audit_logger, max_iterations=iterations)
    else:
        print(f"❌ Unknown consumer: {target}")
        sys.exit(1)


if __name__ == "__main__":
    run_single_consumer()