# file: 50_03_dlq_redrive.py
# Purpose: Move messages from DLQ back to source queue for reprocessing
# Use AFTER fixing the root cause of failures

import boto3
import json
import time
from datetime import datetime

REGION = "us-east-2"

# Mapping: DLQ → Source queue
DLQ_TO_SOURCE = {
    "quickcart-file-process-q-dlq": "quickcart-file-process-q",
    "quickcart-notification-q-dlq": "quickcart-notification-q",
    "quickcart-etl-trigger-q-dlq": "quickcart-etl-trigger-q"
}


def redrive_dlq_native(dlq_name, max_messages_per_second=10):
    """
    Use SQS native message move task (available since 2023).
    
    This is the RECOMMENDED approach:
    → SQS handles the move internally
    → Atomic: message can't be in both queues
    → Rate-limited: won't overwhelm the consumer
    → Tracks progress via task ID
    """
    sqs_client = boto3.client("sqs", region_name=REGION)

    source_name = DLQ_TO_SOURCE.get(dlq_name)
    if not source_name:
        print(f"❌ Unknown DLQ: {dlq_name}")
        return

    dlq_url = sqs_client.get_queue_url(QueueName=dlq_name)["QueueUrl"]
    source_url = sqs_client.get_queue_url(QueueName=source_name)["QueueUrl"]

    # Get DLQ ARN
    dlq_attrs = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["QueueArn", "ApproximateNumberOfMessages"]
    )["Attributes"]

    dlq_arn = dlq_attrs["QueueArn"]
    message_count = int(dlq_attrs.get("ApproximateNumberOfMessages", 0))

    if message_count == 0:
        print(f"ℹ️  DLQ {dlq_name} is empty — nothing to redrive")
        return

    print(f"{'='*60}")
    print(f"🔄 REDRIVE: {dlq_name} → {source_name}")
    print(f"{'='*60}")
    print(f"   Messages to redrive: {message_count}")
    print(f"   Rate limit: {max_messages_per_second} msg/sec")

    # Confirm before proceeding
    confirm = input(f"\n   ⚠️  Have you fixed the root cause? (yes/no): ")
    if confirm.lower() != "yes":
        print(f"   🛑 REDRIVE ABORTED — fix root cause first")
        return

    # Start message move task
    try:
        response = sqs_client.start_message_move_task(
            SourceArn=dlq_arn,
            MaxNumberOfMessagesPerSecond=max_messages_per_second
        )
        task_handle = response["TaskHandle"]
        print(f"\n   🚀 Redrive task started: {task_handle}")

        # Monitor progress
        while True:
            status = sqs_client.list_message_move_tasks(
                SourceArn=dlq_arn,
                MaxResults=1
            )

            if status.get("Results"):
                task = status["Results"][0]
                task_status = task.get("Status", "UNKNOWN")
                moved = task.get("ApproximateNumberOfMessagesMoved", 0)
                total = task.get("ApproximateNumberOfMessagesToMove", message_count)
                failures = task.get("FailureReason", "")

                if task_status == "COMPLETED":
                    print(f"\n   ✅ REDRIVE COMPLETE")
                    print(f"      Moved: {moved} messages")
                    break
                elif task_status == "FAILED":
                    print(f"\n   ❌ REDRIVE FAILED: {failures}")
                    break
                elif task_status == "CANCELLING" or task_status == "CANCELLED":
                    print(f"\n   ⚠️  REDRIVE CANCELLED")
                    break
                else:
                    pct = (moved / max(total, 1)) * 100
                    print(f"   ⏳ {task_status}: {moved}/{total} ({pct:.0f}%)", end="\r")

            time.sleep(2)

    except Exception as e:
        if "StartMessageMoveTask" in str(e) or "not supported" in str(e).lower():
            print(f"   ⚠️  Native redrive not available — using manual redrive")
            redrive_dlq_manual(dlq_name, source_name)
        else:
            raise


def redrive_dlq_manual(dlq_name, source_name):
    """
    Manual redrive: receive from DLQ → send to source → delete from DLQ.
    
    Fallback for environments where StartMessageMoveTask isn't available.
    """
    sqs_client = boto3.client("sqs", region_name=REGION)

    dlq_url = sqs_client.get_queue_url(QueueName=dlq_name)["QueueUrl"]
    source_url = sqs_client.get_queue_url(QueueName=source_name)["QueueUrl"]

    print(f"\n   🔄 Manual redrive: {dlq_name} → {source_name}")

    moved = 0
    failed = 0

    while True:
        # Receive from DLQ
        response = sqs_client.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=10,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            VisibilityTimeout=30,
            WaitTimeSeconds=5
        )

        messages = response.get("Messages", [])
        if not messages:
            break

        for msg in messages:
            try:
                # Send to source queue
                send_params = {
                    "QueueUrl": source_url,
                    "MessageBody": msg["Body"]
                }

                # Preserve message attributes
                if msg.get("MessageAttributes"):
                    send_params["MessageAttributes"] = msg["MessageAttributes"]

                sqs_client.send_message(**send_params)

                # Delete from DLQ
                sqs_client.delete_message(
                    QueueUrl=dlq_url,
                    ReceiptHandle=msg["ReceiptHandle"]
                )

                moved += 1
                print(f"   ✅ Moved: {msg['MessageId'][:12]}... ({moved} total)")

            except Exception as e:
                failed += 1
                print(f"   ❌ Failed: {msg['MessageId'][:12]}... — {e}")

    print(f"\n   📊 Redrive complete: {moved} moved, {failed} failed")


def purge_dlq(dlq_name):
    """
    Delete ALL messages from a DLQ.
    
    USE ONLY WHEN:
    → All messages have been investigated
    → Root cause identified and fixed
    → Messages are confirmed unrecoverable
    → Team agrees to discard
    """
    sqs_client = boto3.client("sqs", region_name=REGION)
    dlq_url = sqs_client.get_queue_url(QueueName=dlq_name)["QueueUrl"]

    # Get count first
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    count = int(attrs.get("ApproximateNumberOfMessages", 0))

    print(f"\n⚠️  PURGE DLQ: {dlq_name}")
    print(f"   Messages to delete: {count}")

    if count == 0:
        print(f"   ℹ️  DLQ is already empty")
        return

    confirm = input(f"   ⚠️  This will DELETE all {count} messages permanently. Type 'PURGE' to confirm: ")
    if confirm != "PURGE":
        print(f"   🛑 PURGE ABORTED")
        return

    sqs_client.purge_queue(QueueUrl=dlq_url)
    print(f"   ✅ DLQ purged: {count} messages deleted")
    print(f"   ⚠️  Note: purge may take up to 60 seconds to complete")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python 50_03_dlq_redrive.py redrive quickcart-file-process-q-dlq")
        print("  python 50_03_dlq_redrive.py purge quickcart-file-process-q-dlq")
        print("  python 50_03_dlq_redrive.py list")
        sys.exit(0)

    action = sys.argv[1]

    if action == "redrive":
        dlq_name = sys.argv[2]
        redrive_dlq_native(dlq_name)

    elif action == "purge":
        dlq_name = sys.argv[2]
        purge_dlq(dlq_name)

    elif action == "list":
        for dlq, source in DLQ_TO_SOURCE.items():
            print(f"   {dlq} ← {source}")