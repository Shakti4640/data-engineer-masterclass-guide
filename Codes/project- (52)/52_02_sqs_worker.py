# file: 52_02_sqs_worker.py
# Purpose: The actual worker script that runs on each EC2 instance
# This gets uploaded to S3 and downloaded by User Data on boot
# Builds on Project 27: EC2 SQS consumer pattern

import boto3
import json
import signal
import sys
import time
import os
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# --- CONFIGURATION (from environment variables set by User Data) ---
QUEUE_URL = os.environ.get("SQS_QUEUE_URL", "")
REGION = os.environ.get("AWS_REGION", "us-east-2")
WORKER_ID = os.environ.get("WORKER_ID", f"worker-{os.getpid()}")
MAX_MESSAGES_PER_POLL = 10
VISIBILITY_TIMEOUT = 300      # 5 minutes (from Project 13)
WAIT_TIME_SECONDS = 20        # Long polling (from Project 27)
HEARTBEAT_INTERVAL = 60       # Publish heartbeat every 60 seconds

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format=f"[{WORKER_ID}] %(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/var/log/sqs-worker.log")
    ]
)
logger = logging.getLogger(__name__)

# --- GRACEFUL SHUTDOWN ---
shutdown_requested = False


def signal_handler(signum, frame):
    """
    Handle SIGTERM from ASG during scale-in
    
    CRITICAL BEHAVIOR:
    → ASG sends SIGTERM before terminating instance
    → Worker must finish current message batch
    → Then exit cleanly
    → Unfinished messages return to queue after visibility timeout
    """
    global shutdown_requested
    logger.info(f"Received signal {signum} — finishing current batch then shutting down")
    shutdown_requested = True


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


class SQSWorker:
    """
    Production-grade SQS queue consumer with:
    → Batch polling (10 messages at a time)
    → Batch deletion (single API call for all processed)
    → Graceful shutdown on SIGTERM
    → CloudWatch heartbeat metrics
    → Comprehensive error handling
    """

    def __init__(self, queue_url, region):
        self.queue_url = queue_url
        self.sqs_client = boto3.client("sqs", region_name=region)
        self.cw_client = boto3.client("cloudwatch", region_name=region)
        self.messages_processed = 0
        self.messages_failed = 0
        self.last_heartbeat = 0

    def process_message(self, message_body):
        """
        Process a single message — this is where your BUSINESS LOGIC goes
        
        For ETL queue: trigger Glue job, load to Redshift, etc.
        For Order queue: update order status, notify customer, etc.
        For Alert queue: page on-call, create incident ticket, etc.
        
        MUST BE IDEMPOTENT (from Project 27):
        → Same message processed twice should produce same result
        → Use message_id as deduplication key
        """
        event_name = message_body.get("event_name", "unknown")
        event_type = message_body.get("event_type", "unknown")
        message_id = message_body.get("message_id", "unknown")

        logger.info(f"Processing: {event_name} (type={event_type}, id={message_id})")

        # --- SIMULATED PROCESSING ---
        # In production: replace with actual business logic
        if event_type == "etl_event":
            # Trigger Glue job, COPY to Redshift, etc.
            time.sleep(0.1)  # Simulate processing time
            logger.info(f"  ETL event processed: {event_name}")

        elif event_type == "order_event":
            # Update order tracking, notify shipping, etc.
            time.sleep(0.05)
            logger.info(f"  Order event processed: {event_name}")

        elif event_type == "alert_event":
            # Page on-call, create PagerDuty incident
            time.sleep(0.02)
            logger.info(f"  Alert event processed: {event_name}")

        elif event_type == "data_quality_event":
            # Log DQ failure, update dashboard, notify team
            time.sleep(0.03)
            logger.info(f"  Data quality event processed: {event_name}")

        else:
            logger.warning(f"  Unknown event type: {event_type}")

        return True

    def poll_and_process(self):
        """
        Single poll cycle: receive → process → delete
        Returns number of messages processed
        """
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=MAX_MESSAGES_PER_POLL,
                WaitTimeSeconds=WAIT_TIME_SECONDS,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
                MessageAttributeNames=["All"]
            )
        except ClientError as e:
            logger.error(f"Failed to receive messages: {e}")
            return 0

        messages = response.get("Messages", [])
        if not messages:
            return 0

        logger.info(f"Received {len(messages)} messages")

        # --- PROCESS EACH MESSAGE ---
        successful_handles = []
        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                success = self.process_message(body)

                if success:
                    successful_handles.append({
                        "Id": msg["MessageId"],
                        "ReceiptHandle": msg["ReceiptHandle"]
                    })
                    self.messages_processed += 1
                else:
                    self.messages_failed += 1
                    logger.warning(f"Processing returned False for {msg['MessageId']}")

            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in message: {msg['MessageId']}")
                self.messages_failed += 1
            except Exception as e:
                logger.error(f"Error processing {msg['MessageId']}: {e}")
                self.messages_failed += 1
                # Message will return to queue after visibility timeout
                # Then DLQ after maxReceiveCount (from Project 50)

        # --- BATCH DELETE SUCCESSFUL MESSAGES ---
        if successful_handles:
            try:
                response = self.sqs_client.delete_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=successful_handles
                )
                failed = response.get("Failed", [])
                if failed:
                    logger.warning(f"Failed to delete {len(failed)} messages")
            except ClientError as e:
                logger.error(f"Batch delete failed: {e}")

        return len(successful_handles)

    def publish_heartbeat(self):
        """
        Publish worker health metric to CloudWatch
        
        WHY:
        → ASG only checks if EC2 is reachable (basic health)
        → Does NOT check if worker PROCESS is running
        → Custom heartbeat metric detects crashed workers
        """
        current_time = time.time()
        if current_time - self.last_heartbeat < HEARTBEAT_INTERVAL:
            return  # Not time yet

        try:
            self.cw_client.put_metric_data(
                Namespace="QuickCart/SQSWorkers",
                MetricData=[
                    {
                        "MetricName": "WorkerHeartbeat",
                        "Value": 1,
                        "Unit": "Count",
                        "Dimensions": [
                            {"Name": "WorkerId", "Value": WORKER_ID},
                            {"Name": "QueueName", "Value": self.queue_url.split("/")[-1]}
                        ]
                    },
                    {
                        "MetricName": "MessagesProcessed",
                        "Value": self.messages_processed,
                        "Unit": "Count",
                        "Dimensions": [
                            {"Name": "WorkerId", "Value": WORKER_ID},
                            {"Name": "QueueName", "Value": self.queue_url.split("/")[-1]}
                        ]
                    }
                ]
            )
            self.last_heartbeat = current_time
        except Exception as e:
            logger.warning(f"Failed to publish heartbeat: {e}")

    def run(self):
        """
        Main worker loop — runs until SIGTERM received
        """
        logger.info("=" * 60)
        logger.info(f"SQS Worker starting")
        logger.info(f"Queue: {self.queue_url}")
        logger.info(f"Worker ID: {WORKER_ID}")
        logger.info(f"PID: {os.getpid()}")
        logger.info("=" * 60)

        while not shutdown_requested:
            processed = self.poll_and_process()
            self.publish_heartbeat()

            if processed == 0:
                # No messages — long poll already waited 20 seconds
                # Just continue (long poll handles the wait)
                pass

        # --- GRACEFUL SHUTDOWN ---
        logger.info("=" * 60)
        logger.info(f"Shutting down gracefully")
        logger.info(f"Total processed: {self.messages_processed}")
        logger.info(f"Total failed: {self.messages_failed}")
        logger.info("=" * 60)


if __name__ == "__main__":
    if not QUEUE_URL:
        logger.error("SQS_QUEUE_URL environment variable not set!")
        logger.error("Set it in User Data or export manually")
        sys.exit(1)

    worker = SQSWorker(queue_url=QUEUE_URL, region=REGION)
    worker.run()