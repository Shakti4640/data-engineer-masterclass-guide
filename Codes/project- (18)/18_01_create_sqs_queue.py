# file: 18_01_create_sqs_queue.py
# Purpose: Create SQS queue that S3 is allowed to send messages to
# Dependency: S3 bucket from Project 1, SQS concept from Project 13

import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"
QUEUE_NAME = "quickcart-file-processing-queue"
SOURCE_BUCKET = "quickcart-raw-data-prod"


def get_account_id():
    """Fetch current AWS account ID (same pattern as Project 17)"""
    sts_client = boto3.client("sts", region_name=REGION)
    return sts_client.get_caller_identity()["Account"]


def create_sqs_queue_with_s3_permission():
    sqs_client = boto3.client("sqs", region_name=REGION)
    account_id = get_account_id()

    # ── STEP 1: CONSTRUCT QUEUE ARN ──
    # We need the ARN for the policy BEFORE the queue exists
    # SQS ARN format: arn:aws:sqs:{region}:{account}:{queue_name}
    queue_arn = f"arn:aws:sqs:{REGION}:{account_id}:{QUEUE_NAME}"

    # ── STEP 2: BUILD QUEUE POLICY ──
    # Allows S3 to send messages to this queue
    # Same pattern as SNS Topic Policy in Project 17
    queue_policy = {
        "Version": "2012-10-17",
        "Id": "S3ToSQSPolicy",
        "Statement": [
            {
                "Sid": "AllowS3ToSendMessage",
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": "SQS:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:::{SOURCE_BUCKET}"
                    },
                    "StringEquals": {
                        "aws:SourceAccount": account_id
                    }
                }
            }
        ]
    }

    # ── STEP 3: CREATE QUEUE ──
    print("=" * 70)
    print("STEP 1: Creating SQS Queue")
    print("=" * 70)

    try:
        response = sqs_client.create_queue(
            QueueName=QUEUE_NAME,
            Attributes={
                # --- VISIBILITY TIMEOUT ---
                # How long a message is invisible after being received
                # Set to 10 minutes: our file processing takes ~5 min max
                # Rule: 2-3x expected processing time
                "VisibilityTimeout": "600",  # 10 minutes (in seconds)

                # --- MESSAGE RETENTION ---
                # How long unprocessed messages stay in queue
                # Max: 14 days (1,209,600 seconds)
                # If worker is down for a week, messages survive
                "MessageRetentionPeriod": "1209600",  # 14 days

                # --- RECEIVE MESSAGE WAIT TIME ---
                # Enable long polling at queue level
                # All ReceiveMessage calls wait up to 20 seconds
                # Overridable per-call with WaitTimeSeconds parameter
                "ReceiveMessageWaitTimeSeconds": "20",  # long polling

                # --- QUEUE POLICY ---
                # Allows S3 to send messages (set at creation time)
                "Policy": json.dumps(queue_policy),

                # --- DELAY ---
                # Delay before message becomes visible after being sent
                # 0 = immediate availability (no delay)
                "DelaySeconds": "0"
            },
            tags={
                "Purpose": "S3-File-Processing-Queue",
                "SourceBucket": SOURCE_BUCKET,
                "Team": "Data-Engineering",
                "VisibilityTimeout": "600-seconds",
                "Retention": "14-days"
            }
        )
        queue_url = response["QueueUrl"]
        print(f"   ✅ Queue created: {QUEUE_NAME}")
        print(f"   Queue URL: {queue_url}")

    except ClientError as e:
        if "QueueAlreadyExists" in str(e):
            # Queue exists — get its URL
            response = sqs_client.get_queue_url(QueueName=QUEUE_NAME)
            queue_url = response["QueueUrl"]
            print(f"   ℹ️  Queue already exists: {queue_url}")

            # Update policy in case it changed
            sqs_client.set_queue_attributes(
                QueueUrl=queue_url,
                Attributes={"Policy": json.dumps(queue_policy)}
            )
            print(f"   ✅ Queue Policy updated")
        else:
            raise

    # ── STEP 4: VERIFY QUEUE CONFIGURATION ──
    print("\n" + "=" * 70)
    print("STEP 2: Verifying queue configuration")
    print("=" * 70)

    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["All"]
    )["Attributes"]

    print(f"   Queue ARN:              {attrs['QueueArn']}")
    print(f"   Visibility Timeout:     {attrs['VisibilityTimeout']}s "
          f"({int(attrs['VisibilityTimeout'])//60} minutes)")
    print(f"   Message Retention:      {attrs['MessageRetentionPeriod']}s "
          f"({int(attrs['MessageRetentionPeriod'])//86400} days)")
    print(f"   Receive Wait Time:      {attrs['ReceiveMessageWaitTimeSeconds']}s "
          f"(long polling)")
    print(f"   Delay Seconds:          {attrs['DelaySeconds']}s")
    print(f"   Messages Available:     {attrs['ApproximateNumberOfMessages']}")
    print(f"   Messages In-Flight:     {attrs['ApproximateNumberOfMessagesNotVisible']}")

    # Verify policy allows S3
    policy = json.loads(attrs.get("Policy", "{}"))
    s3_allowed = False
    for stmt in policy.get("Statement", []):
        if stmt.get("Principal", {}).get("Service") == "s3.amazonaws.com":
            s3_allowed = True
            break
    print(f"   S3 Send Permission:     {'✅ Yes' if s3_allowed else '❌ No'}")

    return queue_url, queue_arn


if __name__ == "__main__":
    queue_url, queue_arn = create_sqs_queue_with_s3_permission()
    print(f"\n🎯 Queue URL (save this): {queue_url}")
    print(f"🎯 Queue ARN (save this): {queue_arn}")
    print(f"\n   → Next: run 18_02_configure_s3_to_sqs.py")