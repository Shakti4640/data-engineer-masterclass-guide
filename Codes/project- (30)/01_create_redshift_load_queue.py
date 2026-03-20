# file: 01_create_redshift_load_queue.py
# Creates SQS queue dedicated to Redshift COPY operations
# Separate from Project 27 queue (different processing, different timeout)

import boto3
import json

REGION = "us-east-2"
QUEUE_NAME = "quickcart-redshift-load-queue"
DLQ_NAME = "quickcart-redshift-load-dlq"
BUCKET_NAME = "quickcart-raw-data-prod"


def create_redshift_load_queue():
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("=" * 60)
    print("📬 CREATING REDSHIFT LOAD QUEUE")
    print("=" * 60)

    # --- DLQ FIRST ---
    try:
        dlq_response = sqs_client.create_queue(
            QueueName=DLQ_NAME,
            Attributes={
                "MessageRetentionPeriod": "1209600",    # 14 days
                "ReceiveMessageWaitTimeSeconds": "20"
            }
        )
        dlq_url = dlq_response["QueueUrl"]
        print(f"✅ DLQ created: {DLQ_NAME}")
    except Exception:
        dlq_url = sqs_client.get_queue_url(QueueName=DLQ_NAME)["QueueUrl"]
        print(f"ℹ️  DLQ exists: {DLQ_NAME}")

    dlq_arn = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    # --- MAIN QUEUE ---
    redrive_policy = {
        "deadLetterTargetArn": dlq_arn,
        "maxReceiveCount": "3"
    }

    try:
        queue_response = sqs_client.create_queue(
            QueueName=QUEUE_NAME,
            Attributes={
                "VisibilityTimeout": "600",             # 10 minutes (COPY can be slow)
                "MessageRetentionPeriod": "345600",     # 4 days
                "ReceiveMessageWaitTimeSeconds": "20",
                "RedrivePolicy": json.dumps(redrive_policy)
            }
        )
        queue_url = queue_response["QueueUrl"]
        print(f"✅ Queue created: {QUEUE_NAME}")
    except Exception:
        queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        print(f"ℹ️  Queue exists: {QUEUE_NAME}")

    # Get queue ARN for S3 notification policy
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    # --- SQS POLICY: Allow S3 to send events ---
    sqs_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowS3Events",
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "SQS:SendMessage",
            "Resource": queue_arn,
            "Condition": {
                "ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{BUCKET_NAME}"}
            }
        }]
    }

    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(sqs_policy)}
    )

    print(f"\n📋 Configuration:")
    print(f"   Queue ARN:     {queue_arn}")
    print(f"   Visibility:    600s (10 min — enough for large COPY)")
    print(f"   DLQ:           {DLQ_NAME} (after 3 failures)")
    print(f"   S3 policy:     ✅ S3 can send events")

    return queue_url, queue_arn


def configure_s3_notification(queue_arn):
    """Configure S3 to send events for mariadb_exports/ prefix"""
    s3_client = boto3.client("s3", region_name=REGION)

    # Get existing notification config
    try:
        existing = s3_client.get_bucket_notification_configuration(Bucket=BUCKET_NAME)
        queue_configs = existing.get("QueueConfigurations", [])
        topic_configs = existing.get("TopicConfigurations", [])
        lambda_configs = existing.get("LambdaFunctionConfigurations", [])

        # Remove old Redshift load config if exists
        queue_configs = [q for q in queue_configs if q.get("Id") != "RedshiftLoadTrigger"]
    except Exception:
        queue_configs = []
        topic_configs = []
        lambda_configs = []

    # Add new config
    queue_configs.append({
        "Id": "RedshiftLoadTrigger",
        "QueueArn": queue_arn,
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
            "Key": {
                "FilterRules": [
                    {"Name": "prefix", "Value": "mariadb_exports/"},
                    {"Name": "suffix", "Value": ".csv.gz"}
                ]
            }
        }
    })

    notification_config = {
        "QueueConfigurations": queue_configs
    }
    if topic_configs:
        notification_config["TopicConfigurations"] = topic_configs
    if lambda_configs:
        notification_config["LambdaFunctionConfigurations"] = lambda_configs

    s3_client.put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration=notification_config
    )

    print(f"\n✅ S3 notification configured:")
    print(f"   Prefix: mariadb_exports/")
    print(f"   Suffix: .csv.gz")
    print(f"   Target: {QUEUE_NAME}")


if __name__ == "__main__":
    queue_url, queue_arn = create_redshift_load_queue()
    configure_s3_notification(queue_arn)