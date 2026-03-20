# file: 51_06_body_based_filtering.py
# Purpose: Filter on message BODY when publisher doesn't set MessageAttributes

import boto3
import json
import time

REGION = "us-east-2"


def demonstrate_body_based_filtering():
    """
    SCENARIO:
    → 3rd party system publishes to our SNS topic via webhook
    → They send JSON body but NO MessageAttributes
    → We can't change their publisher code
    → Solution: FilterPolicyScope = "MessageBody"
    
    EXAMPLE 3rd party payload (body only, no attributes):
    {
        "source": "stripe",
        "event": "payment.succeeded",
        "data": {
            "amount": 29999,
            "currency": "usd"
        }
    }
    """
    sns_client = boto3.client("sns", region_name=REGION)
    sqs_client = boto3.client("sqs", region_name=REGION)

    print("=" * 70)
    print("🔬 BODY-BASED FILTERING DEMO")
    print("=" * 70)

    # --- Create dedicated topic for this demo ---
    topic_response = sns_client.create_topic(Name="quickcart-webhook-events")
    topic_arn = topic_response["TopicArn"]
    print(f"\n✅ Topic: {topic_arn}")

    # --- Create two queues ---
    # Queue A: only payment events
    # Queue B: only refund events
    queues = {}
    for queue_name in ["quickcart-payments-queue", "quickcart-refunds-queue"]:
        resp = sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={"ReceiveMessageWaitTimeSeconds": "5"}
        )
        queue_url = resp["QueueUrl"]
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        queue_arn = attrs["Attributes"]["QueueArn"]

        # Set SQS policy for SNS
        policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "sns.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}}
            }]
        }
        sqs_client.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={"Policy": json.dumps(policy)}
        )

        queues[queue_name] = {"url": queue_url, "arn": queue_arn}
        print(f"✅ Queue: {queue_name}")

    # --- Subscribe with BODY-BASED filter policies ---

    # Payment queue: event field starts with "payment."
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queues["quickcart-payments-queue"]["arn"],
        Attributes={
            "RawMessageDelivery": "true",
            "FilterPolicyScope": "MessageBody",    # <-- KEY DIFFERENCE
            "FilterPolicy": json.dumps({
                "event": [{"prefix": "payment."}]
            })
        }
    )
    print(f"\n✅ Payments subscription — body filter: event starts with 'payment.'")

    # Refund queue: event field starts with "refund."
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queues["quickcart-refunds-queue"]["arn"],
        Attributes={
            "RawMessageDelivery": "true",
            "FilterPolicyScope": "MessageBody",    # <-- KEY DIFFERENCE
            "FilterPolicy": json.dumps({
                "event": [{"prefix": "refund."}]
            })
        }
    )
    print(f"✅ Refunds subscription — body filter: event starts with 'refund.'")

    # --- Publish test messages (body only — NO MessageAttributes) ---
    print(f"\n{'─' * 60}")
    print("📤 Publishing test messages (body-only, no attributes)...")

    test_messages = [
        {
            "source": "stripe",
            "event": "payment.succeeded",
            "data": {"amount": 29999, "currency": "usd"}
        },
        {
            "source": "stripe",
            "event": "payment.failed",
            "data": {"amount": 5000, "currency": "usd", "error": "card_declined"}
        },
        {
            "source": "stripe",
            "event": "refund.created",
            "data": {"amount": 15000, "currency": "usd", "reason": "customer_request"}
        },
        {
            "source": "stripe",
            "event": "customer.updated",
            "data": {"customer_id": "cus_abc123"}
        }
    ]

    for msg in test_messages:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(msg)
            # NOTE: NO MessageAttributes — that's the whole point
        )
        print(f"  Published: {msg['event']}")

    # --- Wait and verify ---
    print(f"\n⏳ Waiting 5 seconds for delivery...")
    time.sleep(5)

    print(f"\n{'─' * 60}")
    print("📥 Checking queue contents...")

    for queue_name, queue_info in queues.items():
        messages = []
        while True:
            resp = sqs_client.receive_message(
                QueueUrl=queue_info["url"],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2
            )
            if "Messages" not in resp:
                break
            for m in resp["Messages"]:
                body = json.loads(m["Body"])
                messages.append(body["event"])
                sqs_client.delete_message(
                    QueueUrl=queue_info["url"],
                    ReceiptHandle=m["ReceiptHandle"]
                )

        print(f"\n  {queue_name}:")
        print(f"    Received {len(messages)} messages: {messages}")

    print(f"\n📊 EXPECTED RESULTS:")
    print(f"  payments-queue: [payment.succeeded, payment.failed]")
    print(f"  refunds-queue:  [refund.created]")
    print(f"  customer.updated → delivered to NEITHER (no matching filter)")

    # --- Cleanup ---
    for queue_name, queue_info in queues.items():
        sqs_client.delete_queue(QueueUrl=queue_info["url"])
    sns_client.delete_topic(TopicArn=topic_arn)
    print(f"\n🧹 Cleanup complete")

    print("=" * 70)


if __name__ == "__main__":
    demonstrate_body_based_filtering()