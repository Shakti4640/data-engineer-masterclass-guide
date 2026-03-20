# file: 51_02_event_publisher.py
# Purpose: Publish events with proper MessageAttributes for SNS filtering

import boto3
import json
import uuid
from datetime import datetime

REGION = "us-east-2"


def load_infrastructure():
    with open("/tmp/filtered_infra.json", "r") as f:
        return json.load(f)


class EventPublisher:
    """
    Centralized event publisher for QuickCart
    
    WHY a class:
    → Enforces consistent MessageAttributes across all publishers
    → Prevents the #1 pitfall: forgetting attributes
    → Single place to add new attributes system-wide
    """

    # Valid event types — enforced to prevent typos
    VALID_EVENT_TYPES = [
        "etl_event", "order_event", "alert_event", "data_quality_event"
    ]

    VALID_SEVERITIES = ["info", "warning", "critical"]

    def __init__(self, topic_arn, region=REGION):
        self.sns_client = boto3.client("sns", region_name=region)
        self.topic_arn = topic_arn

    def publish(self, event_type, severity, source_system,
                event_name, payload, environment="prod"):
        """
        Publish event with MANDATORY MessageAttributes
        
        Parameters:
        → event_type: controls which queue receives the message
        → severity: secondary filter dimension
        → source_system: who generated this event
        → event_name: specific event (e.g., "glue_job_failed")
        → payload: dict with event details (becomes message body)
        → environment: prod/staging/dev
        """

        # --- VALIDATION (prevents silent filter misses) ---
        if event_type not in self.VALID_EVENT_TYPES:
            raise ValueError(
                f"Invalid event_type '{event_type}'. "
                f"Valid: {self.VALID_EVENT_TYPES}"
            )

        if severity not in self.VALID_SEVERITIES:
            raise ValueError(
                f"Invalid severity '{severity}'. "
                f"Valid: {self.VALID_SEVERITIES}"
            )

        # --- BUILD MESSAGE BODY ---
        message_id = str(uuid.uuid4())
        message_body = {
            "message_id": message_id,
            "event_type": event_type,
            "event_name": event_name,
            "severity": severity,
            "source_system": source_system,
            "environment": environment,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "payload": payload
        }

        # --- BUILD MESSAGE ATTRIBUTES ---
        # These are what SNS filter policies evaluate
        # MUST be set here — body fields are NOT checked
        # (unless FilterPolicyScope = "MessageBody")
        message_attributes = {
            "event_type": {
                "DataType": "String",
                "StringValue": event_type
            },
            "severity": {
                "DataType": "String",
                "StringValue": severity
            },
            "source_system": {
                "DataType": "String",
                "StringValue": source_system
            },
            "environment": {
                "DataType": "String",
                "StringValue": environment
            },
            "event_name": {
                "DataType": "String",
                "StringValue": event_name
            }
        }

        # --- PUBLISH ---
        response = self.sns_client.publish(
            TopicArn=self.topic_arn,
            Message=json.dumps(message_body),
            MessageAttributes=message_attributes
        )

        sns_message_id = response["MessageId"]

        print(f"  📤 Published: {event_name}")
        print(f"     Type: {event_type} | Severity: {severity}")
        print(f"     SNS MessageId: {sns_message_id}")

        return {
            "message_id": message_id,
            "sns_message_id": sns_message_id,
            "event_type": event_type,
            "event_name": event_name
        }


def publish_test_events():
    """
    Publish a variety of events to test filter routing
    Each event should land in ONLY the correct queue(s)
    """
    infra = load_infrastructure()
    publisher = EventPublisher(topic_arn=infra["topic_arn"])

    print("=" * 70)
    print("🚀 PUBLISHING TEST EVENTS WITH MESSAGE ATTRIBUTES")
    print("=" * 70)

    events_to_publish = [
        # --- ETL EVENTS (should go to: ETL Queue + Audit Queue) ---
        {
            "event_type": "etl_event",
            "severity": "info",
            "source_system": "glue",
            "event_name": "file_landed",
            "payload": {
                "bucket": "quickcart-raw-data-prod",
                "key": "daily_exports/orders/orders_20250115.csv",
                "size_bytes": 52428800
            }
        },
        {
            "event_type": "etl_event",
            "severity": "critical",
            "source_system": "glue",
            "event_name": "glue_job_failed",
            "payload": {
                "job_name": "quickcart-orders-etl",
                "run_id": "jr_abc123",
                "error": "OutOfMemoryError: Java heap space"
            }
        },

        # --- ORDER EVENTS (should go to: Order Queue + Audit Queue) ---
        {
            "event_type": "order_event",
            "severity": "info",
            "source_system": "order_mgmt",
            "event_name": "order_placed",
            "payload": {
                "order_id": "ORD-20250115-000001",
                "customer_id": "CUST-000042",
                "total": 299.99
            }
        },
        {
            "event_type": "order_event",
            "severity": "info",
            "source_system": "order_mgmt",
            "event_name": "order_shipped",
            "payload": {
                "order_id": "ORD-20250115-000001",
                "tracking_number": "1Z999AA10123456784"
            }
        },

        # --- ALERT EVENTS (should go to: Alert Queue + Audit Queue) ---
        {
            "event_type": "alert_event",
            "severity": "critical",
            "source_system": "monitoring",
            "event_name": "pipeline_failed",
            "payload": {
                "pipeline": "nightly-mariadb-to-redshift",
                "step_failed": "redshift_copy",
                "error": "Connection refused"
            }
        },

        # --- DATA QUALITY EVENTS (should go to: Alert Queue + Audit Queue) ---
        {
            "event_type": "data_quality_event",
            "severity": "warning",
            "source_system": "data_quality",
            "event_name": "dq_check_failed",
            "payload": {
                "table": "orders_gold",
                "rule": "row_count >= 100000",
                "actual": 45000,
                "expected_min": 100000
            }
        },
        {
            "event_type": "data_quality_event",
            "severity": "critical",
            "source_system": "data_quality",
            "event_name": "schema_drift_detected",
            "payload": {
                "table": "customers_raw",
                "new_columns": ["loyalty_points", "referral_code"],
                "removed_columns": []
            }
        }
    ]

    results = []
    for i, event in enumerate(events_to_publish, 1):
        print(f"\n--- Event {i}/{len(events_to_publish)} ---")
        result = publisher.publish(**event)
        results.append(result)

    # --- EXPECTED ROUTING TABLE ---
    print("\n" + "=" * 70)
    print("📊 EXPECTED ROUTING (based on filter policies)")
    print("=" * 70)
    print(f"{'Event':<30} {'ETL':^6} {'Order':^6} {'Alert':^6} {'Audit':^6}")
    print("-" * 70)

    routing_expectations = [
        ("file_landed (etl)",           "✅", "❌", "❌", "✅"),
        ("glue_job_failed (etl)",       "✅", "❌", "❌", "✅"),
        ("order_placed (order)",        "❌", "✅", "❌", "✅"),
        ("order_shipped (order)",       "❌", "✅", "❌", "✅"),
        ("pipeline_failed (alert)",     "❌", "❌", "✅", "✅"),
        ("dq_check_failed (dq)",        "❌", "❌", "✅", "✅"),
        ("schema_drift (dq)",           "❌", "❌", "✅", "✅"),
    ]

    for event_name, etl, order, alert, audit in routing_expectations:
        print(f"  {event_name:<30} {etl:^6} {order:^6} {alert:^6} {audit:^6}")

    print(f"\n  Total published: {len(events_to_publish)}")
    print(f"  ETL Queue should receive: 2")
    print(f"  Order Queue should receive: 2")
    print(f"  Alert Queue should receive: 3")
    print(f"  Audit Queue should receive: 7 (all)")
    print("=" * 70)

    return results


if __name__ == "__main__":
    publish_test_events()