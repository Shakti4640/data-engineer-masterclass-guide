# file: 54_01_create_notification_topics.py
# Purpose: Create severity-tiered SNS topics for pipeline alerts

import boto3
import json

REGION = "us-east-2"

NOTIFICATION_TIERS = [
    {
        "topic_name": "quickcart-pipeline-warnings",
        "severity": "WARNING",
        "description": "Non-critical pipeline issues — email only",
        "subscribers": [
            {
                "protocol": "email",
                "endpoint": "data-team@quickcart.example.com"
            }
        ]
    },
    {
        "topic_name": "quickcart-pipeline-critical",
        "severity": "CRITICAL",
        "description": "Critical pipeline failures — email + SMS",
        "subscribers": [
            {
                "protocol": "email",
                "endpoint": "data-team@quickcart.example.com"
            },
            {
                "protocol": "email",
                "endpoint": "oncall-data@quickcart.example.com"
            }
            # SMS subscription (uncomment with real number):
            # {
            #     "protocol": "sms",
            #     "endpoint": "+15551234567"
            # }
        ]
    },
    {
        "topic_name": "quickcart-pipeline-sla-breach",
        "severity": "SLA_BREACH",
        "description": "SLA violations — email + SMS + PagerDuty",
        "subscribers": [
            {
                "protocol": "email",
                "endpoint": "data-team@quickcart.example.com"
            },
            {
                "protocol": "email",
                "endpoint": "cto@quickcart.example.com"
            }
            # PagerDuty integration (uncomment with real endpoint):
            # {
            #     "protocol": "https",
            #     "endpoint": "https://events.pagerduty.com/integration/..."
            # }
        ]
    }
]


def create_notification_infrastructure():
    """Create tiered SNS topics with appropriate subscribers"""
    sns_client = boto3.client("sns", region_name=REGION)

    print("=" * 70)
    print("🔔 CREATING TIERED NOTIFICATION INFRASTRUCTURE")
    print("=" * 70)

    topic_arns = {}

    for tier in NOTIFICATION_TIERS:
        print(f"\n📌 {tier['severity']} — {tier['topic_name']}")
        print(f"   {tier['description']}")

        # Create topic
        response = sns_client.create_topic(
            Name=tier["topic_name"],
            Tags=[
                {"Key": "Team", "Value": "Data-Engineering"},
                {"Key": "Severity", "Value": tier["severity"]},
                {"Key": "Purpose", "Value": "Pipeline-Monitoring"}
            ]
        )
        topic_arn = response["TopicArn"]
        topic_arns[tier["severity"]] = topic_arn
        print(f"   ✅ Topic: {topic_arn}")

        # Add subscribers
        for sub in tier["subscribers"]:
            try:
                sns_client.subscribe(
                    TopicArn=topic_arn,
                    Protocol=sub["protocol"],
                    Endpoint=sub["endpoint"]
                )
                print(f"   ✅ Subscriber: {sub['protocol']}:{sub['endpoint']}")
                if sub["protocol"] == "email":
                    print(f"      ⚠️  CHECK EMAIL — must click confirmation link!")
            except Exception as e:
                print(f"   ⚠️  Subscriber failed: {e}")

    # Save topic ARNs for alarm configuration
    config = {"topic_arns": topic_arns}
    with open("/tmp/monitoring_config.json", "w") as f:
        json.dump(config, f, indent=2)

    print(f"\n💾 Config saved to /tmp/monitoring_config.json")

    # Summary
    print(f"\n{'=' * 70}")
    print(f"📊 NOTIFICATION ROUTING MATRIX")
    print(f"{'=' * 70}")
    print(f"{'Severity':<15} {'Email Team':^12} {'Email OnCall':^12} {'SMS':^8} {'PagerDuty':^10}")
    print(f"{'─' * 60}")
    print(f"{'WARNING':<15} {'✅':^12} {'':^12} {'':^8} {'':^10}")
    print(f"{'CRITICAL':<15} {'✅':^12} {'✅':^12} {'✅':^8} {'':^10}")
    print(f"{'SLA_BREACH':<15} {'✅':^12} {'✅':^12} {'✅':^8} {'✅':^10}")
    print(f"{'=' * 70}")

    return topic_arns


if __name__ == "__main__":
    create_notification_infrastructure()