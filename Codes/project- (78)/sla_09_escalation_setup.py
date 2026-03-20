# file: sla/09_escalation_setup.py
# Purpose: Create tiered SNS topics for escalation chain

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def create_escalation_topics():
    """
    Create 4-tier SNS topic hierarchy
    
    Each tier has different subscribers:
    → Info: Slack webhook (entire team sees)
    → Warning: Slack on-call channel
    → Critical: PagerDuty integration (phone call)
    → Emergency: PagerDuty escalation (manager phone call)
    """
    sns = boto3.client("sns", region_name=REGION)

    topics = {
        "pipeline-info-alerts": {
            "display_name": "DataPlatform Info",
            "description": "Tier 0 — informational pipeline events",
            "subscribers": [
                {
                    "protocol": "email",
                    "endpoint": "data-team@quickcart.com"
                }
                # In production: HTTPS → Slack webhook
                # {
                #     "protocol": "https",
                #     "endpoint": "https://hooks.slack.com/services/XXX/YYY/ZZZ"
                # }
            ]
        },
        "pipeline-warning-alerts": {
            "display_name": "DataPlatform Warning",
            "description": "Tier 1 — auto-recovery in progress, attention needed",
            "subscribers": [
                {
                    "protocol": "email",
                    "endpoint": "data-oncall@quickcart.com"
                }
            ]
        },
        "pipeline-critical-alerts": {
            "display_name": "DataPlatform Critical",
            "description": "Tier 2 — auto-recovery failed, manual intervention required",
            "subscribers": [
                {
                    "protocol": "email",
                    "endpoint": "data-oncall-primary@quickcart.com"
                }
                # In production: HTTPS → PagerDuty Events API v2
                # {
                #     "protocol": "https",
                #     "endpoint": "https://events.pagerduty.com/integration/XXX/enqueue"
                # }
            ]
        },
        "pipeline-emergency-alerts": {
            "display_name": "DataPlatform Emergency",
            "description": "Tier 3 — SLA breach imminent, management escalation",
            "subscribers": [
                {
                    "protocol": "email",
                    "endpoint": "eng-manager@quickcart.com"
                },
                {
                    "protocol": "sms",
                    "endpoint": "+15551234567"
                }
            ]
        }
    }

    created_arns = {}

    for topic_name, config in topics.items():
        # Create topic
        response = sns.create_topic(
            Name=topic_name,
            Attributes={
                "DisplayName": config["display_name"]
            },
            Tags=[
                {"Key": "Purpose", "Value": "SLA-Escalation"},
                {"Key": "Tier", "Value": topic_name.split("-")[1]},
                {"Key": "ManagedBy", "Value": "SLA-System"}
            ]
        )
        topic_arn = response["TopicArn"]
        created_arns[topic_name] = topic_arn
        print(f"✅ Topic created: {topic_name}")

        # Add subscribers
        for sub in config["subscribers"]:
            sns.subscribe(
                TopicArn=topic_arn,
                Protocol=sub["protocol"],
                Endpoint=sub["endpoint"]
            )
            print(f"   📨 Subscriber added: {sub['protocol']} → {sub['endpoint']}")

    print(f"\n{'=' * 60}")
    print(f"✅ All escalation topics created")
    print(f"   ⚠️  Email subscribers must CONFIRM subscription via email link")
    return created_arns


if __name__ == "__main__":
    create_escalation_topics()