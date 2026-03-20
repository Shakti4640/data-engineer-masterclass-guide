# file: 41_04_create_eventbridge_rule.py
# Purpose: Create EventBridge rule that triggers Step Functions on S3 file arrival

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

RULE_NAME = "quickcart-s3-file-arrival-trigger"
STATE_MACHINE_ARN = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:quickcart-etl-pipeline"
EVENTBRIDGE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/quickcart-eventbridge-to-stepfn-role"
BUCKET_NAME = "quickcart-raw-data-prod"


def create_eventbridge_rule():
    """
    Create EventBridge rule that:
    1. Matches S3 Object Created events
    2. Filters to specific bucket + prefix + suffix
    3. Triggers Step Functions with the event as input
    """
    events_client = boto3.client("events", region_name=REGION)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # EVENT PATTERN
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # This pattern MUST match the exact structure of S3 → EventBridge events
    # Test at: EventBridge Console → Sandbox → Event Pattern
    event_pattern = {
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {
            "bucket": {
                "name": [BUCKET_NAME]
            },
            "object": {
                "key": [
                    {"prefix": "daily_exports/"},
                    {"suffix": ".csv"}
                ]
            }
        }
    }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CREATE RULE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    rule_response = events_client.put_rule(
        Name=RULE_NAME,
        EventPattern=json.dumps(event_pattern),
        State="ENABLED",
        Description="Triggers ETL pipeline when CSV lands in daily_exports/ prefix",
        Tags=[
            {"Key": "Environment", "Value": "Production"},
            {"Key": "Project", "Value": "QuickCart-ETL"}
        ]
    )

    rule_arn = rule_response["RuleArn"]
    print(f"✅ EventBridge Rule created: {RULE_NAME}")
    print(f"   ARN: {rule_arn}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ADD TARGET (Step Functions)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    events_client.put_targets(
        Rule=RULE_NAME,
        Targets=[
            {
                "Id": "StepFunctionsETLPipeline",
                "Arn": STATE_MACHINE_ARN,
                "RoleArn": EVENTBRIDGE_ROLE_ARN,

                # IMPORTANT: This controls what JSON is passed to Step Functions
                # "MATCHED_EVENT" = pass the full S3 event as state machine input
                # Alternative: use InputTransformer to reshape the JSON
            }
        ]
    )

    print(f"   ✅ Target added: Step Functions → {STATE_MACHINE_ARN}")
    print(f"   ✅ Using role: {EVENTBRIDGE_ROLE_ARN}")

    return rule_arn


def verify_rule():
    """Verify the rule and target are correctly configured"""
    events_client = boto3.client("events", region_name=REGION)

    # Check rule
    rule = events_client.describe_rule(Name=RULE_NAME)
    print(f"\n🔍 Rule Verification:")
    print(f"   Name: {rule['Name']}")
    print(f"   State: {rule['State']}")
    print(f"   Pattern: {json.loads(rule['EventPattern'])}")

    # Check targets
    targets = events_client.list_targets_by_rule(Rule=RULE_NAME)
    for target in targets["Targets"]:
        print(f"   Target: {target['Id']} → {target['Arn']}")


if __name__ == "__main__":
    create_eventbridge_rule()
    verify_rule()