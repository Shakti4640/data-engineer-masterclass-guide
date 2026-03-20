# file: 55_05_create_eventbridge_trigger.py
# Purpose: Schedule pipeline execution via EventBridge (replaces all crons)

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
RULE_NAME = "quickcart-nightly-pipeline-trigger"


def load_config():
    with open("/tmp/step_functions_config.json", "r") as f:
        return json.load(f)


def create_eventbridge_schedule():
    """
    Create EventBridge Rule that triggers Step Functions on schedule
    
    THIS REPLACES ALL INDIVIDUAL CRONS:
    → No more: cron for upload, cron for DataBrew, cron for Glue, etc.
    → One rule: trigger Step Functions at 1 AM
    → Step Functions handles ordering, dependencies, retries
    
    TIMING:
    → Old: 12:00 AM upload, 2:00 AM DataBrew, 3:00 AM Crawler, 4:00 AM ETL
    → New: 1:00 AM trigger → Step Functions runs everything in sequence
    → Saves 4+ hours of buffer time
    """
    config = load_config()
    events_client = boto3.client("events", region_name=REGION)
    iam_client = boto3.client("iam")

    print("=" * 70)
    print("⏰ CREATING EVENTBRIDGE SCHEDULED TRIGGER")
    print("=" * 70)

    # --- Create EventBridge role to invoke Step Functions ---
    eb_role_name = "QuickCart-EventBridge-StepFunctions-Role"
    eb_trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    eb_permission_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "states:StartExecution",
            "Resource": config["state_machine_arn"]
        }]
    }

    try:
        iam_client.create_role(
            RoleName=eb_role_name,
            AssumeRolePolicyDocument=json.dumps(eb_trust_policy),
            Description="Allow EventBridge to start Step Functions execution"
        )
    except iam_client.exceptions.EntityAlreadyExistsException:
        pass

    iam_client.put_role_policy(
        RoleName=eb_role_name,
        PolicyName="InvokeStepFunctions",
        PolicyDocument=json.dumps(eb_permission_policy)
    )
    eb_role = iam_client.get_role(RoleName=eb_role_name)
    eb_role_arn = eb_role["Role"]["Arn"]
    print(f"  ✅ EventBridge role: {eb_role_name}")

    # --- Create EventBridge Rule ---
    events_client.put_rule(
        Name=RULE_NAME,
        ScheduleExpression="cron(0 1 * * ? *)",    # 1:00 AM UTC daily
        State="ENABLED",
        Description="Trigger QuickCart nightly pipeline at 1 AM UTC daily",
        Tags=[
            {"Key": "Team", "Value": "Data-Engineering"},
            {"Key": "Pipeline", "Value": "nightly-etl"}
        ]
    )
    print(f"  ✅ EventBridge Rule: {RULE_NAME}")
    print(f"     Schedule: cron(0 1 * * ? *) → 1:00 AM UTC daily")

    # --- Add Step Functions as target ---
    events_client.put_targets(
        Rule=RULE_NAME,
        Targets=[
            {
                "Id": "StepFunctionsTarget",
                "Arn": config["state_machine_arn"],
                "RoleArn": eb_role_arn,
                "Input": json.dumps({
                    "execution_date": "auto",
                    "pipeline": "nightly-etl",
                    "triggered_by": "eventbridge-schedule",
                    "source_bucket": "quickcart-raw-data-prod"
                })
            }
        ]
    )
    print(f"  ✅ Target: {config['state_machine_name']}")
    print(f"     Input includes: execution_date, pipeline, triggered_by")

    # --- Also show S3 event trigger option ---
    print(f"\n{'─' * 50}")
    print(f"📋 ALTERNATIVE TRIGGER: S3 Event (from Project 41)")
    print(f"{'─' * 50}")
    print(f"  Instead of cron, trigger when file LANDS in S3:")
    print(f"  → S3 PutObject event → EventBridge Rule → Step Functions")
    print(f"  → More responsive: pipeline starts IMMEDIATELY after upload")
    print(f"  → No wasted buffer time")
    print(f"")
    print(f"  EventBridge pattern for S3 trigger:")
    print(f'  {{')
    print(f'    "source": ["aws.s3"],')
    print(f'    "detail-type": ["Object Created"],')
    print(f'    "detail": {{')
    print(f'      "bucket": {{"name": ["quickcart-raw-data-prod"]}},')
    print(f'      "object": {{"key": [{{"prefix": "daily_exports/"}}]}}')
    print(f'    }}')
    print(f'  }}')

    # --- Summary ---
    print(f"\n{'=' * 70}")
    print(f"📊 SCHEDULING SUMMARY")
    print(f"{'=' * 70}")
    print(f"  BEFORE (6 independent crons):")
    print(f"    12:00 AM  Upload cron")
    print(f"     2:00 AM  DataBrew cron")
    print(f"     3:00 AM  Crawler cron")
    print(f"     4:00 AM  Glue ETL cron")
    print(f"     4:30 AM  Athena cron")
    print(f"     5:00 AM  Redshift cron")
    print(f"    Total buffer: 5 hours | No dependency | No error handling")
    print(f"")
    print(f"  AFTER (1 EventBridge rule → Step Functions):")
    print(f"     1:00 AM  EventBridge → Step Functions → ALL steps in sequence")
    print(f"    Total time: ~30 min | Full dependency | Built-in retry + catch")
    print(f"    Time saved: ~4.5 hours per night")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    create_eventbridge_schedule()