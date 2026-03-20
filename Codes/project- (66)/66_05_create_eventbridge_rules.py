# file: 66_05_create_eventbridge_rules.py
# Run from: Account B (222222222222)
# Purpose: Create EventBridge rules that route events to Step Function

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
SM_ARN = f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:event-driven-data-pipeline"

events = boto3.client("events", region_name=REGION)
iam = boto3.client("iam")


def create_eventbridge_role():
    """
    IAM role that allows EventBridge to invoke Step Functions
    
    EventBridge needs permission to call states:StartExecution
    This role is assumed by EventBridge when firing rules
    """
    role_name = "EventBridgePipelineTriggerRole"

    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "states:StartExecution",
            "Resource": SM_ARN
        }]
    }

    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust)
        )
    except iam.exceptions.EntityAlreadyExistsException:
        pass

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="StartStepFunction",
        PolicyDocument=json.dumps(policy)
    )

    role_arn = f"arn:aws:iam::{ACCOUNT_B_ID}:role/{role_name}"
    print(f"✅ EventBridge role ready: {role_arn}")
    return role_arn


def create_s3_file_arrival_rules(eb_role_arn):
    """
    Rules that trigger pipeline when files land in S3
    
    RULE PER SOURCE BUCKET:
    → Partner feeds: *.csv in quickcart-partner-feeds
    → Payment data: *.json in quickcart-payment-data
    → Marketing: *.csv in quickcart-marketing-uploads
    
    EVENT PATTERN FILTERING:
    → Only triggers on Object Created (PutObject, CompleteMultipartUpload)
    → Filters by bucket name AND key suffix
    → Ignores: _SUCCESS files, .crc files, temp files
    """
    rules = [
        {
            "name": "partner-file-arrival",
            "description": "Partner CSV lands in S3 → trigger ETL pipeline",
            "pattern": {
                "source": ["aws.s3"],
                "detail-type": ["Object Created"],
                "detail": {
                    "bucket": {"name": ["quickcart-partner-feeds"]},
                    "object": {
                        "key": [{"prefix": "daily/"}, {"suffix": ".csv"}]
                    }
                }
            }
        },
        {
            "name": "payment-file-arrival",
            "description": "Payment JSON lands in S3 → trigger ETL pipeline",
            "pattern": {
                "source": ["aws.s3"],
                "detail-type": ["Object Created"],
                "detail": {
                    "bucket": {"name": ["quickcart-payment-data"]},
                    "object": {
                        "key": [{"suffix": ".json"}]
                    }
                }
            }
        },
        {
            "name": "marketing-file-arrival",
            "description": "Marketing CSV uploaded → trigger ETL pipeline",
            "pattern": {
                "source": ["aws.s3"],
                "detail-type": ["Object Created"],
                "detail": {
                    "bucket": {"name": ["quickcart-marketing-uploads"]},
                    "object": {
                        "key": [{"suffix": ".csv"}]
                    }
                }
            }
        }
    ]

    for rule in rules:
        events.put_rule(
            Name=rule["name"],
            Description=rule["description"],
            EventPattern=json.dumps(rule["pattern"]),
            State="ENABLED",
            Tags=[
                {"Key": "Purpose", "Value": "EventDrivenPipeline"},
                {"Key": "TriggerType", "Value": "S3FileArrival"}
            ]
        )

        events.put_targets(
            Rule=rule["name"],
            Targets=[{
                "Id": f"pipeline-{rule['name']}",
                "Arn": SM_ARN,
                "RoleArn": eb_role_arn,
                "Input": None  # Pass full event to Step Function
            }]
        )
        print(f"✅ Rule created: {rule['name']}")


def create_glue_completion_rule(eb_role_arn):
    """
    Rule that triggers when ANY Glue ETL job succeeds
    → Used to re-evaluate: are all sources complete?
    → Triggers the coordinator logic in Step Function
    
    GLUE STATE CHANGE EVENTS:
    → Glue natively emits to EventBridge when job state changes
    → No configuration needed — automatic
    → States: STARTING, RUNNING, STOPPING, SUCCEEDED, FAILED, TIMEOUT
    """
    rule_name = "glue-job-completion"

    events.put_rule(
        Name=rule_name,
        Description="Any Glue ETL job completes → check if all sources ready",
        EventPattern=json.dumps({
            "source": ["aws.glue"],
            "detail-type": ["Glue Job State Change"],
            "detail": {
                "state": ["SUCCEEDED"],
                "jobName": [
                    "etl-partner-feed",
                    "etl-payment-data",
                    "etl-marketing-campaign",
                    "etl-inventory-api-pull",
                    "cdc-mariadb-to-redshift"
                ]
            }
        }),
        State="ENABLED",
        Tags=[
            {"Key": "Purpose", "Value": "EventDrivenPipeline"},
            {"Key": "TriggerType", "Value": "GlueCompletion"}
        ]
    )

    events.put_targets(
        Rule=rule_name,
        Targets=[{
            "Id": "pipeline-glue-completion",
            "Arn": SM_ARN,
            "RoleArn": eb_role_arn
        }]
    )
    print(f"✅ Rule created: {rule_name}")


def create_scheduled_rules(eb_role_arn):
    """
    Scheduled rules for sources that don't have event triggers
    → CDC: 2:00 AM daily (MariaDB doesn't emit events)
    → Inventory API: hourly (pull-based, not push-based)
    
    THESE REPLACE: Glue Triggers (cron-based) from previous projects
    """
    schedules = [
        {
            "name": "scheduled-cdc-trigger",
            "description": "Daily 2 AM: trigger MariaDB CDC pipeline",
            "schedule": "cron(0 2 * * ? *)",
            "input": json.dumps({
                "source": "aws.events",
                "detail-type": "Scheduled Event",
                "detail": {},
                "resources": [f"arn:aws:events:{REGION}:{ACCOUNT_B_ID}:rule/scheduled-cdc-trigger"]
            })
        },
        {
            "name": "scheduled-inventory-trigger",
            "description": "Hourly: trigger inventory API pull",
            "schedule": "rate(1 hour)",
            "input": json.dumps({
                "source": "aws.events",
                "detail-type": "Scheduled Event",
                "detail": {},
                "resources": [f"arn:aws:events:{REGION}:{ACCOUNT_B_ID}:rule/scheduled-inventory-trigger"]
            })
        }
    ]

    for sched in schedules:
        events.put_rule(
            Name=sched["name"],
            Description=sched["description"],
            ScheduleExpression=sched["schedule"],
            State="ENABLED",
            Tags=[
                {"Key": "Purpose", "Value": "EventDrivenPipeline"},
                {"Key": "TriggerType", "Value": "Schedule"}
            ]
        )

        events.put_targets(
            Rule=sched["name"],
            Targets=[{
                "Id": f"pipeline-{sched['name']}",
                "Arn": SM_ARN,
                "RoleArn": eb_role_arn,
                "Input": sched["input"]
            }]
        )
        print(f"✅ Schedule created: {sched['name']} ({sched['schedule']})")


def create_timeout_watchdog_rule(eb_role_arn):
    """
    Safety net: if a required source hasn't arrived by 5 AM
    → Fire alert so team can investigate
    → This catches the "source never arrives" scenario
    """
    rule_name = "pipeline-timeout-watchdog"

    events.put_rule(
        Name=rule_name,
        Description="5 AM daily: check if any required source is still missing",
        ScheduleExpression="cron(0 5 * * ? *)",
        State="ENABLED",
        Tags=[
            {"Key": "Purpose", "Value": "EventDrivenPipeline"},
            {"Key": "TriggerType", "Value": "Watchdog"}
        ]
    )

    # Target: Lambda that checks DynamoDB state and alerts if incomplete
    events.put_targets(
        Rule=rule_name,
        Targets=[{
            "Id": "pipeline-watchdog",
            "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT_B_ID}:function:pipeline-watchdog",
            "Input": json.dumps({"check": "missing_sources"})
        }]
    )
    print(f"✅ Watchdog created: {rule_name} (daily at 5 AM)")


def disable_old_glue_triggers():
    """
    Disable all old cron-based Glue Triggers
    → Prevents duplicate processing
    → EventBridge is now the SOLE orchestrator
    """
    glue = boto3.client("glue", region_name=REGION)

    old_triggers = [
        "trigger-nightly-cross-account-etl",
        "trigger-nightly-cdc",
        "trigger-weekly-full-reconciliation",
        "trigger-compact-orders-weekly",
        "trigger-compact-customers-weekly",
        "trigger-compact-products-weekly"
    ]

    for trigger_name in old_triggers:
        try:
            glue.stop_trigger(Name=trigger_name)
            print(f"✅ Disabled Glue Trigger: {trigger_name}")
        except glue.exceptions.EntityNotFoundException:
            print(f"ℹ️  Trigger not found: {trigger_name}")
        except Exception as e:
            print(f"⚠️  Could not disable {trigger_name}: {str(e)[:80]}")


if __name__ == "__main__":
    print("=" * 60)
    print("📡 CREATING EVENTBRIDGE RULES")
    print("=" * 60)

    eb_role_arn = create_eventbridge_role()
    print()

    import time
    time.sleep(10)  # Role propagation

    create_s3_file_arrival_rules(eb_role_arn)
    print()
    create_glue_completion_rule(eb_role_arn)
    print()
    create_scheduled_rules(eb_role_arn)
    print()
    create_timeout_watchdog_rule(eb_role_arn)
    print()
    disable_old_glue_triggers()

    print(f"\n{'='*60}")
    print("✅ ALL EVENTBRIDGE RULES DEPLOYED")
    print("   S3 arrival rules: 3 (partner, payment, marketing)")
    print("   Glue completion rule: 1 (any ETL job succeeds)")
    print("   Schedule rules: 2 (CDC daily, inventory hourly)")
    print("   Watchdog: 1 (5 AM missing source check)")
    print("   Old Glue triggers: DISABLED")
    print(f"{'='*60}")
