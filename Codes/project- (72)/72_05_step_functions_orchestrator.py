# file: 72_05_step_functions_orchestrator.py
# Creates the Step Functions state machine that orchestrates the micro-batch pipeline
# Triggered by EventBridge when new files land in Bronze S3

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = [REDACTED:BANK_ACCOUNT_NUMBER]1"
BUCKET_NAME = "orders-data-prod"

# Governance SNS topic for data product events (Project 71)
GOVERNANCE_SNS_TOPIC = f"arn:aws:sns:{REGION}:444444444444:data-product-events"


def create_step_function_role(iam_client):
    """IAM role for Step Functions to invoke Glue, Athena, SNS"""
    role_name = "ClickstreamMicroBatchOrchestrator"

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "states.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "GlueJobControl",
                "Effect": "Allow",
                "Action": [
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun"
                ],
                "Resource": f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:job/clickstream-microbatch-etl"
            },
            {
                "Sid": "SNSPublish",
                "Effect": "Allow",
                "Action": "sns:Publish",
                "Resource": [
                    GOVERNANCE_SNS_TOPIC,
                    f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:clickstream-alerts"
                ]
            },
            {
                "Sid": "CloudWatchMetrics",
                "Effect": "Allow",
                "Action": "cloudwatch:PutMetricData",
                "Resource": "*"
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Step Functions role for clickstream micro-batch orchestration"
        )
        print(f"✅ IAM role created: {role_name}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  IAM role exists: {role_name}")

    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName="microbatch-orchestrator-policy",
        PolicyDocument=json.dumps(permission_policy)
    )

    return f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"


def create_state_machine(sfn_client, role_arn):
    """
    State Machine Flow:
    
    1. AccumulationWait (60s) — let more Firehose files arrive
    2. StartGlueETL — kick off Bronze → Silver → Gold
    3. WaitForGlue — poll until complete
    4. PublishDataProductEvent — notify Data Mesh consumers
    5. EmitSuccessMetric — CloudWatch custom metric
    
    Error handling:
    → Glue failure → retry once → if still fails → alert SNS
    → SNS publish failure → log but don't fail pipeline
    """
    state_machine_name = "clickstream-microbatch-pipeline"

    definition = {
        "Comment": "Clickstream micro-batch: Bronze → Silver → Gold → Publish",
        "StartAt": "AccumulationWait",
        "States": {

            # --- WAIT FOR MORE FILES TO ACCUMULATE ---
            "AccumulationWait": {
                "Type": "Wait",
                "Seconds": 60,
                "Comment": "Wait 60s for more Firehose files to land before processing",
                "Next": "StartGlueETL"
            },

            # --- START GLUE MICRO-BATCH JOB ---
            "StartGlueETL": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "clickstream-microbatch-etl",
                    "Arguments": {
                        "--BUCKET_NAME": BUCKET_NAME,
                        "--BRONZE_PREFIX": "bronze/clickstream/",
                        "--SILVER_PREFIX": "silver/clickstream/",
                        "--GOLD_PREFIX": "gold/clickstream_events/",
                        "--GLUE_DB_GOLD": "orders_gold",
                        "--GLUE_TABLE_GOLD": "clickstream_events",
                        "--PROCESSING_WINDOW_MINUTES": "15",
                        "--enable-metrics": "true",
                        "--enable-auto-scaling": "true"
                    },
                    "NumberOfWorkers": 2,
                    "WorkerType": "G.1X",
                    "Timeout": 10  # 10 minutes max
                },
                "ResultPath": "$.glueResult",
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 1,
                        "BackoffRate": 2.0
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "HandleGlueFailure",
                        "ResultPath": "$.error"
                    }
                ],
                "Next": "PublishDataProductEvent"
            },

            # --- PUBLISH TO DATA MESH (Project 71 pattern) ---
            "PublishDataProductEvent": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": GOVERNANCE_SNS_TOPIC,
                    "Subject": "[DataMesh] orders.clickstream_events published (micro-batch)",
                    "Message.$": "States.Format('{{\"event_type\": \"data_product.published\", \"source_domain\": \"orders\", \"data_product\": {{\"name\": \"clickstream_events\", \"version\": \"v1\", \"type\": \"micro-batch\"}}, \"glue_job_run_id\": \"{}\", \"timestamp\": \"{}\"}}', $.glueResult.JobRunId, $$.State.EnteredTime)",
                    "MessageAttributes": {
                        "source_domain": {
                            "DataType": "String",
                            "StringValue": "orders"
                        },
                        "event_type": {
                            "DataType": "String",
                            "StringValue": "data_product.published"
                        },
                        "product_name": {
                            "DataType": "String",
                            "StringValue": "clickstream_events"
                        },
                        "delivery_type": {
                            "DataType": "String",
                            "StringValue": "micro-batch"
                        }
                    }
                },
                "ResultPath": "$.snsResult",
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "EmitSuccessMetric",
                        "ResultPath": "$.snsError",
                        "Comment": "SNS failure is non-critical — continue to success"
                    }
                ],
                "Next": "EmitSuccessMetric"
            },

            # --- SUCCESS METRIC ---
            "EmitSuccessMetric": {
                "Type": "Task",
                "Resource": "arn:aws:states:::cloudwatch:putMetricData",
                "Parameters": {
                    "Namespace": "QuickCart/DataMesh/Clickstream",
                    "MetricData": [
                        {
                            "MetricName": "MicroBatchSuccess",
                            "Value": 1,
                            "Unit": "Count",
                            "Dimensions": [
                                {
                                    "Name": "Pipeline",
                                    "Value": "clickstream-microbatch"
                                }
                            ]
                        }
                    ]
                },
                "ResultPath": "$.metricResult",
                "Next": "PipelineComplete"
            },

            # --- SUCCESS END ---
            "PipelineComplete": {
                "Type": "Succeed",
                "Comment": "Micro-batch cycle completed successfully"
            },

            # --- GLUE FAILURE HANDLER ---
            "HandleGlueFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:clickstream-alerts",
                    "Subject": "🚨 ALERT: Clickstream micro-batch Glue ETL FAILED",
                    "Message.$": "States.Format('Glue ETL failed after retry. Error: {}. Manual investigation required. Runbook: https://wiki.quickcart.com/runbooks/clickstream-glue-failure', $.error.Cause)"
                },
                "ResultPath": "$.alertResult",
                "Next": "EmitFailureMetric"
            },

            # --- FAILURE METRIC ---
            "EmitFailureMetric": {
                "Type": "Task",
                "Resource": "arn:aws:states:::cloudwatch:putMetricData",
                "Parameters": {
                    "Namespace": "QuickCart/DataMesh/Clickstream",
                    "MetricData": [
                        {
                            "MetricName": "MicroBatchFailure",
                            "Value": 1,
                            "Unit": "Count",
                            "Dimensions": [
                                {
                                    "Name": "Pipeline",
                                    "Value": "clickstream-microbatch"
                                }
                            ]
                        }
                    ]
                },
                "ResultPath": "$.failMetricResult",
                "Next": "PipelineFailed"
            },

            # --- FAILURE END ---
            "PipelineFailed": {
                "Type": "Fail",
                "Error": "GlueETLFailed",
                "Cause": "Clickstream micro-batch Glue ETL failed after retry"
            }
        }
    }

    try:
        response = sfn_client.create_state_machine(
            name=state_machine_name,
            definition=json.dumps(definition, indent=2),
            roleArn=role_arn,
            type="STANDARD",
            loggingConfiguration={
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [{
                    "cloudWatchLogsLogGroup": {
                        "logGroupArn": f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:/aws/stepfunctions/{state_machine_name}:*"
                    }
                }]
            },
            tags=[
                {"key": "Domain", "value": "orders"},
                {"key": "Pipeline", "value": "clickstream-realtime"},
                {"key": "MeshRole", "value": "producer-orchestrator"}
            ]
        )
        sm_arn = response["stateMachineArn"]
        print(f"✅ State machine created: {state_machine_name}")
        print(f"   ARN: {sm_arn}")
        return sm_arn

    except sfn_client.exceptions.StateMachineAlreadyExists:
        # Get existing ARN
        machines = sfn_client.list_state_machines()
        for sm in machines["stateMachines"]:
            if sm["name"] == state_machine_name:
                print(f"ℹ️  State machine exists: {state_machine_name}")
                return sm["stateMachineArn"]


def create_eventbridge_trigger(events_client, state_machine_arn, role_arn):
    """
    EventBridge rule: trigger Step Functions when new files land in Bronze S3
    
    DEDUPLICATION STRATEGY:
    → Firehose writes many files per minute
    → We DON'T want to start a new Step Function per file
    → Solution: Step Functions starts, but has 60s Wait state
    → If already running, EventBridge rule has NO built-in dedup
    → Use: input transformer to create idempotency key based on time window
    → Better: use EventBridge → SQS → single Lambda that checks "is SF running?"
    
    SIMPLER ALTERNATIVE (used here):
    → Schedule-based: run every 5 minutes regardless
    → Guaranteed cadence, no over-triggering
    → Misses the "instant trigger" but meets 15-min SLA easily
    """
    rule_name = "clickstream-microbatch-schedule"

    events_client.put_rule(
        Name=rule_name,
        ScheduleExpression="rate(5 minutes)",  # Every 5 minutes
        State="ENABLED",
        Description="Trigger clickstream micro-batch every 5 minutes",
        Tags=[
            {"Key": "Pipeline", "Value": "clickstream-realtime"}
        ]
    )
    print(f"✅ EventBridge rule: {rule_name} (every 5 minutes)")

    # Add Step Functions as target
    events_client.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "clickstream-microbatch-sf",
                "Arn": state_machine_arn,
                "RoleArn": role_arn,
                "Input": json.dumps({
                    "trigger": "scheduled",
                    "schedule": "rate(5 minutes)",
                    "pipeline": "clickstream-microbatch"
                })
            }
        ]
    )
    print(f"   Target: Step Functions → {state_machine_arn.split(':')[-1]}")

    # Need EventBridge role to start Step Functions
    return rule_name


def main():
    print("=" * 70)
    print("🔧 CREATING STEP FUNCTIONS ORCHESTRATOR: Micro-Batch Pipeline")
    print("=" * 70)

    iam_client = boto3.client("iam", region_name=REGION)
    sfn_client = boto3.client("stepfunctions", region_name=REGION)
    events_client = boto3.client("events", region_name=REGION)

    # Step 1: IAM Role
    print("\n--- Step 1: IAM Role ---")
    role_arn = create_step_function_role(iam_client)

    # Step 2: State Machine
    print("\n--- Step 2: State Machine ---")
    sm_arn = create_state_machine(sfn_client, role_arn)

    # Step 3: EventBridge Schedule
    print("\n--- Step 3: EventBridge Trigger ---")
    create_eventbridge_trigger(events_client, sm_arn, role_arn)

    print("\n" + "=" * 70)
    print("✅ ORCHESTRATOR SETUP COMPLETE")
    print(f"   Schedule: every 5 minutes")
    print(f"   Flow: Wait → Glue ETL → SNS Publish → Metric")
    print("=" * 70)


if __name__ == "__main__":
    main()