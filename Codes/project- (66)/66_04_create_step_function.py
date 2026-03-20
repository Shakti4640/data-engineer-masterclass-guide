# file: 66_04_create_step_function.py
# Run from: Account B (222222222222)
# Purpose: Create the event-driven pipeline Step Function

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def create_step_function():
    """
    Main pipeline state machine
    
    FLOW:
    1. Classify Event (Lambda) — determine source and action
    2. Choice: route based on event_type
       → file_arrival: Start source-specific Glue ETL
       → scheduled_cdc: Start CDC Glue job
       → scheduled_inventory: Start inventory Glue job
       → etl_completed: Check if all sources done
    3. After ETL completes: Check all_sources_complete
    4. If all complete: Build gold layer → Refresh MVs → Notify
    5. If not all: End (wait for next event)
    """
    sfn = boto3.client("stepfunctions", region_name=REGION)

    state_machine = {
        "Comment": "Event-Driven Data Pipeline — processes events from S3, Glue, Schedule",
        "StartAt": "ClassifyEvent",
        "States": {
            "ClassifyEvent": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "pipeline-event-router",
                    "Payload.$": "$"
                },
                "ResultSelector": {
                    "event_type.$": "$.Payload.event_type",
                    "source_name.$": "$.Payload.source_name",
                    "glue_job.$": "$.Payload.glue_job",
                    "file_bucket.$": "$.Payload.file_bucket",
                    "file_key.$": "$.Payload.file_key",
                    "batch_date.$": "$.Payload.batch_date",
                    "all_sources_complete.$": "$.Payload.all_sources_complete"
                },
                "ResultPath": "$.classification",
                "Next": "RouteByEventType",
                "Retry": [{"ErrorEquals": ["States.ALL"], "IntervalSeconds": 5, "MaxAttempts": 2}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure", "ResultPath": "$.error"}]
            },

            "RouteByEventType": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.classification.event_type",
                        "StringEquals": "file_arrival",
                        "Next": "RunSourceETL"
                    },
                    {
                        "Variable": "$.classification.event_type",
                        "StringEquals": "scheduled_cdc",
                        "Next": "RunSourceETL"
                    },
                    {
                        "Variable": "$.classification.event_type",
                        "StringEquals": "scheduled_inventory",
                        "Next": "RunSourceETL"
                    },
                    {
                        "Variable": "$.classification.event_type",
                        "StringEquals": "etl_completed",
                        "Next": "CheckAllSourcesComplete"
                    },
                    {
                        "Variable": "$.classification.event_type",
                        "StringEquals": "unknown_source",
                        "Next": "IgnoreUnknownEvent"
                    }
                ],
                "Default": "IgnoreUnknownEvent"
            },

            "RunSourceETL": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName.$": "$.classification.glue_job",
                    "Arguments": {
                        "--source_name.$": "$.classification.source_name",
                        "--file_bucket.$": "$.classification.file_bucket",
                        "--file_key.$": "$.classification.file_key",
                        "--batch_date.$": "$.classification.batch_date"
                    }
                },
                "ResultPath": "$.etl_result",
                "TimeoutSeconds": 3600,
                "Next": "MarkSourceComplete",
                "Retry": [
                    {
                        "ErrorEquals": ["Glue.ConcurrentRunsExceededException"],
                        "IntervalSeconds": 120,
                        "MaxAttempts": 5,
                        "BackoffRate": 1.5
                    },
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 60,
                        "MaxAttempts": 2,
                        "BackoffRate": 2.0
                    }
                ],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "MarkSourceFailed",
                    "ResultPath": "$.error"
                }]
            },

            "MarkSourceComplete": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:updateItem",
                "Parameters": {
                    "TableName": "pipeline_batch_state",
                    "Key": {
                        "batch_date": {"S.$": "$.classification.batch_date"},
                        "source_name": {"S.$": "$.classification.source_name"}
                    },
                    "UpdateExpression": "SET #s = :s, completed_at = :t",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {
                        ":s": {"S": "completed"},
                        ":t": {"S.$": "$$.State.EnteredTime"}
                    }
                },
                "ResultPath": "$.dynamodb_result",
                "Next": "CheckAllSourcesComplete"
            },

            "MarkSourceFailed": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:updateItem",
                "Parameters": {
                    "TableName": "pipeline_batch_state",
                    "Key": {
                        "batch_date": {"S.$": "$.classification.batch_date"},
                        "source_name": {"S.$": "$.classification.source_name"}
                    },
                    "UpdateExpression": "SET #s = :s, error_message = :e",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {
                        ":s": {"S": "failed"},
                        ":e": {"S.$": "States.Format('{}', $.error)"}
                    }
                },
                "ResultPath": "$.dynamodb_result",
                "Next": "NotifyFailure"
            },

            "CheckAllSourcesComplete": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "pipeline-event-router",
                    "Payload": {
                        "source": "aws.states",
                        "detail-type": "CheckCompletion",
                        "detail": {
                            "batch_date.$": "$.classification.batch_date"
                        }
                    }
                },
                "ResultSelector": {
                    "all_complete.$": "$.Payload.all_sources_complete"
                },
                "ResultPath": "$.completion_check",
                "Next": "AllSourcesReady?"
            },

            "AllSourcesReady?": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.completion_check.all_complete",
                        "BooleanEquals": True,
                        "Next": "BuildGoldLayer"
                    }
                ],
                "Default": "WaitForOtherSources"
            },

            "WaitForOtherSources": {
                "Type": "Succeed",
                "Comment": "This source is done. Other sources still pending. Next event will re-check."
            },

            "BuildGoldLayer": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "build-gold-aggregations",
                    "Arguments": {
                        "--batch_date.$": "$.classification.batch_date"
                    }
                },
                "ResultPath": "$.gold_result",
                "TimeoutSeconds": 3600,
                "Next": "RefreshMaterializedViews",
                "Retry": [{"ErrorEquals": ["States.ALL"], "IntervalSeconds": 60, "MaxAttempts": 2}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure", "ResultPath": "$.error"}]
            },

            "RefreshMaterializedViews": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "refresh-materialized-views",
                    "Payload": {
                        "trigger": "event_driven_pipeline",
                        "batch_date.$": "$.classification.batch_date"
                    }
                },
                "ResultPath": "$.mv_result",
                "TimeoutSeconds": 600,
                "Next": "NotifySuccess",
                "Retry": [{"ErrorEquals": ["States.ALL"], "IntervalSeconds": 30, "MaxAttempts": 2}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure", "ResultPath": "$.error"}]
            },

            "NotifySuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts",
                    "Subject": "✅ Pipeline Complete — All Sources Processed",
                    "Message.$": "States.Format('Event-driven pipeline completed for batch {}.\nAll sources processed. Gold layer built. MVs refreshed.\nDashboards serving fresh data.', $.classification.batch_date)"
                },
                "End": True
            },

            "NotifyFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts",
                    "Subject": "❌ Pipeline FAILED — Check Step Functions Console",
                    "Message.$": "States.Format('Pipeline failed for batch {}.\nSource: {}\nCheck Step Functions execution for details.', $.classification.batch_date, $.classification.source_name)"
                },
                "End": True
            },

            "IgnoreUnknownEvent": {
                "Type": "Succeed",
                "Comment": "Event did not match any known source — ignored"
            }
        }
    }

    # IAM Role
    iam = boto3.client("iam")
    role_name = "StepFunctionsEventDrivenPipelineRole"

    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}]
    }

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["lambda:InvokeFunction"],
                "Resource": f"arn:aws:lambda:{REGION}:{ACCOUNT_B_ID}:function:*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:StartJobRun", "glue:GetJobRun",
                    "glue:GetJobRuns", "glue:BatchStopJobRun"
                ],
                "Resource": f"arn:aws:glue:{REGION}:{ACCOUNT_B_ID}:job/*"
            },
            {
                "Effect": "Allow",
                "Action": ["sns:Publish"],
                "Resource": f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:UpdateItem",
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:PutItem"
                ],
                "Resource": f"arn:aws:dynamodb:{REGION}:{ACCOUNT_B_ID}:table/pipeline_batch_state"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "events:PutTargets",
                    "events:PutRule",
                    "events:DescribeRule"
                ],
                "Resource": f"arn:aws:events:{REGION}:{ACCOUNT_B_ID}:rule/*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups"
                ],
                "Resource": "*"
            }
        ]
    }

    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="Role for event-driven pipeline Step Function"
        )
        print(f"✅ IAM Role created: {role_name}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  Role already exists")

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="EventDrivenPipelinePolicy",
        PolicyDocument=json.dumps(policy)
    )
    print(f"✅ Policy attached")

    role_arn = f"arn:aws:iam::{ACCOUNT_B_ID}:role/{role_name}"

    import time
    time.sleep(10)  # Wait for role propagation

    # Create State Machine
    sm_name = "event-driven-data-pipeline"
    try:
        response = sfn.create_state_machine(
            name=sm_name,
            definition=json.dumps(state_machine),
            roleArn=role_arn,
            type="STANDARD",
            loggingConfiguration={
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": f"arn:aws:logs:{REGION}:{ACCOUNT_B_ID}:log-group:/aws/stepfunctions/{sm_name}:*"
                        }
                    }
                ]
            },
            tags=[
                {"key": "Purpose", "value": "EventDrivenPipeline"},
                {"key": "ManagedBy", "value": "DataEngineering"}
            ]
        )
        print(f"✅ Step Function created: {response['stateMachineArn']}")
    except sfn.exceptions.StateMachineAlreadyExists:
        sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:{sm_name}"
        sfn.update_state_machine(
            stateMachineArn=sm_arn,
            definition=json.dumps(state_machine),
            roleArn=role_arn
        )
        print(f"✅ Step Function updated: {sm_arn}")

    return f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:{sm_name}"


if __name__ == "__main__":
    print("=" * 60)
    print("🔧 CREATING EVENT-DRIVEN STEP FUNCTION")
    print("=" * 60)
    sm_arn = create_step_function()
    print(f"\n📋 State Machine ARN: {sm_arn}")
