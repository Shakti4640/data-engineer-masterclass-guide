# file: 41_03_create_state_machine.py
# Purpose: Create the Step Functions state machine that orchestrates the ETL

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

STATE_MACHINE_NAME = "quickcart-etl-pipeline"
STEPFN_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/quickcart-stepfn-etl-pipeline-role"
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-pipeline-alerts"


def build_state_machine_definition():
    """
    Build the Amazon States Language (ASL) definition.
    
    FLOW:
    1. Extract entity name from S3 key (Pass state + intrinsic functions)
    2. Start Glue Crawler (.sync — waits for completion)
    3. Start Glue ETL Job (.sync — waits for completion)
    4. Notify success via SNS
    CATCH: Any failure → Notify failure via SNS
    
    STATE MACHINE INPUT (from EventBridge):
    {
      "version": "0",
      "source": "aws.s3",
      "detail-type": "Object Created",
      "detail": {
        "bucket": {"name": "quickcart-raw-data-prod"},
        "object": {
          "key": "daily_exports/orders/orders_20250115.csv",
          "size": 52428800,
          "etag": "..."
        }
      }
    }
    """

    definition = {
        "Comment": "QuickCart ETL Pipeline — Triggered by S3 file arrival",
        "StartAt": "ExtractEntityInfo",
        "States": {

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STATE 1: Extract entity name from S3 object key
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            "ExtractEntityInfo": {
                "Type": "Pass",
                "Comment": "Parse S3 key to extract entity type and build resource names",
                "Parameters": {
                    # Pull key fields from the EventBridge event
                    "bucket.$": "$.detail.bucket.name",
                    "objectKey.$": "$.detail.object.key",
                    "objectSize.$": "$.detail.object.size",

                    # Split the key: "daily_exports/orders/orders_20250115.csv"
                    # States.StringSplit returns: ["daily_exports", "orders", "orders_20250115.csv"]
                    # States.ArrayGetItem at index 1 returns: "orders"
                    "entityName.$": "States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 1)",

                    # Build Glue resource names dynamically
                    # Crawler: quickcart-orders-crawler
                    # ETL Job: quickcart-orders-etl
                    "crawlerName.$": "States.Format('quickcart-{}-crawler', States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 1))",
                    "etlJobName.$": "States.Format('quickcart-{}-etl', States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 1))",

                    # Timestamp for tracking
                    "pipelineTriggeredAt.$": "$$.Execution.StartTime"
                },
                "Next": "StartGlueCrawler"
            },

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STATE 2: Start Glue Crawler
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            "StartGlueCrawler": {
                "Type": "Task",
                "Comment": "Start the Glue Crawler to update catalog with new data",
                "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                "Parameters": {
                    "Name.$": "$.crawlerName"
                },
                "ResultPath": "$.crawlerResult",
                "Retry": [
                    {
                        "ErrorEquals": ["Glue.CrawlerRunningException"],
                        "Comment": "Crawler already running — wait and retry",
                        "IntervalSeconds": 30,
                        "MaxAttempts": 3,
                        "BackoffRate": 2.0
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "NotifyFailure",
                        "ResultPath": "$.error"
                    }
                ],
                "Next": "WaitForCrawler"
            },

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STATE 3: Wait and poll crawler status
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            "WaitForCrawler": {
                "Type": "Wait",
                "Seconds": 30,
                "Next": "GetCrawlerStatus"
            },

            "GetCrawlerStatus": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
                "Parameters": {
                    "Name.$": "$.crawlerName"
                },
                "ResultPath": "$.crawlerStatus",
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "NotifyFailure",
                        "ResultPath": "$.error"
                    }
                ],
                "Next": "CheckCrawlerDone"
            },

            "CheckCrawlerDone": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.crawlerStatus.Crawler.State",
                        "StringEquals": "READY",
                        "Next": "StartGlueETLJob"
                    },
                    {
                        "Variable": "$.crawlerStatus.Crawler.State",
                        "StringEquals": "STOPPING",
                        "Next": "WaitForCrawler"
                    },
                    {
                        "Variable": "$.crawlerStatus.Crawler.State",
                        "StringEquals": "RUNNING",
                        "Next": "WaitForCrawler"
                    }
                ],
                "Default": "WaitForCrawler"
            },

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STATE 4: Start Glue ETL Job (.sync — waits)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            "StartGlueETLJob": {
                "Type": "Task",
                "Comment": "Start Glue ETL — .sync means Step Functions waits for completion",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName.$": "$.etlJobName",
                    "Arguments": {
                        "--source_bucket.$": "$.bucket",
                        "--source_key.$": "$.objectKey",
                        "--entity_name.$": "$.entityName"
                    }
                },
                "ResultPath": "$.etlResult",
                "TimeoutSeconds": 1800,
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "Comment": "Glue job failed — retry once after 60 seconds",
                        "IntervalSeconds": 60,
                        "MaxAttempts": 1,
                        "BackoffRate": 1.0
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "NotifyFailure",
                        "ResultPath": "$.error"
                    }
                ],
                "Next": "NotifySuccess"
            },

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # STATE 5: Success notification
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            "NotifySuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": SNS_TOPIC_ARN,
                    "Subject": "✅ QuickCart ETL Pipeline — SUCCESS",
                    "Message.$": "States.Format('Pipeline completed successfully.\n\nEntity: {}\nFile: {}\nBucket: {}\nExecution: {}\nStarted: {}', $.entityName, $.objectKey, $.bucket, $$.Execution.Id, $.pipelineTriggeredAt)"
                },
                "ResultPath": "$.snsResult",
                "End": True
            },

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # CATCH STATE: Failure notification
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            "NotifyFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": SNS_TOPIC_ARN,
                    "Subject": "❌ QuickCart ETL Pipeline — FAILURE",
                    "Message.$": "States.Format('Pipeline FAILED.\n\nEntity: {}\nFile: {}\nError: {}\nExecution: {}', $.entityName, $.objectKey, $.error, $$.Execution.Id)"
                },
                "Next": "PipelineFailed"
            },

            "PipelineFailed": {
                "Type": "Fail",
                "Error": "PipelineExecutionFailed",
                "Cause": "One or more steps in the ETL pipeline failed. Check execution history."
            }
        }
    }

    return definition


def create_state_machine():
    sfn_client = boto3.client("stepfunctions", region_name=REGION)

    definition = build_state_machine_definition()

    try:
        response = sfn_client.create_state_machine(
            name=STATE_MACHINE_NAME,
            definition=json.dumps(definition, indent=2),
            roleArn=STEPFN_ROLE_ARN,
            type="STANDARD",
            loggingConfiguration={
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:/aws/stepfunctions/quickcart-etl-pipeline:*"
                        }
                    }
                ]
            },
            tags=[
                {"key": "Environment", "value": "Production"},
                {"key": "Project", "value": "QuickCart-ETL"}
            ]
        )

        print(f"✅ State Machine created: {STATE_MACHINE_NAME}")
        print(f"   ARN: {response['stateMachineArn']}")
        print(f"   Created: {response['creationDate']}")
        return response["stateMachineArn"]

    except sfn_client.exceptions.StateMachineAlreadyExists:
        # Update existing
        # First, get the ARN
        list_response = sfn_client.list_state_machines()
        for sm in list_response["stateMachines"]:
            if sm["name"] == STATE_MACHINE_NAME:
                sm_arn = sm["stateMachineArn"]
                sfn_client.update_state_machine(
                    stateMachineArn=sm_arn,
                    definition=json.dumps(definition, indent=2),
                    roleArn=STEPFN_ROLE_ARN
                )
                print(f"✅ State Machine updated: {STATE_MACHINE_NAME}")
                print(f"   ARN: {sm_arn}")
                return sm_arn


if __name__ == "__main__":
    sm_arn = create_state_machine()

    # Print the definition for visual inspection
    print(f"\n📋 State Machine Definition:")
    print(json.dumps(build_state_machine_definition(), indent=2))