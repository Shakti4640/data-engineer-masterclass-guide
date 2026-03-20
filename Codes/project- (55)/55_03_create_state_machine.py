# file: 55_03_create_state_machine.py
# Purpose: Create Step Functions state machine for complete pipeline orchestration

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
STATE_MACHINE_NAME = "quickcart-nightly-pipeline"
SF_ROLE_NAME = "QuickCart-StepFunctions-Pipeline-Role"
LAMBDA_FUNCTION_NAME = "quickcart-dq-validator"


def get_role_arn():
    iam_client = boto3.client("iam")
    role = iam_client.get_role(RoleName=SF_ROLE_NAME)
    return role["Role"]["Arn"]


def build_state_machine_definition():
    """
    Build the complete pipeline state machine in Amazon States Language
    
    PIPELINE FLOW:
    1. Start Crawler (sync)
    2. Run DataBrew Clean (sync via Lambda wrapper)
    3. Run Glue ETL (sync)
    4. Validate Data Quality (Lambda)
    5. Choice: pass/fail
    6. If pass: Parallel (Athena CTAS + Redshift notification)
    7. Notify Success
    
    Any failure → Catch → Notify Failure → FAIL
    """

    # SNS topic ARNs (from Project 54)
    critical_topic = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-pipeline-critical"
    warning_topic = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-pipeline-warnings"
    lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_FUNCTION_NAME}"

    definition = {
        "Comment": "QuickCart Nightly Data Pipeline — Crawl → Clean → Transform → Validate → Serve → Notify",
        "StartAt": "InitializePipeline",
        "States": {

            # ═══════════════════════════════════════
            # STATE 0: Initialize — set pipeline context
            # ═══════════════════════════════════════
            "InitializePipeline": {
                "Type": "Pass",
                "Comment": "Set pipeline context for downstream states",
                "Result": {
                    "pipeline": "nightly-etl",
                    "started_at": "2025-01-15T01:00:00Z"
                },
                "ResultPath": "$.pipeline_context",
                "Next": "StartCrawler"
            },

            # ═══════════════════════════════════════
            # STATE 1: Start Glue Crawler
            # Discovers new partitions in S3 Bronze zone
            # ═══════════════════════════════════════
            "StartCrawler": {
                "Type": "Task",
                "Comment": "Run Glue Crawler to discover new data in S3",
                "Resource": "arn:aws:states:::glue:startCrawler.sync",
                "Parameters": {
                    "Name": "quickcart-raw-crawler"
                },
                "TimeoutSeconds": 600,      # 10 min max
                "ResultPath": "$.crawler_result",
                "Retry": [
                    {
                        "ErrorEquals": [
                            "Glue.CrawlerRunningException"
                        ],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 3,
                        "BackoffRate": 2.0
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "HandleCrawlerFailure",
                        "ResultPath": "$.error"
                    }
                ],
                "Next": "RunDataBrewClean"
            },

            # Crawler failure handler
            "HandleCrawlerFailure": {
                "Type": "Pass",
                "Comment": "Crawler failed — decide whether to continue or stop",
                "Result": {
                    "failed_step": "StartCrawler",
                    "severity": "WARNING",
                    "message": "Crawler failed but pipeline can continue with existing catalog"
                },
                "ResultPath": "$.failure_info",
                # Crawler failure is WARNING — continue with existing catalog
                "Next": "RunDataBrewClean"
            },

            # ═══════════════════════════════════════
            # STATE 2: Run DataBrew Cleaning Job
            # Cleans raw data in Bronze → writes to Silver
            # ═══════════════════════════════════════
            "RunDataBrewClean": {
                "Type": "Task",
                "Comment": "Run DataBrew recipe job to clean customer data",
                "Resource": "arn:aws:states:::databrew:startJobRun.sync",
                "Parameters": {
                    "Name": "quickcart-customers-clean-job"
                },
                "TimeoutSeconds": 1800,     # 30 min max
                "ResultPath": "$.databrew_result",
                "Retry": [
                    {
                        "ErrorEquals": [
                            "DataBrew.ServiceUnavailableException"
                        ],
                        "IntervalSeconds": 60,
                        "MaxAttempts": 2,
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
                "Next": "RunGlueETL"
            },

            # ═══════════════════════════════════════
            # STATE 3: Run Glue ETL Job
            # Transforms Silver → Gold
            # ═══════════════════════════════════════
            "RunGlueETL": {
                "Type": "Task",
                "Comment": "Run Glue ETL to transform Silver to Gold layer",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "quickcart-orders-etl",
                    "Arguments": {
                        "--execution_date.$": "$.pipeline_context.started_at",
                        "--pipeline.$": "$.pipeline_context.pipeline"
                    }
                },
                "TimeoutSeconds": 3600,     # 1 hour max
                "ResultPath": "$.glue_result",
                "Retry": [
                    {
                        "ErrorEquals": [
                            "Glue.ConcurrentRunsExceededException",
                            "Glue.InternalServiceException"
                        ],
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
                "Next": "ValidateDataQuality"
            },

            # ═══════════════════════════════════════
            # STATE 4: Validate Data Quality
            # Lambda checks row counts, nulls, duplicates
            # ═══════════════════════════════════════
            "ValidateDataQuality": {
                "Type": "Task",
                "Comment": "Run data quality checks on transformed data",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": lambda_arn,
                    "Payload": {
                        "pipeline.$": "$.pipeline_context.pipeline",
                        "execution_date.$": "$.pipeline_context.started_at"
                    }
                },
                "TimeoutSeconds": 300,      # 5 min max
                "ResultSelector": {
                    "quality_passed.$": "$.Payload.quality_passed",
                    "summary.$": "$.Payload.summary",
                    "failed_checks.$": "$.Payload.failed_checks"
                },
                "ResultPath": "$.dq_result",
                "Retry": [
                    {
                        "ErrorEquals": [
                            "Lambda.ServiceException",
                            "Lambda.TooManyRequestsException"
                        ],
                        "IntervalSeconds": 5,
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
                "Next": "CheckQualityResult"
            },

            # ═══════════════════════════════════════
            # STATE 5: Choice — Quality Pass/Fail
            # ═══════════════════════════════════════
            "CheckQualityResult": {
                "Type": "Choice",
                "Comment": "Route based on data quality check result",
                "Choices": [
                    {
                        "Variable": "$.dq_result.quality_passed",
                        "BooleanEquals": True,
                        "Next": "ServeDataParallel"
                    },
                    {
                        "Variable": "$.dq_result.quality_passed",
                        "BooleanEquals": False,
                        "Next": "NotifyDQFailure"
                    }
                ],
                "Default": "NotifyDQFailure"
            },

            # DQ Failure notification (separate from general failure)
            "NotifyDQFailure": {
                "Type": "Task",
                "Comment": "Alert team about data quality failure",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": critical_topic,
                    "Subject": "🔴 PIPELINE DQ FAILURE — Data quality checks failed",
                    "Message": {
                        "pipeline": "nightly-etl",
                        "status": "DATA_QUALITY_FAILED",
                        "summary.$": "$.dq_result.summary",
                        "failed_checks.$": "$.dq_result.failed_checks",
                        "action": "Review DQ results. Fix source data or adjust thresholds. Rerun pipeline.",
                        "dashboard": "https://console.aws.amazon.com/cloudwatch/..."
                    }
                },
                "ResultPath": "$.notification_result",
                "Next": "PipelineFailed"
            },

            # ═══════════════════════════════════════
            # STATE 6: Parallel — Serve to Multiple Targets
            # Athena CTAS + Redshift notification run simultaneously
            # ═══════════════════════════════════════
            "ServeDataParallel": {
                "Type": "Parallel",
                "Comment": "Serve Gold data to Athena and Redshift in parallel",
                "Branches": [
                    # Branch 1: Athena CTAS
                    {
                        "StartAt": "RunAthenaCTAS",
                        "States": {
                            "RunAthenaCTAS": {
                                "Type": "Task",
                                "Comment": "Create Athena summary table from Gold data",
                                "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                                "Parameters": {
                                    "QueryString": "CREATE TABLE quickcart_db.orders_summary_daily AS SELECT order_date, COUNT(*) as order_count, SUM(total_amount) as total_revenue FROM quickcart_db.orders_gold GROUP BY order_date",
                                    "WorkGroup": "primary",
                                    "ResultConfiguration": {
                                        "OutputLocation": "s3://quickcart-raw-data-prod/athena-results/"
                                    }
                                },
                                "TimeoutSeconds": 1800,
                                "Retry": [
                                    {
                                        "ErrorEquals": [
                                            "Athena.TooManyRequestsException"
                                        ],
                                        "IntervalSeconds": 10,
                                        "MaxAttempts": 3,
                                        "BackoffRate": 2.0
                                    }
                                ],
                                "End": True
                            }
                        }
                    },
                    # Branch 2: Notify Redshift team (placeholder for COPY)
                    {
                        "StartAt": "NotifyRedshiftReady",
                        "States": {
                            "NotifyRedshiftReady": {
                                "Type": "Task",
                                "Comment": "Notify that Gold data is ready for Redshift COPY",
                                "Resource": "arn:aws:states:::sns:publish",
                                "Parameters": {
                                    "TopicArn": warning_topic,
                                    "Subject": "📦 Gold data ready for Redshift COPY",
                                    "Message": {
                                        "pipeline": "nightly-etl",
                                        "status": "GOLD_DATA_READY",
                                        "s3_path": "s3://quickcart-raw-data-prod/gold/orders/",
                                        "action": "Redshift COPY can proceed"
                                    }
                                },
                                "End": True
                            }
                        }
                    }
                ],
                "ResultPath": "$.parallel_result",
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "NotifyFailure",
                        "ResultPath": "$.error"
                    }
                ],
                "Next": "NotifySuccess"
            },

            # ═══════════════════════════════════════
            # STATE 7: Notify Success
            # ═══════════════════════════════════════
            "NotifySuccess": {
                "Type": "Task",
                "Comment": "Pipeline completed successfully — notify team",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": warning_topic,
                    "Subject": "✅ PIPELINE SUCCESS — Nightly ETL completed",
                    "Message": {
                        "pipeline": "nightly-etl",
                        "status": "SUCCEEDED",
                        "steps_completed": [
                            "Crawler", "DataBrew", "Glue ETL",
                            "DQ Validation", "Athena CTAS", "Redshift Notify"
                        ],
                        "dq_summary.$": "$.dq_result.summary",
                        "dashboard": "https://console.aws.amazon.com/cloudwatch/..."
                    }
                },
                "ResultPath": "$.success_notification",
                "Next": "PipelineSucceeded"
            },

            # ═══════════════════════════════════════
            # STATE 8: Pipeline Succeeded (terminal)
            # ═══════════════════════════════════════
            "PipelineSucceeded": {
                "Type": "Succeed",
                "Comment": "Pipeline completed successfully"
            },

            # ═══════════════════════════════════════
            # STATE 9: Generic Failure Notification
            # Catch-all for any unhandled failure
            # ═══════════════════════════════════════
            "NotifyFailure": {
                "Type": "Task",
                "Comment": "Pipeline failed — send critical alert",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": critical_topic,
                    "Subject": "🔴 PIPELINE FAILED — Nightly ETL encountered an error",
                    "Message": {
                        "pipeline": "nightly-etl",
                        "status": "FAILED",
                        "error.$": "$.error",
                        "action": "1. Check Step Functions execution history for details. 2. Check component logs in CloudWatch. 3. Fix root cause and rerun.",
                        "runbook": "https://wiki.quickcart.internal/runbooks/pipeline-failure",
                        "dashboard": "https://console.aws.amazon.com/cloudwatch/..."
                    }
                },
                "ResultPath": "$.failure_notification",
                "Next": "PipelineFailed"
            },

            # ═══════════════════════════════════════
            # STATE 10: Pipeline Failed (terminal)
            # ═══════════════════════════════════════
            "PipelineFailed": {
                "Type": "Fail",
                "Comment": "Pipeline failed — execution marked as FAILED",
                "Cause": "One or more pipeline steps failed",
                "Error": "PipelineExecutionFailed"
            }
        }
    }

    return definition


def create_state_machine():
    """Create the Step Functions state machine"""
    sfn_client = boto3.client("stepfunctions", region_name=REGION)
    role_arn = get_role_arn()
    definition = build_state_machine_definition()

    print("=" * 70)
    print("🚀 CREATING STEP FUNCTIONS STATE MACHINE")
    print("=" * 70)

    # Check if exists
    state_machine_arn = None
    try:
        response = sfn_client.list_state_machines()
        for sm in response["stateMachines"]:
            if sm["name"] == STATE_MACHINE_NAME:
                state_machine_arn = sm["stateMachineArn"]
                break
    except Exception:
        pass

    if state_machine_arn:
        # Update existing
        sfn_client.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=json.dumps(definition),
            roleArn=role_arn,
            loggingConfiguration={
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:/aws/stepfunctions/{STATE_MACHINE_NAME}:*"
                        }
                    }
                ]
            }
        )
        print(f"ℹ️  State machine updated: {state_machine_arn}")
    else:
        # Create new
        response = sfn_client.create_state_machine(
            name=STATE_MACHINE_NAME,
            definition=json.dumps(definition),
            roleArn=role_arn,
            type="STANDARD",
            loggingConfiguration={
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:/aws/stepfunctions/{STATE_MACHINE_NAME}:*"
                        }
                    }
                ]
            },
            tags=[
                {"key": "Team", "value": "Data-Engineering"},
                {"key": "Pipeline", "value": "nightly-etl"},
                {"key": "Environment", "value": "Production"}
            ]
        )
        state_machine_arn = response["stateMachineArn"]
        print(f"✅ State machine created: {state_machine_arn}")

    # Print state flow
    print(f"\n📊 PIPELINE STATE FLOW:")
    print(f"{'─' * 50}")
    states = definition["States"]
    print(f"  START → InitializePipeline")
    for state_name, state_def in states.items():
        state_type = state_def["Type"]
        next_state = state_def.get("Next", "END")
        timeout = state_def.get("TimeoutSeconds", "N/A")
        retry_count = len(state_def.get("Retry", []))
        has_catch = "Catch" in state_def

        icon = {
            "Task": "⚙️", "Choice": "🔀", "Parallel": "⏸️",
            "Pass": "➡️", "Succeed": "✅", "Fail": "❌",
            "Wait": "⏳"
        }.get(state_type, "❓")

        catch_str = " [CATCH→NotifyFailure]" if has_catch else ""
        retry_str = f" [RETRY×{retry_count}]" if retry_count > 0 else ""
        timeout_str = f" [TIMEOUT:{timeout}s]" if timeout != "N/A" else ""

        print(f"  {icon} {state_name} ({state_type}){timeout_str}{retry_str}{catch_str}")
        if state_type not in ["Succeed", "Fail"]:
            if state_type == "Choice":
                for choice in state_def.get("Choices", []):
                    print(f"       ├─ if {choice.get('Variable','')} → {choice.get('Next','')}")
                print(f"       └─ default → {state_def.get('Default','')}")
            elif state_type == "Parallel":
                for i, branch in enumerate(state_def.get("Branches", [])):
                    branch_start = branch.get("StartAt", "?")
                    print(f"       ├─ Branch {i+1}: {branch_start}")
                print(f"       └─ then → {next_state}")
            else:
                print(f"       └─ → {next_state}")

    print(f"{'─' * 50}")

    # Save config
    config = {
        "state_machine_arn": state_machine_arn,
        "state_machine_name": STATE_MACHINE_NAME,
        "role_arn": role_arn
    }
    with open("/tmp/step_functions_config.json", "w") as f:
        json.dump(config, f, indent=2)
    print(f"\n💾 Config saved to /tmp/step_functions_config.json")

    # Console URL
    console_url = (
        f"https://{REGION}.console.aws.amazon.com/states/home?"
        f"region={REGION}#/statemachines/view/{state_machine_arn}"
    )
    print(f"\n🔗 Console URL: {console_url}")
    print(f"   → Open to see visual workflow diagram")

    return state_machine_arn


if __name__ == "__main__":
    create_state_machine()