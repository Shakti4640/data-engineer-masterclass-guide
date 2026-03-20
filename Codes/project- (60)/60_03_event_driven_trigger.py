# file: 60_03_event_driven_trigger.py
# Purpose: Wire SQS event (from Project 59) to trigger the cross-account Glue job

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID [REDACTED:BANK_ACCOUNT_NUMBER[REDACTED:BANK_ACCOUNT_NUMBER]2"


def create_event_driven_pipeline():
    """
    Create the trigger chain:
    Account A's SNS → Account B's SQS → EventBridge Pipe → Step Functions → Glue Job
    
    This connects Project 59 (events) with Project 55 (orchestration)
    and this project (cross-account Glue ETL)
    """
    print("=" * 70)
    print("⚡ CREATING EVENT-DRIVEN CROSS-ACCOUNT PIPELINE TRIGGER")
    print("=" * 70)

    # The full chain:
    print(f"""
    TRIGGER CHAIN:
    ══════════════
    
    Account A Pipeline Completes
         │
         ▼
    Account A SNS: "quickcart-pipeline-events"
    (publishes: event_type="etl_completed")
         │
         │ cross-account delivery (Project 59)
         ▼
    Account B SQS: "acct-a-pipeline-events-queue"
    (filter: event_type IN [etl_completed, dq_passed])
         │
         │ EventBridge Pipe (or Lambda consumer)
         ▼
    Account B Step Functions: "acct-b-analytics-pipeline"
         │
         ├── State 1: Parse event context
         │   (extract: execution_date, tables_updated)
         │
         ├── State 2: Start Glue Job (cross-account ETL)
         │   (this project's job: cross-account-analytics-etl)
         │   Arguments: execution_date from event
         │
         ├── State 3: Validate Results
         │   (check: row counts, null rates)
         │
         ├── State 4: Notify Success
         │   (Account B's SNS topic)
         │
         └── Catch: Notify Failure
             (Account B's alert topic)
    """)

    # Generate Step Functions state machine for Account B
    state_machine_definition = {
        "Comment": "Account B Analytics Pipeline — triggered by Account A events",
        "StartAt": "ParseEvent",
        "States": {
            "ParseEvent": {
                "Type": "Pass",
                "Comment": "Extract execution context from SQS event",
                "Parameters": {
                    "execution_date.$": "$.detail.execution_date",
                    "source_account.$": "$.detail.source_account",
                    "tables_updated.$": "$.detail.tables_updated"
                },
                "ResultPath": "$.context",
                "Next": "RunCrossAccountETL"
            },

            "RunCrossAccountETL": {
                "Type": "Task",
                "Comment": "Run cross-account Glue ETL job",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "cross-account-analytics-etl",
                    "Arguments": {
                        "--execution_date.$": "$.context.execution_date"
                    }
                },
                "TimeoutSeconds": 14400,
                "Retry": [
                    {
                        "ErrorEquals": [
                            "Glue.ConcurrentRunsExceededException"
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
                "ResultPath": "$.glue_result",
                "Next": "NotifySuccess"
            },

            "NotifySuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:quickcart-analytics-pipeline-events",
                    "Subject": "✅ Analytics Pipeline Complete",
                    "Message": {
                        "status": "SUCCEEDED",
                        "pipeline": "cross-account-analytics",
                        "context.$": "$.context"
                    }
                },
                "End": True
            },

            "NotifyFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:quickcart-analytics-alerts",
                    "Subject": "🔴 Analytics Pipeline FAILED",
                    "Message": {
                        "status": "FAILED",
                        "pipeline": "cross-account-analytics",
                        "error.$": "$.error",
                        "context.$": "$.context"
                    }
                },
                "Next": "PipelineFailed"
            },

            "PipelineFailed": {
                "Type": "Fail",
                "Cause": "Cross-account analytics ETL failed",
                "Error": "AnalyticsPipelineFailed"
            }
        }
    }

    print(f"\n📋 STEP FUNCTIONS DEFINITION:")
    print(json.dumps(state_machine_definition, indent=2))

    with open("/tmp/acct_b_analytics_pipeline.json", "w") as f:
        json.dump(state_machine_definition, f, indent=2)
    print(f"\n💾 Saved to /tmp/acct_b_analytics_pipeline.json")

    return state_machine_definition


if __name__ == "__main__":
    create_event_driven_pipeline()