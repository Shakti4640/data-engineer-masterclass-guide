# file: 68_04_update_step_function.py
# Run from: Account [REDACTED:BANK_ACCOUNT_NUMBER]222)
# Purpose: Add quality gate steps to event-driven pipeline (Project 66)

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def get_updated_state_machine():
    """
    Updated Step Function with quality gates inserted
    
    ORIGINAL FLOW (Project 66):
    ClassifyEvent → RouteByEventType → RunSourceETL → MarkComplete → CheckAll
    
    NEW FLOW:
    ClassifyEvent → RouteByEventType → PreFlightCheck → RunSourceETL(+DQ) → 
    CheckDQResult → MarkComplete/MarkFailed → CheckAll
    
    KEY CHANGES:
    1. PreFlightCheck (Lambda) before Glue job
    2. Glue job now includes DQ checks (68_03)
    3. CheckDQResult evaluates Glue output for quality status
    4. Quality failure → MarkFailed → Alert (doesn't trigger gold layer)
    """
    state_machine = {
        "Comment": "Event-Driven Pipeline with Data Quality Gates",
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
                        "Next": "PreFlightQualityCheck"
                    },
                    {
                        "Variable": "$.classification.event_type",
                        "StringEquals": "scheduled_cdc",
                        "Next": "RunSourceETL"
                    },
                    {
                        "Variable": "$.classification.event_type",
                        "StringEquals": "scheduled_inventory",
                        "Next": "PreFlightQualityCheck"
                    },
                    {
                        "Variable": "$.classification.event_type",
                        "StringEquals": "etl_completed",
                        "Next": "CheckAllSourcesComplete"
                    }
                ],
                "Default": "IgnoreUnknownEvent"
            },

            # ============================================================
            # NEW: Pre-flight quality check (Lambda — fast, cheap)
            # ============================================================
            "PreFlightQualityCheck": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "data-quality-preflight",
                    "Payload": {
                        "source_name.$": "$.classification.source_name",
                        "file_bucket.$": "$.classification.file_bucket",
                        "file_key.$": "$.classification.file_key",
                        "batch_date.$": "$.classification.batch_date"
                    }
                },
                "ResultSelector": {
                    "preflight_passed.$": "$.Payload.preflight_passed",
                    "checks.$": "$.Payload.checks"
                },
                "ResultPath": "$.preflight",
                "TimeoutSeconds": 60,
                "Next": "PreFlightPassed?",
                "Retry": [{"ErrorEquals": ["States.ALL"], "IntervalSeconds": 5, "MaxAttempts": 2}],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "NotifyQualityFailure",
                    "ResultPath": "$.error"
                }]
            },

            "PreFlightPassed?": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.preflight.preflight_passed",
                        "BooleanEquals": True,
                        "Next": "RunSourceETL"
                    }
                ],
                "Default": "NotifyQualityFailure"
            },

            # ============================================================
            # ETL with integrated DQ (Glue job — 68_03)
            # ============================================================
            "RunSourceETL": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "etl-with-data-quality",
                    "Arguments": {
                        "--source_name.$": "$.classification.source_name",
                        "--file_bucket.$": "$.classification.file_bucket",
                        "--file_key.$": "$.classification.file_key",
                        "--batch_date.$": "$.classification.batch_date",
                        "--dq_severity": "HARD_FAIL"
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
                    }
                ],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "ClassifyETLFailure",
                    "ResultPath": "$.error"
                }]
            },

            # ============================================================
            # NEW: Classify whether ETL failure was DQ or infrastructure
            # ============================================================
            "ClassifyETLFailure": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.error.Cause",
                        "StringMatches": "*Data quality HARD_FAIL*",
                        "Next": "NotifyQualityFailure"
                    }
                ],
                "Default": "MarkSourceFailed"
            },

            # ============================================================
            # NEW: Quality failure notification (distinct from infra failure)
            # ============================================================
            "NotifyQualityFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts",
                    "Subject": "🔴 DATA QUALITY GATE BLOCKED PIPELINE",
                    "Message.$": "States.Format('Data quality check FAILED for source: {}.\nBatch: {}\nPipeline STOPPED. Data quarantined.\n\nAction required:\n1. Check quarantine bucket\n2. Contact source system owner\n3. Fix data issue\n4. Manually re-trigger pipeline\n\nDO NOT override quality gates without approval.', $.classification.source_name, $.classification.batch_date)"
                },
                "ResultPath": "$.notification",
                "Next": "MarkSourceQualityFailed"
            },

            "MarkSourceQualityFailed": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:updateItem",
                "Parameters": {
                    "TableName": "pipeline_batch_state",
                    "Key": {
                        "batch_date": {"S.$": "$.classification.batch_date"},
                        "source_name": {"S.$": "$.classification.source_name"}
                    },
                    "UpdateExpression": "SET #s = :s, quality_status = :q",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {
                        ":s": {"S": "quality_failed"},
                        ":q": {"S": "HARD_FAIL"}
                    }
                },
                "ResultPath": "$.dynamodb_result",
                "End": True
            },

            # Remaining states from Project 66 (unchanged)
            "MarkSourceComplete": {
                "Type": "Task",
                "Resource": "arn:aws:states:::dynamodb:updateItem",
                "Parameters": {
                    "TableName": "pipeline_batch_state",
                    "Key": {
                        "batch_date": {"S.$": "$.classification.batch_date"},
                        "source_name": {"S.$": "$.classification.source_name"}
                    },
                    "UpdateExpression": "SET #s = :s, completed_at = :t, quality_status = :q",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {
                        ":s": {"S": "completed"},
                        ":t": {"S.$": "$$.State.EnteredTime"},
                        ":q": {"S": "PASSED"}
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
                    "UpdateExpression": "SET #s = :s, quality_status = :q",
                    "ExpressionAttributeNames": {"#s": "status"},
                    "ExpressionAttributeValues": {
                        ":s": {"S": "failed"},
                        ":q": {"S": "INFRA_FAILURE"}
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
                        "detail": {"batch_date.$": "$.classification.batch_date"}
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
                "Choices": [{
                    "Variable": "$.completion_check.all_complete",
                    "BooleanEquals": True,
                    "Next": "GoldLayerQualityCheck"
                }],
                "Default": "WaitForOtherSources"
            },

            # ============================================================
            # NEW: Gold layer quality check BEFORE building gold
            # ============================================================
            "GoldLayerQualityCheck": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "gold-layer-quality-check",
                    "Payload": {
                        "batch_date.$": "$.classification.batch_date",
                        "check_type": "pre_gold_validation"
                    }
                },
                "ResultSelector": {
                    "gold_quality_passed.$": "$.Payload.passed"
                },
                "ResultPath": "$.gold_quality",
                "TimeoutSeconds": 300,
                "Next": "GoldQualityPassed?",
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "NotifyFailure",
                    "ResultPath": "$.error"
                }]
            },

            "GoldQualityPassed?": {
                "Type": "Choice",
                "Choices": [{
                    "Variable": "$.gold_quality.gold_quality_passed",
                    "BooleanEquals": True,
                    "Next": "BuildGoldLayer"
                }],
                "Default": "NotifyQualityFailure"
            },

            "BuildGoldLayer": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "build-gold-aggregations",
                    "Arguments": {"--batch_date.$": "$.classification.batch_date"}
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
                    "Subject": "✅ Pipeline Complete — All Quality Gates Passed",
                    "Message.$": "States.Format('Pipeline completed for batch {}.\nAll sources processed. All quality checks PASSED.\nGold layer built. MVs refreshed.\nDashboards serving TRUSTED data. ✅', $.classification.batch_date)"
                },
                "End": True
            },

            "NotifyFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts",
                    "Subject": "❌ Pipeline FAILED — Infrastructure Error",
                    "Message.$": "States.Format('Pipeline failed for batch {}. Source: {}. Check Step Functions console.', $.classification.batch_date, $.classification.source_name)"
                },
                "End": True
            },

            "WaitForOtherSources": {
                "Type": "Succeed",
                "Comment": "This source done. Waiting for other sources."
            },

            "IgnoreUnknownEvent": {
                "Type": "Succeed",
                "Comment": "Unknown event type — ignored"
            }
        }
    }

    return state_machine


def deploy_updated_step_function():
    """Deploy the quality-gate-enhanced Step Function"""
    sfn = boto3.client("stepfunctions", region_name=REGION)

    sm_name = "event-driven-data-pipeline"
    sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:{sm_name}"
    role_arn = f"arn:aws:iam::{ACCOUNT_B_ID}:role/StepFunctionsEventDrivenPipelineRole"

    state_machine = get_updated_state_machine()

    try:
        sfn.update_state_machine(
            stateMachineArn=sm_arn,
            definition=json.dumps(state_machine),
            roleArn=role_arn
        )
        print(f"✅ Step Function updated with quality gates: {sm_arn}")
    except Exception as e:
        print(f"❌ Failed to update Step Function: {str(e)[:200]}")

    print(f"\n   New flow:")
    print(f"   Event → Classify → PreFlight(λ) → ETL+DQ(Glue) → CheckAll")
    print(f"   → GoldQC(λ) → BuildGold(Glue) → RefreshMVs(λ) → Notify")
    print(f"   Quality failure at ANY gate → Quarantine + Alert + STOP")


if __name__ == "__main__":
    print("=" * 60)
    print("🔧 DEPLOYING QUALITY-GATED PIPELINE")
    print("=" * 60)
    deploy_updated_step_function()