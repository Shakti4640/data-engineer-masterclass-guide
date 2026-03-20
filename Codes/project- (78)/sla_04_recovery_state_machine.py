# file: sla/04_recovery_state_machine.py
# Purpose: Create Step Functions state machine for auto-recovery with
#          retry, scale-up, escalation, and incident tracking

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def build_state_machine_definition():
    """
    Build the ASL (Amazon States Language) definition for pipeline recovery
    
    FLOW:
    StartRecovery
      → LogIncidentStart
      → DetermineFailureType
        → [TRANSIENT] → RetryWithSameConfig (3x with backoff)
           → [SUCCESS] → LogRecovery → NotifyRecovered → End
           → [FAIL] → RetryWithScaledConfig (2x DPU)
              → [SUCCESS] → LogRecovery → NotifyRecovered → End
              → [FAIL] → EscalateToHuman
                 → NotifyWarning → Wait(10min) → CheckResolved
                    → [YES] → LogRecovery → End
                    → [NO] → NotifyCritical → Wait(15min) → CheckResolved
                       → [YES] → LogRecovery → End
                       → [NO] → NotifyEmergency → End
        → [PERSISTENT] → EscalateToHuman (skip retries)
    """
    definition = {
        "Comment": "Pipeline Auto-Recovery with Tiered Escalation",
        "StartAt": "LogIncidentStart",
        "States": {

            # ═══════════════════════════════════════
            # STEP 1: Log incident start
            # ═══════════════════════════════════════
            "LogIncidentStart": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:sla-log-incident",
                    "Payload": {
                        "action": "START",
                        "pipeline.$": "$.pipeline_name",
                        "failed_job.$": "$.failed_job_name",
                        "error_message.$": "$.error_message",
                        "error_type.$": "$.error_type",
                        "timestamp.$": "$$.State.EnteredTime"
                    }
                },
                "ResultPath": "$.incident",
                "Next": "NotifyInfo"
            },

            # ═══════════════════════════════════════
            # STEP 2: Send info notification (Tier 0)
            # ═══════════════════════════════════════
            "NotifyInfo": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-info-alerts",
                    "Subject": "ℹ️ Pipeline Failure Detected — Auto-Recovery Initiated",
                    "Message.$": "States.Format('Pipeline: {}\\nFailed Job: {}\\nError: {}\\nAction: Auto-retry initiated\\nIncident: {}', $.pipeline_name, $.failed_job_name, $.error_message, $.incident.Payload.incident_id)"
                },
                "ResultPath": "$.notify_info_result",
                "Next": "DetermineFailureType"
            },

            # ═══════════════════════════════════════
            # STEP 3: Classify failure type
            # ═══════════════════════════════════════
            "DetermineFailureType": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.error_type",
                        "StringEquals": "PERSISTENT",
                        "Next": "EscalateWarning"
                    }
                ],
                "Default": "RetryGlueJob"
            },

            # ═══════════════════════════════════════
            # STEP 4: Retry Glue job (same config)
            # ═══════════════════════════════════════
            "RetryGlueJob": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName.$": "$.failed_job_name",
                    "Arguments": {
                        "--enable-metrics": "true",
                        "--enable-continuous-cloudwatch-log": "true",
                        "--recovery-attempt": "1"
                    }
                },
                "ResultPath": "$.retry_result",
                "Retry": [
                    {
                        "ErrorEquals": [
                            "Glue.JobRunException",
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
                        "ResultPath": "$.retry_error",
                        "Next": "RetryWithScaledDPU"
                    }
                ],
                "Next": "LogRecoverySuccess"
            },

            # ═══════════════════════════════════════
            # STEP 5: Retry with scaled-up DPU
            # ═══════════════════════════════════════
            "RetryWithScaledDPU": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:sla-scale-and-retry",
                    "Payload": {
                        "job_name.$": "$.failed_job_name",
                        "pipeline_name.$": "$.pipeline_name",
                        "previous_error.$": "$.retry_error",
                        "scale_factor": 2
                    }
                },
                "ResultPath": "$.scaled_retry_result",
                "Retry": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "IntervalSeconds": 120,
                        "MaxAttempts": 1,
                        "BackoffRate": 1.0
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "ResultPath": "$.scaled_retry_error",
                        "Next": "EscalateWarning"
                    }
                ],
                "Next": "LogRecoverySuccess"
            },

            # ═══════════════════════════════════════
            # STEP 6: Escalation Tier 1 — Warning
            # ═══════════════════════════════════════
            "EscalateWarning": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-warning-alerts",
                    "Subject": "⚠️ Auto-Recovery Failed — Manual Attention Needed",
                    "Message.$": "States.Format('🔴 PIPELINE FAILURE — AUTO-RECOVERY EXHAUSTED\\n\\nPipeline: {}\\nFailed Job: {}\\nError: {}\\nRetry Attempts: 3 (all failed)\\nIncident: {}\\n\\n📊 DOWNSTREAM IMPACT:\\nCheck lineage: python lineage_cli.py downstream --node \"{}\"\\n\\n📋 RUNBOOK:\\n{}', $.pipeline_name, $.failed_job_name, $.error_message, $.incident.Payload.incident_id, $.failed_job_name, $.runbook_url)"
                },
                "ResultPath": "$.warning_result",
                "Next": "WaitForManualResolution"
            },

            # ═══════════════════════════════════════
            # STEP 7: Wait and check if resolved
            # ═══════════════════════════════════════
            "WaitForManualResolution": {
                "Type": "Wait",
                "Seconds": 600,
                "Next": "CheckAlarmState"
            },

            "CheckAlarmState": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:sla-check-alarm-state",
                    "Payload": {
                        "pipeline_name.$": "$.pipeline_name",
                        "incident_id.$": "$.incident.Payload.incident_id"
                    }
                },
                "ResultPath": "$.alarm_check",
                "Next": "IsResolved"
            },

            "IsResolved": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.alarm_check.Payload.is_resolved",
                        "BooleanEquals": True,
                        "Next": "LogRecoverySuccess"
                    }
                ],
                "Default": "EscalateCritical"
            },

            # ═══════════════════════════════════════
            # STEP 8: Escalation Tier 2 — Critical
            # ═══════════════════════════════════════
            "EscalateCritical": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-critical-alerts",
                    "Subject": "🔴 CRITICAL — Pipeline Down 15+ Minutes — Paging On-Call",
                    "Message.$": "States.Format('🚨 CRITICAL PIPELINE FAILURE\\n\\nPipeline: {}\\nFailed Job: {}\\nDown Since: {}\\nIncident: {}\\n\\nAll auto-recovery attempts failed.\\nManual resolution has not occurred within 10 minutes.\\n\\n📋 IMMEDIATE ACTION REQUIRED:\\n1. Check Glue job logs in CloudWatch\\n2. Run: python lineage_cli.py investigate --node \"{}\"\\n3. Follow runbook: {}\\n\\nIf you cannot resolve, escalate to engineering manager.', $.pipeline_name, $.failed_job_name, $.incident.Payload.started_at, $.incident.Payload.incident_id, $.failed_job_name, $.runbook_url)"
                },
                "ResultPath": "$.critical_result",
                "Next": "WaitForCriticalResolution"
            },

            "WaitForCriticalResolution": {
                "Type": "Wait",
                "Seconds": 900,
                "Next": "CheckAlarmStateFinal"
            },

            "CheckAlarmStateFinal": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:sla-check-alarm-state",
                    "Payload": {
                        "pipeline_name.$": "$.pipeline_name",
                        "incident_id.$": "$.incident.Payload.incident_id"
                    }
                },
                "ResultPath": "$.final_check",
                "Next": "IsFinallyResolved"
            },

            "IsFinallyResolved": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.final_check.Payload.is_resolved",
                        "BooleanEquals": True,
                        "Next": "LogRecoverySuccess"
                    }
                ],
                "Default": "EscalateEmergency"
            },

            # ═══════════════════════════════════════
            # STEP 9: Escalation Tier 3 — Emergency
            # ═══════════════════════════════════════
            "EscalateEmergency": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-emergency-alerts",
                    "Subject": "🚨 EMERGENCY — Pipeline Down 30+ Minutes — SLA BREACH IMMINENT",
                    "Message.$": "States.Format('🚨🚨🚨 EMERGENCY — SLA BREACH IMMINENT\\n\\nPipeline: {}\\nIncident: {}\\nDowntime: 30+ minutes\\n\\nAll automated and manual recovery attempts have failed.\\nEngineering manager escalation required.\\n\\nSLA Target: 99.9% (max 43 min/month downtime)\\nThis incident alone may breach monthly SLA.', $.pipeline_name, $.incident.Payload.incident_id)"
                },
                "ResultPath": "$.emergency_result",
                "Next": "LogIncidentUnresolved"
            },

            # ═══════════════════════════════════════
            # SUCCESS PATH: Log recovery
            # ═══════════════════════════════════════
            "LogRecoverySuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:sla-log-incident",
                    "Payload": {
                        "action": "RESOLVE",
                        "incident_id.$": "$.incident.Payload.incident_id",
                        "pipeline.$": "$.pipeline_name",
                        "resolution": "AUTO_RECOVERED",
                        "timestamp.$": "$$.State.EnteredTime"
                    }
                },
                "ResultPath": "$.recovery_log",
                "Next": "NotifyRecovered"
            },

            "NotifyRecovered": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-info-alerts",
                    "Subject": "✅ Pipeline Recovered Successfully",
                    "Message.$": "States.Format('✅ PIPELINE RECOVERED\\n\\nPipeline: {}\\nIncident: {}\\nRecovery Time: {}\\nResolution: Auto-recovered\\n\\nNo further action required.', $.pipeline_name, $.incident.Payload.incident_id, $$.State.EnteredTime)"
                },
                "ResultPath": "$.recovery_notify",
                "Next": "PublishSLAMetric"
            },

            "PublishSLAMetric": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:sla-publish-metric",
                    "Payload": {
                        "incident_id.$": "$.incident.Payload.incident_id",
                        "pipeline.$": "$.pipeline_name",
                        "resolution": "RECOVERED"
                    }
                },
                "ResultPath": "$.metric_result",
                "Next": "SuccessEnd"
            },

            # ═══════════════════════════════════════
            # FAILURE PATH: Log unresolved
            # ═══════════════════════════════════════
            "LogIncidentUnresolved": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:sla-log-incident",
                    "Payload": {
                        "action": "ESCALATED",
                        "incident_id.$": "$.incident.Payload.incident_id",
                        "pipeline.$": "$.pipeline_name",
                        "resolution": "UNRESOLVED_ESCALATED",
                        "timestamp.$": "$$.State.EnteredTime"
                    }
                },
                "ResultPath": "$.unresolved_log",
                "Next": "FailureEnd"
            },

            # ═══════════════════════════════════════
            # TERMINAL STATES
            # ═══════════════════════════════════════
            "SuccessEnd": {
                "Type": "Succeed"
            },

            "FailureEnd": {
                "Type": "Fail",
                "Error": "PipelineUnresolved",
                "Cause": "All auto-recovery and escalation attempts exhausted"
            }
        }
    }

    return definition


def create_state_machine():
    """Deploy the Step Functions state machine"""
    sfn = boto3.client("stepfunctions", region_name=REGION)

    definition = build_state_machine_definition()

    try:
        response = sfn.create_state_machine(
            name="PipelineAutoRecovery",
            definition=json.dumps(definition, indent=2),
            roleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/StepFunctionsRecoveryRole",
            type="STANDARD",
            loggingConfiguration={
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:/aws/stepfunctions/PipelineAutoRecovery:*"
                        }
                    }
                ]
            },
            tags=[
                {"key": "Purpose", "value": "SLA-AutoRecovery"},
                {"key": "Team", "value": "DataPlatform"}
            ]
        )
        print(f"✅ State machine created: {response['stateMachineArn']}")
        return response["stateMachineArn"]

    except sfn.exceptions.StateMachineAlreadyExists:
        # Update existing
        arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:PipelineAutoRecovery"
        sfn.update_state_machine(
            stateMachineArn=arn,
            definition=json.dumps(definition, indent=2),
            roleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/StepFunctionsRecoveryRole"
        )
        print(f"✅ State machine updated: {arn}")
        return arn


if __name__ == "__main__":
    arn = create_state_machine()
    print(f"\n🎯 Recovery state machine ready: {arn}")