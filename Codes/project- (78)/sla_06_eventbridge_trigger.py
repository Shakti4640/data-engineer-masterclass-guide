# file: sla/06_eventbridge_trigger.py
# Purpose: Wire Glue job failures to auto-trigger the recovery state machine
# EventBridge catches Glue state changes and starts Step Functions

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def create_glue_failure_rule():
    """
    EventBridge rule that catches Glue job FAILED/TIMEOUT events
    
    HOW IT WORKS:
    → Glue automatically publishes state change events to EventBridge
    → Event pattern matches: detail.state = "FAILED" or "TIMEOUT"
    → Target: Step Functions state machine with input transformation
    
    EVENT STRUCTURE FROM GLUE:
    {
      "source": "aws.glue",
      "detail-type": "Glue Job State Change",
      "detail": {
        "jobName": "job_clean_orders",
        "state": "FAILED",
        "message": "OutOfMemoryError: GC overhead limit exceeded",
        "jobRunId": "jr_20250115_001"
      }
    }
    """
    events = boto3.client("events", region_name=REGION)

    # Get monitored job names from registry
    from pipeline_registry import get_all_glue_jobs
    monitored_jobs = list(get_all_glue_jobs().keys())

    # Event pattern: Glue job fails or times out
    event_pattern = {
        "source": ["aws.glue"],
        "detail-type": ["Glue Job State Change"],
        "detail": {
            "jobName": monitored_jobs,
            "state": ["FAILED", "TIMEOUT", "ERROR"]
        }
    }

    # Create rule
    events.put_rule(
        Name="sla-glue-job-failure-trigger",
        Description="Triggers auto-recovery when monitored Glue jobs fail",
        EventPattern=json.dumps(event_pattern),
        State="ENABLED",
        Tags=[
            {"Key": "Purpose", "Value": "SLA-AutoRecovery"},
            {"Key": "ManagedBy", "Value": "SLA-System"}
        ]
    )
    print("✅ EventBridge rule created: sla-glue-job-failure-trigger")

    # Create input transformer to map Glue event → Step Functions input
    sfn_arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:PipelineAutoRecovery"

    # Build pipeline lookup for input transformation
    all_jobs = get_all_glue_jobs()
    job_pipeline_map = {
        job: info["pipeline"] for job, info in all_jobs.items()
    }

    events.put_targets(
        Rule="sla-glue-job-failure-trigger",
        Targets=[
            {
                "Id": "StepFunctionsRecovery",
                "Arn": sfn_arn,
                "RoleArn": f"arn:aws:iam::{ACCOUNT_ID}:role/EventBridgeStepFunctionsRole",
                "InputTransformer": {
                    "InputPathsMap": {
                        "jobName": "$.detail.jobName",
                        "state": "$.detail.state",
                        "message": "$.detail.message",
                        "runId": "$.detail.jobRunId"
                    },
                    "InputTemplate": json.dumps({
                        "failed_job_name": "<jobName>",
                        "error_message": "<message>",
                        "error_type": "TRANSIENT",
                        "original_run_id": "<runId>",
                        "original_state": "<state>",
                        "pipeline_name": "RESOLVE_AT_RUNTIME",
                        "runbook_url": "RESOLVE_AT_RUNTIME"
                    })
                }
            }
        ]
    )
    print("✅ EventBridge target configured → Step Functions")

    # ALTERNATIVE: Use Lambda as intermediary to resolve pipeline name
    # This is cleaner than InputTransformer for complex lookups
    create_event_router_lambda(events, all_jobs)


def create_event_router_lambda(events_client, all_jobs):
    """
    Lambda that receives Glue failure event, enriches with pipeline context,
    and starts Step Functions with proper input
    
    WHY LAMBDA INTERMEDIARY:
    → InputTransformer can't do lookups (jobName → pipeline mapping)
    → Lambda can enrich the event with pipeline config, runbook URL, etc.
    → Also classifies error type (TRANSIENT vs PERSISTENT)
    """
    # This Lambda code would be deployed separately
    lambda_code = '''
import boto3
import json
import os

REGION = os.environ.get("AWS_REGION", "us-east-2")

# Job → Pipeline mapping (loaded from environment or DynamoDB)
JOB_PIPELINE_MAP = json.loads(os.environ.get("JOB_PIPELINE_MAP", "{}"))
PIPELINE_CONFIGS = json.loads(os.environ.get("PIPELINE_CONFIGS", "{}"))

# Error classification rules
TRANSIENT_ERRORS = [
    "OutOfMemoryError",
    "Connection timed out",
    "throttling",
    "TooManyRequestsException",
    "ServiceUnavailable",
    "TIMEOUT"
]

PERSISTENT_ERRORS = [
    "Access Denied",
    "NoSuchBucket",
    "NoSuchKey",
    "InvalidInputException",
    "EntityNotFoundException"
]


def classify_error(error_message, state):
    """Classify error as TRANSIENT or PERSISTENT"""
    if state == "TIMEOUT":
        return "TRANSIENT"
    
    error_upper = (error_message or "").upper()
    
    for pattern in PERSISTENT_ERRORS:
        if pattern.upper() in error_upper:
            return "PERSISTENT"
    
    for pattern in TRANSIENT_ERRORS:
        if pattern.upper() in error_upper:
            return "TRANSIENT"
    
    # Default: treat unknown errors as transient (will be retried)
    return "TRANSIENT"


def handler(event, context):
    """Route Glue failure event to Step Functions with enriched context"""
    detail = event.get("detail", {})
    job_name = detail.get("jobName", "unknown")
    error_message = detail.get("message", "No error message")
    state = detail.get("state", "UNKNOWN")
    
    # Look up pipeline
    pipeline_info = JOB_PIPELINE_MAP.get(job_name, {})
    pipeline_name = pipeline_info.get("pipeline", "unknown")
    
    # Get pipeline config
    pipeline_config = PIPELINE_CONFIGS.get(pipeline_name, {})
    runbook_url = pipeline_config.get(
        "runbook_url", 
        "https://wiki.quickcart.com/runbook/generic"
    )
    
    # Classify error
    error_type = classify_error(error_message, state)
    
    # Build Step Functions input
    sfn_input = {
        "failed_job_name": job_name,
        "pipeline_name": pipeline_name,
        "error_message": error_message[:500],
        "error_type": error_type,
        "original_run_id": detail.get("jobRunId", "unknown"),
        "original_state": state,
        "runbook_url": runbook_url,
        "tier": pipeline_config.get("tier", 3),
        "recovery_config": pipeline_config.get("recovery_config", {})
    }
    
    # Start Step Functions execution
    sfn = boto3.client("stepfunctions", region_name=REGION)
    
    execution_name = f"recovery-{job_name}-{context.aws_request_id[:8]}"
    
    sfn.start_execution(
        stateMachineArn=os.environ["STATE_MACHINE_ARN"],
        name=execution_name,
        input=json.dumps(sfn_input)
    )
    
    print(f"Started recovery: {execution_name} for {job_name} ({error_type})")
    return {"execution": execution_name}
'''

    print(f"\n📝 Lambda router code generated")
    print(f"   Deploy as: sla-event-router")
    print(f"   Trigger: EventBridge rule sla-glue-job-failure-trigger")
    return lambda_code


if __name__ == "__main__":
    create_glue_failure_rule()