# file: sla/05_lambda_functions.py
# Purpose: All Lambda functions referenced by the Step Functions state machine

import boto3
import json
import uuid
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-2"
INCIDENT_TABLE = "SLAIncidentTracker"


# ═══════════════════════════════════════════════════════════════
# LAMBDA 1: Log Incident (Start / Resolve / Escalate)
# ═══════════════════════════════════════════════════════════════

def handler_log_incident(event, context):
    """
    Track incident lifecycle in DynamoDB
    
    Actions:
    → START: create new incident record
    → RESOLVE: update with resolution time + calculate MTTR
    → ESCALATED: mark as escalated, still open
    """
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(INCIDENT_TABLE)

    action = event["action"]
    timestamp = event.get("timestamp", datetime.now(timezone.utc).isoformat())

    if action == "START":
        incident_id = f"INC-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}"

        table.put_item(
            Item={
                "incident_id": incident_id,
                "pipeline": event["pipeline"],
                "failed_job": event.get("failed_job", "unknown"),
                "error_message": event.get("error_message", "")[:500],
                "error_type": event.get("error_type", "UNKNOWN"),
                "started_at": timestamp,
                "status": "OPEN",
                "recovery_attempts": 0,
                "resolution": None,
                "resolved_at": None,
                "mttr_minutes": None,
                "month_key": datetime.now().strftime("%Y-%m")
            }
        )

        logger.info(f"Incident created: {incident_id}")
        return {
            "incident_id": incident_id,
            "started_at": timestamp,
            "status": "OPEN"
        }

    elif action == "RESOLVE":
        incident_id = event["incident_id"]

        # Get start time to calculate MTTR
        response = table.get_item(Key={"incident_id": incident_id})
        item = response.get("Item", {})
        started_at = item.get("started_at", timestamp)

        # Calculate MTTR
        start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        mttr_minutes = round((end_dt - start_dt).total_seconds() / 60, 1)

        table.update_item(
            Key={"incident_id": incident_id},
            UpdateExpression=(
                "SET #status = :status, resolved_at = :resolved, "
                "resolution = :resolution, mttr_minutes = :mttr, "
                "recovery_attempts = recovery_attempts + :one"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "RESOLVED",
                ":resolved": timestamp,
                ":resolution": event.get("resolution", "UNKNOWN"),
                ":mttr": int(mttr_minutes),
                ":one": 1
            }
        )

        logger.info(
            f"Incident resolved: {incident_id} | MTTR: {mttr_minutes}min"
        )
        return {
            "incident_id": incident_id,
            "status": "RESOLVED",
            "mttr_minutes": mttr_minutes
        }

    elif action == "ESCALATED":
        incident_id = event["incident_id"]

        table.update_item(
            Key={"incident_id": incident_id},
            UpdateExpression=(
                "SET #status = :status, "
                "escalated_at = :escalated, "
                "recovery_attempts = recovery_attempts + :one"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "ESCALATED",
                ":escalated": timestamp,
                ":one": 1
            }
        )

        logger.info(f"Incident escalated: {incident_id}")
        return {
            "incident_id": incident_id,
            "status": "ESCALATED"
        }


# ═══════════════════════════════════════════════════════════════
# LAMBDA 2: Scale Up Glue Job and Retry
# ═══════════════════════════════════════════════════════════════

def handler_scale_and_retry(event, context):
    """
    Retry a Glue job with increased DPU
    
    Logic:
    → Read current job config
    → Multiply DPU by scale_factor (default 2x)
    → Cap at max_dpu from pipeline registry
    → Start new run with scaled config
    → Wait for completion (sync)
    """
    glue = boto3.client("glue", region_name=REGION)

    job_name = event["job_name"]
    pipeline_name = event["pipeline_name"]
    scale_factor = event.get("scale_factor", 2)

    # Get current job config
    job_response = glue.get_job(JobName=job_name)
    current_config = job_response["Job"]

    # Determine current and new DPU
    current_workers = current_config.get("NumberOfWorkers", 10)
    new_workers = min(current_workers * scale_factor, 40)  # Cap at 40

    logger.info(
        f"Scaling {job_name}: {current_workers} → {new_workers} workers"
    )

    # Start job with increased capacity
    run_response = glue.start_job_run(
        JobName=job_name,
        Arguments={
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--recovery-attempt": "scaled-retry",
            "--scaled-workers": str(new_workers)
        },
        NumberOfWorkers=int(new_workers),
        WorkerType=current_config.get("WorkerType", "G.1X")
    )

    run_id = run_response["JobRunId"]
    logger.info(f"Started scaled retry: {run_id}")

    # Wait for completion (poll — Step Functions handles timeout)
    import time
    max_wait = 3600  # 1 hour max
    elapsed = 0

    while elapsed < max_wait:
        status_response = glue.get_job_run(
            JobName=job_name,
            RunId=run_id
        )
        state = status_response["JobRun"]["JobRunState"]

        if state == "SUCCEEDED":
            logger.info(f"Scaled retry succeeded: {run_id}")
            return {
                "status": "SUCCEEDED",
                "run_id": run_id,
                "workers_used": new_workers
            }
        elif state in ("FAILED", "ERROR", "TIMEOUT"):
            error_msg = status_response["JobRun"].get(
                "ErrorMessage", "Unknown error"
            )
            logger.error(f"Scaled retry failed: {error_msg}")
            raise Exception(
                f"Scaled retry failed with {new_workers} workers: {error_msg}"
            )

        time.sleep(30)
        elapsed += 30

    raise Exception(f"Scaled retry timed out after {max_wait}s")


# ═══════════════════════════════════════════════════════════════
# LAMBDA 3: Check Alarm State (Is Pipeline Recovered?)
# ═══════════════════════════════════════════════════════════════

def handler_check_alarm_state(event, context):
    """
    Check if the composite alarm for a pipeline has returned to OK
    
    Used by Step Functions Wait → Check pattern:
    → Wait 10 minutes
    → Check if alarm is OK
    → If OK → resolved
    → If ALARM → continue escalation
    """
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    pipeline_name = event["pipeline_name"]
    alarm_name = f"sla-{pipeline_name}-composite-unhealthy"

    response = cloudwatch.describe_alarms(
        AlarmNames=[alarm_name]
    )

    alarms = response.get("CompositeAlarms", [])

    if not alarms:
        # Try metric alarms if composite not found
        response = cloudwatch.describe_alarms(
            AlarmNames=[alarm_name]
        )
        alarms = response.get("MetricAlarms", [])

    if alarms:
        state = alarms[0]["StateValue"]
        is_resolved = state == "OK"
    else:
        # Alarm not found — assume resolved (fail open)
        logger.warning(f"Alarm {alarm_name} not found — assuming resolved")
        is_resolved = True

    logger.info(
        f"Alarm check: {alarm_name} = {state if alarms else 'NOT_FOUND'} "
        f"| Resolved: {is_resolved}"
    )

    return {
        "alarm_name": alarm_name,
        "alarm_state": state if alarms else "NOT_FOUND",
        "is_resolved": is_resolved,
        "checked_at": datetime.now(timezone.utc).isoformat()
    }


# ═══════════════════════════════════════════════════════════════
# LAMBDA 4: Publish SLA Metric
# ═══════════════════════════════════════════════════════════════

def handler_publish_sla_metric(event, context):
    """
    Publish SLA-related metrics to CloudWatch after incident resolution
    
    Metrics published:
    → IncidentCount (count of incidents per pipeline)
    → MTTRMinutes (mean time to recovery)
    → AutoRecoverySuccess (1 = auto-recovered, 0 = manual)
    """
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)
    dynamodb = boto3.resource("dynamodb", region_name=REGION)

    incident_id = event["incident_id"]
    pipeline = event["pipeline"]
    resolution = event.get("resolution", "UNKNOWN")

    # Get incident details
    table = dynamodb.Table(INCIDENT_TABLE)
    response = table.get_item(Key={"incident_id": incident_id})
    item = response.get("Item", {})

    mttr = item.get("mttr_minutes", 0)
    auto_recovered = 1 if resolution == "RECOVERED" else 0

    now = datetime.now(timezone.utc)

    cloudwatch.put_metric_data(
        Namespace="QuickCart/SLA",
        MetricData=[
            {
                "MetricName": "IncidentCount",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": pipeline}
                ],
                "Timestamp": now,
                "Value": 1,
                "Unit": "Count"
            },
            {
                "MetricName": "MTTRMinutes",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": pipeline}
                ],
                "Timestamp": now,
                "Value": mttr,
                "Unit": "Count"
            },
            {
                "MetricName": "AutoRecoverySuccess",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": pipeline}
                ],
                "Timestamp": now,
                "Value": auto_recovered,
                "Unit": "Count"
            }
        ]
    )

    logger.info(
        f"SLA metrics published: {pipeline} | "
        f"MTTR={mttr}min | AutoRecovered={auto_recovered}"
    )

    return {"metrics_published": True, "mttr": mttr}