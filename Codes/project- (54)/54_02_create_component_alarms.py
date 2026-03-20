# file: 54_02_create_component_alarms.py
# Purpose: Create CloudWatch alarms for each pipeline component

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/monitoring_config.json", "r") as f:
        return json.load(f)


def create_all_pipeline_alarms():
    """
    Create individual alarms for each pipeline component
    
    ALARM DESIGN PRINCIPLES:
    → Critical: immediate action needed (data loss, pipeline broken)
    → Warning: attention needed but not urgent (slow, degraded)
    → Each alarm has: description, runbook link, appropriate severity topic
    """
    config = load_config()
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    warning_topic = config["topic_arns"]["WARNING"]
    critical_topic = config["topic_arns"]["CRITICAL"]
    sla_topic = config["topic_arns"]["SLA_BREACH"]

    print("=" * 70)
    print("🚨 CREATING PIPELINE COMPONENT ALARMS")
    print("=" * 70)

    alarms_created = []

    # ═══════════════════════════════════════════════════
    # ALARM 1: Glue ETL Job Failure (CRITICAL)
    # ═══════════════════════════════════════════════════
    alarm_name = "Pipeline-GlueETL-JobFailed"
    cw_client.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=json.dumps({
            "summary": "Glue ETL job 'quickcart-orders-etl' has FAILED",
            "impact": "Gold layer will have stale data. Downstream dashboards affected.",
            "runbook": "https://wiki.quickcart.internal/runbooks/glue-etl-failure",
            "action": "1. Check Glue console for error details. "
                      "2. Check CloudWatch Logs /aws-glue/jobs/error. "
                      "3. Fix root cause and rerun job manually.",
            "severity": "CRITICAL",
            "team": "data-engineering",
            "oncall_slack": "#data-oncall"
        }),
        Namespace="AWS/Glue",
        MetricName="glue.driver.aggregate.numFailedTasks",
        Dimensions=[
            {"Name": "JobName", "Value": "quickcart-orders-etl"},
            {"Name": "JobRunId", "Value": "ALL"},
            {"Name": "Type", "Value": "count"}
        ],
        Statistic="Sum",
        Period=300,                     # 5-minute evaluation
        EvaluationPeriods=1,            # Fire on FIRST failure
        DatapointsToAlarm=1,
        Threshold=0,
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",    # No data = job hasn't run = OK
        AlarmActions=[critical_topic],
        OKActions=[warning_topic],          # Send "resolved" to warning topic
        Tags=[
            {"Key": "Pipeline", "Value": "nightly-etl"},
            {"Key": "Severity", "Value": "Critical"},
            {"Key": "Component", "Value": "Glue-ETL"}
        ]
    )
    alarms_created.append(alarm_name)
    print(f"  ✅ {alarm_name} → CRITICAL")

    # ═══════════════════════════════════════════════════
    # ALARM 2: Glue Crawler Failure (WARNING)
    # ═══════════════════════════════════════════════════
    alarm_name = "Pipeline-GlueCrawler-Failed"
    cw_client.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=json.dumps({
            "summary": "Glue Crawler failed to update Data Catalog",
            "impact": "New partitions may not be discoverable by Athena/Spectrum.",
            "runbook": "https://wiki.quickcart.internal/runbooks/crawler-failure",
            "action": "1. Check Crawler logs in CloudWatch. "
                      "2. Verify S3 source path exists and has new data. "
                      "3. Check Crawler IAM role permissions. "
                      "4. Rerun crawler manually.",
            "severity": "WARNING"
        }),
        Namespace="AWS/Glue",
        MetricName="glue.driver.aggregate.numFailedTasks",
        Dimensions=[
            {"Name": "JobName", "Value": "quickcart-raw-crawler"},
            {"Name": "JobRunId", "Value": "ALL"},
            {"Name": "Type", "Value": "count"}
        ],
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=2,            # Must fail 2 consecutive times
        DatapointsToAlarm=2,            # Tolerates single transient failure
        Threshold=0,
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[warning_topic],
        OKActions=[warning_topic],
        Tags=[
            {"Key": "Pipeline", "Value": "nightly-etl"},
            {"Key": "Severity", "Value": "Warning"},
            {"Key": "Component", "Value": "Glue-Crawler"}
        ]
    )
    alarms_created.append(alarm_name)
    print(f"  ✅ {alarm_name} → WARNING")

    # ═══════════════════════════════════════════════════
    # ALARM 3: SQS Dead Letter Queue Messages (CRITICAL)
    # ═══════════════════════════════════════════════════
    alarm_name = "Pipeline-SQS-DLQ-NotEmpty"
    cw_client.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=json.dumps({
            "summary": "Messages found in Dead Letter Queue — data loss risk!",
            "impact": "Failed messages are NOT being processed. "
                      "Data may be missing from downstream tables.",
            "runbook": "https://wiki.quickcart.internal/runbooks/dlq-messages",
            "action": "1. Check DLQ: aws sqs receive-message --queue-url <dlq-url>. "
                      "2. Inspect message body for error cause. "
                      "3. Fix root cause in consumer. "
                      "4. Redrive messages from DLQ to main queue.",
            "severity": "CRITICAL"
        }),
        Namespace="AWS/SQS",
        MetricName="ApproximateNumberOfMessagesVisible",
        Dimensions=[
            {"Name": "QueueName", "Value": "quickcart-etl-queue-dlq"}
        ],
        Statistic="Maximum",
        Period=60,                      # Check every minute
        EvaluationPeriods=1,            # Alarm on FIRST message in DLQ
        DatapointsToAlarm=1,
        Threshold=0,
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[critical_topic],
        OKActions=[warning_topic],
        Tags=[
            {"Key": "Pipeline", "Value": "nightly-etl"},
            {"Key": "Severity", "Value": "Critical"},
            {"Key": "Component", "Value": "SQS-DLQ"}
        ]
    )
    alarms_created.append(alarm_name)
    print(f"  ✅ {alarm_name} → CRITICAL")

    # ═══════════════════════════════════════════════════
    # ALARM 4: DataBrew Job Failure (CRITICAL)
    # ═══════════════════════════════════════════════════
    alarm_name = "Pipeline-DataBrew-JobFailed"
    cw_client.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=json.dumps({
            "summary": "DataBrew cleaning job failed — dirty data may reach Gold layer",
            "impact": "Silver zone NOT updated. Downstream ETL will use stale/dirty data.",
            "runbook": "https://wiki.quickcart.internal/runbooks/databrew-failure",
            "action": "1. Check DataBrew console → Jobs → quickcart-customers-clean-job. "
                      "2. Review error message in job run details. "
                      "3. Common: new state value not in mapping, S3 permission, schema change. "
                      "4. Fix recipe or source data → rerun job.",
            "severity": "CRITICAL"
        }),
        # DataBrew publishes to custom namespace
        Namespace="AWS/GlueDataBrew",
        MetricName="JobsFailed",
        Dimensions=[
            {"Name": "JobName", "Value": "quickcart-customers-clean-job"}
        ],
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=1,
        DatapointsToAlarm=1,
        Threshold=0,
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[critical_topic],
        OKActions=[warning_topic],
        Tags=[
            {"Key": "Pipeline", "Value": "nightly-etl"},
            {"Key": "Severity", "Value": "Critical"},
            {"Key": "Component", "Value": "DataBrew"}
        ]
    )
    alarms_created.append(alarm_name)
    print(f"  ✅ {alarm_name} → CRITICAL")

    # ═══════════════════════════════════════════════════
    # ALARM 5: EC2 Worker Heartbeat Missing (WARNING)
    # From Project 52: workers publish heartbeat metrics
    # ═══════════════════════════════════════════════════
    alarm_name = "Pipeline-Worker-HeartbeatMissing"
    cw_client.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=json.dumps({
            "summary": "SQS worker heartbeat not received — worker may have crashed",
            "impact": "Queue processing slowed or stopped. Backlog growing.",
            "runbook": "https://wiki.quickcart.internal/runbooks/worker-heartbeat",
            "action": "1. Check ASG: are instances running? "
                      "2. SSH into worker → check systemctl status sqs-worker. "
                      "3. Check /var/log/sqs-worker.log for errors. "
                      "4. If worker crashed: ASG should auto-replace. Wait 5 min.",
            "severity": "WARNING"
        }),
        Namespace="QuickCart/SQSWorkers",
        MetricName="WorkerHeartbeat",
        Dimensions=[
            {"Name": "QueueName", "Value": "quickcart-etl-queue"}
        ],
        Statistic="Sum",
        Period=300,                     # 5-minute check
        EvaluationPeriods=2,            # Must be missing for 10 minutes
        DatapointsToAlarm=2,
        Threshold=0,
        ComparisonOperator="LessThanOrEqualToThreshold",
        TreatMissingData="breaching",   # Missing heartbeat IS the problem
        AlarmActions=[warning_topic],
        OKActions=[warning_topic],
        Tags=[
            {"Key": "Pipeline", "Value": "queue-processing"},
            {"Key": "Severity", "Value": "Warning"},
            {"Key": "Component", "Value": "EC2-Worker"}
        ]
    )
    alarms_created.append(alarm_name)
    print(f"  ✅ {alarm_name} → WARNING (TreatMissingData=breaching)")

    # ═══════════════════════════════════════════════════
    # ALARM 6: S3 Upload Not Received (CRITICAL)
    # Expected daily upload by 12:30 AM — if missing by 1 AM = problem
    # ═══════════════════════════════════════════════════
    alarm_name = "Pipeline-S3Upload-NotReceived"
    cw_client.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=json.dumps({
            "summary": "Daily CSV upload to S3 not detected — source data missing",
            "impact": "Entire downstream pipeline has no new data to process.",
            "runbook": "https://wiki.quickcart.internal/runbooks/s3-upload-missing",
            "action": "1. Check EC2 cron job: crontab -l. "
                      "2. Check upload script log: /var/log/s3-upload.log. "
                      "3. Check MariaDB export: was CSV generated? "
                      "4. Check S3: aws s3 ls s3://quickcart-raw-data-prod/daily_exports/",
            "severity": "CRITICAL"
        }),
        Namespace="QuickCart/Pipeline",
        MetricName="DailyUploadSuccess",
        Statistic="Sum",
        Period=3600,                    # 1-hour check
        EvaluationPeriods=1,
        DatapointsToAlarm=1,
        Threshold=0,
        ComparisonOperator="LessThanOrEqualToThreshold",
        TreatMissingData="breaching",   # No metric = upload didn't happen
        AlarmActions=[critical_topic],
        OKActions=[warning_topic],
        Tags=[
            {"Key": "Pipeline", "Value": "nightly-etl"},
            {"Key": "Severity", "Value": "Critical"},
            {"Key": "Component", "Value": "S3-Upload"}
        ]
    )
    alarms_created.append(alarm_name)
    print(f"  ✅ {alarm_name} → CRITICAL (TreatMissingData=breaching)")

    # Save alarm names for composite alarm
    config = load_config()
    config["alarm_names"] = alarms_created
    with open("/tmp/monitoring_config.json", "w") as f:
        json.dump(config, f, indent=2)

    print(f"\n📊 Total alarms created: {len(alarms_created)}")
    return alarms_created


if __name__ == "__main__":
    create_all_pipeline_alarms()