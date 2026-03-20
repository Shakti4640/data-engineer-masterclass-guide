# file: 66_07_pipeline_monitoring.py
# Run from: Account B
# Purpose: Create CloudWatch dashboard for event-driven pipeline visibility

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def create_pipeline_dashboard():
    """
    CloudWatch dashboard showing:
    1. EventBridge rule invocations (events received)
    2. Step Function executions (started, succeeded, failed)
    3. Glue job durations (per source)
    4. Pipeline state (DynamoDB — sources completed per batch)
    5. End-to-end latency (file arrival → gold layer ready)
    """
    cw = boto3.client("cloudwatch", region_name=REGION)

    dashboard_body = {
        "widgets": [
            {
                "type": "text",
                "x": 0, "y": 0, "width": 24, "height": 1,
                "properties": {
                    "markdown": "# 🚀 Event-Driven Data Pipeline Dashboard"
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 1, "width": 12, "height": 6,
                "properties": {
                    "title": "EventBridge Rule Invocations",
                    "metrics": [
                        ["AWS/Events", "Invocations", "RuleName", "partner-file-arrival", {"label": "Partner"}],
                        ["AWS/Events", "Invocations", "RuleName", "payment-file-arrival", {"label": "Payment"}],
                        ["AWS/Events", "Invocations", "RuleName", "marketing-file-arrival", {"label": "Marketing"}],
                        ["AWS/Events", "Invocations", "RuleName", "scheduled-cdc-trigger", {"label": "CDC"}],
                        ["AWS/Events", "Invocations", "RuleName", "scheduled-inventory-trigger", {"label": "Inventory"}],
                        ["AWS/Events", "Invocations", "RuleName", "glue-job-completion", {"label": "Glue Complete"}]
                    ],
                    "period": 3600,
                    "stat": "Sum",
                    "region": REGION,
                    "view": "timeSeries"
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 1, "width": 12, "height": 6,
                "properties": {
                    "title": "EventBridge Failed Invocations",
                    "metrics": [
                        ["AWS/Events", "FailedInvocations", "RuleName", "partner-file-arrival"],
                        ["AWS/Events", "FailedInvocations", "RuleName", "payment-file-arrival"],
                        ["AWS/Events", "FailedInvocations", "RuleName", "glue-job-completion"]
                    ],
                    "period": 3600,
                    "stat": "Sum",
                    "region": REGION,
                    "view": "timeSeries"
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 7, "width": 12, "height": 6,
                "properties": {
                    "title": "Step Function Executions",
                    "metrics": [
                        ["AWS/States", "ExecutionsStarted", "StateMachineArn",
                         f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:event-driven-data-pipeline",
                         {"label": "Started", "color": "#2ca02c"}],
                        ["AWS/States", "ExecutionsSucceeded", "StateMachineArn",
                         f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:event-driven-data-pipeline",
                         {"label": "Succeeded", "color": "#1f77b4"}],
                        ["AWS/States", "ExecutionsFailed", "StateMachineArn",
                         f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:event-driven-data-pipeline",
                         {"label": "Failed", "color": "#d62728"}],
                        ["AWS/States", "ExecutionsTimedOut", "StateMachineArn",
                         f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:event-driven-data-pipeline",
                         {"label": "Timed Out", "color": "#ff7f0e"}]
                    ],
                    "period": 3600,
                    "stat": "Sum",
                    "region": REGION
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 7, "width": 12, "height": 6,
                "properties": {
                    "title": "Step Function Execution Duration (seconds)",
                    "metrics": [
                        ["AWS/States", "ExecutionTime", "StateMachineArn",
                         f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:event-driven-data-pipeline",
                         {"stat": "Average", "label": "Avg Duration"}],
                        ["AWS/States", "ExecutionTime", "StateMachineArn",
                         f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:event-driven-data-pipeline",
                         {"stat": "Maximum", "label": "Max Duration"}]
                    ],
                    "period": 86400,
                    "region": REGION
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 13, "width": 24, "height": 6,
                "properties": {
                    "title": "Glue ETL Job Durations (minutes)",
                    "metrics": [
                        ["AWS/Glue", "glue.driver.aggregate.elapsedTime", "JobName", "cdc-mariadb-to-redshift", "JobRunId", "ALL", "Type", "gauge", {"label": "CDC", "stat": "Maximum"}],
                        ["AWS/Glue", "glue.driver.aggregate.elapsedTime", "JobName", "etl-partner-feed", "JobRunId", "ALL", "Type", "gauge", {"label": "Partner", "stat": "Maximum"}],
                        ["AWS/Glue", "glue.driver.aggregate.elapsedTime", "JobName", "etl-payment-data", "JobRunId", "ALL", "Type", "gauge", {"label": "Payment", "stat": "Maximum"}],
                        ["AWS/Glue", "glue.driver.aggregate.elapsedTime", "JobName", "build-gold-aggregations", "JobRunId", "ALL", "Type", "gauge", {"label": "Gold Build", "stat": "Maximum"}]
                    ],
                    "period": 86400,
                    "region": REGION,
                    "view": "timeSeries"
                }
            }
        ]
    }

    cw.put_dashboard(
        DashboardName="EventDrivenPipeline",
        DashboardBody=json.dumps(dashboard_body)
    )
    print("✅ CloudWatch Dashboard created: EventDrivenPipeline")
    print(f"   URL: https://{REGION}.console.aws.amazon.com/cloudwatch/home?region={REGION}#dashboards:name=EventDrivenPipeline")


def create_pipeline_alarms():
    """
    CloudWatch alarms for pipeline health
    """
    cw = boto3.client("cloudwatch", region_name=REGION)
    sns_topic = f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts"

    alarms = [
        {
            "name": "pipeline-step-function-failures",
            "description": "Step Function execution failed",
            "namespace": "AWS/States",
            "metric": "ExecutionsFailed",
            "dimensions": [
                {"Name": "StateMachineArn",
                 "Value": f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:event-driven-data-pipeline"}
            ],
            "threshold": 1,
            "comparison": "GreaterThanOrEqualToThreshold",
            "period": 3600,
            "eval_periods": 1
        },
        {
            "name": "pipeline-eventbridge-failures",
            "description": "EventBridge failed to invoke Step Function",
            "namespace": "AWS/Events",
            "metric": "FailedInvocations",
            "dimensions": [
                {"Name": "RuleName", "Value": "partner-file-arrival"}
            ],
            "threshold": 1,
            "comparison": "GreaterThanOrEqualToThreshold",
            "period": 3600,
            "eval_periods": 1
        },
        {
            "name": "pipeline-no-events-24h",
            "description": "No pipeline events received in 24 hours",
            "namespace": "AWS/States",
            "metric": "ExecutionsStarted",
            "dimensions": [
                {"Name": "StateMachineArn",
                 "Value": f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:event-driven-data-pipeline"}
            ],
            "threshold": 1,
            "comparison": "LessThanThreshold",
            "period": 86400,
            "eval_periods": 1
        }
    ]

    for alarm in alarms:
        cw.put_metric_alarm(
            AlarmName=alarm["name"],
            AlarmDescription=alarm["description"],
            Namespace=alarm["namespace"],
            MetricName=alarm["metric"],
            Dimensions=alarm["dimensions"],
            Statistic="Sum",
            Period=alarm["period"],
            EvaluationPeriods=alarm["eval_periods"],
            Threshold=alarm["threshold"],
            ComparisonOperator=alarm["comparison"],
            AlarmActions=[sns_topic],
            TreatMissingData="notBreaching"
        )
        print(f"✅ Alarm created: {alarm['name']}")


if __name__ == "__main__":
    print("=" * 60)
    print("📊 CREATING PIPELINE MONITORING")
    print("=" * 60)

    create_pipeline_dashboard()
    print()
    create_pipeline_alarms()