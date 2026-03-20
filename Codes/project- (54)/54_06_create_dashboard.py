# file: 54_06_create_dashboard.py
# Purpose: Create comprehensive pipeline health dashboard

import boto3
import json

REGION = "us-east-2"
DASHBOARD_NAME = "QuickCart-Pipeline-Health"


def create_pipeline_dashboard():
    """
    Create CloudWatch Dashboard showing complete pipeline health
    
    DASHBOARD LAYOUT (4 columns):
    Row 1: Composite Alarm Status | Pipeline Latency | Data Quality | Rows Processed
    Row 2: Glue ETL Metrics | DataBrew Metrics | SQS Queue Depth | DLQ Depth
    Row 3: EC2 Worker Count | Worker Errors | Upload Status | Recent Errors (logs)
    
    WIDGET TYPES:
    → metric: line/bar graph of metric over time
    → alarm: colored status indicator (green/red/gray)
    → log: recent log events matching a query
    → text: markdown text for instructions/links
    """
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    dashboard_body = {
        "widgets": [
            # ═══════════════════════════════════════
            # ROW 1: HIGH-LEVEL HEALTH INDICATORS
            # ═══════════════════════════════════════

            # Widget 1: Composite Alarm Status
            {
                "type": "alarm",
                "x": 0, "y": 0, "width": 6, "height": 3,
                "properties": {
                    "title": "🚦 Pipeline Health",
                    "alarms": [
                        f"arn:aws:cloudwatch:{REGION}:{get_account_id()}:alarm:Pipeline-CRITICAL-DataFlowBroken",
                        f"arn:aws:cloudwatch:{REGION}:{get_account_id()}:alarm:Pipeline-WARNING-Degraded",
                        f"arn:aws:cloudwatch:{REGION}:{get_account_id()}:alarm:Pipeline-SLA-Breach"
                    ]
                }
            },

            # Widget 2: Pipeline Latency
            {
                "type": "metric",
                "x": 6, "y": 0, "width": 6, "height": 3,
                "properties": {
                    "title": "⏱️ Pipeline Latency (hours)",
                    "metrics": [
                        [
                            "QuickCart/Pipeline", "PipelineLatencySeconds",
                            "Pipeline", "nightly-etl",
                            {"stat": "Maximum", "period": 86400}
                        ]
                    ],
                    "view": "singleValue",
                    "region": REGION,
                    "period": 86400
                }
            },

            # Widget 3: Data Quality Score
            {
                "type": "metric",
                "x": 12, "y": 0, "width": 6, "height": 3,
                "properties": {
                    "title": "📊 Data Quality Score (%)",
                    "metrics": [
                        [
                            "QuickCart/Pipeline", "DataQualityScore",
                            "Pipeline", "nightly-etl",
                            "Table", "customers_silver",
                            {"stat": "Average", "period": 86400}
                        ]
                    ],
                    "view": "singleValue",
                    "region": REGION,
                    "yAxis": {"left": {"min": 0, "max": 100}}
                }
            },

            # Widget 4: Daily Rows Processed
            {
                "type": "metric",
                "x": 18, "y": 0, "width": 6, "height": 3,
                "properties": {
                    "title": "📈 Rows Processed (Daily)",
                    "metrics": [
                        [
                            "QuickCart/Pipeline", "RowsProcessed",
                            "Pipeline", "nightly-etl",
                            "Table", "orders",
                            {"stat": "Sum", "period": 86400}
                        ]
                    ],
                    "view": "singleValue",
                    "region": REGION
                }
            },

            # ═══════════════════════════════════════
            # ROW 2: COMPONENT METRICS
            # ═══════════════════════════════════════

            # Widget 5: All Component Alarms
            {
                "type": "alarm",
                "x": 0, "y": 3, "width": 12, "height": 3,
                "properties": {
                    "title": "🔔 Component Alarm Status",
                    "alarms": [
                        f"arn:aws:cloudwatch:{REGION}:{get_account_id()}:alarm:Pipeline-GlueETL-JobFailed",
                        f"arn:aws:cloudwatch:{REGION}:{get_account_id()}:alarm:Pipeline-DataBrew-JobFailed",
                        f"arn:aws:cloudwatch:{REGION}:{get_account_id()}:alarm:Pipeline-SQS-DLQ-NotEmpty",
                        f"arn:aws:cloudwatch:{REGION}:{get_account_id()}:alarm:Pipeline-S3Upload-NotReceived",
                        f"arn:aws:cloudwatch:{REGION}:{get_account_id()}:alarm:Pipeline-GlueCrawler-Failed",
                        f"arn:aws:cloudwatch:{REGION}:{get_account_id()}:alarm:Pipeline-Worker-HeartbeatMissing"
                    ]
                }
            },

            # Widget 6: SQS Queue Depths
            {
                "type": "metric",
                "x": 12, "y": 3, "width": 12, "height": 3,
                "properties": {
                    "title": "📬 SQS Queue Depths",
                    "metrics": [
                        [
                            "AWS/SQS", "ApproximateNumberOfMessagesVisible",
                            "QueueName", "quickcart-etl-queue",
                            {"label": "ETL Queue", "stat": "Maximum"}
                        ],
                        [
                            "AWS/SQS", "ApproximateNumberOfMessagesVisible",
                            "QueueName", "quickcart-etl-queue-dlq",
                            {"label": "ETL DLQ 🔴", "stat": "Maximum", "color": "#d62728"}
                        ]
                    ],
                    "view": "timeSeries",
                    "region": REGION,
                    "period": 300,
                    "yAxis": {"left": {"min": 0}}
                }
            },

            # ═══════════════════════════════════════
            # ROW 3: WORKER & ERROR DETAILS
            # ═══════════════════════════════════════

            # Widget 7: EC2 Worker Instance Count (from Project 52)
            {
                "type": "metric",
                "x": 0, "y": 6, "width": 8, "height": 4,
                "properties": {
                    "title": "🖥️ EC2 Worker Instances (ASG)",
                    "metrics": [
                        [
                            "AWS/AutoScaling", "GroupInServiceInstances",
                            "AutoScalingGroupName", "quickcart-etl-workers-asg",
                            {"label": "In Service", "stat": "Average", "color": "#2ca02c"}
                        ],
                        [
                            "AWS/AutoScaling", "GroupDesiredCapacity",
                            "AutoScalingGroupName", "quickcart-etl-workers-asg",
                            {"label": "Desired", "stat": "Average", "color": "#1f77b4"}
                        ]
                    ],
                    "view": "timeSeries",
                    "region": REGION,
                    "period": 60,
                    "yAxis": {"left": {"min": 0}}
                }
            },

            # Widget 8: Error Counts from Metric Filters
            {
                "type": "metric",
                "x": 8, "y": 6, "width": 8, "height": 4,
                "properties": {
                    "title": "❌ Error Counts (from logs)",
                    "metrics": [
                        [
                            "QuickCart/Pipeline", "GlueETLErrorCount",
                            {"label": "Glue ETL Errors", "stat": "Sum"}
                        ],
                        [
                            "QuickCart/Pipeline", "GlueETLOOMCount",
                            {"label": "Glue OOM Errors", "stat": "Sum", "color": "#d62728"}
                        ],
                        [
                            "QuickCart/Pipeline", "SQSWorkerProcessingErrors",
                            {"label": "Worker Errors", "stat": "Sum"}
                        ],
                        [
                            "QuickCart/Pipeline", "DataBrewStepFailures",
                            {"label": "DataBrew Failures", "stat": "Sum"}
                        ]
                    ],
                    "view": "timeSeries",
                    "region": REGION,
                    "period": 300,
                    "yAxis": {"left": {"min": 0}}
                }
            },

            # Widget 9: Runbook Links (text widget)
            {
                "type": "text",
                "x": 16, "y": 6, "width": 8, "height": 4,
                "properties": {
                    "markdown": """## 📋 Quick Links
                    
**Runbooks:**
- [Glue ETL Failure](https://wiki.quickcart.internal/runbooks/glue-etl-failure)
- [DataBrew Failure](https://wiki.quickcart.internal/runbooks/databrew-failure)
- [DLQ Messages](https://wiki.quickcart.internal/runbooks/dlq-messages)
- [S3 Upload Missing](https://wiki.quickcart.internal/runbooks/s3-upload-missing)

**On-Call:**
- Slack: #data-oncall
- PagerDuty: [Escalation Policy](https://quickcart.pagerduty.com)
- Schedule: [On-Call Rotation](https://wiki.quickcart.internal/oncall)
"""
                }
            }
        ]
    }

    # Create/update dashboard
    cw_client.put_dashboard(
        DashboardName=DASHBOARD_NAME,
        DashboardBody=json.dumps(dashboard_body)
    )

    dashboard_url = (
        f"https://{REGION}.console.aws.amazon.com/cloudwatch/home?"
        f"region={REGION}#dashboards:name={DASHBOARD_NAME}"
    )

    print("=" * 70)
    print(f"📊 DASHBOARD CREATED: {DASHBOARD_NAME}")
    print("=" * 70)
    print(f"  URL: {dashboard_url}")
    print(f"  Widgets: {len(dashboard_body['widgets'])}")
    print(f"  Auto-refresh: set in console (recommend 60 seconds)")
    print(f"")
    print(f"  SHARE THIS URL WITH:")
    print(f"  → CTO (bookmark for morning check)")
    print(f"  → On-call engineer (first thing to check on page)")
    print(f"  → Data team (ambient awareness)")
    print("=" * 70)


def get_account_id():
    """Get current AWS account ID for alarm ARN construction"""
    sts_client = boto3.client("sts")
    return sts_client.get_caller_identity()["Account"]


if __name__ == "__main__":
    create_pipeline_dashboard()