# file: 73_05_platform_health_dashboard.py
# Comprehensive monitoring dashboard for the entire data lake platform
# Single pane of glass for platform team

import boto3
import json
from datetime import datetime, timedelta

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def create_platform_dashboard():
    """
    CloudWatch dashboard covering ALL platform components:
    → Ingestion health (Kinesis, Firehose, Glue JDBC)
    → Storage metrics (S3 size, Redshift storage)
    → Transform health (Glue job success/failure)
    → Query performance (Athena, Redshift)
    → Cost tracking
    → Data freshness SLAs
    """
    cw = boto3.client("cloudwatch", region_name=REGION)

    dashboard_body = {
        "widgets": [
            # ═══ HEADER ═══
            {
                "type": "text",
                "x": 0, "y": 0, "width": 24, "height": 2,
                "properties": {
                    "markdown": (
                        "# 🏗️ QuickCart Data Lake Platform — Health Dashboard\n"
                        "Unified monitoring: Ingestion → Storage → Transform → Query → Governance"
                    )
                }
            },

            # ═══ ROW 1: REAL-TIME INGESTION ═══
            {
                "type": "text",
                "x": 0, "y": 2, "width": 24, "height": 1,
                "properties": {"markdown": "## 🌊 Real-Time Ingestion (Kinesis → Firehose → S3)"}
            },
            {
                "type": "metric",
                "x": 0, "y": 3, "width": 8, "height": 6,
                "properties": {
                    "title": "Kinesis: Incoming Records/sec",
                    "metrics": [
                        ["AWS/Kinesis", "IncomingRecords",
                         "StreamName", "clickstream-events",
                         {"stat": "Sum", "period": 60}]
                    ],
                    "view": "timeSeries",
                    "region": REGION,
                    "yAxis": {"left": {"min": 0}}
                }
            },
            {
                "type": "metric",
                "x": 8, "y": 3, "width": 8, "height": 6,
                "properties": {
                    "title": "Kinesis: Iterator Age (Consumer Lag)",
                    "metrics": [
                        ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds",
                         "StreamName", "clickstream-events",
                         {"stat": "Maximum", "period": 60}]
                    ],
                    "view": "timeSeries",
                    "region": REGION,
                    "annotations": {
                        "horizontal": [
                            {"label": "⚠️ 5 min lag", "value": 300000, "color": "#ff9900"},
                            {"label": "🚨 10 min lag", "value": 600000, "color": "#d13212"}
                        ]
                    }
                }
            },
            {
                "type": "metric",
                "x": 16, "y": 3, "width": 8, "height": 6,
                "properties": {
                    "title": "Firehose: Delivery to S3",
                    "metrics": [
                        ["AWS/Firehose", "DeliveryToS3.Records",
                         "DeliveryStreamName", "clickstream-delivery",
                         {"stat": "Sum", "period": 300}],
                        ["AWS/Firehose", "DeliveryToS3.DataFreshness",
                         "DeliveryStreamName", "clickstream-delivery",
                         {"stat": "Average", "period": 300, "yAxis": "right"}]
                    ],
                    "view": "timeSeries",
                    "region": REGION
                }
            },

            # ═══ ROW 2: BATCH ETL ═══
            {
                "type": "text",
                "x": 0, "y": 9, "width": 24, "height": 1,
                "properties": {"markdown": "## ⚙️ Batch ETL (Glue Jobs)"}
            },
            {
                "type": "metric",
                "x": 0, "y": 10, "width": 12, "height": 6,
                "properties": {
                    "title": "Glue Job Success vs Failure (24h)",
                    "metrics": [
                        ["Glue", "glue.driver.aggregate.numCompletedStages",
                         {"stat": "Sum", "period": 3600}],
                        ["Glue", "glue.driver.aggregate.numFailedStages",
                         {"stat": "Sum", "period": 3600}]
                    ],
                    "view": "bar",
                    "region": REGION
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 10, "width": 12, "height": 6,
                "properties": {
                    "title": "Step Functions: Pipeline Executions",
                    "metrics": [
                        ["AWS/States", "ExecutionsSucceeded",
                         "StateMachineArn",
                         f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:quickcart-master-nightly-pipeline",
                         {"stat": "Sum", "period": 86400}],
                        ["AWS/States", "ExecutionsFailed",
                         "StateMachineArn",
                         f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:quickcart-master-nightly-pipeline",
                         {"stat": "Sum", "period": 86400}]
                    ],
                    "view": "singleValue",
                    "region": REGION
                }
            },

            # ═══ ROW 3: QUERY PERFORMANCE ═══
            {
                "type": "text",
                "x": 0, "y": 16, "width": 24, "height": 1,
                "properties": {"markdown": "## 📊 Query Performance (Athena + Redshift)"}
            },
            {
                "type": "metric",
                "x": 0, "y": 17, "width": 12, "height": 6,
                "properties": {
                    "title": "Athena: Data Scanned (GB) — Cost Driver",
                    "metrics": [
                        ["AWS/Athena", "ProcessedBytes",
                         "WorkGroup", "mesh-analytics",
                         {"stat": "Sum", "period": 3600}]
                    ],
                    "view": "timeSeries",
                    "region": REGION
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 17, "width": 12, "height": 6,
                "properties": {
                    "title": "Redshift Serverless: RPU Utilization",
                    "metrics": [
                        ["AWS/Redshift-Serverless", "ComputeCapacity",
                         "Workgroup", "quickcart-analytics",
                         {"stat": "Average", "period": 300}]
                    ],
                    "view": "timeSeries",
                    "region": REGION
                }
            },

            # ═══ ROW 4: DATA FRESHNESS SLA ═══
            {
                "type": "text",
                "x": 0, "y": 23, "width": 24, "height": 1,
                "properties": {"markdown": "## ⏱️ Data Product Freshness (SLA Tracking)"}
            },
            {
                "type": "metric",
                "x": 0, "y": 24, "width": 8, "height": 6,
                "properties": {
                    "title": "Clickstream Micro-Batch Latency",
                    "metrics": [
                        ["QuickCart/DataMesh/Clickstream", "ProcessingDurationSeconds",
                         "Pipeline", "clickstream-microbatch",
                         {"stat": "p99", "period": 300}]
                    ],
                    "view": "gauge",
                    "region": REGION,
                    "yAxis": {"left": {"min": 0, "max": 900}},
                    "annotations": {
                        "horizontal": [
                            {"label": "SLA: 15 min", "value": 900, "color": "#d13212"},
                            {"label": "Target: 10 min", "value": 600, "color": "#ff9900"}
                        ]
                    }
                }
            },
            {
                "type": "metric",
                "x": 8, "y": 24, "width": 8, "height": 6,
                "properties": {
                    "title": "Nightly Batch: Duration (minutes)",
                    "metrics": [
                        ["AWS/States", "ExecutionTime",
                         "StateMachineArn",
                         f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:quickcart-master-nightly-pipeline",
                         {"stat": "Average", "period": 86400}]
                    ],
                    "view": "singleValue",
                    "region": REGION
                }
            },
            {
                "type": "metric",
                "x": 16, "y": 24, "width": 8, "height": 6,
                "properties": {
                    "title": "Data Mesh: Product Events Published",
                    "metrics": [
                        ["AWS/SNS", "NumberOfMessagesPublished",
                         "TopicName", "data-product-events",
                         {"stat": "Sum", "period": 86400}]
                    ],
                    "view": "singleValue",
                    "region": REGION
                }
            },

            # ═══ ROW 5: COST TRACKING ═══
            {
                "type": "text",
                "x": 0, "y": 30, "width": 24, "height": 1,
                "properties": {"markdown": "## 💰 Platform Cost Indicators"}
            },
            {
                "type": "metric",
                "x": 0, "y": 31, "width": 8, "height": 6,
                "properties": {
                    "title": "S3: Bucket Size (GB)",
                    "metrics": [
                        ["AWS/S3", "BucketSizeBytes",
                         "BucketName", "orders-data-prod",
                         "StorageType", "StandardStorage",
                         {"stat": "Average", "period": 86400}]
                    ],
                    "view": "timeSeries",
                    "region": REGION
                }
            },
            {
                "type": "metric",
                "x": 8, "y": 31, "width": 8, "height": 6,
                "properties": {
                    "title": "Athena: Queries Per Day",
                    "metrics": [
                        ["AWS/Athena", "TotalExecutionTime",
                         "WorkGroup", "mesh-analytics",
                         {"stat": "SampleCount", "period": 86400}]
                    ],
                    "view": "timeSeries",
                    "region": REGION
                }
            },
            {
                "type": "metric",
                "x": 16, "y": 31, "width": 8, "height": 6,
                "properties": {
                    "title": "SQS: Consumer Queue Depth",
                    "metrics": [
                        ["AWS/SQS", "ApproximateNumberOfMessagesVisible",
                         "QueueName", "mesh-analytics-data-events",
                         {"stat": "Maximum", "period": 300}],
                        ["AWS/SQS", "ApproximateNumberOfMessagesVisible",
                         "QueueName", "mesh-analytics-data-events-dlq",
                         {"stat": "Maximum", "period": 300}]
                    ],
                    "view": "timeSeries",
                    "region": REGION,
                    "annotations": {
                        "horizontal": [
                            {"label": "DLQ alert", "value": 1, "color": "#d13212"}
                        ]
                    }
                }
            }
        ]
    }

    cw.put_dashboard(
        DashboardName="DataLakePlatform-Health",
        DashboardBody=json.dumps(dashboard_body)
    )
    print("✅ Platform health dashboard created: DataLakePlatform-Health")
    print("   → View at: https://console.aws.amazon.com/cloudwatch/home"
          f"?region={REGION}#dashboards:name=DataLakePlatform-Health")


def create_platform_alarms():
    """
    Critical alarms for the entire platform
    Each alarm → SNS → PagerDuty/Slack (Project 54/78 pattern)
    """
    cw = boto3.client("cloudwatch", region_name=REGION)
    alert_topic = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-alerts"

    alarms = [
        {
            "AlarmName": "platform-nightly-pipeline-failed",
            "AlarmDescription": "Master nightly pipeline failed — Gold layer NOT updated",
            "Namespace": "AWS/States",
            "MetricName": "ExecutionsFailed",
            "Dimensions": [{
                "Name": "StateMachineArn",
                "Value": f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:quickcart-master-nightly-pipeline"
            }],
            "Statistic": "Sum",
            "Period": 86400,
            "EvaluationPeriods": 1,
            "Threshold": 1,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": "notBreaching"
        },
        {
            "AlarmName": "platform-kinesis-consumer-lag",
            "AlarmDescription": "Kinesis consumer falling behind — data loss risk",
            "Namespace": "AWS/Kinesis",
            "MetricName": "GetRecords.IteratorAgeMilliseconds",
            "Dimensions": [{"Name": "StreamName", "Value": "clickstream-events"}],
            "Statistic": "Maximum",
            "Period": 60,
            "EvaluationPeriods": 5,
            "Threshold": 300000,  # 5 minutes
            "ComparisonOperator": "GreaterThanThreshold",
            "TreatMissingData": "breaching"
        },
        {
            "AlarmName": "platform-glue-job-failures",
            "AlarmDescription": "Multiple Glue job failures in past hour",
            "Namespace": "Glue",
            "MetricName": "glue.driver.aggregate.numFailedStages",
            "Statistic": "Sum",
            "Period": 3600,
            "EvaluationPeriods": 1,
            "Threshold": 3,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": "notBreaching"
        },
        {
            "AlarmName": "platform-dlq-messages",
            "AlarmDescription": "Messages in DLQ — consumer processing failures",
            "Namespace": "AWS/SQS",
            "MetricName": "ApproximateNumberOfMessagesVisible",
            "Dimensions": [{"Name": "QueueName", "Value": "mesh-analytics-data-events-dlq"}],
            "Statistic": "Maximum",
            "Period": 300,
            "EvaluationPeriods": 1,
            "Threshold": 1,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": "notBreaching"
        },
        {
            "AlarmName": "platform-athena-cost-spike",
            "AlarmDescription": "Athena scanning excessive data — possible full table scan",
            "Namespace": "AWS/Athena",
            "MetricName": "ProcessedBytes",
            "Dimensions": [{"Name": "WorkGroup", "Value": "mesh-analytics"}],
            "Statistic": "Sum",
            "Period": 3600,
            "EvaluationPeriods": 1,
            "Threshold": 100 * 1024 * 1024 * 1024,  # 100 GB in one hour
            "ComparisonOperator": "GreaterThanThreshold",
            "TreatMissingData": "notBreaching"
        }
    ]

    print("\n🚨 Creating platform alarms:")
    for alarm_config in alarms:
        alarm_config["AlarmActions"] = [alert_topic]
        alarm_config["OKActions"] = [alert_topic]
        alarm_config["Tags"] = [
            {"Key": "Platform", "Value": "data-lake"},
            {"Key": "Criticality", "Value": "P1"}
        ]

        cw.put_metric_alarm(**alarm_config)
        print(f"   ✅ {alarm_config['AlarmName']}")


def main():
    print("=" * 70)
    print("📊 PLATFORM HEALTH DASHBOARD & ALARMS")
    print("   Single pane of glass for entire data lake platform")
    print("=" * 70)

    create_platform_dashboard()
    create_platform_alarms()

    print("\n" + "=" * 70)
    print("✅ MONITORING SETUP COMPLETE")
    print("   Dashboard: DataLakePlatform-Health")
    print("   Alarms: 5 critical platform alarms → SNS alerts")
    print("=" * 70)


if __name__ == "__main__":
    main()