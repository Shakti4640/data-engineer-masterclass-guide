# file: 68_05_quality_dashboard.py
# Run from: Account B (222222222222)
# Purpose: CloudWatch dashboard + alarms for data quality monitoring

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
SNS_TOPIC = f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts"


def create_quality_dashboard():
    """
    CloudWatch dashboard showing data quality across all sources
    """
    cw = boto3.client("cloudwatch", region_name=REGION)

    sources = ["partner_feed", "payment_data", "marketing_campaign", "cdc_mariadb_orders"]

    # Build pass rate widgets per source
    pass_rate_metrics = []
    bad_row_metrics = []
    for source in sources:
        pass_rate_metrics.append([
            "QuickCart/DataQuality", "PassRate",
            "SourceName", source,
            {"stat": "Average", "period": 86400, "label": source}
        ])
        bad_row_metrics.append([
            "QuickCart/DataQuality", "BadRows",
            "SourceName", source,
            {"stat": "Sum", "period": 86400, "label": source}
        ])

    dashboard_body = {
        "widgets": [
            {
                "type": "text",
                "x": 0, "y": 0, "width": 24, "height": 1,
                "properties": {
                    "markdown": "# 🔍 Data Quality Dashboard — All Sources"
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 1, "width": 12, "height": 6,
                "properties": {
                    "title": "Quality Pass Rate by Source (%)",
                    "metrics": pass_rate_metrics,
                    "region": REGION,
                    "view": "timeSeries",
                    "yAxis": {"left": {"min": 0, "max": 100}},
                    "annotations": {
                        "horizontal": [
                            {"label": "SLA: 95%", "value": 95, "color": "#ff0000"}
                        ]
                    }
                }
            },
            {
                "type": "metric",
                "x": 12, "y": 1, "width": 12, "height": 6,
                "properties": {
                    "title": "Bad Rows Quarantined by Source",
                    "metrics": bad_row_metrics,
                    "region": REGION,
                    "view": "bar"
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 7, "width": 24, "height": 6,
                "properties": {
                    "title": "Quality Rules Failed (daily count)",
                    "metrics": [
                        ["QuickCart/DataQuality", "RulesFailed",
                         "SourceName", source,
                         {"stat": "Sum", "period": 86400, "label": source}]
                        for source in sources
                    ],
                    "region": REGION,
                    "view": "timeSeries"
                }
            },
            {
                "type": "metric",
                "x": 0, "y": 13, "width": 12, "height": 3,
                "properties": {
                    "title": "Today's Quality Summary",
                    "metrics": [
                        ["QuickCart/DataQuality", "GoodRows",
                         "SourceName", "partner_feed",
                         {"stat": "Sum", "period": 86400, "label": "Partner Good"}],
                        ["QuickCart/DataQuality", "BadRows",
                         "SourceName", "partner_feed",
                         {"stat": "Sum", "period": 86400, "label": "Partner Bad"}]
                    ],
                    "region": REGION,
                    "view": "singleValue"
                }
            }
        ]
    }

    cw.put_dashboard(
        DashboardName="DataQuality",
        DashboardBody=json.dumps(dashboard_body)
    )
    print(f"✅ Dashboard created: DataQuality")
    print(f"   URL: https://{REGION}.console.aws.amazon.com/cloudwatch/home#dashboards:name=DataQuality")


def create_quality_alarms():
    """
    Alarms that fire when quality degrades
    """
    cw = boto3.client("cloudwatch", region_name=REGION)

    sources = ["partner_feed", "payment_data", "marketing_campaign"]

    for source in sources:
        # Alarm 1: Pass rate drops below 95%
        cw.put_metric_alarm(
            AlarmName=f"dq-low-pass-rate-{source}",
            AlarmDescription=f"Data quality pass rate for {source} dropped below 95%",
            Namespace="QuickCart/DataQuality",
            MetricName="PassRate",
            Dimensions=[{"Name": "SourceName", "Value": source}],
            Statistic="Average",
            Period=86400,
            EvaluationPeriods=1,
            Threshold=95.0,
            ComparisonOperator="LessThanThreshold",
            AlarmActions=[SNS_TOPIC],
            TreatMissingData="notBreaching"
        )
        print(f"✅ Alarm: dq-low-pass-rate-{source} (threshold: < 95%)")

        # Alarm 2: Bad rows exceed threshold
        cw.put_metric_alarm(
            AlarmName=f"dq-high-bad-rows-{source}",
            AlarmDescription=f"Too many bad rows quarantined for {source}",
            Namespace="QuickCart/DataQuality",
            MetricName="BadRows",
            Dimensions=[{"Name": "SourceName", "Value": source}],
            Statistic="Sum",
            Period=86400,
            EvaluationPeriods=1,
            Threshold=1000,
            ComparisonOperator="GreaterThanThreshold",
            AlarmActions=[SNS_TOPIC],
            TreatMissingData="notBreaching"
        )
        print(f"✅ Alarm: dq-high-bad-rows-{source} (threshold: > 1000 rows)")

    # Alarm 3: Any rules failed (aggregate across sources)
    cw.put_metric_alarm(
        AlarmName="dq-any-rules-failed",
        AlarmDescription="Data quality rules failed for any source",
        Namespace="QuickCart/DataQuality",
        MetricName="RulesFailed",
        Statistic="Sum",
        Period=3600,
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[SNS_TOPIC],
        TreatMissingData="notBreaching"
    )
    print(f"✅ Alarm: dq-any-rules-failed (any rule failure triggers)")


if __name__ == "__main__":
    print("=" * 60)
    print("📊 DEPLOYING DATA QUALITY MONITORING")
    print("=" * 60)
    create_quality_dashboard()
    print()
    create_quality_alarms()