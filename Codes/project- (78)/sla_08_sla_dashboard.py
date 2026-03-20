# file: sla/08_sla_dashboard.py
# Purpose: Create CloudWatch Dashboard showing real-time SLA metrics

import boto3
import json

REGION = "us-east-2"


def create_sla_dashboard():
    """
    Create CloudWatch Dashboard with:
    → Row 1: Pipeline Health Status (green/red per pipeline)
    → Row 2: Data Freshness (staleness in minutes)
    → Row 3: Incident Count + MTTR Trend
    → Row 4: Auto-Recovery Success Rate
    → Row 5: Monthly Uptime Percentage
    """
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    dashboard_body = {
        "widgets": [
            # ═══════════════════════════════════════
            # ROW 1: Pipeline Health Status
            # ═══════════════════════════════════════
            {
                "type": "metric",
                "x": 0, "y": 0, "width": 12, "height": 3,
                "properties": {
                    "title": "🟢 Pipeline Health Status",
                    "metrics": [
                        [
                            "QuickCart/DataPlatform",
                            "PipelineHealthy",
                            "Pipeline", "finance_summary",
                            {"label": "Finance Summary", "stat": "Minimum"}
                        ],
                        [
                            "QuickCart/DataPlatform",
                            "PipelineHealthy",
                            "Pipeline", "customer_segments",
                            {"label": "Customer Segments", "stat": "Minimum"}
                        ]
                    ],
                    "view": "singleValue",
                    "period": 900,
                    "stat": "Minimum",
                    "region": REGION
                }
            },

            # Composite Alarm Status
            {
                "type": "alarm",
                "x": 12, "y": 0, "width": 12, "height": 3,
                "properties": {
                    "title": "🔔 Composite Alarm Status",
                    "alarms": [
                        f"arn:aws:cloudwatch:{REGION}:*:alarm:sla-finance_summary-composite-unhealthy",
                        f"arn:aws:cloudwatch:{REGION}:*:alarm:sla-customer_segments-composite-unhealthy"
                    ]
                }
            },

            # ═══════════════════════════════════════
            # ROW 2: Data Freshness
            # ═══════════════════════════════════════
            {
                "type": "metric",
                "x": 0, "y": 3, "width": 24, "height": 6,
                "properties": {
                    "title": "📊 Data Freshness (Staleness in Minutes)",
                    "metrics": [
                        [
                            "QuickCart/DataPlatform",
                            "DataStalenessMinutes",
                            "Pipeline", "finance_summary",
                            "Layer", "gold",
                            {"label": "Finance Summary (threshold: 120min)"}
                        ],
                        [
                            "QuickCart/DataPlatform",
                            "DataStalenessMinutes",
                            "Pipeline", "customer_segments",
                            "Layer", "gold",
                            {"label": "Customer Segments (threshold: 1440min)"}
                        ]
                    ],
                    "view": "timeSeries",
                    "period": 900,
                    "stat": "Maximum",
                    "region": REGION,
                    "yAxis": {"left": {"min": 0, "label": "Minutes"}},
                    "annotations": {
                        "horizontal": [
                            {
                                "label": "Finance Threshold (120min)",
                                "value": 120,
                                "color": "#d62728"
                            }
                        ]
                    }
                }
            },

            # ═══════════════════════════════════════
            # ROW 3: Incident Count + MTTR
            # ═══════════════════════════════════════
            {
                "type": "metric",
                "x": 0, "y": 9, "width": 12, "height": 6,
                "properties": {
                    "title": "🔴 Incidents (Last 30 Days)",
                    "metrics": [
                        [
                            "QuickCart/SLA",
                            "IncidentCount",
                            "Pipeline", "finance_summary",
                            {"label": "Finance Summary", "stat": "Sum"}
                        ],
                        [
                            "QuickCart/SLA",
                            "IncidentCount",
                            "Pipeline", "customer_segments",
                            {"label": "Customer Segments", "stat": "Sum"}
                        ]
                    ],
                    "view": "timeSeries",
                    "period": 86400,
                    "stat": "Sum",
                    "region": REGION
                }
            },

            {
                "type": "metric",
                "x": 12, "y": 9, "width": 12, "height": 6,
                "properties": {
                    "title": "⏱️ Mean Time To Recovery (Minutes)",
                    "metrics": [
                        [
                            "QuickCart/SLA",
                            "MTTRMinutes",
                            "Pipeline", "finance_summary",
                            {"label": "Finance MTTR", "stat": "Average"}
                        ],
                        [
                            "QuickCart/SLA",
                            "MTTRMinutes",
                            "Pipeline", "customer_segments",
                            {"label": "Segments MTTR", "stat": "Average"}
                        ]
                    ],
                    "view": "timeSeries",
                    "period": 86400,
                    "stat": "Average",
                    "region": REGION,
                    "yAxis": {"left": {"min": 0, "label": "Minutes"}},
                    "annotations": {
                        "horizontal": [
                            {
                                "label": "Target MTTR (10min)",
                                "value": 10,
                                "color": "#2ca02c"
                            }
                        ]
                    }
                }
            },

            # ═══════════════════════════════════════
            # ROW 4: Auto-Recovery Rate
            # ═══════════════════════════════════════
            {
                "type": "metric",
                "x": 0, "y": 15, "width": 12, "height": 6,
                "properties": {
                    "title": "🤖 Auto-Recovery Success Rate",
                    "metrics": [
                        [
                            "QuickCart/SLA",
                            "AutoRecoverySuccess",
                            "Pipeline", "finance_summary",
                            {"label": "Finance (1=auto, 0=manual)", "stat": "Average"}
                        ]
                    ],
                    "view": "timeSeries",
                    "period": 86400,
                    "stat": "Average",
                    "region": REGION,
                    "yAxis": {"left": {"min": 0, "max": 1}}
                }
            },

            # ═══════════════════════════════════════
            # ROW 5: Uptime Text Widget
            # ═══════════════════════════════════════
            {
                "type": "text",
                "x": 12, "y": 15, "width": 12, "height": 6,
                "properties": {
                    "markdown": (
                        "## 📋 SLA Targets\n\n"
                        "| Pipeline | Tier | SLA Target | Max Downtime/Month |\n"
                        "|---|---|---|---|\n"
                        "| Finance Summary | 1 | 99.9% | 43 min |\n"
                        "| Customer Segments | 2 | 99.0% | 7.3 hrs |\n\n"
                        "## 📞 Escalation Path\n\n"
                        "1. **T+0**: Slack #data-pipeline-status\n"
                        "2. **T+5**: Slack #data-oncall\n"
                        "3. **T+15**: PagerDuty → On-call engineer\n"
                        "4. **T+30**: PagerDuty → Engineering manager\n\n"
                        "## 🔗 Quick Links\n\n"
                        "- [Runbook Index](https://wiki.quickcart.com/runbooks)\n"
                        "- [Lineage CLI](https://wiki.quickcart.com/lineage)\n"
                        "- [Incident Tracker](https://console.aws.amazon.com/dynamodb)\n"
                    )
                }
            }
        ]
    }

    cloudwatch.put_dashboard(
        DashboardName="DataPlatform-SLA-Dashboard",
        DashboardBody=json.dumps(dashboard_body)
    )

    print("✅ CloudWatch Dashboard created: DataPlatform-SLA-Dashboard")
    print(
        f"   View: https://{REGION}.console.aws.amazon.com/"
        f"cloudwatch/home?region={REGION}#dashboards:"
        f"name=DataPlatform-SLA-Dashboard"
    )


if __name__ == "__main__":
    create_sla_dashboard()
