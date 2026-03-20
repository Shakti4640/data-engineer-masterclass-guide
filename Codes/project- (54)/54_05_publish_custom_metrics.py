# file: 54_05_publish_custom_metrics.py
# Purpose: Publish custom pipeline metrics that AWS services don't auto-publish

import boto3
import json
import time
from datetime import datetime

REGION = "us-east-2"


def publish_pipeline_metrics():
    """
    Publish custom metrics for pipeline components that don't
    auto-publish to CloudWatch
    
    IN PRODUCTION:
    → Call these functions from within your pipeline scripts
    → Upload script (Project 2) calls publish_upload_success()
    → Glue ETL job calls publish_etl_completion()
    → Scheduled Lambda calls publish_pipeline_latency()
    """
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    print("=" * 70)
    print("📊 PUBLISHING CUSTOM PIPELINE METRICS")
    print("=" * 70)

    timestamp = datetime.utcnow()

    # ═══════════════════════════════════════════════════
    # METRIC 1: Daily Upload Success
    # Called by upload script (Project 2) after successful S3 upload
    # ═══════════════════════════════════════════════════
    cw_client.put_metric_data(
        Namespace="QuickCart/Pipeline",
        MetricData=[
            {
                "MetricName": "DailyUploadSuccess",
                "Timestamp": timestamp,
                "Value": 1,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": "nightly-etl"},
                    {"Name": "Component", "Value": "S3Upload"}
                ]
            }
        ]
    )
    print(f"  ✅ DailyUploadSuccess = 1 (upload completed)")

    # ═══════════════════════════════════════════════════
    # METRIC 2: Upload File Count
    # How many files were uploaded (detect partial uploads)
    # ═══════════════════════════════════════════════════
    cw_client.put_metric_data(
        Namespace="QuickCart/Pipeline",
        MetricData=[
            {
                "MetricName": "DailyUploadFileCount",
                "Timestamp": timestamp,
                "Value": 3,     # orders + customers + products
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": "nightly-etl"},
                    {"Name": "Component", "Value": "S3Upload"}
                ]
            }
        ]
    )
    print(f"  ✅ DailyUploadFileCount = 3 (all 3 files uploaded)")

    # ═══════════════════════════════════════════════════
    # METRIC 3: Pipeline End-to-End Latency
    # Time from source file creation to Gold layer availability
    # ═══════════════════════════════════════════════════
    pipeline_start = datetime(2025, 1, 15, 0, 0, 0)    # Midnight: export starts
    pipeline_end = datetime(2025, 1, 15, 5, 30, 0)     # 5:30 AM: Gold ready
    latency_seconds = (pipeline_end - pipeline_start).total_seconds()

    cw_client.put_metric_data(
        Namespace="QuickCart/Pipeline",
        MetricData=[
            {
                "MetricName": "PipelineLatencySeconds",
                "Timestamp": timestamp,
                "Value": latency_seconds,
                "Unit": "Seconds",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": "nightly-etl"}
                ]
            }
        ]
    )
    print(f"  ✅ PipelineLatencySeconds = {latency_seconds:.0f}s ({latency_seconds/3600:.1f} hours)")

    # ═══════════════════════════════════════════════════
    # METRIC 4: Data Quality Score
    # Percentage of rows passing all quality checks
    # Published after DataBrew or Glue DQ runs (Project 68)
    # ═══════════════════════════════════════════════════
    cw_client.put_metric_data(
        Namespace="QuickCart/Pipeline",
        MetricData=[
            {
                "MetricName": "DataQualityScore",
                "Timestamp": timestamp,
                "Value": 94.5,      # 94.5% of rows pass all checks
                "Unit": "Percent",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": "nightly-etl"},
                    {"Name": "Table", "Value": "customers_silver"}
                ]
            }
        ]
    )
    print(f"  ✅ DataQualityScore = 94.5% (customers_silver)")

    # ═══════════════════════════════════════════════════
    # METRIC 5: Rows Processed
    # Track volume over time — detect anomalies
    # ═══════════════════════════════════════════════════
    cw_client.put_metric_data(
        Namespace="QuickCart/Pipeline",
        MetricData=[
            {
                "MetricName": "RowsProcessed",
                "Timestamp": timestamp,
                "Value": 245000,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": "nightly-etl"},
                    {"Name": "Table", "Value": "orders"}
                ]
            }
        ]
    )
    print(f"  ✅ RowsProcessed = 245,000 (orders table)")

    print(f"\n{'=' * 70}")
    print(f"📊 All custom metrics published to namespace: QuickCart/Pipeline")
    print(f"   → Available in CloudWatch console within 1-2 minutes")
    print(f"   → Alarms from Step 2 will evaluate these metrics")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    publish_pipeline_metrics()