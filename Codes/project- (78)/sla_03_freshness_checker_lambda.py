# file: sla/03_freshness_checker_lambda.py
# Purpose: Lambda that checks data freshness and publishes CloudWatch metrics
# Triggered by EventBridge schedule every 15 minutes

import boto3
import json
import logging
from datetime import datetime, timezone, timedelta
from pipeline_registry import PIPELINE_REGISTRY

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-2"

# S3 prefixes where gold-layer data lives
GOLD_LAYER_PATHS = {
    "finance_summary": {
        "bucket": "quickcart-gold-prod",
        "prefix": "finance_summary/"
    },
    "customer_segments": {
        "bucket": "quickcart-gold-prod",
        "prefix": "customer_segments/"
    }
}


def get_latest_file_timestamp(s3_client, bucket, prefix):
    """
    Find the most recent file modification time in an S3 prefix
    
    WHY NOT USE GLUE CATALOG:
    → Catalog may not be updated if crawler hasn't run
    → S3 file timestamp is ground truth
    → Direct check avoids dependency on another service
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    latest_time = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            modified = obj["LastModified"]
            if latest_time is None or modified > latest_time:
                latest_time = modified

    return latest_time


def check_pipeline_freshness(s3_client, cloudwatch, pipeline_name, config):
    """
    Check data freshness for a single pipeline
    Publish staleness metric to CloudWatch
    """
    path_config = GOLD_LAYER_PATHS.get(pipeline_name)
    if not path_config:
        logger.warning(f"No S3 path configured for {pipeline_name}")
        return None

    latest_timestamp = get_latest_file_timestamp(
        s3_client,
        path_config["bucket"],
        path_config["prefix"]
    )

    now = datetime.now(timezone.utc)

    if latest_timestamp is None:
        # No files found — data is infinitely stale
        staleness_minutes = 99999
        logger.error(
            f"Pipeline {pipeline_name}: NO FILES FOUND in "
            f"s3://{path_config['bucket']}/{path_config['prefix']}"
        )
    else:
        delta = now - latest_timestamp
        staleness_minutes = int(delta.total_seconds() / 60)
        logger.info(
            f"Pipeline {pipeline_name}: latest data = "
            f"{latest_timestamp.isoformat()}, "
            f"staleness = {staleness_minutes} min"
        )

    # Publish to CloudWatch
    cloudwatch.put_metric_data(
        Namespace="QuickCart/DataPlatform",
        MetricData=[
            {
                "MetricName": "DataStalenessMinutes",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": pipeline_name},
                    {"Name": "Layer", "Value": "gold"}
                ],
                "Timestamp": now,
                "Value": staleness_minutes,
                "Unit": "Count"
            }
        ]
    )

    # Also publish binary healthy/unhealthy metric
    max_staleness = config["max_staleness_minutes"]
    is_healthy = 1 if staleness_minutes <= max_staleness else 0

    cloudwatch.put_metric_data(
        Namespace="QuickCart/DataPlatform",
        MetricData=[
            {
                "MetricName": "PipelineHealthy",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": pipeline_name}
                ],
                "Timestamp": now,
                "Value": is_healthy,
                "Unit": "Count"
            }
        ]
    )

    return {
        "pipeline": pipeline_name,
        "staleness_minutes": staleness_minutes,
        "threshold_minutes": max_staleness,
        "is_healthy": is_healthy,
        "latest_data_time": (
            latest_timestamp.isoformat() if latest_timestamp else None
        )
    }


def lambda_handler(event, context):
    """
    Entry point — check freshness for all registered pipelines
    
    EventBridge rule: rate(15 minutes)
    """
    s3_client = boto3.client("s3", region_name=REGION)
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    results = []

    for pipeline_name, config in PIPELINE_REGISTRY.items():
        result = check_pipeline_freshness(
            s3_client, cloudwatch, pipeline_name, config
        )
        if result:
            results.append(result)

    # Log summary
    unhealthy = [r for r in results if not r["is_healthy"]]
    logger.info(
        f"Freshness check complete: "
        f"{len(results)} pipelines checked, "
        f"{len(unhealthy)} unhealthy"
    )

    if unhealthy:
        for u in unhealthy:
            logger.warning(
                f"  🔴 {u['pipeline']}: {u['staleness_minutes']}min "
                f"(threshold: {u['threshold_minutes']}min)"
            )

    return {"statusCode": 200, "results": results}