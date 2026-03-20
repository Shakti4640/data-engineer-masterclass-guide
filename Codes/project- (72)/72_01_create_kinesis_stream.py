# file: 72_01_create_kinesis_stream.py
# Account A (Orders Domain): Create Kinesis Data Stream for clickstream events

import boto3
import json
import time

# --- CONFIGURATION ---
REGION = "us-east-2"
STREAM_NAME = "clickstream-events"
SHARD_COUNT = 2  # Each shard: 1 MB/s in, 2 MB/s out, 1000 records/s in


def create_kinesis_stream(kinesis_client):
    """
    Create Kinesis Data Stream with provisioned capacity
    
    WHY PROVISIONED (not on-demand):
    → Clickstream traffic is predictable (peak during business hours)
    → Provisioned: $0.015/shard-hour
    → On-demand: $0.023/shard-hour (50% more)
    → At 2 shards: saves ~$5.76/month
    """
    try:
        kinesis_client.create_stream(
            StreamName=STREAM_NAME,
            ShardCount=SHARD_COUNT,
            StreamModeDetails={
                "StreamMode": "PROVISIONED"
            }
        )
        print(f"✅ Kinesis stream creating: {STREAM_NAME} ({SHARD_COUNT} shards)")
    except kinesis_client.exceptions.ResourceInUseException:
        print(f"ℹ️  Stream already exists: {STREAM_NAME}")

    # Wait for stream to become ACTIVE
    print("   Waiting for ACTIVE status...", end="", flush=True)
    waiter = kinesis_client.get_waiter("stream_exists")
    waiter.wait(StreamName=STREAM_NAME)
    print(" ✅")

    # Get stream details
    desc = kinesis_client.describe_stream(StreamName=STREAM_NAME)
    stream_arn = desc["StreamDescription"]["StreamARN"]
    status = desc["StreamDescription"]["StreamStatus"]
    shard_count = len(desc["StreamDescription"]["Shards"])

    print(f"   ARN: {stream_arn}")
    print(f"   Status: {status}")
    print(f"   Shards: {shard_count}")

    # Enable enhanced monitoring for detailed shard-level metrics
    kinesis_client.enable_enhanced_monitoring(
        StreamName=STREAM_NAME,
        ShardLevelMetrics=[
            "IncomingBytes",
            "IncomingRecords",
            "OutgoingBytes",
            "OutgoingRecords",
            "WriteProvisionedThroughputExceeded",
            "ReadProvisionedThroughputExceeded",
            "IteratorAgeMilliseconds"
        ]
    )
    print("   ✅ Enhanced shard-level monitoring enabled")

    # Tag for Data Mesh identification
    kinesis_client.add_tags_to_stream(
        StreamName=STREAM_NAME,
        Tags={
            "Domain": "orders",
            "Pipeline": "clickstream-realtime",
            "DataProduct": "clickstream_events",
            "Environment": "production",
            "MeshRole": "producer-ingestion"
        }
    )
    print("   ✅ Tags applied")

    return stream_arn


def setup_auto_scaling(app_autoscaling_client, stream_arn):
    """
    Configure auto-scaling for shard count
    
    Scale UP when: IncomingBytes > 70% of capacity (0.7 MB/s per shard)
    Scale DOWN when: IncomingBytes < 30% of capacity
    
    This handles Black Friday spikes automatically
    """
    resource_id = f"stream/{STREAM_NAME}"

    try:
        # Register scalable target
        app_autoscaling_client.register_scalable_target(
            ServiceNamespace="kinesis",
            ResourceId=resource_id,
            ScalableDimension="kinesis:stream:ShardCount",
            MinCapacity=2,      # Minimum 2 shards (always)
            MaxCapacity=30      # Maximum 30 shards (Black Friday)
        )
        print(f"✅ Auto-scaling registered: min=2, max=30 shards")

        # Scale-up policy
        app_autoscaling_client.put_scaling_policy(
            PolicyName="clickstream-scale-up",
            ServiceNamespace="kinesis",
            ResourceId=resource_id,
            ScalableDimension="kinesis:stream:ShardCount",
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": 70.0,  # 70% utilization target
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "KinesisStreamIncomingBytes"
                },
                "ScaleInCooldown": 600,   # Wait 10 min before scaling down
                "ScaleOutCooldown": 120   # Scale up quickly (2 min cooldown)
            }
        )
        print("✅ Auto-scaling policy: target 70% utilization")

    except Exception as e:
        print(f"⚠️  Auto-scaling setup: {e}")
        print("   (May require Application Auto Scaling permissions)")


def create_cloudwatch_alarms(cw_client):
    """
    Critical alarms for stream health
    
    Alarm 1: Iterator age > 5 minutes (consumer falling behind)
    Alarm 2: Write throttling (shard capacity exceeded)
    """
    # Alarm: Iterator Age (consumer lag)
    cw_client.put_metric_alarm(
        AlarmName="clickstream-iterator-age-high",
        AlarmDescription="Kinesis consumer is falling behind — records may expire",
        Namespace="AWS/Kinesis",
        MetricName="GetRecords.IteratorAgeMilliseconds",
        Dimensions=[
            {"Name": "StreamName", "Value": STREAM_NAME}
        ],
        Statistic="Maximum",
        Period=60,
        EvaluationPeriods=3,
        Threshold=300000,  # 5 minutes in milliseconds
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="breaching",
        Tags=[
            {"Key": "Pipeline", "Value": "clickstream-realtime"}
        ]
        # In production: add AlarmActions → SNS topic ARN (Project 54)
    )
    print("✅ Alarm: iterator-age-high (threshold: 5 min)")

    # Alarm: Write Throttling
    cw_client.put_metric_alarm(
        AlarmName="clickstream-write-throttled",
        AlarmDescription="Kinesis write throttled — need more shards",
        Namespace="AWS/Kinesis",
        MetricName="WriteProvisionedThroughputExceeded",
        Dimensions=[
            {"Name": "StreamName", "Value": STREAM_NAME}
        ],
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=2,
        Threshold=10,  # More than 10 throttled records in 1 minute
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",
        Tags=[
            {"Key": "Pipeline", "Value": "clickstream-realtime"}
        ]
    )
    print("✅ Alarm: write-throttled (threshold: 10/min)")


def main():
    print("=" * 70)
    print("🌊 CREATING KINESIS DATA STREAM: Clickstream Ingestion")
    print("=" * 70)

    kinesis_client = boto3.client("kinesis", region_name=REGION)
    autoscaling_client = boto3.client("application-autoscaling", region_name=REGION)
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    stream_arn = create_kinesis_stream(kinesis_client)
    setup_auto_scaling(autoscaling_client, stream_arn)
    create_cloudwatch_alarms(cw_client)

    print(f"\n✅ Stream ready to receive events: {STREAM_NAME}")


if __name__ == "__main__":
    main()