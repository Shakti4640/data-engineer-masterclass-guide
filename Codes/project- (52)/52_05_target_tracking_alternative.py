# file: 52_05_target_tracking_alternative.py
# Purpose: Demonstrate Target Tracking as simpler alternative to Step Scaling

import boto3
import json
import time
import math

REGION = "us-east-2"


def load_config():
    with open("/tmp/asg_config.json", "r") as f:
        return json.load(f)


def publish_custom_metric_for_target_tracking():
    """
    Target Tracking needs a CUSTOM metric:
    → BacklogPerInstance = MessagesVisible / InServiceInstances
    
    WHY CUSTOM:
    → SQS publishes ApproximateNumberOfMessagesVisible (total backlog)
    → ASG publishes GroupInServiceInstances (instance count)
    → But Target Tracking needs: backlog PER instance
    → Must calculate and publish ourselves
    
    IN PRODUCTION: run this as a Lambda on 1-minute schedule
    """
    config = load_config()
    sqs_client = boto3.client("sqs", region_name=REGION)
    asg_client = boto3.client("autoscaling", region_name=REGION)
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    print("=" * 70)
    print("📊 PUBLISHING BacklogPerInstance CUSTOM METRIC")
    print("=" * 70)

    # Get queue depth
    queue_attrs = sqs_client.get_queue_attributes(
        QueueUrl=config["queue_url"],
        AttributeNames=["ApproximateNumberOfMessagesVisible"]
    )
    visible = int(queue_attrs["Attributes"]["ApproximateNumberOfMessagesVisible"])

    # Get instance count
    asg_response = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[config["asg_name"]]
    )
    asg = asg_response["AutoScalingGroups"][0]
    in_service = len([
        i for i in asg["Instances"]
        if i["LifecycleState"] == "InService"
    ])

    # Calculate backlog per instance
    if in_service > 0:
        backlog_per_instance = visible / in_service
    else:
        backlog_per_instance = visible  # No instances = entire backlog

    # Publish custom metric
    cw_client.put_metric_data(
        Namespace="QuickCart/SQSWorkers",
        MetricData=[
            {
                "MetricName": "BacklogPerInstance",
                "Value": backlog_per_instance,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "QueueName", "Value": config["queue_name"]},
                    {"Name": "ASGName", "Value": config["asg_name"]}
                ]
            }
        ]
    )

    print(f"  Queue depth:           {visible:,}")
    print(f"  In-service instances:  {in_service}")
    print(f"  Backlog per instance:  {backlog_per_instance:.0f}")
    print(f"  ✅ Metric published to CloudWatch")

    return backlog_per_instance


def create_target_tracking_policy():
    """
    Create Target Tracking policy:
    → "Keep BacklogPerInstance at 500"
    
    HOW IT WORKS:
    → If BacklogPerInstance > 500 → ASG adds instances
    → If BacklogPerInstance < 500 → ASG removes instances
    → ASG auto-creates and manages alarms internally
    → ASG calculates: needed_instances = total_messages / 500
    
    EXAMPLE:
    → 5,000 messages, target 500/instance → need 10 instances
    → 500 messages, target 500/instance → need 1 instance
    → 50,000 messages, target 500/instance → need 100 (capped at MaxSize)
    
    COMPARISON WITH STEP SCALING:
    ┌────────────────────┬─────────────────────┬──────────────────────────┐
    │ Aspect             │ Step Scaling         │ Target Tracking          │
    ├────────────────────┼─────────────────────┼──────────────────────────┤
    │ Configuration      │ Complex (alarms +   │ Simple (one target value)│
    │                    │ steps + policies)    │                          │
    ├────────────────────┼─────────────────────┼──────────────────────────┤
    │ Control            │ Fine-grained         │ AWS manages details      │
    ├────────────────────┼─────────────────────┼──────────────────────────┤
    │ Alarms             │ You create manually  │ ASG creates auto         │
    ├────────────────────┼─────────────────────┼──────────────────────────┤
    │ Scale-in cooldown  │ You configure        │ Default 300s (editable)  │
    ├────────────────────┼─────────────────────┼──────────────────────────┤
    │ Custom metric      │ Not required         │ Required for SQS-based   │
    ├────────────────────┼─────────────────────┼──────────────────────────┤
    │ Best for           │ Known thresholds     │ Proportional scaling     │
    └────────────────────┴─────────────────────┴──────────────────────────┘
    """
    config = load_config()
    asg_client = boto3.client("autoscaling", region_name=REGION)

    print("\n" + "=" * 70)
    print("🎯 CREATING TARGET TRACKING SCALING POLICY")
    print("=" * 70)

    TARGET_VALUE = 500  # messages per instance

    response = asg_client.put_scaling_policy(
        AutoScalingGroupName=config["asg_name"],
        PolicyName="ETL-Queue-TargetTracking-BacklogPerInstance",
        PolicyType="TargetTrackingScaling",
        TargetTrackingConfiguration={
            "CustomizedMetricSpecification": {
                "MetricName": "BacklogPerInstance",
                "Namespace": "QuickCart/SQSWorkers",
                "Dimensions": [
                    {"Name": "QueueName", "Value": config["queue_name"]},
                    {"Name": "ASGName", "Value": config["asg_name"]}
                ],
                "Statistic": "Average"
            },
            "TargetValue": TARGET_VALUE,
            "ScaleOutCooldown": 60,     # Wait 1 min between scale-out actions
            "ScaleInCooldown": 600,     # Wait 10 min before scale-in
            "DisableScaleIn": False     # Allow scale-in (set True to only scale out)
        }
    )

    print(f"  Target: {TARGET_VALUE} messages per instance")
    print(f"  Scale-out cooldown: 60 seconds")
    print(f"  Scale-in cooldown: 600 seconds")
    print(f"  Policy ARN: {response['PolicyARN']}")

    # Show expected behavior
    print(f"\n  EXPECTED BEHAVIOR:")
    test_depths = [0, 500, 1000, 2500, 5000, 10000, 25000, 50000]
    for depth in test_depths:
        needed = max(1, math.ceil(depth / TARGET_VALUE))
        capped = min(needed, 20)  # MaxSize = 20
        print(f"    {depth:>8,} msgs → need {needed:>3} instances → actual {capped:>3} (max=20)")

    print("=" * 70)

    # IMPORTANT NOTE
    print("\n⚠️  NOTE: Target Tracking and Step Scaling can COEXIST on same ASG")
    print("   → Both policies evaluate independently")
    print("   → ASG takes the action that results in MORE instances (scale-out)")
    print("   → ASG takes the action that results in FEWER instances (scale-in)")
    print("   → In practice: use ONE or the OTHER, not both")
    print("   → This demo shows both for COMPARISON purposes")


if __name__ == "__main__":
    # Publish custom metric (run this on a schedule in production)
    publish_custom_metric_for_target_tracking()

    # Create target tracking policy
    create_target_tracking_policy()