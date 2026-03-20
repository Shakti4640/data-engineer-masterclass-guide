# file: 52_06_inspect_scaling_activity.py
# Purpose: View all ASG scaling decisions and their reasons

import boto3
import json
from datetime import datetime, timedelta

REGION = "us-east-2"


def load_config():
    with open("/tmp/asg_config.json", "r") as f:
        return json.load(f)


def inspect_asg_state():
    """Show current ASG state: instances, capacity, health"""
    config = load_config()
    asg_client = boto3.client("autoscaling", region_name=REGION)

    response = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[config["asg_name"]]
    )

    if not response["AutoScalingGroups"]:
        print(f"❌ ASG not found: {config['asg_name']}")
        return

    asg = response["AutoScalingGroups"][0]

    print("=" * 70)
    print(f"📊 ASG STATE: {config['asg_name']}")
    print("=" * 70)

    print(f"\n  Capacity:")
    print(f"    Min:     {asg['MinSize']}")
    print(f"    Max:     {asg['MaxSize']}")
    print(f"    Desired: {asg['DesiredCapacity']}")

    print(f"\n  Instances ({len(asg['Instances'])}):")
    if asg["Instances"]:
        print(f"    {'Instance ID':<22} {'AZ':<18} {'State':<16} {'Health':<10}")
        print(f"    {'-' * 60}")
        for inst in asg["Instances"]:
            print(
                f"    {inst['InstanceId']:<22} "
                f"{inst['AvailabilityZone']:<18} "
                f"{inst['LifecycleState']:<16} "
                f"{inst['HealthStatus']:<10}"
            )
    else:
        print(f"    (no instances)")

    # Scaling policies
    policies = asg_client.describe_policies(
        AutoScalingGroupName=config["asg_name"]
    )
    print(f"\n  Scaling Policies ({len(policies['ScalingPolicies'])}):")
    for policy in policies["ScalingPolicies"]:
        print(f"    → {policy['PolicyName']} ({policy['PolicyType']})")
        if policy.get("StepAdjustments"):
            for step in policy["StepAdjustments"]:
                lower = step.get("MetricIntervalLowerBound", "-∞")
                upper = step.get("MetricIntervalUpperBound", "+∞")
                adj = step["ScalingAdjustment"]
                sign = "+" if adj > 0 else ""
                print(f"      [{lower} → {upper}]: {sign}{adj} instances")

    return asg


def inspect_scaling_activities(max_activities=20):
    """
    View recent scaling activities — THE most important debugging tool
    
    Each activity shows:
    → What happened (launch/terminate)
    → Why it happened (alarm, manual, health check)
    → When it happened
    → Whether it succeeded or failed
    """
    config = load_config()
    asg_client = boto3.client("autoscaling", region_name=REGION)

    response = asg_client.describe_scaling_activities(
        AutoScalingGroupName=config["asg_name"],
        MaxRecords=max_activities
    )

    activities = response.get("Activities", [])

    print(f"\n{'=' * 70}")
    print(f"📜 SCALING ACTIVITY HISTORY (last {max_activities})")
    print(f"{'=' * 70}")

    if not activities:
        print("  (no scaling activities)")
        return

    for activity in activities:
        timestamp = activity["StartTime"].strftime("%Y-%m-%d %H:%M:%S")
        status = activity["StatusCode"]
        description = activity.get("Description", "N/A")
        cause = activity.get("Cause", "N/A")

        # Status icon
        if status == "Successful":
            icon = "✅"
        elif status == "Failed":
            icon = "❌"
        elif status in ["WaitingForSpotInstanceId", "PreInService", "InProgress"]:
            icon = "⏳"
        else:
            icon = "❓"

        print(f"\n  {icon} {timestamp} — {status}")
        print(f"     Description: {description}")

        # Truncate long cause strings
        if len(cause) > 200:
            cause = cause[:200] + "..."
        print(f"     Cause: {cause}")

    print(f"\n{'=' * 70}")


def inspect_alarm_state():
    """Check state of scaling alarms"""
    config = load_config()
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    alarm_names = [
        f"{config['asg_name']}-Queue-High",
        f"{config['asg_name']}-Queue-Low"
    ]

    response = cw_client.describe_alarms(AlarmNames=alarm_names)

    print(f"\n{'=' * 70}")
    print(f"🔔 SCALING ALARM STATE")
    print(f"{'=' * 70}")

    for alarm in response.get("MetricAlarms", []):
        state = alarm["StateValue"]
        if state == "ALARM":
            icon = "🔴"
        elif state == "OK":
            icon = "🟢"
        else:
            icon = "⚪"

        print(f"\n  {icon} {alarm['AlarmName']}")
        print(f"     State: {state}")
        print(f"     Metric: {alarm['MetricName']}")
        print(f"     Threshold: {alarm['ComparisonOperator']} {alarm['Threshold']}")
        print(f"     Last updated: {alarm.get('StateUpdatedTimestamp', 'N/A')}")
        print(f"     Reason: {alarm.get('StateReason', 'N/A')[:150]}")

    print(f"\n{'=' * 70}")


if __name__ == "__main__":
    inspect_asg_state()
    inspect_scaling_activities()
    inspect_alarm_state()