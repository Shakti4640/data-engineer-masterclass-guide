# file: 29_monitor_ec2.py
# Purpose: Quick monitoring dashboard for the EC2 instance
# DEPENDS ON: Steps 3-6 (instance running with scripts)

import boto3
from datetime import datetime, timedelta, timezone

REGION = "us-east-2"
INSTANCE_NAME = "quickcart-data-worker"


def find_instance_id(ec2_client, name):
    """Find instance ID by Name tag"""
    response = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [name]},
            {"Name": "instance-state-name",
             "Values": ["running", "stopped", "pending", "stopping"]}
        ]
    )
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            return instance["InstanceId"], instance["State"]["Name"]
    return None, None


def get_cpu_utilization(cw_client, instance_id, hours=6):
    """
    Get CPU utilization from CloudWatch
    
    FREE METRICS (every 5 minutes):
    → CPUUtilization
    → NetworkIn / NetworkOut
    → DiskReadOps / DiskWriteOps
    → StatusCheckFailed
    
    DETAILED MONITORING ($2.10/month, every 1 minute):
    → Same metrics but 1-minute resolution
    → Enable via: ec2_client.monitor_instances(InstanceIds=[id])
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)

    response = cw_client.get_metric_statistics(
        Namespace="AWS/EC2",
        MetricName="CPUUtilization",
        Dimensions=[
            {"Name": "InstanceId", "Value": instance_id}
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=300,  # 5-minute intervals
        Statistics=["Average", "Maximum"]
    )

    datapoints = sorted(response["Datapoints"], key=lambda x: x["Timestamp"])
    return datapoints


def get_network_metrics(cw_client, instance_id, hours=6):
    """Get network in/out metrics"""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)

    metrics = {}
    for metric_name in ["NetworkIn", "NetworkOut"]:
        response = cw_client.get_metric_statistics(
            Namespace="AWS/EC2",
            MetricName=metric_name,
            Dimensions=[
                {"Name": "InstanceId", "Value": instance_id}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=300,
            Statistics=["Sum"]
        )
        datapoints = sorted(response["Datapoints"], key=lambda x: x["Timestamp"])
        metrics[metric_name] = datapoints

    return metrics


def get_status_checks(ec2_client, instance_id):
    """Get instance status checks"""
    response = ec2_client.describe_instance_status(
        InstanceIds=[instance_id],
        IncludeAllInstances=True
    )

    if response["InstanceStatuses"]:
        status = response["InstanceStatuses"][0]
        return {
            "instance_state": status["InstanceState"]["Name"],
            "system_status": status["SystemStatus"]["Status"],
            "instance_status": status["InstanceStatus"]["Status"]
        }
    return None


def display_monitoring_dashboard():
    """Display a monitoring dashboard for the instance"""
    ec2_client = boto3.client("ec2", region_name=REGION)
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    instance_id, state = find_instance_id(ec2_client, INSTANCE_NAME)

    if not instance_id:
        print(f"❌ Instance '{INSTANCE_NAME}' not found")
        return

    print("=" * 70)
    print(f"📊 EC2 MONITORING DASHBOARD — {INSTANCE_NAME}")
    print(f"   Instance: {instance_id} | State: {state}")
    print(f"   Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 70)

    if state != "running":
        print(f"\n   ⚠️  Instance is {state} — limited metrics available")

    # --- STATUS CHECKS ---
    status = get_status_checks(ec2_client, instance_id)
    if status:
        print(f"\n🏥 STATUS CHECKS:")
        sys_icon = "✅" if status["system_status"] == "ok" else "❌"
        inst_icon = "✅" if status["instance_status"] == "ok" else "❌"
        print(f"   {sys_icon} System Status: {status['system_status']}")
        print(f"   {inst_icon} Instance Status: {status['instance_status']}")

    # --- CPU UTILIZATION ---
    print(f"\n📈 CPU UTILIZATION (last 6 hours):")
    cpu_data = get_cpu_utilization(cw_client, instance_id, hours=6)

    if cpu_data:
        print(f"   {'Time (UTC)':<20} {'Avg %':>8} {'Max %':>8} {'Graph'}")
        print("   " + "-" * 60)

        for dp in cpu_data[-12:]:  # Last 12 data points (1 hour)
            time_str = dp["Timestamp"].strftime("%H:%M")
            avg = dp["Average"]
            max_val = dp["Maximum"]
            bar = "█" * int(avg / 2) + "░" * (50 - int(avg / 2))
            print(f"   {time_str:<20} {avg:>7.1f}% {max_val:>7.1f}% {bar[:30]}")

        # Summary
        all_avg = [dp["Average"] for dp in cpu_data]
        all_max = [dp["Maximum"] for dp in cpu_data]
        print(f"\n   Average: {sum(all_avg) / len(all_avg):.1f}%")
        print(f"   Peak:    {max(all_max):.1f}%")

        if max(all_max) > 90:
            print("   ⚠️  HIGH CPU — consider upgrading instance type")
        elif max(all_max) < 10:
            print("   💡 LOW CPU — consider downgrading to save cost")
    else:
        print("   (no data yet — metrics appear after ~10 minutes)")

    # --- NETWORK ---
    print(f"\n🌐 NETWORK (last 6 hours):")
    network_data = get_network_metrics(cw_client, instance_id, hours=6)

    for direction, datapoints in network_data.items():
        if datapoints:
            total_bytes = sum(dp["Sum"] for dp in datapoints)
            total_mb = total_bytes / (1024 ** 2)
            label = "↓ Inbound" if "In" in direction else "↑ Outbound"
            print(f"   {label}: {total_mb:.1f} MB")
        else:
            print(f"   {direction}: (no data)")

    # --- COST ESTIMATE ---
    print(f"\n💰 COST (running since launch):")
    desc = ec2_client.describe_instances(InstanceIds=[instance_id])
    inst = desc["Reservations"][0]["Instances"][0]
    launch_time = inst["LaunchTime"]
    running_hours = (datetime.now(timezone.utc) - launch_time).total_seconds() / 3600

    hourly_rate = 0.0208  # t3.small
    compute_cost = running_hours * hourly_rate
    ebs_daily = 20 * 0.08 / 30  # 20GB gp3 per day
    ebs_cost = (running_hours / 24) * ebs_daily

    print(f"   Running since: {launch_time.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"   Hours running: {running_hours:.1f}")
    print(f"   Compute cost:  ${compute_cost:.2f}")
    print(f"   EBS cost:      ${ebs_cost:.2f}")
    print(f"   Total so far:  ${compute_cost + ebs_cost:.2f}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    display_monitoring_dashboard()