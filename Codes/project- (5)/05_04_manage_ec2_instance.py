# file: 26_manage_ec2_instance.py
# Purpose: Start, stop, reboot, describe, terminate EC2 instances
# DEPENDS ON: Step 3 (instance exists)

import boto3
import sys
from botocore.exceptions import ClientError

REGION = "us-east-2"
INSTANCE_NAME = "quickcart-data-worker"


def find_instance_by_name(ec2_client, name):
    """Find instance ID by Name tag"""
    response = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [name]},
            {"Name": "instance-state-name",
             "Values": ["running", "pending", "stopped", "stopping"]}
        ]
    )

    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            return instance["InstanceId"], instance["State"]["Name"]

    return None, None


def describe_instance(ec2_client, instance_id):
    """Show detailed instance information"""
    response = ec2_client.describe_instances(InstanceIds=[instance_id])
    inst = response["Reservations"][0]["Instances"][0]

    print(f"\n📋 INSTANCE DETAILS: {instance_id}")
    print("-" * 55)
    print(f"   State:          {inst['State']['Name']}")
    print(f"   Type:           {inst['InstanceType']}")
    print(f"   AZ:             {inst['Placement']['AvailabilityZone']}")
    print(f"   Public IP:      {inst.get('PublicIpAddress', 'N/A')}")
    print(f"   Private IP:     {inst.get('PrivateIpAddress', 'N/A')}")
    print(f"   Launch Time:    {inst['LaunchTime']}")
    print(f"   AMI:            {inst['ImageId']}")
    print(f"   Key Pair:       {inst.get('KeyName', 'N/A')}")

    # Security groups
    sgs = [sg["GroupName"] for sg in inst.get("SecurityGroups", [])]
    print(f"   Security Groups: {', '.join(sgs)}")

    # IAM Role (will be None until Project 6)
    iam_profile = inst.get("IamInstanceProfile", {})
    print(f"   IAM Profile:    {iam_profile.get('Arn', 'NONE — see Project 6')}")

    # Root volume
    for bdm in inst.get("BlockDeviceMappings", []):
        vol_id = bdm["Ebs"]["VolumeId"]
        print(f"   Root Volume:    {vol_id}")

    # Tags
    print(f"   Tags:")
    for tag in inst.get("Tags", []):
        print(f"     {tag['Key']}: {tag['Value']}")

    return inst


def stop_instance(ec2_client, instance_id):
    """
    Stop instance — preserves EBS data, releases public IP
    
    WHAT HAPPENS:
    → OS receives shutdown signal
    → Instance moves to 'stopping' then 'stopped'
    → Compute billing STOPS
    → EBS billing CONTINUES ($0.08/GB/month for gp3)
    → Public IP RELEASED (gets new one on start)
    → Private IP RETAINED
    → RAM contents LOST
    → Instance may move to different physical host on start
    """
    print(f"\n⏹️  Stopping instance: {instance_id}")
    ec2_client.stop_instances(InstanceIds=[instance_id])

    waiter = ec2_client.get_waiter("instance_stopped")
    print("   ⏳ Waiting for instance to stop...")
    waiter.wait(InstanceIds=[instance_id])
    print("   ✅ Instance stopped")
    print("   → Compute billing stopped")
    print("   → EBS billing continues")
    print("   → Public IP released")


def start_instance(ec2_client, instance_id):
    """Start a stopped instance"""
    print(f"\n▶️  Starting instance: {instance_id}")
    ec2_client.start_instances(InstanceIds=[instance_id])

    waiter = ec2_client.get_waiter("instance_running")
    print("   ⏳ Waiting for instance to start...")
    waiter.wait(InstanceIds=[instance_id])
    print("   ✅ Instance running")

    # Get new public IP
    response = ec2_client.describe_instances(InstanceIds=[instance_id])
    inst = response["Reservations"][0]["Instances"][0]
    public_ip = inst.get("PublicIpAddress", "N/A")
    print(f"   New Public IP: {public_ip}")
    print(f"   SSH: ssh -i ~/.ssh/quickcart-data-team-key.pem ubuntu@{public_ip}")


def reboot_instance(ec2_client, instance_id):
    """
    Reboot — does NOT release public IP or change host
    Equivalent to pressing reset button
    """
    print(f"\n🔄 Rebooting instance: {instance_id}")
    ec2_client.reboot_instances(InstanceIds=[instance_id])
    print("   ✅ Reboot initiated")
    print("   → Instance stays on same host")
    print("   → Public IP preserved")
    print("   → Takes ~30-60 seconds")


def terminate_instance(ec2_client, instance_id):
    """
    PERMANENTLY delete instance and root volume
    
    ⚠️ IRREVERSIBLE:
    → Instance deleted
    → Root EBS volume deleted (if DeleteOnTermination=True)
    → All data on instance LOST
    → Cannot be recovered
    """
    print(f"\n💀 TERMINATING instance: {instance_id}")
    print("   ⚠️  This is IRREVERSIBLE — all data will be lost")

    confirm = input("   Type 'TERMINATE' to confirm: ")
    if confirm != "TERMINATE":
        print("   ❌ Aborted")
        return

    ec2_client.terminate_instances(InstanceIds=[instance_id])
    print("   ✅ Termination initiated")
    print("   → Instance will move to 'terminated' state")
    print("   → Root volume will be deleted")
    print("   → Instance record visible for ~1 hour, then gone")


def get_instance_cost_estimate(instance_type="t3.small"):
    """Estimate monthly cost"""
    # On-demand pricing (us-east-2, approximate)
    pricing = {
        "t3.micro":  0.0104,
        "t3.small":  0.0208,
        "t3.medium": 0.0416,
        "t3.large":  0.0832,
        "m5.large":  0.0960,
    }

    hourly = pricing.get(instance_type, 0.0208)
    monthly_24x7 = hourly * 24 * 30
    monthly_12x7 = hourly * 12 * 30
    ebs_20gb = 20 * 0.08  # gp3 pricing

    print(f"\n💰 COST ESTIMATE: {instance_type}")
    print("-" * 40)
    print(f"   Hourly:        ${hourly:.4f}")
    print(f"   24/7 monthly:  ${monthly_24x7:.2f}")
    print(f"   12h/day:       ${monthly_12x7:.2f}")
    print(f"   EBS (20GB gp3): ${ebs_20gb:.2f}/month")
# file: 26_manage_ec2_instance.py (continued)

    print(f"   Total 24/7:    ${monthly_24x7 + ebs_20gb:.2f}/month")
    print(f"   Total 12h/day: ${monthly_12x7 + ebs_20gb:.2f}/month")
    print(f"   Stopped:       ${ebs_20gb:.2f}/month (EBS only)")


if __name__ == "__main__":
    ec2_client = boto3.client("ec2", region_name=REGION)

    print("=" * 60)
    print("🖥️  EC2 INSTANCE MANAGEMENT")
    print("=" * 60)

    # Find instance
    instance_id, state = find_instance_by_name(ec2_client, INSTANCE_NAME)

    if not instance_id:
        print(f"❌ No instance found with name '{INSTANCE_NAME}'")
        print("   → Run 25_launch_ec2_instance.py first")
        sys.exit(1)

    print(f"   Found: {instance_id} ({state})")

    # Show details
    describe_instance(ec2_client, instance_id)
    get_instance_cost_estimate(INSTANCE_TYPE)

    # Menu
    print(f"\n📋 AVAILABLE ACTIONS:")
    print(f"   1. Describe (already shown above)")
    print(f"   2. Stop instance")
    print(f"   3. Start instance")
    print(f"   4. Reboot instance")
    print(f"   5. Terminate instance (DANGEROUS)")
    print(f"   6. Exit")

    choice = input("\n   Enter choice (1-6): ").strip()

    if choice == "2":
        stop_instance(ec2_client, instance_id)
    elif choice == "3":
        start_instance(ec2_client, instance_id)
    elif choice == "4":
        reboot_instance(ec2_client, instance_id)
    elif choice == "5":
        terminate_instance(ec2_client, instance_id)
    elif choice == "6":
        print("   👋 Exiting")
    else:
        print("   ℹ️  No action taken")