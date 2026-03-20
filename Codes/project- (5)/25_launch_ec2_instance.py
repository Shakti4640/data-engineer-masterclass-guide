# file: 25_launch_ec2_instance.py
# Purpose: Launch Ubuntu EC2 instance with user data for auto-setup
# DEPENDS ON: Steps 1 (key pair) and 2 (security group)

import boto3
import time
from botocore.exceptions import ClientError

REGION = "us-east-2"
KEY_NAME = "quickcart-data-team-key"
SG_NAME = "quickcart-data-worker-sg"
INSTANCE_NAME = "quickcart-data-worker"
INSTANCE_TYPE = "t3.small"


def get_latest_ubuntu_ami(ec2_client):
    """
    Find the latest Ubuntu 22.04 LTS AMI for the region
    
    WHY NOT HARDCODE AMI ID:
    → AMI IDs differ per region
    → New AMIs released monthly with security patches
    → Always use latest for security
    """
    response = ec2_client.describe_images(
        Owners=["099720109477"],  # Canonical (Ubuntu publisher)
        Filters=[
            {"Name": "name", "Values": ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]},
            {"Name": "state", "Values": ["available"]},
            {"Name": "architecture", "Values": ["x86_64"]},
        ]
    )

    # Sort by creation date — latest first
    images = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)

    if not images:
        print("❌ No Ubuntu 22.04 AMI found")
        return None

    ami = images[0]
    print(f"   Latest Ubuntu 22.04 AMI: {ami['ImageId']}")
    print(f"   Name: {ami['Name']}")
    print(f"   Created: {ami['CreationDate']}")
    return ami["ImageId"]


def get_security_group_id(ec2_client):
    """Find security group by name"""
    response = ec2_client.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [SG_NAME]}]
    )
    if response["SecurityGroups"]:
        return response["SecurityGroups"][0]["GroupId"]
    print(f"❌ Security group '{SG_NAME}' not found — run Step 2 first")
    return None


def build_user_data_script():
    """
    Shell script that runs on FIRST BOOT as root
    
    Installs everything needed for data engineering:
    → System updates
    → Python 3 + pip
    → AWS CLI v2
    → boto3, pymysql, pandas, psycopg2 (Redshift)
    → Creates project directory structure
    → Configures timezone
    """
    return """#!/bin/bash
set -e  # Exit on any error
exec > /var/log/user-data.log 2>&1  # Log everything

echo "========================================="
echo "QuickCart Data Worker Setup — $(date)"
echo "========================================="

# --- SYSTEM UPDATES ---
echo ">>> Updating system packages..."
apt-get update -y
apt-get upgrade -y

# --- PYTHON SETUP ---
echo ">>> Installing Python and pip..."
apt-get install -y python3 python3-pip python3-venv

# --- ESSENTIAL SYSTEM PACKAGES ---
echo ">>> Installing system dependencies..."
apt-get install -y \\
    curl \\
    unzip \\
    jq \\
    mysql-client \\
    postgresql-client \\
    htop \\
    tree

# --- AWS CLI v2 ---
echo ">>> Installing AWS CLI v2..."
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
unzip -q /tmp/awscliv2.zip -d /tmp/
/tmp/aws/install
rm -rf /tmp/aws /tmp/awscliv2.zip

# --- PYTHON PACKAGES (system-wide for simplicity) ---
echo ">>> Installing Python packages..."
pip3 install --no-cache-dir \\
    boto3 \\
    pymysql \\
    pandas \\
    psycopg2-binary \\
    requests \\
    python-dateutil

# --- PROJECT DIRECTORY ---
echo ">>> Creating project structure..."
mkdir -p /home/ubuntu/quickcart/{scripts,logs,data/exports,data/downloads,config}
chown -R ubuntu:ubuntu /home/ubuntu/quickcart

# --- TIMEZONE ---
echo ">>> Setting timezone to UTC..."
timedlink /usr/share/zoneinfo/UTC /etc/localtime 2>/dev/null || true
echo "UTC" > /etc/timezone

# --- AUTOMATIC SECURITY UPDATES ---
echo ">>> Configuring automatic security updates..."
apt-get install -y unattended-upgrades
dpkg-reconfigure -plow unattended-upgrades

# --- SWAP (for t3.small with 2GB RAM) ---
echo ">>> Creating 2GB swap file..."
fallocate -l 2G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

# --- VERIFY ---
echo ""
echo "========================================="
echo "SETUP COMPLETE — $(date)"
echo "========================================="
echo "Python: $(python3 --version)"
echo "Pip: $(pip3 --version)"
echo "AWS CLI: $(aws --version)"
echo "boto3: $(python3 -c 'import boto3; print(boto3.__version__)')"
echo "Swap: $(swapon --show)"
echo "Disk: $(df -h /)"
echo "========================================="
"""


def launch_instance():
    ec2_client = boto3.client("ec2", region_name=REGION)

    # --- CHECK IF INSTANCE ALREADY RUNNING ---
    existing = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [INSTANCE_NAME]},
            {"Name": "instance-state-name", "Values": ["running", "pending", "stopped"]}
        ]
    )

    for reservation in existing["Reservations"]:
        for instance in reservation["Instances"]:
            instance_id = instance["InstanceId"]
            state = instance["State"]["Name"]
            print(f"ℹ️  Instance '{INSTANCE_NAME}' already exists: {instance_id} ({state})")
            if state == "stopped":
                print("   Starting stopped instance...")
                ec2_client.start_instances(InstanceIds=[instance_id])
            return instance_id

    # --- GET AMI AND SG ---
    ami_id = get_latest_ubuntu_ami(ec2_client)
    sg_id = get_security_group_id(ec2_client)

    if not ami_id or not sg_id:
        return None

    # --- LAUNCH ---
    print(f"\n🚀 Launching EC2 instance...")
    print(f"   Name: {INSTANCE_NAME}")
    print(f"   Type: {INSTANCE_TYPE}")
    print(f"   AMI: {ami_id}")
    print(f"   Key: {KEY_NAME}")
    print(f"   SG: {sg_id}")

    response = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=INSTANCE_TYPE,
        KeyName=KEY_NAME,
        SecurityGroupIds=[sg_id],
        MinCount=1,
        MaxCount=1,

        # --- BLOCK DEVICE (root volume) ---
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "VolumeSize": 20,          # 20 GB (default is 8)
                    "VolumeType": "gp3",       # Latest generation, cheaper
                    "DeleteOnTermination": True, # Clean up when instance terminated
                    "Encrypted": True           # Encrypt root volume
                }
            }
        ],

        # --- USER DATA (setup script) ---
        UserData=build_user_data_script(),

        # --- METADATA SERVICE ---
        MetadataOptions={
            "HttpTokens": "required",          # IMDSv2 only (secure)
            "HttpPutResponseHopLimit": 1,
            "HttpEndpoint": "enabled"
        },

        # --- TAGS ---
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": INSTANCE_NAME},
                    {"Key": "Environment", "Value": "Production"},
                    {"Key": "Team", "Value": "Data-Engineering"},
                    {"Key": "Project", "Value": "QuickCart"},
                    {"Key": "CostCenter", "Value": "ENG-001"},
                    {"Key": "ManagedBy", "Value": "boto3-script"}
                ]
            },
            {
                "ResourceType": "volume",
                "Tags": [
                    {"Key": "Name", "Value": f"{INSTANCE_NAME}-root"},
                    {"Key": "Team", "Value": "Data-Engineering"}
                ]
            }
        ]
    )

    instance = response["Instances"][0]
    instance_id = instance["InstanceId"]
    print(f"\n   ✅ Instance launched: {instance_id}")
    print(f"   State: {instance['State']['Name']}")

    # --- WAIT FOR RUNNING ---
    print("\n   ⏳ Waiting for instance to reach 'running' state...")
    waiter = ec2_client.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id])
    print("   ✅ Instance is running")

    # --- WAIT FOR STATUS CHECKS ---
    print("   ⏳ Waiting for status checks to pass...")
    waiter = ec2_client.get_waiter("instance_status_ok")
    waiter.wait(InstanceIds=[instance_id])
    print("   ✅ Status checks passed (2/2)")

    # --- GET PUBLIC IP ---
    desc = ec2_client.describe_instances(InstanceIds=[instance_id])
    instance_info = desc["Reservations"][0]["Instances"][0]
    public_ip = instance_info.get("PublicIpAddress", "N/A")
    private_ip = instance_info.get("PrivateIpAddress", "N/A")
    az = instance_info["Placement"]["AvailabilityZone"]

    print(f"\n{'=' * 60}")
    print(f"🖥️  INSTANCE READY")
    print(f"{'=' * 60}")
    print(f"   Instance ID:  {instance_id}")
    print(f"   Public IP:    {public_ip}")
    print(f"   Private IP:   {private_ip}")
    print(f"   AZ:           {az}")
    print(f"   Type:         {INSTANCE_TYPE}")
    print(f"   Key:          {KEY_NAME}")
    print(f"\n   📋 SSH COMMAND:")
    print(f"   ssh -i ~/.ssh/{KEY_NAME}.pem ubuntu@{public_ip}")
    print(f"\n   ⏳ User data script may take 3-5 minutes to complete")
    print(f"   Check progress: ssh in → tail -f /var/log/user-data.log")
    print(f"{'=' * 60}")

    return instance_id


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 LAUNCHING EC2 INSTANCE")
    print("=" * 60)
    launch_instance()