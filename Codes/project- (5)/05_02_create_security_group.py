# file: 24_create_security_group.py
# Purpose: Create security group allowing SSH from specific IPs only
# DEPENDS ON: Default VPC must exist in the region

import boto3
import urllib.request
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"
SG_NAME = "quickcart-data-worker-sg"
SG_DESCRIPTION = "Security group for QuickCart data engineering EC2 instances"


def get_my_public_ip():
    """Get current machine's public IP for SSH rule"""
    try:
        response = urllib.request.urlopen("https://api.ipify.org?format=json", timeout=5)
        data = json.loads(response.read())
        return data["ip"]
    except Exception:
        print("⚠️  Could not detect public IP — using 0.0.0.0/0 (NOT recommended)")
        return None


def get_default_vpc_id(ec2_client):
    """Get the default VPC ID for the region"""
    response = ec2_client.describe_vpcs(
        Filters=[{"Name": "isDefault", "Values": ["true"]}]
    )

    if response["Vpcs"]:
        vpc_id = response["Vpcs"][0]["VpcId"]
        print(f"   Default VPC: {vpc_id}")
        return vpc_id
    else:
        print("❌ No default VPC found — create one or specify VPC ID")
        return None


def create_security_group():
    ec2_client = boto3.client("ec2", region_name=REGION)

    # --- GET DEFAULT VPC ---
    vpc_id = get_default_vpc_id(ec2_client)
    if not vpc_id:
        return None

    # --- CHECK IF SG ALREADY EXISTS ---
    try:
        response = ec2_client.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [SG_NAME]},
                {"Name": "vpc-id", "Values": [vpc_id]}
            ]
        )
        if response["SecurityGroups"]:
            sg_id = response["SecurityGroups"][0]["GroupId"]
            print(f"ℹ️  Security group already exists: {sg_id}")
            return sg_id
    except ClientError:
        pass

    # --- CREATE SECURITY GROUP ---
    print(f"🛡️  Creating security group: {SG_NAME}")

    response = ec2_client.create_security_group(
        GroupName=SG_NAME,
        Description=SG_DESCRIPTION,
        VpcId=vpc_id,
        TagSpecifications=[
            {
                "ResourceType": "security-group",
                "Tags": [
                    {"Key": "Name", "Value": SG_NAME},
                    {"Key": "Environment", "Value": "Production"},
                    {"Key": "Team", "Value": "Data-Engineering"},
                ]
            }
        ]
    )

    sg_id = response["GroupId"]
    print(f"   ✅ Created: {sg_id}")

    # --- ADD INBOUND RULES ---
    my_ip = get_my_public_ip()

    inbound_rules = []

    if my_ip:
        # SSH from current IP only
        inbound_rules.append({
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [
                {
                    "CidrIp": f"{my_ip}/32",
                    "Description": "SSH from data team current IP"
                }
            ]
        })
        print(f"   ✅ SSH allowed from: {my_ip}/32")
    else:
        # Fallback — restrict to /32 when IP is known
        inbound_rules.append({
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [
                {
                    "CidrIp": "0.0.0.0/0",
                    "Description": "SSH from anywhere — CHANGE THIS to your IP"
                }
            ]
        })
        print(f"   ⚠️  SSH allowed from: 0.0.0.0/0 — UPDATE THIS!")

    ec2_client.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=inbound_rules
    )

    # --- VERIFY ---
    print(f"\n📋 Security Group Rules for {sg_id}:")
    response = ec2_client.describe_security_groups(GroupIds=[sg_id])
    sg = response["SecurityGroups"][0]

    print(f"   INBOUND:")
    for rule in sg["IpPermissions"]:
        port = f"{rule.get('FromPort', 'ALL')}-{rule.get('ToPort', 'ALL')}"
        for ip_range in rule.get("IpRanges", []):
            print(f"     {rule['IpProtocol']:<6} port {port:<10} from {ip_range['CidrIp']:<20} "
                  f"({ip_range.get('Description', '')})")

    print(f"   OUTBOUND:")
    for rule in sg["IpPermissionsEgress"]:
        for ip_range in rule.get("IpRanges", []):
            print(f"     {rule['IpProtocol']:<6} ALL ports    to   {ip_range['CidrIp']:<20}")

    return sg_id


if __name__ == "__main__":
    print("=" * 60)
    print("🛡️  CREATING EC2 SECURITY GROUP")
    print("=" * 60)
    sg_id = create_security_group()
    if sg_id:
        print(f"\n✅ Security Group ID: {sg_id}")
        print(f"   Save this — needed for instance launch")