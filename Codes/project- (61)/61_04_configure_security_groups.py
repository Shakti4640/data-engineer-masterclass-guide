# file: 61_04_configure_security_groups.py
# Purpose: Allow cross-VPC traffic through security groups

import boto3

REGION = "us-east-2"


def configure_account_a_sg():
    """
    Account A: MariaDB Security Group
    → Must allow INBOUND on port 3306 from Account B's Glue subnet
    
    PRINCIPLE: Allow only the Glue subnet CIDR, not all of Account B
    → 10.2.6.0/24 (Glue ENI subnet) — not 10.2.0.0/16 (entire VPC)
    """
    ec2 = boto3.client("ec2", region_name=REGION)
    SG_MARIADB = "sg-0aaa1111mariadb"

    try:
        ec2.authorize_security_group_ingress(
            GroupId=SG_MARIADB,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 3306,
                    "ToPort": 3306,
                    "IpRanges": [
                        {
                            "CidrIp": "10.2.6.0/24",
                            "Description": "Glue ENIs from Account B Analytics"
                        }
                    ]
                }
            ]
        )
        print(f"✅ Account A SG: Allowed 10.2.6.0/24 → port 3306")
        print(f"   SG: {SG_MARIADB}")
    except Exception as e:
        if "InvalidPermission.Duplicate" in str(e):
            print("ℹ️  Rule already exists")
        else:
            raise


def create_glue_sg_account_b():
    """
    Account B: Create dedicated security group for Glue ENIs
    
    WHY DEDICATED SG:
    → Glue creates ENIs in your VPC
    → These ENIs need outbound to MariaDB (3306) and Redshift (5439)
    → Also need SELF-REFERENCING inbound for Glue worker communication
    → Self-referencing rule is CRITICAL — Glue workers talk to each other
    """
    ec2 = boto3.client("ec2", region_name=REGION)
    VPC_B = "vpc-0bbb2222bbbb2222b"

    # Create SG
    response = ec2.create_security_group(
        GroupName="sg-glue-eni-cross-account",
        Description="SG for Glue ENIs - cross-account MariaDB to Redshift ETL",
        VpcId=VPC_B,
        TagSpecifications=[
            {
                "ResourceType": "security-group",
                "Tags": [
                    {"Key": "Name", "Value": "sg-glue-eni-cross-account"},
                    {"Key": "ManagedBy", "Value": "DataEngineering"}
                ]
            }
        ]
    )
    sg_id = response["GroupId"]
    print(f"✅ Created Glue SG: {sg_id}")

    # --- SELF-REFERENCING RULE (CRITICAL FOR GLUE) ---
    # Glue workers communicate on ALL TCP ports with each other
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 0,
                "ToPort": 65535,
                "UserIdGroupPairs": [
                    {
                        "GroupId": sg_id,
                        "Description": "Glue worker-to-worker communication"
                    }
                ]
            }
        ]
    )
    print(f"✅ Self-referencing rule added (all TCP ports)")

    # --- OUTBOUND RULES ---
    # Default SG allows all outbound — but if restricted:
    # Need: 3306 to Account A MariaDB, 5439 to Redshift, 443 to S3
    # For explicit outbound (if default outbound removed):
    ec2.authorize_security_group_egress(
        GroupId=sg_id,
        IpPermissions=[
            {
                # MariaDB in Account A
                "IpProtocol": "tcp",
                "FromPort": 3306,
                "ToPort": 3306,
                "IpRanges": [
                    {
                        "CidrIp": "10.1.3.50/32",
                        "Description": "MariaDB in Account A"
                    }
                ]
            },
            {
                # Redshift in Account B
                "IpProtocol": "tcp",
                "FromPort": 5439,
                "ToPort": 5439,
                "IpRanges": [
                    {
                        "CidrIp": "10.2.5.0/24",
                        "Description": "Redshift subnet in Account B"
                    }
                ]
            },
            {
                # S3 VPC Endpoint (uses prefix list, but CIDR for simplicity)
                "IpProtocol": "tcp",
                "FromPort": 443,
                "ToPort": 443,
                "IpRanges": [
                    {
                        "CidrIp": "0.0.0.0/0",
                        "Description": "S3 VPC Endpoint + Glue service"
                    }
                ]
            }
        ]
    )
    print(f"✅ Outbound rules added (3306, 5439, 443)")

    return sg_id


def configure_redshift_sg_account_b():
    """
    Account B: Redshift SG must allow Glue ENI subnet
    """
    ec2 = boto3.client("ec2", region_name=REGION)
    SG_REDSHIFT = "sg-0bbb2222redshift"

    try:
        ec2.authorize_security_group_ingress(
            GroupId=SG_REDSHIFT,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5439,
                    "ToPort": 5439,
                    "IpRanges": [
                        {
                            "CidrIp": "10.2.6.0/24",
                            "Description": "Glue ENIs for ETL"
                        }
                    ]
                }
            ]
        )
        print(f"✅ Redshift SG: Allowed 10.2.6.0/24 → port 5439")
    except Exception as e:
        if "InvalidPermission.Duplicate" in str(e):
            print("ℹ️  Rule already exists")
        else:
            raise


if __name__ == "__main__":
    print("=" * 50)
    print("Run from Account A:")
    print("=" * 50)
    # configure_account_a_sg()

    print("\n" + "=" * 50)
    print("Run from Account B:")
    print("=" * 50)
    # glue_sg = create_glue_sg_account_b()
    # configure_redshift_sg_account_b()