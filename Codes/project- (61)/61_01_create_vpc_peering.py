# file: 61_01_create_vpc_peering.py
# Run from: Account A (111111111111) — the requester
# Purpose: Create VPC peering request to Account B

import boto3
import time

# --- CONFIGURATION ---
REGION = "us-east-2"

# Account A (this account)
ACCOUNT_A_VPC_ID = "vpc-0aaa1111aaaa1111a"  # 10.1.0.0/16

# Account B (peer account)
ACCOUNT_B_ID =[REDACTED:BANK_ACCOUNT_NUMBER]22"
ACCOUNT_B_VPC_ID = "vpc-0bbb2222bbbb2222b"  # 10.2.0.0/16


def create_peering_request():
    """
    Step 1: Account A requests peering with Account B
    
    INTERNAL MECHANICS:
    → AWS creates a peering connection object in BOTH accounts
    → Status starts as "initiating" → moves to "pending-acceptance"
    → Account B must ACCEPT within 7 days or it expires
    → No traffic flows until accepted + routes + SGs configured
    """
    ec2 = boto3.client("ec2", region_name=REGION)

    response = ec2.create_vpc_peering_connection(
        VpcId=ACCOUNT_A_VPC_ID,                # Our VPC (requester)
        PeerVpcId=ACCOUNT_B_VPC_ID,            # Their VPC (accepter)
        PeerOwnerId=ACCOUNT_B_ID,              # Their account ID
        PeerRegion=REGION,                      # Same region
        TagSpecifications=[
            {
                "ResourceType": "vpc-peering-connection",
                "Tags": [
                    {"Key": "Name", "Value": "AcctA-to-AcctB-Analytics"},
                    {"Key": "Purpose", "Value": "MariaDB-to-Redshift-ETL"},
                    {"Key": "ManagedBy", "Value": "DataEngineering"}
                ]
            }
        ]
    )

    pcx_id = response["VpcPeeringConnection"]["VpcPeeringConnectionId"]
    status = response["VpcPeeringConnection"]["Status"]["Code"]

    print(f"✅ Peering connection created: {pcx_id}")
    print(f"   Status: {status}")
    print(f"   Requester VPC: {ACCOUNT_A_VPC_ID} (Account A)")
    print(f"   Accepter VPC: {ACCOUNT_B_VPC_ID} (Account B: {ACCOUNT_B_ID})")
    print(f"\n⏳ Waiting for Account B to accept...")
    print(f"   → Account B must run: 61_02_accept_vpc_peering.py")
    print(f"   → Peering ID to share: {pcx_id}")

    return pcx_id


def enable_dns_on_peering(pcx_id):
    """
    Enable DNS resolution for peering (Account A side)
    
    WHY: Without this, private DNS hostnames don't resolve
    across the peering connection. Glue might use hostname
    instead of IP in JDBC URL — this ensures it works.
    
    NOTE: Must be called AFTER peering is in "active" state
    """
    ec2 = boto3.client("ec2", region_name=REGION)

    ec2.modify_vpc_peering_connection_options(
        VpcPeeringConnectionId=pcx_id,
        RequesterPeeringConnectionOptions={
            "AllowDnsResolutionFromRemoteVpc": True
        }
    )
    print(f"✅ DNS resolution enabled on Account A side for {pcx_id}")


if __name__ == "__main__":
    pcx_id = create_peering_request()

    # Save for later scripts
    with open("/tmp/peering_id.txt", "w") as f:
        f.write(pcx_id)