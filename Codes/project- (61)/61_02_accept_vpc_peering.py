# file: 61_02_accept_vpc_peering.py
# Run from: Account B (222222222222) — the accepter
# Purpose: Accept VPC peering request from Account A

import boto3
import time

# --- CONFIGURATION ---
REGION = "us-east-2"
PEERING_CONNECTION_ID = "pcx-0abcdef1234567890"  # From Account A's output


def accept_peering():
    """
    Step 2: Account B accepts the peering request
    
    IMPORTANT:
    → Until accepted, peering is "pending-acceptance"
    → After accepted, status becomes "active"
    → Even after "active": NO traffic flows until routes are added
    """
    ec2 = boto3.client("ec2", region_name=REGION)

    response = ec2.accept_vpc_peering_connection(
        VpcPeeringConnectionId=PEERING_CONNECTION_ID
    )

    status = response["VpcPeeringConnection"]["Status"]["Code"]
    print(f"✅ Peering accepted: {PEERING_CONNECTION_ID}")
    print(f"   Status: {status}")

    # Tag it in Account B's console
    ec2.create_tags(
        Resources=[PEERING_CONNECTION_ID],
        Tags=[
            {"Key": "Name", "Value": "AcctB-from-AcctA-Operations"},
            {"Key": "Purpose", "Value": "MariaDB-to-Redshift-ETL"},
            {"Key": "ManagedBy", "Value": "DataEngineering"}
        ]
    )
    print("✅ Tagged in Account B")

    # Enable DNS resolution on accepter side
    ec2.modify_vpc_peering_connection_options(
        VpcPeeringConnectionId=PEERING_CONNECTION_ID,
        AccepterPeeringConnectionOptions={
            "AllowDnsResolutionFromRemoteVpc": True
        }
    )
    print("✅ DNS resolution enabled on Account B side")


if __name__ == "__main__":
    accept_peering()