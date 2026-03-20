# file: 61_03_configure_routes.py
# Purpose: Add routes in BOTH VPCs pointing to peering connection
# Must run TWICE: once per account (or use cross-account role)

import boto3

REGION = "us-east-2"
PEERING_CONNECTION_ID = "pcx-0abcdef1234567890"


def add_route_account_a():
    """
    Account A route table:
    → Destination: 10.2.0.0/16 (Account B's CIDR)
    → Target: pcx-0abcdef1234567890
    
    WHERE TO ADD:
    → Route table associated with MariaDB EC2's subnet
    → If MariaDB is in subnet 10.1.3.0/24, find that subnet's RT
    """
    ec2 = boto3.client("ec2", region_name=REGION)

    # Account A's route table for MariaDB subnet
    ROUTE_TABLE_ID_A = "rtb-0aaa1111aaaa1111a"

    try:
        ec2.create_route(
            RouteTableId=ROUTE_TABLE_ID_A,
            DestinationCidrBlock="10.2.0.0/16",
            VpcPeeringConnectionId=PEERING_CONNECTION_ID
        )
        print(f"✅ Account A: Route added")
        print(f"   {ROUTE_TABLE_ID_A}: 10.2.0.0/16 → {PEERING_CONNECTION_ID}")
    except ec2.exceptions.ClientError as e:
        if "RouteAlreadyExists" in str(e):
            print("ℹ️  Route already exists in Account A")
        else:
            raise


def add_route_account_b():
    """
    Account B route table:
    → Destination: 10.1.0.0/16 (Account A's CIDR)
    → Target: pcx-0abcdef1234567890
    
    WHERE TO ADD:
    → Route table associated with Glue's subnet (10.2.6.0/24)
    → ALSO: Redshift's subnet (10.2.5.0/24) if Glue needs to reach it
      (usually same RT if both in private subnets)
    """
    ec2 = boto3.client("ec2", region_name=REGION)

    # Account B's route table for Glue subnet
    ROUTE_TABLE_ID_B = "rtb-0bbb2222bbbb2222b"

    try:
        ec2.create_route(
            RouteTableId=ROUTE_TABLE_ID_B,
            DestinationCidrBlock="10.1.0.0/16",
            VpcPeeringConnectionId=PEERING_CONNECTION_ID
        )
        print(f"✅ Account B: Route added")
        print(f"   {ROUTE_TABLE_ID_B}: 10.1.0.0/16 → {PEERING_CONNECTION_ID}")
    except ec2.exceptions.ClientError as e:
        if "RouteAlreadyExists" in str(e):
            print("ℹ️  Route already exists in Account B")
        else:
            raise


if __name__ == "__main__":
    # Run the appropriate function based on which account you're in
    # In practice: use separate scripts or parameterize
    print("Run add_route_account_a() from Account A")
    print("Run add_route_account_b() from Account B")