# file: 61_06_s3_vpc_endpoint.py
# Run from: Account B (222222222222)
# Purpose: Create S3 Gateway VPC Endpoint so Glue can access S3
#          without NAT Gateway (cheaper and faster)

import boto3

REGION = "us-east-2"
VPC_B = "vpc-0bbb2222bbbb2222b"
ROUTE_TABLE_B = "rtb-0bbb2222bbbb2222b"  # Glue subnet's route table


def create_s3_endpoint():
    """
    WHY THIS IS NEEDED:
    → Glue jobs need S3 for: script storage, temp directory, output writes
    → Glue ENIs are in a PRIVATE subnet (no internet access)
    → Without S3 endpoint: Glue cannot reach S3 at all
    → S3 Gateway Endpoint: FREE, adds route to S3 via AWS backbone
    
    Already conceptually covered in Project 31 (Glue JDBC Connection)
    → Here we just ensure it exists for Account B
    """
    ec2 = boto3.client("ec2", region_name=REGION)

    try:
        response = ec2.create_vpc_endpoint(
            VpcId=VPC_B,
            ServiceName=f"com.amazonaws.{REGION}.s3",
            VpcEndpointType="Gateway",
            RouteTableIds=[ROUTE_TABLE_B],
            TagSpecifications=[
                {
                    "ResourceType": "vpc-endpoint",
                    "Tags": [
                        {"Key": "Name", "Value": "s3-endpoint-analytics-vpc"},
                        {"Key": "ManagedBy", "Value": "DataEngineering"}
                    ]
                }
            ]
        )

        endpoint_id = response["VpcEndpoint"]["VpcEndpointId"]
        print(f"✅ S3 VPC Endpoint created: {endpoint_id}")
        print(f"   Attached to route table: {ROUTE_TABLE_B}")
        return endpoint_id

    except Exception as e:
        if "RouteAlreadyExists" in str(e):
            print("ℹ️  S3 endpoint route already exists")
        else:
            raise


if __name__ == "__main__":
    create_s3_endpoint()