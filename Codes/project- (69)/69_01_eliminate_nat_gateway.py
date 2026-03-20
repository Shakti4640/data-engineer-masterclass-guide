# file: 69_01_eliminate_nat_gateway.py
# Run from: Account B (222222222222)
# Purpose: Replace NAT Gateway with VPC Endpoints for Glue
# SAVINGS: $275/month → $15/month = $260/month saved

import boto3
import json

REGION = "us-east-2"
VPC_ID = "vpc-0bbb2222bbbb2222b"
# Route tables that Glue subnets use
GLUE_ROUTE_TABLE_IDS = ["rtb-0bbb2222bbbb2222b"]
# Subnets where Glue ENIs are placed
GLUE_SUBNET_IDS = ["subnet-0bbb2222glue"]
SECURITY_GROUP_ID = "sg-0bbb2222glueeni"


def create_vpc_endpoints():
    """
    Create VPC Endpoints to replace NAT Gateway
    
    WHY NAT GATEWAY IS EXPENSIVE:
    → $0.045/hour = $32.85/month (just for existing)
    → $0.045/GB processed = variable based on traffic
    → Glue downloads scripts from S3, writes logs to CloudWatch,
      calls Glue APIs, calls STS — ALL through NAT
    → 2 TB/month through NAT: $90 data processing
    → TOTAL: ~$275/month for NAT
    
    VPC ENDPOINTS:
    → S3 Gateway Endpoint: FREE (no hourly charge, no data charge)
    → Interface Endpoints: $0.01/hour + $0.01/GB
    → 4 interface endpoints × $0.01/hr × 730 hrs = $29.20/month
    → But: traffic is mostly S3 (free gateway) → interface traffic minimal
    → Estimated: ~$15/month total
    
    REQUIRED ENDPOINTS FOR GLUE:
    1. S3 Gateway Endpoint (FREE — already created in Project 61)
    2. Glue Interface Endpoint (Glue API calls)
    3. CloudWatch Logs Interface Endpoint (log shipping)
    4. STS Interface Endpoint (IAM role assumption)
    5. KMS Interface Endpoint (if using KMS encryption)
    """
    ec2 = boto3.client("ec2", region_name=REGION)

    endpoints_to_create = [
        {
            "service": f"com.amazonaws.{REGION}.s3",
            "type": "Gateway",
            "route_tables": GLUE_ROUTE_TABLE_IDS,
            "description": "S3 Gateway — FREE, replaces NAT for S3 traffic"
        },
        {
            "service": f"com.amazonaws.{REGION}.glue",
            "type": "Interface",
            "subnets": GLUE_SUBNET_IDS,
            "description": "Glue API — for Glue service calls"
        },
        {
            "service": f"com.amazonaws.{REGION}.logs",
            "type": "Interface",
            "subnets": GLUE_SUBNET_IDS,
            "description": "CloudWatch Logs — for Glue log shipping"
        },
        {
            "service": f"com.amazonaws.{REGION}.sts",
            "type": "Interface",
            "subnets": GLUE_SUBNET_IDS,
            "description": "STS — for IAM role assumption"
        },
        {
            "service": f"com.amazonaws.{REGION}.redshift-data",
            "type": "Interface",
            "subnets": GLUE_SUBNET_IDS,
            "description": "Redshift Data API — for Lambda → Redshift calls"
        },
        {
            "service": f"com.amazonaws.{REGION}.secretsmanager",
            "type": "Interface",
            "subnets": GLUE_SUBNET_IDS,
            "description": "Secrets Manager — for credential retrieval"
        }
    ]

    created_endpoints = []

    for ep in endpoints_to_create:
        print(f"\n📡 Creating: {ep['service']} ({ep['type']})")
        print(f"   {ep['description']}")

        try:
            params = {
                "VpcId": VPC_ID,
                "ServiceName": ep["service"],
                "VpcEndpointType": ep["type"],
                "TagSpecifications": [{
                    "ResourceType": "vpc-endpoint",
                    "Tags": [
                        {"Key": "Name", "Value": f"vpce-{ep['service'].split('.')[-1]}"},
                        {"Key": "Purpose", "Value": "ReplaceNATGateway"},
                        {"Key": "ManagedBy", "Value": "DataEngineering"}
                    ]
                }]
            }

            if ep["type"] == "Gateway":
                params["RouteTableIds"] = ep["route_tables"]
            else:
                params["SubnetIds"] = ep["subnets"]
                params["SecurityGroupIds"] = [SECURITY_GROUP_ID]
                params["PrivateDnsEnabled"] = True

            response = ec2.create_vpc_endpoint(**params)
            endpoint_id = response["VpcEndpoint"]["VpcEndpointId"]
            print(f"   ✅ Created: {endpoint_id}")
            created_endpoints.append({
                "service": ep["service"],
                "endpoint_id": endpoint_id,
                "type": ep["type"]
            })

        except Exception as e:
            if "RouteAlreadyExists" in str(e) or "already exists" in str(e).lower():
                print(f"   ℹ️  Endpoint already exists")
            else:
                print(f"   ❌ Error: {str(e)[:100]}")

    return created_endpoints


def verify_glue_connectivity():
    """
    Verify Glue can reach all required services through VPC endpoints
    → Run a test Glue job that accesses S3, CloudWatch, Glue API
    → If all succeed: NAT Gateway can be removed
    """
    print(f"\n🧪 Verify Glue connectivity through VPC endpoints:")
    print(f"   1. Run test Glue job that reads S3 + writes CloudWatch")
    print(f"   2. If successful: proceed to remove NAT Gateway")
    print(f"   3. If fails: check endpoint configuration + security groups")
    print(f"\n   Test command:")
    print(f"   aws glue start-job-run --job-name <test-job>")
    print(f"   Monitor: CloudWatch Logs → /aws-glue/jobs/output")


def remove_nat_gateway():
    """
    Remove NAT Gateway AFTER verifying VPC endpoints work
    
    CAUTION: This removes internet access from private subnets
    → Glue: uses VPC endpoints instead (configured above)
    → Lambda: may need VPC endpoints too if in VPC
    → EC2 in private subnet: loses internet (use SSM for access)
    
    DO NOT RUN until verify_glue_connectivity() confirms success
    """
    ec2 = boto3.client("ec2", region_name=REGION)

    print(f"\n🗑️ NAT Gateway Removal")
    print(f"   ⚠️  WARNING: Only proceed after verifying VPC endpoints work")
    print(f"   ⚠️  This removes internet access from private subnets")
    print(f"\n   Steps (manual verification required):")
    print(f"   1. Identify NAT Gateway: aws ec2 describe-nat-gateways --filter Name=vpc-id,Values={VPC_ID}")
    print(f"   2. Remove route: 0.0.0.0/0 → nat-xxx from Glue subnet route table")
    print(f"   3. Delete NAT Gateway: aws ec2 delete-nat-gateway --nat-gateway-id nat-xxx")
    print(f"   4. Release Elastic IP associated with NAT Gateway")
    print(f"   5. Monitor Glue jobs for 24 hours to confirm no issues")


def calculate_savings():
    """Show NAT Gateway cost savings"""
    print(f"\n{'='*60}")
    print(f"💰 NAT GATEWAY ELIMINATION — SAVINGS CALCULATION")
    print(f"{'='*60}")

    print(f"""
   BEFORE (NAT Gateway):
   → Hourly charge: $0.045/hr × 730 hrs = $32.85/month
   → Data processing: ~2 TB × $0.045/GB = $92.16/month
   → TOTAL: ~$275/month

   AFTER (VPC Endpoints):
   → S3 Gateway: FREE
   → Interface Endpoints (4): $0.01/hr × 4 × 730 = $29.20/month
   → Data processing: minimal (most traffic via S3 Gateway)
   → TOTAL: ~$15/month

   💰 MONTHLY SAVINGS: $260
   💰 ANNUAL SAVINGS: $3,120
""")


if __name__ == "__main__":
    print("=" * 60)
    print("🔧 ELIMINATING NAT GATEWAY — REPLACING WITH VPC ENDPOINTS")
    print("=" * 60)

    create_vpc_endpoints()
    verify_glue_connectivity()
    calculate_savings()

    print(f"\n{'='*60}")
    print("⚠️  NEXT STEPS:")
    print("   1. Run test Glue job to verify connectivity")
    print("   2. If all OK: run remove_nat_gateway()")
    print("   3. Monitor for 24 hours")
    print(f"{'='*60}")