# file: 01_create_redshift_cluster.py
# Purpose: Provision Redshift cluster with proper configuration
# Prerequisites: VPC, subnets, security group already exist (Project 5)

import boto3
import json
import time
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
REGION = "us-east-2"
CLUSTER_IDENTIFIER = "quickcart-analytics"
DB_NAME = "quickcart_dw"
MASTER_USER = "admin_user"
MASTER_PASSWORD = "QuickCart_DW_2025!"     # In production: use Secrets Manager (Project 70)
NODE_TYPE = "dc2.large"
NUM_NODES = 1                              # Single node to start
CLUSTER_TYPE = "single-node"               # "multi-node" for 2+ nodes

# Network (replace with your actual VPC resources)
VPC_SECURITY_GROUP_ID = "sg-0123456789abcdef0"   # Allow 5439 from VPC CIDR
CLUSTER_SUBNET_GROUP = "quickcart-redshift-subnet"
AVAILABILITY_ZONE = "us-east-2a"

# IAM Role for S3 access (COPY/UNLOAD — Projects 21, 22)
REDSHIFT_IAM_ROLE_ARN = "arn:aws:iam::123456789012:role/RedshiftS3AccessRole"


def create_subnet_group(redshift_client):
    """
    Create subnet group — tells Redshift which subnets to use
    Must have at least 2 subnets in different AZs for Multi-AZ
    """
    try:
        redshift_client.create_cluster_subnet_group(
            ClusterSubnetGroupName=CLUSTER_SUBNET_GROUP,
            Description="QuickCart Redshift subnets",
            SubnetIds=[
                "subnet-0aaa1111",      # Private subnet AZ-a
                "subnet-0bbb2222"       # Private subnet AZ-b
            ],
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Project", "Value": "QuickCart"}
            ]
        )
        print(f"✅ Subnet group '{CLUSTER_SUBNET_GROUP}' created")
    except ClientError as e:
        if "already exists" in str(e).lower() or "AlreadyExists" in str(e):
            print(f"ℹ️  Subnet group '{CLUSTER_SUBNET_GROUP}' already exists")
        else:
            raise


def create_cluster(redshift_client):
    """
    Create the Redshift cluster with production settings
    """
    print(f"\n🚀 Creating Redshift cluster: {CLUSTER_IDENTIFIER}")
    print(f"   Node type: {NODE_TYPE}")
    print(f"   Nodes: {NUM_NODES}")
    print(f"   Database: {DB_NAME}")
    
    try:
        response = redshift_client.create_cluster(
            # --- IDENTITY ---
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            DBName=DB_NAME,
            MasterUsername=MASTER_USER,
            MasterUserPassword=MASTER_PASSWORD,
            
            # --- COMPUTE ---
            NodeType=NODE_TYPE,
            ClusterType=CLUSTER_TYPE,
            NumberOfNodes=NUM_NODES if CLUSTER_TYPE == "multi-node" else None,
            
            # --- NETWORK ---
            ClusterSubnetGroupName=CLUSTER_SUBNET_GROUP,
            VpcSecurityGroupIds=[VPC_SECURITY_GROUP_ID],
            AvailabilityZone=AVAILABILITY_ZONE,
            PubliclyAccessible=False,         # PRIVATE — no public IP
            
            # --- SECURITY ---
            Encrypted=True,                   # Encryption at rest (KMS)
            # KmsKeyId: uses default aws/redshift key
            
            # --- IAM ---
            IamRoles=[REDSHIFT_IAM_ROLE_ARN],  # For COPY/UNLOAD S3 access
            
            # --- NETWORKING OPTIMIZATION ---
            EnhancedVpcRouting=True,           # S3 traffic stays in VPC
            
            # --- MAINTENANCE ---
            PreferredMaintenanceWindow="sun:03:00-sun:04:00",  # Sunday 3-4 AM
            AutomatedSnapshotRetentionPeriod=7,    # Keep snapshots 7 days
            
            # --- LOGGING ---
            LoggingProperties={
                "BucketName": "quickcart-raw-data-prod",       # From Project 1
                "S3KeyPrefix": "redshift-logs/"
            },
            
            # --- TAGS ---
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Team", "Value": "Data-Engineering"},
                {"Key": "Project", "Value": "QuickCart-Analytics"},
                {"Key": "CostCenter", "Value": "ENG-002"}
            ]
        )
        
        cluster = response["Cluster"]
        print(f"   ✅ Cluster creation initiated")
        print(f"   Status: {cluster['ClusterStatus']}")
        print(f"   Encrypted: {cluster['Encrypted']}")
        
        return True
        
    except ClientError as e:
        if "ClusterAlreadyExists" in str(e):
            print(f"   ℹ️  Cluster '{CLUSTER_IDENTIFIER}' already exists")
            return True
        else:
            print(f"   ❌ Error: {e}")
            raise


def wait_for_cluster(redshift_client, timeout_minutes=20):
    """
    Wait for cluster to become available
    Typical creation time: 5-15 minutes
    """
    print(f"\n⏳ Waiting for cluster to become available...")
    print(f"   (This typically takes 5-15 minutes)")
    
    start = time.time()
    timeout = timeout_minutes * 60
    
    while time.time() - start < timeout:
        try:
            response = redshift_client.describe_clusters(
                ClusterIdentifier=CLUSTER_IDENTIFIER
            )
            cluster = response["Clusters"][0]
            status = cluster["ClusterStatus"]
            elapsed = int(time.time() - start)
            
            print(f"   [{elapsed}s] Status: {status}")
            
            if status == "available":
                endpoint = cluster["Endpoint"]
                print(f"\n   ✅ CLUSTER AVAILABLE!")
                print(f"   Endpoint: {endpoint['Address']}")
                print(f"   Port: {endpoint['Port']}")
                print(f"   Database: {DB_NAME}")
                print(f"   Nodes: {cluster['NumberOfNodes']}")
                print(f"   Node Type: {cluster['NodeType']}")
                print(f"   Encrypted: {cluster['Encrypted']}")
                print(f"   Enhanced VPC Routing: {cluster['EnhancedVpcRouting']}")
                return endpoint
            
            if status in ("deleting", "final-snapshot"):
                print(f"   ❌ Cluster is being deleted!")
                return None
                
        except ClientError as e:
            if "ClusterNotFound" in str(e):
                print(f"   ❌ Cluster not found")
                return None
            raise
        
        time.sleep(30)
    
    print(f"   ⏰ Timed out after {timeout_minutes} minutes")
    return None


def create_iam_role_for_redshift():
    """
    Create IAM role that Redshift assumes to access S3
    
    WHY:
    → COPY command loads data from S3 into Redshift
    → UNLOAD command exports data from Redshift to S3
    → Both need S3 permissions
    → Redshift assumes this role — no credentials in SQL
    → Same concept as EC2 Instance Profile (Project 6)
    """
    iam_client = boto3.client("iam", region_name=REGION)
    role_name = "RedshiftS3AccessRole"
    
    # Trust policy: allows Redshift service to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    # Permissions policy: what the role can do
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:GetBucketLocation",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::quickcart-raw-data-prod",
                    "arn:aws:s3:::quickcart-raw-data-prod/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                "Resource": [
                    "arn:aws:s3:::quickcart-raw-data-prod/redshift-unload/*"
                ]
            }
        ]
    }
    
    try:
        # Create role
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Redshift to access QuickCart S3 bucket",
            Tags=[
                {"Key": "Project", "Value": "QuickCart"},
                {"Key": "Service", "Value": "Redshift"}
            ]
        )
        print(f"✅ IAM Role '{role_name}' created")
        
        # Attach policy
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="RedshiftS3Policy",
            PolicyDocument=json.dumps(s3_policy)
        )
        print(f"✅ S3 policy attached to role")
        
        # Get role ARN
        response = iam_client.get_role(RoleName=role_name)
        role_arn = response["Role"]["Arn"]
        print(f"   Role ARN: {role_arn}")
        return role_arn
        
    except ClientError as e:
        if "EntityAlreadyExists" in str(e):
            response = iam_client.get_role(RoleName=role_name)
            role_arn = response["Role"]["Arn"]
            print(f"ℹ️  Role already exists: {role_arn}")
            return role_arn
        raise


def run_setup():
    """Main execution"""
    print("=" * 60)
    print("🚀 REDSHIFT CLUSTER PROVISIONING")
    print("=" * 60)
    
    # Step 1: Create IAM Role
    print("\n📋 Step 1: IAM Role for S3 Access")
    role_arn = create_iam_role_for_redshift()
    
    # Step 2: Create Subnet Group
    redshift_client = boto3.client("redshift", region_name=REGION)
    print("\n📋 Step 2: Subnet Group")
    create_subnet_group(redshift_client)
    
    # Step 3: Create Cluster
    print("\n📋 Step 3: Create Cluster")
    success = create_cluster(redshift_client)
    
    if success:
        # Step 4: Wait for availability
        print("\n📋 Step 4: Wait for Cluster")
        endpoint = wait_for_cluster(redshift_client)
        
        if endpoint:
            print(f"\n{'='*60}")
            print(f"✅ REDSHIFT CLUSTER READY")
            print(f"{'='*60}")
            print(f"""
CONNECTION DETAILS:
  Host:     {endpoint['Address']}
  Port:     {endpoint['Port']}
  Database: {DB_NAME}
  User:     {MASTER_USER}
  Password: (stored securely — use Secrets Manager in Project 70)

CONNECT VIA:
  psql -h {endpoint['Address']} -p {endpoint['Port']} -U {MASTER_USER} -d {DB_NAME}
  
  OR Python: psycopg2.connect(
      host='{endpoint['Address']}',
      port={endpoint['Port']},
      dbname='{DB_NAME}',
      user='{MASTER_USER}',
      password='***'
  )

NEXT STEPS:
  → Run 02_create_schema.sql to create analytics tables
  → Run 03_verify_cluster.py to check health
  → Project 11: Design fact/dimension tables
  → Project 21: COPY data from S3 into Redshift
            """)


if __name__ == "__main__":
    run_setup()