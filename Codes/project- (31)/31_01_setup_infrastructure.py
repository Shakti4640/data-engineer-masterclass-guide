# file: 31_01_setup_infrastructure.py
# Purpose: Create all AWS resources needed for Glue JDBC extraction
# Prerequisites: VPC exists, MariaDB on EC2 exists (Projects 5, 8, 9)

import boto3
import json
import time

# ─── CONFIGURATION ───────────────────────────────────────────────
AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# VPC & Network (from your existing setup — Projects 5, 8)
VPC_ID = "vpc-0abc123def456"               # YOUR VPC ID
PRIVATE_SUBNET_ID = "subnet-0def456abc"     # Subnet where Glue ENIs go
MARIADB_SG_ID = "sg-0db123456"              # MariaDB's Security Group
ROUTE_TABLE_ID = "rtb-0abc123"              # Private subnet route table

# MariaDB connection details
MARIADB_HOST = "10.0.1.50"                  # Private IP of MariaDB EC2
MARIADB_PORT = "3306"
MARIADB_DATABASE = "quickcart_shop"
MARIADB_USERNAME = "glue_reader"
MARIADB_PASSWORD =[REDACTED:PASSWORD]25"  # Use Secrets Manager in prod (Project 70)

# S3
S3_BUCKET = "quickcart-datalake-prod"
S3_OUTPUT_PATH = f"s3://{S3_BUCKET}/bronze/mariadb/"
S3_TEMP_PATH = f"s3://{S3_BUCKET}/tmp/glue/"
S3_SCRIPT_PATH = f"s3://{S3_BUCKET}/scripts/glue/"

# Glue
GLUE_CONNECTION_NAME = "quickcart-mariadb-conn"
GLUE_JOB_NAME = "quickcart-mariadb-to-s3-parquet"
GLUE_ROLE_NAME = "GlueMariaDBExtractorRole"
GLUE_SG_NAME = "sg-glue-mariadb-extractor"

# ─── CLIENTS ─────────────────────────────────────────────────────
ec2 = boto3.client("ec2", region_name=AWS_REGION)
iam = boto3.client("iam")
glue = boto3.client("glue", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Create Security Group for Glue
# ═══════════════════════════════════════════════════════════════════
def create_glue_security_group():
    """
    Glue workers need a Security Group that:
    1. Allows ALL inbound TCP from ITSELF (self-referencing)
       → Spark workers communicate on random high ports
    2. Allows outbound to MariaDB on port 3306
    3. Allows outbound to S3 via VPC Endpoint (handled by route table)
    """
    print("\n🔒 Creating Glue Security Group...")

    try:
        response = ec2.create_security_group(
            GroupName=GLUE_SG_NAME,
            Description="Security group for Glue JDBC jobs - allows self-referencing",
            VpcId=VPC_ID
        )
        glue_sg_id = response["GroupId"]
        print(f"   Created: {glue_sg_id}")

        # ── RULE 1: Self-referencing (CRITICAL) ──
        # Without this, Glue workers cannot communicate → job hangs
        ec2.authorize_security_group_ingress(
            GroupId=glue_sg_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 0,
                    "ToPort": 65535,
                    "UserIdGroupPairs": [
                        {
                            "GroupId": glue_sg_id,  # ← ITSELF
                            "Description": "Glue workers inter-communication"
                        }
                    ]
                }
            ]
        )
        print("   ✅ Inbound: All TCP from SELF (Glue worker communication)")

        # ── RULE 2: Outbound to MariaDB ──
        # Default SG allows all outbound, but being explicit is better
        # (Default outbound 0.0.0.0/0 all traffic already exists)
        print("   ✅ Outbound: Default all-traffic rule covers MariaDB + S3")

        # ── RULE 3: Allow MariaDB SG to accept from Glue SG ──
        # MariaDB's Security Group must accept inbound from Glue
        ec2.authorize_security_group_ingress(
            GroupId=MARIADB_SG_ID,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 3306,
                    "ToPort": 3306,
                    "UserIdGroupPairs": [
                        {
                            "GroupId": glue_sg_id,
                            "Description": "Allow Glue JDBC connections"
                        }
                    ]
                }
            ]
        )
        print(f"   ✅ MariaDB SG ({MARIADB_SG_ID}): Inbound 3306 from Glue SG")

        return glue_sg_id

    except ec2.exceptions.ClientError as e:
        if "InvalidGroup.Duplicate" in str(e):
            # SG already exists — look it up
            response = ec2.describe_security_groups(
                Filters=[
                    {"Name": "group-name", "Values": [GLUE_SG_NAME]},
                    {"Name": "vpc-id", "Values": [VPC_ID]}
                ]
            )
            glue_sg_id = response["SecurityGroups"][0]["GroupId"]
            print(f"   ℹ️  Already exists: {glue_sg_id}")
            return glue_sg_id
        raise


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Create S3 VPC Gateway Endpoint (if not exists)
# ═══════════════════════════════════════════════════════════════════
def create_s3_vpc_endpoint():
    """
    Glue workers in private subnet CANNOT reach S3 without this.
    Gateway Endpoint is FREE — no data transfer charges.
    
    Without this:
    → Glue reads MariaDB successfully
    → Tries to write Parquet to S3
    → Hangs indefinitely → timeout after 48 hours (default)
    → You see NO error — just silence
    """
    print("\n🌐 Creating S3 VPC Gateway Endpoint...")

    # Check if already exists
    existing = ec2.describe_vpc_endpoints(
        Filters=[
            {"Name": "vpc-id", "Values": [VPC_ID]},
            {"Name": "service-name", "Values": [f"com.amazonaws.{AWS_REGION}.s3"]},
            {"Name": "vpc-endpoint-type", "Values": ["Gateway"]}
        ]
    )

    if existing["VpcEndpoints"]:
        endpoint_id = existing["VpcEndpoints"][0]["VpcEndpointId"]
        print(f"   ℹ️  Already exists: {endpoint_id}")
        return endpoint_id

    response = ec2.create_vpc_endpoint(
        VpcId=VPC_ID,
        ServiceName=f"com.amazonaws.{AWS_REGION}.s3",
        VpcEndpointType="Gateway",
        RouteTableIds=[ROUTE_TABLE_ID]
    )
    endpoint_id = response["VpcEndpoint"]["VpcEndpointId"]
    print(f"   ✅ Created: {endpoint_id}")
    print(f"   → Attached to route table: {ROUTE_TABLE_ID}")
    print(f"   → S3 traffic now stays within AWS network (free + fast)")
    return endpoint_id


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Create IAM Role for Glue
# ═══════════════════════════════════════════════════════════════════
def create_glue_iam_role():
    """
    Glue needs an IAM Role with:
    1. Trust policy: Allow glue.amazonaws.com to assume this role
    2. Permissions: 
       - Read Glue Connection (for JDBC creds)
       - Write to S3 (Parquet output + temp files)
       - Write CloudWatch Logs (monitoring)
       - Create/Delete ENIs in VPC (network access)
    """
    print("\n🔐 Creating IAM Role for Glue...")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # Inline policy — principle of least privilege
    glue_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3Access",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET}",
                    f"arn:aws:s3:::{S3_BUCKET}/*"
                ]
            },
            {
                "Sid": "GlueServiceAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:GetConnection",
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:GetPartitions",
                    "glue:BatchCreatePartition"
                ],
                "Resource": "*"
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            },
            {
                "Sid": "VPCNetworkAccess",
                "Effect": "Allow",
                "Action": [
                    "ec2:DescribeVpcEndpoints",
                    "ec2:DescribeRouteTables",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeVpcAttribute",
                    "ec2:CreateNetworkInterface",
                    "ec2:DeleteNetworkInterface",
                    "ec2:CreateTags",
                    "ec2:DeleteTags"
                ],
                "Resource": "*"
            }
        ]
    }

    try:
        iam.create_role(
            RoleName=GLUE_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Glue JDBC extraction from MariaDB to S3",
            Tags=[
                {"Key": "Project", "Value": "QuickCart-DataLake"},
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print(f"   ✅ Role created: {GLUE_ROLE_NAME}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"   ℹ️  Role already exists: {GLUE_ROLE_NAME}")

    # Attach inline policy
    iam.put_role_policy(
        RoleName=GLUE_ROLE_NAME,
        PolicyName="GlueMariaDBExtractorPolicy",
        PolicyDocument=json.dumps(glue_policy)
    )
    print("   ✅ Inline policy attached")

    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{GLUE_ROLE_NAME}"
    print(f"   → Role ARN: {role_arn}")

    # Wait for role propagation (IAM is eventually consistent)
    print("   ⏳ Waiting 10s for IAM propagation...")
    time.sleep(10)

    return role_arn


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Create Glue Connection (JDBC to MariaDB)
# ═══════════════════════════════════════════════════════════════════
def create_glue_connection(glue_sg_id):
    """
    The Glue Connection bundles:
    → JDBC URL (where to connect)
    → Credentials (how to authenticate)
    → VPC config (how to get network access)
    
    MariaDB uses MySQL JDBC driver (protocol-compatible)
    JDBC URL format: jdbc:mysql://host:port/database
    """
    print("\n🔗 Creating Glue JDBC Connection...")

    jdbc_url = f"jdbc:mysql://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DATABASE}"

    connection_input = {
        "Name": GLUE_CONNECTION_NAME,
        "Description": "JDBC connection to QuickCart MariaDB on EC2",
        "ConnectionType": "JDBC",
        "ConnectionProperties": {
            "JDBC_CONNECTION_URL": jdbc_url,
            "USERNAME": MARIADB_USERNAME,
            "PASSWORD": MARIADB_PASSWORD,
            # MariaDB is MySQL-compatible — Glue uses MySQL JDBC driver
            "JDBC_DRIVER_CLASS_NAME": "com.mysql.cj.jdbc.Driver",
            "JDBC_ENFORCE_SSL": "false"   # Enable in production with certs
        },
        "PhysicalConnectionRequirements": {
            "SubnetId": PRIVATE_SUBNET_ID,
            "SecurityGroupIdList": [glue_sg_id],
            "AvailabilityZone": f"{AWS_REGION}a"  # Must match subnet AZ
        }
    }

    try:
        glue.create_connection(
            ConnectionInput=connection_input
        )
        print(f"   ✅ Connection created: {GLUE_CONNECTION_NAME}")
    except glue.exceptions.AlreadyExistsException:
        # Update if already exists
        glue.update_connection(
            Name=GLUE_CONNECTION_NAME,
            ConnectionInput=connection_input
        )
        print(f"   ✅ Connection updated: {GLUE_CONNECTION_NAME}")

    print(f"   → JDBC URL: {jdbc_url}")
    print(f"   → Subnet: {PRIVATE_SUBNET_ID}")
    print(f"   → Security Group: {glue_sg_id}")

    return GLUE_CONNECTION_NAME


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Test the Glue Connection
# ═══════════════════════════════════════════════════════════════════
def test_glue_connection():
    """
    Glue spins up a test ENI in your VPC, attempts JDBC connection.
    Takes 1-3 minutes.
    
    If this fails: fix network/SG/credentials BEFORE running the job.
    Running a job with a broken connection wastes $$ and time.
    """
    print("\n🧪 Testing Glue Connection (takes 1-3 minutes)...")

    # Note: test_connection is async — you must poll for result
    # In practice, use the Console for quick testing
    # Programmatic testing requires more complex polling logic

    try:
        glue.get_connection(Name=GLUE_CONNECTION_NAME)
        print(f"   ✅ Connection '{GLUE_CONNECTION_NAME}' exists and is configured")
        print("   → Test via Console: Glue → Connections → Select → Test Connection")
        print("   → Select the IAM Role when testing")
    except Exception as e:
        print(f"   ❌ Connection issue: {e}")


# ═══════════════════════════════════════════════════════════════════
# STEP 6: Create MariaDB Read-Only User
# ═══════════════════════════════════════════════════════════════════
def print_mariadb_user_setup():
    """
    Print SQL commands to create the Glue reader user in MariaDB.
    Run these ON the MariaDB EC2 instance.
    """
    print("\n📋 Run these SQL commands ON MariaDB EC2:")
    print("=" * 60)
    print(f"""
-- Connect to MariaDB
mysql -u root -p

-- Create dedicated read-only user for Glue
CREATE USER '{MARIADB_USERNAME}'@'10.0.%' 
    IDENTIFIED BY '{MARIADB_PASSWORD}';

-- Grant SELECT only on the target database
GRANT SELECT ON {MARIADB_DATABASE}.* 
    TO '{MARIADB_USERNAME}'@'10.0.%';

-- Verify grants
SHOW GRANTS FOR '{MARIADB_USERNAME}'@'10.0.%';

-- Flush privileges
FLUSH PRIVILEGES;

-- IMPORTANT: '10.0.%' means this user can only connect
-- from the 10.0.x.x VPC CIDR — Glue workers get IPs in this range
-- This prevents external access even if credentials leak
""")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════
# MAIN: Run all setup steps
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 SETTING UP GLUE JDBC INFRASTRUCTURE")
    print("=" * 60)

    # Step 1
    glue_sg_id = create_glue_security_group()

    # Step 2
    create_s3_vpc_endpoint()

    # Step 3
    role_arn = create_glue_iam_role()

    # Step 4
    create_glue_connection(glue_sg_id)

    # Step 5
    test_glue_connection()

    # Step 6
    print_mariadb_user_setup()

    print("\n" + "=" * 60)
    print("✅ INFRASTRUCTURE READY")
    print("   Next: Upload Glue ETL script → Create Glue Job → Run")
    print("=" * 60)