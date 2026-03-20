# file: 32_01_setup_redshift_infrastructure.py
# Purpose: Create Glue Connection to Redshift + security configuration
# Prerequisites: 
#   - Redshift cluster exists (Projects 10, 11)
#   - Glue SG exists (Project 31 — reused)
#   - S3 VPC Endpoint exists (Project 31 — reused)

import boto3
import json
import time

# ─── CONFIGURATION ───────────────────────────────────────────────
AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# VPC & Network (same VPC as Project 31)
VPC_ID = "vpc-0abc123def456"
PRIVATE_SUBNET_ID = "subnet-0def456abc"
GLUE_SG_ID = "sg-0glue123456"              # From Project 31
REDSHIFT_SG_ID = "sg-0redshift789"          # Redshift cluster SG

# Redshift connection details
REDSHIFT_HOST = "quickcart-dwh.cxyz123.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = "5439"
REDSHIFT_DATABASE = "quickcart_warehouse"
REDSHIFT_USERNAME = "glue_loader"
REDSHIFT_PASSWORD = [REDACTED:PASSWORD]5"  # Secrets Manager in prod (Project 70)

# S3
S3_BUCKET = "quickcart-datalake-prod"
REDSHIFT_TEMP_DIR = f"s3://{S3_BUCKET}/tmp/glue-redshift/"

# IAM
GLUE_ROLE_NAME = "GlueMariaDBExtractorRole"  # Reuse from Project 31
REDSHIFT_ROLE_NAME = "RedshiftS3ReadRole"

# Glue
GLUE_REDSHIFT_CONNECTION = "quickcart-redshift-conn"

# ─── CLIENTS ─────────────────────────────────────────────────────
ec2 = boto3.client("ec2", region_name=AWS_REGION)
iam = boto3.client("iam")
glue = boto3.client("glue", region_name=AWS_REGION)
redshift = boto3.client("redshift", region_name=AWS_REGION)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Allow Glue SG to Reach Redshift on Port 5439
# ═══════════════════════════════════════════════════════════════════
def configure_security_groups():
    """
    Add inbound rule to Redshift SG: allow port 5439 from Glue SG
    
    Network path:
    Glue Worker (sg-glue) → port 5439 → Redshift (sg-redshift)
    
    Glue SG already has:
    → Self-referencing rule (Project 31)
    → Outbound all traffic (default)
    
    Redshift SG needs:
    → Inbound TCP 5439 from Glue SG
    """
    print("\n🔒 Configuring Security Groups for Glue → Redshift...")

    try:
        ec2.authorize_security_group_ingress(
            GroupId=REDSHIFT_SG_ID,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5439,
                    "ToPort": 5439,
                    "UserIdGroupPairs": [
                        {
                            "GroupId": GLUE_SG_ID,
                            "Description": "Allow Glue JDBC to Redshift"
                        }
                    ]
                }
            ]
        )
        print(f"   ✅ Redshift SG ({REDSHIFT_SG_ID}): Inbound 5439 from Glue SG")
    except ec2.exceptions.ClientError as e:
        if "InvalidPermission.Duplicate" in str(e):
            print(f"   ℹ️  Rule already exists")
        else:
            raise


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Create Redshift IAM Role (for COPY from S3)
# ═══════════════════════════════════════════════════════════════════
def create_redshift_s3_role():
    """
    Redshift needs its OWN IAM Role to read S3 during COPY
    
    This is DIFFERENT from Glue's IAM Role:
    → Glue Role: used by Glue workers (write temp files to S3)
    → Redshift Role: used by Redshift cluster (read temp files from S3)
    → Both access the SAME S3 bucket but with different roles
    """
    print("\n🔐 Creating Redshift S3 Read Role...")

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

    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadTempFiles",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:GetBucketLocation",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET}",
                    f"arn:aws:s3:::{S3_BUCKET}/tmp/glue-redshift/*"
                ]
            }
        ]
    }

    try:
        iam.create_role(
            RoleName=REDSHIFT_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Redshift to read temp files from S3 during COPY"
        )
        print(f"   ✅ Role created: {REDSHIFT_ROLE_NAME}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"   ℹ️  Role already exists: {REDSHIFT_ROLE_NAME}")

    iam.put_role_policy(
        RoleName=REDSHIFT_ROLE_NAME,
        PolicyName="RedshiftS3TempReadPolicy",
        PolicyDocument=json.dumps(s3_policy)
    )
    print("   ✅ S3 read policy attached")

    # Wait for IAM propagation
    time.sleep(10)

    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_ROLE_NAME}"
    return role_arn


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Attach IAM Role to Redshift Cluster
# ═══════════════════════════════════════════════════════════════════
def attach_role_to_redshift(role_arn):
    """
    Redshift cluster must have the IAM Role associated
    Otherwise COPY command fails with: "IAM Role not found on cluster"
    """
    print(f"\n🔗 Attaching IAM Role to Redshift cluster...")

    cluster_id = "quickcart-dwh"

    try:
        redshift.modify_cluster_iam_roles(
            ClusterIdentifier=cluster_id,
            AddIamRoles=[role_arn]
        )
        print(f"   ✅ Role {REDSHIFT_ROLE_NAME} attached to cluster {cluster_id}")
        print(f"   ⏳ Takes 1-2 minutes to propagate...")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  Role already attached to cluster")
        else:
            print(f"   ⚠️  {e}")
            print(f"   → May need to attach manually via Console")


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Update Glue IAM Role (add Redshift temp S3 access)
# ═══════════════════════════════════════════════════════════════════
def update_glue_role_for_redshift():
    """
    Glue's IAM Role (from Project 31) needs ADDITIONAL permissions:
    → Write temp CSV files to S3 for Redshift COPY
    → Already has S3 write on bronze/* — need tmp/glue-redshift/*
    
    Project 31 role already has s3:PutObject on bucket/*
    So this may already be covered — but being explicit is better
    """
    print("\n🔐 Updating Glue IAM Role for Redshift temp directory...")

    additional_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "RedshiftTempS3Access",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET}/tmp/glue-redshift/*"
                ]
            }
        ]
    }

    iam.put_role_policy(
        RoleName=GLUE_ROLE_NAME,
        PolicyName="GlueRedshiftTempAccess",
        PolicyDocument=json.dumps(additional_policy)
    )
    print(f"   ✅ Added temp S3 write policy to {GLUE_ROLE_NAME}")


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Create Glue Connection to Redshift
# ═══════════════════════════════════════════════════════════════════
def create_redshift_connection():
    """
    Create Glue JDBC Connection for Redshift
    
    Similar to MariaDB connection (Project 31) but:
    → Different JDBC URL format (jdbc:redshift://...)
    → Different port (5439 vs 3306)
    → Different driver class
    → Same VPC, same Glue SG
    """
    print("\n🔗 Creating Glue Redshift Connection...")

    jdbc_url = (
        f"jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}"
        f"/{REDSHIFT_DATABASE}"
    )

    connection_input = {
        "Name": GLUE_REDSHIFT_CONNECTION,
        "Description": "JDBC connection to QuickCart Redshift warehouse",
        "ConnectionType": "JDBC",
        "ConnectionProperties": {
            "JDBC_CONNECTION_URL": jdbc_url,
            "USERNAME": REDSHIFT_USERNAME,
            "PASSWORD": REDSHIFT_PASSWORD,
            "JDBC_DRIVER_CLASS_NAME": "com.amazon.redshift.jdbc42.Driver",
            "JDBC_ENFORCE_SSL": "true"
        },
        "PhysicalConnectionRequirements": {
            "SubnetId": PRIVATE_SUBNET_ID,
            "SecurityGroupIdList": [GLUE_SG_ID],
            "AvailabilityZone": f"{AWS_REGION}a"
        }
    }

    try:
        glue.create_connection(ConnectionInput=connection_input)
        print(f"   ✅ Connection created: {GLUE_REDSHIFT_CONNECTION}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_connection(
            Name=GLUE_REDSHIFT_CONNECTION,
            ConnectionInput=connection_input
        )
        print(f"   ✅ Connection updated: {GLUE_REDSHIFT_CONNECTION}")

    print(f"   → JDBC URL: {jdbc_url}")


# ═══════════════════════════════════════════════════════════════════
# STEP 6: Print Redshift DDL (run on Redshift)
# ═══════════════════════════════════════════════════════════════════
def print_redshift_ddl():
    """Print SQL to create target tables in Redshift"""
    print("\n📋 Run these SQL commands ON Redshift:")
    print("=" * 70)
    print(f"""
-- ═══════════════════════════════════════════════════════════════
-- Connect to Redshift via Query Editor or psql
-- ═══════════════════════════════════════════════════════════════

-- Create schema for analytics layer
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create Glue loader user
CREATE USER glue_loader PASSWORD[REDACTED:PASSWORD]25';
GRANT ALL ON SCHEMA analytics TO glue_loader;
GRANT ALL ON ALL TABLES IN SCHEMA analytics TO glue_loader;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics 
    GRANT ALL ON TABLES TO glue_loader;

-- ═══════════════════════════════════════════════════════════════
-- FACT TABLE: orders
-- DISTKEY: customer_id (most common JOIN column)
-- SORTKEY: order_date (most common WHERE filter)
-- ═══════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id            VARCHAR(30)     NOT NULL,
    customer_id         VARCHAR(30)     NOT NULL,
    product_id          VARCHAR(30),
    quantity            INTEGER,
    unit_price          DOUBLE PRECISION,
    total_amount        DOUBLE PRECISION,
    status              VARCHAR(20),
    order_date          DATE,
    _extraction_date    VARCHAR(10),
    _extraction_timestamp VARCHAR(30),
    _source_system      VARCHAR(20),
    _source_database    VARCHAR(50),
    _source_table       VARCHAR(50),
    PRIMARY KEY (order_id)
)
DISTKEY(customer_id)
COMPOUND SORTKEY(order_date, status);

-- ═══════════════════════════════════════════════════════════════
-- DIMENSION TABLE: customers
-- DISTKEY: customer_id (enables co-located JOINs with fact_orders)
-- SORTKEY: signup_date
-- ═══════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_id         VARCHAR(30)     NOT NULL,
    name                VARCHAR(100),
    email               VARCHAR(200),
    city                VARCHAR(100),
    state               VARCHAR(50),
    tier                VARCHAR(20),
    signup_date         DATE,
    _extraction_date    VARCHAR(10),
    _extraction_timestamp VARCHAR(30),
    _source_system      VARCHAR(20),
    _source_database    VARCHAR(50),
    _source_table       VARCHAR(50),
    PRIMARY KEY (customer_id)
)
DISTKEY(customer_id)
SORTKEY(signup_date);

-- ═══════════════════════════════════════════════════════════════
-- DIMENSION TABLE: products
-- DISTSTYLE ALL (small table — replicate to all nodes)
-- ═══════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_id          VARCHAR(30)     NOT NULL,
    name                VARCHAR(200),
    category            VARCHAR(100),
    price               DOUBLE PRECISION,
    stock_quantity      INTEGER,
    is_active           VARCHAR(10),
    _extraction_date    VARCHAR(10),
    _extraction_timestamp VARCHAR(30),
    _source_system      VARCHAR(20),
    _source_database    VARCHAR(50),
    _source_table       VARCHAR(50),
    PRIMARY KEY (product_id)
)
DISTSTYLE ALL
SORTKEY(category);

-- Verify tables
SELECT schemaname, tablename, diststyle, sortkey1 
FROM pg_table_def 
JOIN svv_table_info ON tablename = "table"
WHERE schemaname = 'analytics';
""")
    print("=" * 70)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 SETTING UP GLUE → REDSHIFT INFRASTRUCTURE")
    print("=" * 60)

    configure_security_groups()
    role_arn = create_redshift_s3_role()
    attach_role_to_redshift(role_arn)
    update_glue_role_for_redshift()
    create_redshift_connection()
    print_redshift_ddl()

    print("\n" + "=" * 60)
    print("✅ INFRASTRUCTURE READY FOR GLUE → REDSHIFT")
    print("=" * 60)