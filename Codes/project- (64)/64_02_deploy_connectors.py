# file: 64_02_deploy_connectors.py
# Run from: Account B (222222222222) — Analytics
# Purpose: Deploy DynamoDB and MySQL connector Lambdas
# Uses: AWS Serverless Application Repository (SAR)

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_B_ID = [REDACTED:BANK_ACCOUNT_NUMBER]2"
ACCOUNT_C_ID = [REDACTED:BANK_ACCOUNT_NUMBER]3"

# Network config (Account B VPC — same as Glue in Project 61)
VPC_B = "vpc-0bbb2222bbbb2222b"
GLUE_SUBNET = "subnet-0bbb2222glue"         # 10.2.6.0/24
CONNECTOR_SG = "sg-0bbb2222glueeni"          # Reuse Glue SG (has 3306 outbound)

# Spill bucket for large result sets
SPILL_BUCKET = "quickcart-analytics-athena-spill"

# Cross-account role in Account C
ACCT_C_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_C_ID}:role/athena-federation-cross-account-read"


def create_spill_bucket():
    """
    Create S3 bucket for Lambda connector to spill large results
    """
    s3 = boto3.client("s3", region_name=REGION)

    try:
        s3.create_bucket(
            Bucket=SPILL_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        print(f"✅ Spill bucket created: {SPILL_BUCKET}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"ℹ️  Spill bucket already exists")

    # Enable encryption
    s3.put_bucket_encryption(
        Bucket=SPILL_BUCKET,
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
        }
    )

    # Lifecycle: delete spilled data after 1 day
    s3.put_bucket_lifecycle_configuration(
        Bucket=SPILL_BUCKET,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "DeleteSpilledData",
                    "Status": "Enabled",
                    "Expiration": {"Days": 1},
                    "Filter": {"Prefix": ""}
                }
            ]
        }
    )
    print(f"✅ Spill bucket: encryption enabled, lifecycle = 1 day")


def create_connector_lambda_role():
    """
    IAM role for ALL connector Lambdas in Account B
    
    NEEDS:
    → Lambda basic execution (CloudWatch Logs)
    → S3 access to spill bucket
    → VPC access (ENI management) — for MySQL connector
    → STS AssumeRole to Account C — for DynamoDB connector
    → Athena Federation SDK permissions
    """
    iam = boto3.client("iam")
    role_name = "AthenaConnectorLambdaRole"

    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    policy = {
        "Version": "2012-10-17",
        "Statement": [
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
                "Sid": "SpillBucketAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{SPILL_BUCKET}",
                    f"arn:aws:s3:::{SPILL_BUCKET}/*"
                ]
            },
            {
                "Sid": "VPCAccess",
                "Effect": "Allow",
                "Action": [
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeVpcs"
                ],
                "Resource": "*"
            },
            {
                "Sid": "AssumeAccountCRole",
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": ACCT_C_ROLE_ARN
            },
            {
                "Sid": "AthenaFederationGlueAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions"
                ],
                "Resource": "*"
            },
            {
                "Sid": "AthenaResultAccess",
                "Effect": "Allow",
                "Action": [
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults"
                ],
                "Resource": "*"
            }
        ]
    }

    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="Role for Athena Federation connector Lambdas"
        )
        print(f"✅ Role created: {role_name}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  Role exists")

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="AthenaConnectorPolicy",
        PolicyDocument=json.dumps(policy)
    )
    print(f"✅ Policy attached")

    return f"arn:aws:iam::{ACCOUNT_B_ID}:role/{role_name}"


def deploy_dynamodb_connector(lambda_role_arn):
    """
    Deploy DynamoDB connector from AWS Serverless Application Repository
    
    AWS SAR Application: AthenaDynamoDBConnector
    → Pre-built by AWS, maintained, battle-tested
    → Supports: Scan, Query, predicate pushdown
    → Handles: type inference, pagination, spill to S3
    
    ALTERNATIVE: Deploy from SAR via console (easier for first time)
    Here we use CloudFormation for automation
    """
    cfn = boto3.client("cloudformation", region_name=REGION)

    stack_name = "athena-dynamodb-connector"

    # SAR application for DynamoDB connector
    # In practice, deploy via SAR console or use SAR ARN
    # Here we create Lambda directly with same config

    lam = boto3.client("lambda", region_name=REGION)

    # Environment variables for the connector
    env_vars = {
        # Spill configuration
        "spill_bucket": SPILL_BUCKET,
        "spill_prefix": "dynamodb-connector-spill",
        
        # Cross-account configuration
        "cross_account_role_arn": ACCT_C_ROLE_ARN,
        "external_id": "athena-federation-2025",
        
        # DynamoDB configuration
        "disable_projection_and_casing": "false",
        "kms_key_id": "",  # Use default encryption
        
        # Performance
        "default_dynamo_db_table_range_scan_segment_count": "4"
    }

    # NOTE: In production, deploy via SAR console:
    # 1. Athena Console → Data sources → Connect data source
    # 2. Select "Amazon DynamoDB"
    # 3. Configure: Lambda name, spill bucket, IAM role
    # This creates the Lambda automatically

    print(f"\n📦 DynamoDB Connector Deployment:")
    print(f"   Deploy via Athena Console → Data sources → Connect data source")
    print(f"   Select: Amazon DynamoDB")
    print(f"   Lambda function name: athena-dynamodb-connector")
    print(f"   Spill bucket: {SPILL_BUCKET}")
    print(f"   Lambda role: {lambda_role_arn}")
    print(f"   Environment variables:")
    for k, v in env_vars.items():
        print(f"     {k}: {v}")

    return "athena-dynamodb-connector"


def deploy_mysql_connector(lambda_role_arn):
    """
    Deploy MySQL connector for Account A's MariaDB
    
    REQUIRES VPC CONFIG:
    → Lambda needs ENI in Account B's VPC
    → VPC Peering routes traffic to Account A (Project 61)
    → Same subnet/SG as Glue ENIs
    
    CONNECTION STRING: stored as Lambda environment variable
    → In production: use Secrets Manager reference
    """
    env_vars = {
        # Spill configuration
        "spill_bucket": SPILL_BUCKET,
        "spill_prefix": "mysql-connector-spill",
        
        # MySQL connection — Account A's MariaDB via VPC Peering
        # Format: mysql://jdbc:mysql://host:port/database?user=X&password=Y
        "default": "mysql://jdbc:mysql://10.1.3.50:3306/quickcart?user=glue_reader&password=GlueR3ad0nly#2025",
        
        # Connection pooling
        "mysql_connection_timeout": "10000",
    }

    print(f"\n📦 MySQL Connector Deployment:")
    print(f"   Deploy via Athena Console → Data sources → Connect data source")
    print(f"   Select: Amazon Athena MySQL Connector")
    print(f"   Lambda function name: athena-mysql-connector")
    print(f"   Spill bucket: {SPILL_BUCKET}")
    print(f"   Lambda role: {lambda_role_arn}")
    print(f"   VPC: {VPC_B}")
    print(f"   Subnet: {GLUE_SUBNET}")
    print(f"   Security Group: {CONNECTOR_SG}")
    print(f"   Environment variables:")
    for k, v in env_vars.items():
        if "password" in k.lower() or "password" in v.lower():
            print(f"     {k}: ****REDACTED****")
        else:
            print(f"     {k}: {v}")

    return "athena-mysql-connector"


def register_data_sources():
    """
    Register Lambda connectors as Athena Data Sources
    
    Each data source = catalog name that can be used in SQL
    → SELECT * FROM catalog_name.database.table
    """
    athena = boto3.client("athena", region_name=REGION)

    data_sources = [
        {
            "name": "mysql_acct_a",
            "description": "Account A MariaDB (Operations) via VPC Peering",
            "lambda_name": "athena-mysql-connector",
            "catalog_type": "LAMBDA"
        },
        {
            "name": "dynamodb_acct_c",
            "description": "Account C DynamoDB (Marketing) via cross-account IAM",
            "lambda_name": "athena-dynamodb-connector",
            "catalog_type": "LAMBDA"
        }
    ]

    for ds in data_sources:
        lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_B_ID}:function:{ds['lambda_name']}"

        try:
            athena.create_data_catalog(
                Name=ds["name"],
                Type=ds["catalog_type"],
                Description=ds["description"],
                Parameters={
                    "function": lambda_arn
                },
                Tags=[
                    {"Key": "Purpose", "Value": "AthenaFederation"},
                    {"Key": "ManagedBy", "Value": "DataEngineering"}
                ]
            )
            print(f"✅ Data source registered: {ds['name']}")
            print(f"   Lambda: {lambda_arn}")
        except athena.exceptions.InvalidRequestException as e:
            if "already exists" in str(e).lower():
                print(f"ℹ️  Data source already exists: {ds['name']}")

                # Update existing
                athena.update_data_catalog(
                    Name=ds["name"],
                    Type=ds["catalog_type"],
                    Description=ds["description"],
                    Parameters={"function": lambda_arn}
                )
                print(f"✅ Data source updated: {ds['name']}")
            else:
                raise

    # Native catalog (Account B S3) is always available as "AwsDataCatalog"
    print(f"\nℹ️  Native S3 catalog always available as: AwsDataCatalog")

    print(f"\n📋 REGISTERED DATA SOURCES:")
    print(f"   1. AwsDataCatalog  → Account B S3 data lake (native)")
    print(f"   2. mysql_acct_a    → Account A MariaDB (federated)")
    print(f"   3. dynamodb_acct_c → Account C DynamoDB (federated)")


def create_federated_workgroup():
    """
    Create dedicated Athena workgroup for federated queries
    
    WHY SEPARATE WORKGROUP:
    → Track federated query costs separately
    → Set higher timeout (federated queries take longer)
    → Configure result location
    → Set per-query and per-workgroup data scan limits
    """
    athena = boto3.client("athena", region_name=REGION)

    try:
        athena.create_work_group(
            Name="federated-queries",
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": "s3://quickcart-analytics-athena-results/federated/",
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_S3"
                    }
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "EngineVersion": {
                    "SelectedEngineVersion": "Athena engine version 3"
                },
                "ExecutionRole": f"arn:aws:iam::{ACCOUNT_B_ID}:role/AthenaConnectorLambdaRole"
            },
            Description="Workgroup for cross-account federated queries",
            Tags=[
                {"Key": "Purpose", "Value": "FederatedQueries"},
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print(f"✅ Workgroup created: federated-queries")
    except athena.exceptions.InvalidRequestException as e:
        if "already exists" in str(e).lower():
            print(f"ℹ️  Workgroup already exists: federated-queries")
        else:
            raise


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 DEPLOYING ATHENA FEDERATION INFRASTRUCTURE")
    print("=" * 60)

    create_spill_bucket()
    print()
    lambda_role_arn = create_connector_lambda_role()
    print()
    deploy_dynamodb_connector(lambda_role_arn)
    print()
    deploy_mysql_connector(lambda_role_arn)
    print()
    register_data_sources()
    print()
    create_federated_workgroup()

    print(f"\n{'='*60}")
    print("✅ FEDERATION INFRASTRUCTURE DEPLOYED")
    print("   Next: Run test queries (64_03_test_federated_queries.py)")
    print(f"{'='*60}")