# file: 46_02_deploy_connector.py
# Purpose: Deploy the Athena JDBC connector Lambda function
# Uses AWS SAR (Serverless Application Repository) pre-built connector

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# Configuration
CONNECTOR_NAME = "quickcart-mariadb-connector"
SPILL_BUCKET = "quickcart-athena-spill-prod"
SECRET_NAME = "quickcart/mariadb/athena-reader"

# VPC configuration (must match MariaDB's VPC)
VPC_SUBNET_IDS = "subnet-0abc1234,subnet-0def5678"  # Private subnets
VPC_SECURITY_GROUP_ID = "sg-0fedquery1234"           # Allows outbound to MariaDB


def create_spill_bucket():
    """Create S3 bucket for Lambda spill data"""
    s3_client = boto3.client("s3", region_name=REGION)

    try:
        if REGION == "us-east-1":
            s3_client.create_bucket(Bucket=SPILL_BUCKET)
        else:
            s3_client.create_bucket(
                Bucket=SPILL_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": REGION}
            )
        print(f"✅ Spill bucket created: {SPILL_BUCKET}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"ℹ️  Spill bucket exists: {SPILL_BUCKET}")

    # Lifecycle: delete spill files after 1 day
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=SPILL_BUCKET,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "delete-spill-data",
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
                "Expiration": {"Days": 1}
            }]
        }
    )
    print(f"   ✅ Lifecycle: delete after 1 day")

    # Encryption
    s3_client.put_bucket_encryption(
        Bucket=SPILL_BUCKET,
        ServerSideEncryptionConfiguration={
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                "BucketKeyEnabled": True
            }]
        }
    )
    print(f"   ✅ Encryption enabled")


def create_connector_iam_role():
    """Create IAM role for the Lambda connector"""
    iam_client = boto3.client("iam")
    role_name = f"{CONNECTOR_NAME}-role"

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "SecretsManagerAccess",
                "Effect": "Allow",
                "Action": ["secretsmanager:GetSecretValue"],
                "Resource": f"arn:aws:secretsmanager:{REGION}:{ACCOUNT_ID}:secret:quickcart/*"
            },
            {
                "Sid": "SpillBucketAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject", "s3:PutObject",
                    "s3:ListBucket", "s3:DeleteObject",
                    "s3:GetBucketLocation"
                ],
                "Resource": [
                    f"arn:aws:s3:::{SPILL_BUCKET}",
                    f"arn:aws:s3:::{SPILL_BUCKET}/*"
                ]
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup", "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:*"
            },
            {
                "Sid": "VPCAccess",
                "Effect": "Allow",
                "Action": [
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface"
                ],
                "Resource": "*"
            },
            {
                "Sid": "AthenaFederationAccess",
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
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Lambda role for Athena MySQL/MariaDB federated connector"
        )
        print(f"✅ IAM role created: {role_name}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  IAM role exists: {role_name}")

    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName="ConnectorPermissions",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"   ✅ Permissions attached")

    time.sleep(10)  # Wait for IAM propagation
    return f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"


def deploy_connector_lambda(role_arn):
    """
    Deploy the Athena JDBC connector as a Lambda function.
    
    In production, use SAR (Serverless Application Repository):
    → aws serverlessrepo create-cloud-formation-change-set \
        --application-id arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaJdbcConnector
    
    Here we create the Lambda function directly with the connector JAR.
    The JAR is available from:
    → https://github.com/awslabs/aws-athena-query-federation
    → Pre-built: s3://athena-federation-{region}/connectors/
    """
    lambda_client = boto3.client("lambda", region_name=REGION)

    # Connection string format for MySQL/MariaDB connector
    # The connector reads credentials from Secrets Manager
    connection_string = (
        f"mysql://jdbc:mysql://10.0.1.50:3306/quickcart?"
        f"${{{SECRET_NAME}}}"
    )

    try:
        response = lambda_client.create_function(
            FunctionName=CONNECTOR_NAME,
            Description="Athena federated query connector for QuickCart MariaDB",
            Runtime="java11",
            Role=role_arn,
            Handler="com.amazonaws.athena.connectors.jdbc.MultiplexingJdbcCompositeHandler",
            Code={
                # In production: upload the connector JAR to S3 first
                # Here we reference the SAR-deployed package
                "S3Bucket": f"athena-federation-{REGION}",
                "S3Key": "connectors/athena-jdbc-2023.47.1.jar"
            },
            Timeout=900,          # 15 minutes max
            MemorySize=3072,      # 3 GB — handles ~500K rows
            Environment={
                "Variables": {
                    # Connection string with Secrets Manager reference
                    "default": connection_string,
                    "spill_bucket": SPILL_BUCKET,
                    "spill_prefix": "athena-spill",
                    "disable_spill_encryption": "false"
                }
            },
            VpcConfig={
                "SubnetIds": VPC_SUBNET_IDS.split(","),
                "SecurityGroupIds": [VPC_SECURITY_GROUP_ID]
            },
            Tags={
                "Environment": "Production",
                "Project": "QuickCart-FederatedQuery"
            }
        )
        print(f"✅ Lambda connector deployed: {CONNECTOR_NAME}")
        print(f"   ARN: {response['FunctionArn']}")
        return response["FunctionArn"]

    except lambda_client.exceptions.ResourceConflictException:
        # Update existing
        lambda_client.update_function_configuration(
            FunctionName=CONNECTOR_NAME,
            Timeout=900,
            MemorySize=3072,
            Environment={
                "Variables": {
                    "default": connection_string,
                    "spill_bucket": SPILL_BUCKET,
                    "spill_prefix": "athena-spill",
                    "disable_spill_encryption": "false"
                }
            }
        )
        response = lambda_client.get_function(FunctionName=CONNECTOR_NAME)
        arn = response["Configuration"]["FunctionArn"]
        print(f"ℹ️  Lambda connector updated: {CONNECTOR_NAME}")
        print(f"   ARN: {arn}")
        return arn


def set_lambda_concurrency():
    """
    Set reserved concurrency to protect MariaDB from too many connections.
    
    10 concurrent = max 10 federated queries simultaneously
    = max 10 MariaDB connections from federated queries
    """
    lambda_client = boto3.client("lambda", region_name=REGION)

    lambda_client.put_function_concurrency(
        FunctionName=CONNECTOR_NAME,
        ReservedConcurrentExecutions=10
    )
    print(f"✅ Lambda concurrency limited to 10")


if __name__ == "__main__":
    print("=" * 60)
    print("🔧 DEPLOYING ATHENA FEDERATED QUERY CONNECTOR")
    print("=" * 60)

    create_spill_bucket()
    role_arn = create_connector_iam_role()
    connector_arn = deploy_connector_lambda(role_arn)
    set_lambda_concurrency()

    print(f"\n✅ Connector ready: {connector_arn}")
    print(f"   Next: Register as Athena data source")