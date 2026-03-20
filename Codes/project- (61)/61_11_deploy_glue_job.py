# file: 61_11_deploy_glue_job.py
# Run from: Account B (222222222222)
# Purpose: Create the Glue job definition and upload script

import boto3

REGION = "us-east-2"
ACCOUNT_B_ID = [REDACTED:BANK_ACCOUNT_NUMBER]2"
GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_B_ID}:role/GlueCrossAccountETLRole"
SCRIPT_S3_PATH = "s3://quickcart-analytics-scripts/glue/cross_account_mariadb_to_redshift.py"
TEMP_DIR = "s3://quickcart-analytics-temp/glue-redshift-staging/"


def upload_script():
    """Upload Glue script to S3"""
    s3 = boto3.client("s3", region_name=REGION)

    with open("61_10_glue_etl_job.py", "rb") as f:
        s3.upload_fileobj(
            f,
            "quickcart-analytics-scripts",
            "glue/cross_account_mariadb_to_redshift.py"
        )
    print("✅ Glue script uploaded to S3")


def create_glue_job():
    """
    Create the Glue job definition
    
    KEY SETTINGS:
    → Connections: BOTH MariaDB and Redshift connections
      → Glue creates ENIs for ALL listed connections
      → ENI in Glue subnet can reach BOTH MariaDB (via peering) and Redshift (local)
    → Worker Type: G.1X (4 vCPU, 16 GB) — sufficient for GB-scale
    → NumberOfWorkers: 4 — matches hashpartitions in extract
    → Timeout: 60 minutes — safety net for hung jobs
    """
    glue = boto3.client("glue", region_name=REGION)

    try:
        glue.create_job(
            Name="cross-account-mariadb-to-redshift",
            Description=(
                "Nightly ETL: Extract from Account A MariaDB, "
                "transform, load to Account B Redshift"
            ),
            Role=GLUE_ROLE_ARN,
            Command={
                "Name": "glueetl",
                "ScriptLocation": SCRIPT_S3_PATH,
                "PythonVersion": "3"
            },
            DefaultArguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-disable",  # Full load
                "--TempDir": TEMP_DIR,
                "--redshift_temp_dir": TEMP_DIR,
                "--target_schema": "quickcart_ops",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://quickcart-analytics-logs/sparkui/"
            },
            Connections={
                "Connections": [
                    "conn-cross-account-mariadb",
                    "conn-redshift-analytics"
                ]
            },
            GlueVersion="4.0",
            WorkerType="G.1X",
            NumberOfWorkers=4,
            Timeout=60,  # minutes
            MaxRetries=1,
            Tags={
                "Environment": "Production",
                "Pipeline": "CrossAccountETL",
                "Source": "AccountA-MariaDB",
                "Target": "AccountB-Redshift",
                "ManagedBy": "DataEngineering"
            }
        )
        print("✅ Glue job created: cross-account-mariadb-to-redshift")
        print(f"   Script: {SCRIPT_S3_PATH}")
        print(f"   Workers: 4 × G.1X")
        print(f"   Connections: MariaDB (cross-account) + Redshift (local)")

    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Job already exists — updating...")
        glue.update_job(
            JobName="cross-account-mariadb-to-redshift",
            JobUpdate={
                "Role": GLUE_ROLE_ARN,
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": SCRIPT_S3_PATH,
                    "PythonVersion": "3"
                },
                "DefaultArguments": {
                    "--job-language": "python",
                    "--job-bookmark-option": "job-bookmark-disable",
                    "--TempDir": TEMP_DIR,
                    "--redshift_temp_dir": TEMP_DIR,
                    "--target_schema": "quickcart_ops",
                    "--enable-metrics": "true",
                    "--enable-continuous-cloudwatch-log": "true"
                },
                "Connections": {
                    "Connections": [
                        "conn-cross-account-mariadb",
                        "conn-redshift-analytics"
                    ]
                },
                "GlueVersion": "4.0",
                "WorkerType": "G.1X",
                "NumberOfWorkers": 4,
                "Timeout": 60,
                "MaxRetries": 1
            }
        )
        print("✅ Job updated")


def create_glue_iam_role():
    """
    IAM Role for the Glue job
    
    NEEDS:
    → AWSGlueServiceRole (managed policy — for Glue internals)
    → S3 access: script bucket + temp bucket + output bucket
    → Secrets Manager: read credentials (if using secret reference)
    → CloudWatch Logs: write job logs
    → Redshift: no IAM needed — uses JDBC credentials
    → MariaDB: no IAM needed — uses JDBC credentials
    
    DOES NOT NEED:
    → Cross-account STS AssumeRole — network path handles cross-account
    → The VPC Peering carries traffic; IAM doesn't control network routing
    """
    iam = boto3.client("iam", region_name=REGION)

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

    # Custom policy for S3 + Secrets Manager access
    custom_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ScriptAndTempAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::quickcart-analytics-scripts/*",
                    "arn:aws:s3:::quickcart-analytics-scripts",
                    "arn:aws:s3:::quickcart-analytics-temp/*",
                    "arn:aws:s3:::quickcart-analytics-temp",
                    "arn:aws:s3:::quickcart-analytics-logs/*",
                    "arn:aws:s3:::quickcart-analytics-logs"
                ]
            },
            {
                "Sid": "SecretsManagerRead",
                "Effect": "Allow",
                "Action": [
                    "secretsmanager:GetSecretValue"
                ],
                "Resource": [
                    f"arn:aws:secretsmanager:{REGION}:{ACCOUNT_B_ID}:secret:quickcart/*"
                ]
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
            }
        ]
    }

    import json

    try:
        # Create role
        iam.create_role(
            RoleName="GlueCrossAccountETLRole",
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Glue cross-account MariaDB to Redshift ETL",
            Tags=[
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print("✅ IAM Role created: GlueCrossAccountETLRole")
    except iam.exceptions.EntityAlreadyExistsException:
        print("ℹ️  Role already exists")

    # Attach managed policy
    iam.attach_role_policy(
        RoleName="GlueCrossAccountETLRole",
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    )
    print("✅ Attached: AWSGlueServiceRole")

    # Attach custom policy
    try:
        iam.put_role_policy(
            RoleName="GlueCrossAccountETLRole",
            PolicyName="GlueCrossAccountETLCustomPolicy",
            PolicyDocument=json.dumps(custom_policy)
        )
        print("✅ Attached: Custom S3 + Secrets Manager policy")
    except Exception as e:
        print(f"⚠️  Policy attachment: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 DEPLOYING CROSS-ACCOUNT GLUE JOB")
    print("=" * 60)

    create_glue_iam_role()
    print()
    upload_script()
    print()
    create_glue_job()