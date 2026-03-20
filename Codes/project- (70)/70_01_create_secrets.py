# file: 70_01_create_secrets.py
# Run from: Account B[REDACTED:BANK_ACCOUNT_NUMBER]22) for Redshift secret
# Run from: Account A (111111111111) for MariaDB secret
# Purpose: Migrate hardcoded credentials to Secrets Manager

import boto3
import json
import string
import secrets as py_secrets

REGION = "us-east-2"


def generate_strong_password(length=32):
    """Generate cryptographically strong password"""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()_+-="
    # Ensure at least one of each character type
    password = [
        py_secrets.choice(string.ascii_uppercase),
        py_secrets.choice(string.ascii_lowercase),
        py_secrets.choice(string.digits),
        py_secrets.choice("!@#$%^&*()_+-=")
    ]
    password += [py_secrets.choice(alphabet) for _ in range(length - 4)]
    py_secrets.SystemRandom().shuffle(password)
    return "".join(password)


def create_mariadb_secret():
    """
    Create secret for MariaDB glue_reader credentials
    
    RUN FROM: Account A (where MariaDB lives)
    → Secret lives close to database
    → Account B reads via cross-account access (or replicate)
    
    ALTERNATIVELY: Create in Account B if simpler
    → Account B's Glue reads from Account B's Secrets Manager
    → Rotation Lambda in Account B connects to Account A's MariaDB via VPC Peering
    """
    sm = boto3.client("secretsmanager", region_name=REGION)

    secret_name = "quickcart/prod/mariadb/glue-reader"

    secret_value = {
        "engine": "mysql",
        "host": "10.1.3.50",
        "port": 3306,
        "dbname": "quickcart",
        "username": "glue_reader_a",
        "password": generate_strong_password(),
        # Alternating user for rotation
        "alternating_username": "glue_reader_b",
        # Admin credentials for rotation Lambda to change passwords
        "masterarn": "quickcart/prod/mariadb/admin"
    }

    try:
        response = sm.create_secret(
            Name=secret_name,
            Description="MariaDB read-only credentials for Glue ETL pipelines",
            SecretString=json.dumps(secret_value),
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Service", "Value": "MariaDB"},
                {"Key": "Purpose", "Value": "GlueETL"},
                {"Key": "RotationSchedule", "Value": "90days"},
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print(f"✅ Secret created: {secret_name}")
        print(f"   ARN: {response['ARN']}")
        return response["ARN"]

    except sm.exceptions.ResourceExistsException:
        print(f"ℹ️  Secret already exists: {secret_name}")
        desc = sm.describe_secret(SecretId=secret_name)
        return desc["ARN"]


def create_mariadb_admin_secret():
    """
    Admin secret used by rotation Lambda to change passwords
    This secret is NOT rotated automatically — managed manually
    """
    sm = boto3.client("secretsmanager", region_name=REGION)

    secret_name = "quickcart/prod/mariadb/admin"

    secret_value = {
        "engine": "mysql",
        "host": "10.1.3.50",
        "port": 3306,
        "dbname": "quickcart",
        "username": "root",
        "password": "[REDACTED:PASSWORD]"  # In production: generate and store securely
    }

    try:
        response = sm.create_secret(
            Name=secret_name,
            Description="MariaDB admin credentials — used by rotation Lambda only",
            SecretString=json.dumps(secret_value),
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Service", "Value": "MariaDB"},
                {"Key": "Purpose", "Value": "RotationAdmin"},
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print(f"✅ Admin secret created: {secret_name}")
        return response["ARN"]

    except sm.exceptions.ResourceExistsException:
        print(f"ℹ️  Admin secret already exists: {secret_name}")
        desc = sm.describe_secret(SecretId=secret_name)
        return desc["ARN"]


def create_redshift_secret():
    """
    Create secret for Redshift etl_writer credentials
    
    RUN FROM: Account B (where Redshift lives)
    """
    sm = boto3.client("secretsmanager", region_name=REGION)

    secret_name = "quickcart/prod/redshift/etl-writer"

    # For Serverless (Project 67)
    workgroup_name = "quickcart-prod"
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    secret_value = {
        "engine": "redshift",
        "host": f"{workgroup_name}.{account_id}.{REGION}.redshift-serverless.amazonaws.com",
        "port": 5439,
        "dbname": "analytics",
        "username": "etl_writer",
        "password": generate_strong_password(),
        # Redshift-specific
        "dbClusterIdentifier": workgroup_name
    }

    try:
        response = sm.create_secret(
            Name=secret_name,
            Description="Redshift ETL writer credentials for Glue and Lambda",
            SecretString=json.dumps(secret_value),
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Service", "Value": "Redshift"},
                {"Key": "Purpose", "Value": "ETLWriter"},
                {"Key": "RotationSchedule", "Value": "90days"},
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print(f"✅ Secret created: {secret_name}")
        print(f"   ARN: {response['ARN']}")
        return response["ARN"]

    except sm.exceptions.ResourceExistsException:
        print(f"ℹ️  Secret already exists: {secret_name}")
        desc = sm.describe_secret(SecretId=secret_name)
        return desc["ARN"]


def set_resource_policies():
    """
    Set resource policy on each secret: who can read it
    
    PRINCIPLE: each role can ONLY read the secrets it needs
    → GlueCrossAccountETLRole → mariadb/glue-reader + redshift/etl-writer
    → LambdaMVRefreshRole → redshift/etl-writer only
    → AthenaConnectorLambdaRole → mariadb/glue-reader only
    """
    sm = boto3.client("secretsmanager", region_name=REGION)
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    policies = [
        {
            "secret": "quickcart/prod/mariadb/glue-reader",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "AllowGlueAndAthenaRead",
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": [
                                f"arn:aws:iam::{account_id}:role/GlueCrossAccountETLRole",
                                f"arn:aws:iam::{account_id}:role/AthenaConnectorLambdaRole"
                            ]
                        },
                        "Action": ["secretsmanager:GetSecretValue"],
                        "Resource": "*"
                    }
                ]
            }
        },
        {
            "secret": "quickcart/prod/redshift/etl-writer",
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "AllowGlueAndLambdaRead",
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": [
                                f"arn:aws:iam::{account_id}:role/GlueCrossAccountETLRole",
                                f"arn:aws:iam::{account_id}:role/LambdaMVRefreshRole",
                                f"arn:aws:iam::{account_id}:role/StepFunctionsEventDrivenPipelineRole"
                            ]
                        },
                        "Action": ["secretsmanager:GetSecretValue"],
                        "Resource": "*"
                    }
                ]
            }
        }
    ]

    for p in policies:
        try:
            sm.put_resource_policy(
                SecretId=p["secret"],
                ResourcePolicy=json.dumps(p["policy"])
            )
            print(f"✅ Resource policy set: {p['secret']}")
        except Exception as e:
            print(f"⚠️  {p['secret']}: {str(e)[:100]}")


if __name__ == "__main__":
    print("=" * 60)
    print("🔐 CREATING SECRETS IN SECRETS MANAGER")
    print("=" * 60)

    create_mariadb_admin_secret()
    print()
    mariadb_arn = create_mariadb_secret()
    print()
    redshift_arn = create_redshift_secret()
    print()
    set_resource_policies()

    print(f"\n{'='*60}")
    print(f"📋 SECRET ARNs (use these in service configurations):")
    print(f"   MariaDB: {mariadb_arn}")
    print(f"   Redshift: {redshift_arn}")
    print(f"{'='*60}")