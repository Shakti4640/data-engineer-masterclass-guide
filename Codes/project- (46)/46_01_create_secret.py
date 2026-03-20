# file: 46_01_create_secret.py
# Purpose: Store MariaDB read-only credentials in Secrets Manager
# Lambda connector reads these at runtime — no hardcoded passwords

import boto3
import json

REGION = "us-east-2"
SECRET_NAME = "quickcart/mariadb/athena-reader"


def create_mariadb_secret():
    """
    Store MariaDB credentials for the Athena federated query connector.
    
    Format required by Athena JDBC connector:
    → Must contain: username, password, host, port, database
    → Connector reads these keys automatically
    """
    sm_client = boto3.client("secretsmanager", region_name=REGION)

    secret_value = {
        "username": "athena_reader",
        "password": [REDACTED:PASSWORD]!",
        "engine": "mysql",
        "host": "10.0.1.50",
        "port": "3306",
        "dbname": "quickcart"
    }

    try:
        response = sm_client.create_secret(
            Name=SECRET_NAME,
            Description="MariaDB read-only credentials for Athena federated query",
            SecretString=json.dumps(secret_value),
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Project", "Value": "QuickCart-FederatedQuery"},
                {"Key": "Service", "Value": "AthenaConnector"}
            ]
        )
        print(f"✅ Secret created: {SECRET_NAME}")
        print(f"   ARN: {response['ARN']}")
        return response["ARN"]

    except sm_client.exceptions.ResourceExistsException:
        # Update existing
        sm_client.put_secret_value(
            SecretId=SECRET_NAME,
            SecretString=json.dumps(secret_value)
        )
        response = sm_client.describe_secret(SecretId=SECRET_NAME)
        print(f"ℹ️  Secret updated: {SECRET_NAME}")
        print(f"   ARN: {response['ARN']}")
        return response["ARN"]


def create_readonly_mariadb_user():
    """
    SQL to create read-only MariaDB user.
    Run this DIRECTLY on MariaDB (not via this script).
    """
    sql = """
    -- Run these commands on MariaDB directly:
    
    CREATE USER IF NOT EXISTS 'athena_reader'@'%' 
        IDENTIFIED BY 'ReadOnly_Secure_2025!';
    
    GRANT SELECT ON quickcart.* TO 'athena_reader'@'%';
    
    -- Verify:
    SHOW GRANTS FOR 'athena_reader'@'%';
    
    -- Expected output:
    -- GRANT SELECT ON `quickcart`.* TO `athena_reader`@`%`
    """
    print(f"\n📋 Run this SQL on MariaDB to create read-only user:")
    print(sql)


if __name__ == "__main__":
    secret_arn = create_mariadb_secret()
    create_readonly_mariadb_user()