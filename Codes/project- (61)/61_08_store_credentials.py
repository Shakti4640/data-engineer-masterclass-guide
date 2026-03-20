# file: 61_08_store_credentials.py
# Run from: Account B (222222222222)
# Purpose: Store MariaDB credentials securely for Glue to reference
# Builds on: Project 70 concepts (Secrets Manager)

import boto3
import json

REGION = "us-east-2"
SECRET_NAME = "quickcart/cross-account/mariadb-glue-reader"


def store_mariadb_credentials():
    """
    WHY SECRETS MANAGER:
    → Glue Connection can reference a secret ARN
    → Credentials never appear in Glue job code
    → Rotation possible without changing any Glue config
    → Audit trail via CloudTrail: who accessed what, when
    
    ALTERNATIVE: Glue Connection stores password directly
    → Works but: password visible in Glue console
    → No rotation capability
    → No audit trail of credential access
    """
    sm = boto3.client("secretsmanager", region_name=REGION)

    secret_value = {
        "username": "glue_reader",
        "password": "[REDACTED:PASSWORD]",
        "host": "10.1.3.50",
        "port": 3306,
        "database": "quickcart",
        "engine": "mariadb"
    }

    try:
        response = sm.create_secret(
            Name=SECRET_NAME,
            Description="MariaDB credentials for cross-account Glue ETL (Account A source)",
            SecretString=json.dumps(secret_value),
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Source", "Value": "AccountA-MariaDB"},
                {"Key": "Consumer", "Value": "GlueETL"},
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print(f"✅ Secret created: {response['Name']}")
        print(f"   ARN: {response['ARN']}")
        return response["ARN"]

    except sm.exceptions.ResourceExistsException:
        # Update existing secret
        sm.update_secret(
            SecretId=SECRET_NAME,
            SecretString=json.dumps(secret_value)
        )
        print(f"ℹ️  Secret updated: {SECRET_NAME}")

        # Get ARN
        desc = sm.describe_secret(SecretId=SECRET_NAME)
        return desc["ARN"]


if __name__ == "__main__":
    arn = store_mariadb_credentials()
    print(f"\n📋 Use this ARN in Glue Connection: {arn}")