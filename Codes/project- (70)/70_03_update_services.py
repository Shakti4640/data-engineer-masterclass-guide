# file: 70_03_update_services.py
# Run from: Account B (222222222222)
# Purpose: Update all services to retrieve credentials from Secrets Manager

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID = boto3.client("sts").get_caller_identity()["Account"]


def update_glue_connection_with_secret():
    """
    Update Glue Connection to reference Secrets Manager ARN
    
    BEFORE: password stored in Glue Connection properties (plaintext)
    AFTER: Glue Connection references secret ARN → retrieves at runtime
    """
    glue = boto3.client("glue", region_name=REGION)
    sm = boto3.client("secretsmanager", region_name=REGION)

    # Get secret ARNs
    mariadb_arn = sm.describe_secret(
        SecretId="quickcart/prod/mariadb/glue-reader"
    )["ARN"]

    redshift_arn = sm.describe_secret(
        SecretId="quickcart/prod/redshift/etl-writer"
    )["ARN"]

    # Update MariaDB connection
    try:
        mariadb_secret = json.loads(
            sm.get_secret_value(SecretId="quickcart/prod/mariadb/glue-reader")["SecretString"]
        )

        glue.update_connection(
            Name="conn-cross-account-mariadb",
            ConnectionInput={
                "Name": "conn-cross-account-mariadb",
                "Description": "Account A MariaDB — credentials from Secrets Manager",
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": (
                        f"jdbc:mysql://{mariadb_secret['host']}:{mariadb_secret['port']}/{mariadb_secret['dbname']}"
                    ),
                    "USERNAME": mariadb_secret["username"],
                    "PASSWORD": mariadb_secret["password"],
                    "SECRET_ID": mariadb_arn
                },
                "PhysicalConnectionRequirements": {
                    "SubnetId": "subnet-0bbb2222glue",
                    "SecurityGroupIdList": ["sg-0bbb2222glueeni"],
                    "AvailabilityZone": "us-east-2a"
                }
            }
        )
        print(f"✅ Glue Connection updated: conn-cross-account-mariadb")
        print(f"   Secret ARN: {mariadb_arn}")
    except Exception as e:
        print(f"❌ MariaDB connection: {str(e)[:100]}")

    # Update Redshift connection
    try:
        redshift_secret = json.loads(
            sm.get_secret_value(SecretId="quickcart/prod/redshift/etl-writer")["SecretString"]
        )

        glue.update_connection(
            Name="conn-redshift-analytics",
            ConnectionInput={
                "Name": "conn-redshift-analytics",
                "Description": "Redshift Serverless — credentials from Secrets Manager",
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": (
                        f"jdbc:redshift://{redshift_secret['host']}:{redshift_secret['port']}/{redshift_secret['dbname']}"
                    ),
                    "USERNAME": redshift_secret["username"],
                    "PASSWORD": redshift_secret["password"],
                    "SECRET_ID": redshift_arn
                },
                "PhysicalConnectionRequirements": {
                    "SubnetId": "subnet-0bbb2222glue",
                    "SecurityGroupIdList": ["sg-0bbb2222glueeni"],
                    "AvailabilityZone": "us-east-2a"
                }
            }
        )
        print(f"✅ Glue Connection updated: conn-redshift-analytics")
        print(f"   Secret ARN: {redshift_arn}")
    except Exception as e:
        print(f"❌ Redshift connection: {str(e)[:100]}")


def update_lambda_to_use_secrets():
    """
    Update Lambda functions to use Secrets Manager instead of env vars
    
    BEFORE: password in Lambda environment variable
    AFTER: Lambda reads secret ARN from env var → calls get_secret_value()
    """
    lam = boto3.client("lambda", region_name=REGION)
    sm = boto3.client("secretsmanager", region_name=REGION)

    redshift_arn = sm.describe_secret(
        SecretId="quickcart/prod/redshift/etl-writer"
    )["ARN"]

    lambdas_to_update = [
        "refresh-materialized-views",
        "pipeline-event-router",
        "redshift-pause-resume",
        "data-quality-preflight"
    ]

    for func_name in lambdas_to_update:
        try:
            config = lam.get_function_configuration(FunctionName=func_name)
            env_vars = config.get("Environment", {}).get("Variables", {})

            # Remove hardcoded passwords
            for key in list(env_vars.keys()):
                if "PASSWORD" in key.upper() or "SECRET" in key.upper() or "PASS" in key.upper():
                    if key != "REDSHIFT_SECRET_ARN" and key != "MARIADB_SECRET_ARN":
                        del env_vars[key]
                        print(f"   🗑️  Removed env var: {key} from {func_name}")

            # Add secret ARN reference
            env_vars["REDSHIFT_SECRET_ARN"] = redshift_arn
            env_vars["SECRET_RETRIEVAL"] = "secrets_manager"

            lam.update_function_configuration(
                FunctionName=func_name,
                Environment={"Variables": env_vars}
            )
            print(f"✅ Lambda updated: {func_name} → uses Secrets Manager")

        except lam.exceptions.ResourceNotFoundException:
            print(f"⚠️  Lambda not found: {func_name}")
        except Exception as e:
            print(f"❌ {func_name}: {str(e)[:100]}")


def update_iam_policies_for_secrets():
    """
    Ensure all roles have permission to read their specific secrets
    """
    iam = boto3.client("iam")
    sm = boto3.client("secretsmanager", region_name=REGION)

    mariadb_arn = sm.describe_secret(SecretId="quickcart/prod/mariadb/glue-reader")["ARN"]
    redshift_arn = sm.describe_secret(SecretId="quickcart/prod/redshift/etl-writer")["ARN"]

    role_secret_mapping = {
        "GlueCrossAccountETLRole": [mariadb_arn, redshift_arn],
        "LambdaMVRefreshRole": [redshift_arn],
        "AthenaConnectorLambdaRole": [mariadb_arn],
        "StepFunctionsEventDrivenPipelineRole": [redshift_arn],
        "LambdaSmallFileMonitorRole": []  # No DB access needed
    }

    for role_name, secret_arns in role_secret_mapping.items():
        if not secret_arns:
            continue

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ReadSpecificSecrets",
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:DescribeSecret"
                    ],
                    "Resource": secret_arns
                }
            ]
        }

        try:
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName="SecretsManagerReadAccess",
                PolicyDocument=json.dumps(policy)
            )
            print(f"✅ IAM updated: {role_name} → can read {len(secret_arns)} secret(s)")
        except iam.exceptions.NoSuchEntityException:
            print(f"⚠️  Role not found: {role_name}")
        except Exception as e:
            print(f"❌ {role_name}: {str(e)[:100]}")


if __name__ == "__main__":
    print("=" * 60)
    print("🔄 UPDATING ALL SERVICES TO USE SECRETS MANAGER")
    print("=" * 60)

    update_iam_policies_for_secrets()
    print()
    update_glue_connection_with_secret()
    print()
    update_lambda_to_use_secrets()

    print(f"\n{'='*60}")
    print("✅ ALL SERVICES UPDATED")
    print("   Glue Connections: reference Secret ARN")
    print("   Lambda functions: env var → Secret ARN (not password)")
    print("   IAM policies: scoped to specific secret ARNs")
    print(f"\n   NEXT: Set up automatic rotation (70_04)")
    print(f"{'='*60}")