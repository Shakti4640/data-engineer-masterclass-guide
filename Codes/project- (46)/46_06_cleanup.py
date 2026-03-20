# file: 46_06_cleanup.py
# Purpose: Remove all federated query resources

import boto3

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]


def cleanup():
    print("🧹 CLEANING UP PROJECT 46 RESOURCES")
    print("=" * 50)

    # 1. Delete Athena data catalog
    athena_client = boto3.client("athena", region_name=REGION)
    try:
        athena_client.delete_data_catalog(Name="mariadb_source")
        print("✅ Deleted Athena data catalog: mariadb_source")
    except Exception as e:
        print(f"⏭️  Data catalog: {e}")

    # 2. Delete Athena workgroup (must be empty)
    try:
        athena_client.delete_work_group(
            WorkGroup="federated-adhoc",
            RecursiveDeleteOption=True
        )
        print("✅ Deleted workgroup: federated-adhoc")
    except Exception as e:
        print(f"⏭️  Workgroup: {e}")

    # 3. Delete Lambda function
    lambda_client = boto3.client("lambda", region_name=REGION)
    try:
        lambda_client.delete_function(FunctionName="quickcart-mariadb-connector")
        print("✅ Deleted Lambda: quickcart-mariadb-connector")
    except Exception as e:
        print(f"⏭️  Lambda: {e}")

    # 4. Delete IAM role
    iam_client = boto3.client("iam")
    role_name = "quickcart-mariadb-connector-role"
    try:
        policies = iam_client.list_role_policies(RoleName=role_name)
        for policy_name in policies["PolicyNames"]:
            iam_client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
        iam_client.delete_role(RoleName=role_name)
        print(f"✅ Deleted IAM role: {role_name}")
    except Exception as e:
        print(f"⏭️  IAM role: {e}")

    # 5. Delete secret
    sm_client = boto3.client("secretsmanager", region_name=REGION)
    try:
        sm_client.delete_secret(
            SecretId="quickcart/mariadb/athena-reader",
            ForceDeleteWithoutRecovery=True
        )
        print("✅ Deleted secret: quickcart/mariadb/athena-reader")
    except Exception as e:
        print(f"⏭️  Secret: {e}")

    # NOTE: Not deleting spill bucket (may contain other data)
    print("\n✅ CLEANUP COMPLETE")
    print("   ℹ️  Spill bucket retained: quickcart-athena-spill-prod")
    print("   ℹ️  MariaDB user 'athena_reader' retained (drop manually if needed)")


if __name__ == "__main__":
    cleanup()