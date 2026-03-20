# file: 46_03_register_athena_data_source.py
# Purpose: Register the Lambda connector as an Athena data source catalog
# After this, analysts can query: SELECT * FROM "mariadb_source".quickcart.orders

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

CATALOG_NAME = "mariadb_source"
CONNECTOR_NAME = "quickcart-mariadb-connector"
CONNECTOR_ARN = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{CONNECTOR_NAME}"


def register_data_source():
    """
    Register the Lambda connector as an Athena data source.
    
    After registration:
    → Analysts can query: SELECT * FROM "mariadb_source".quickcart.orders
    → "mariadb_source" = catalog name (registered here)
    → "quickcart" = database name (MariaDB database)
    → "orders" = table name (MariaDB table)
    """
    athena_client = boto3.client("athena", region_name=REGION)

    try:
        athena_client.create_data_catalog(
            Name=CATALOG_NAME,
            Type="LAMBDA",
            Description="QuickCart MariaDB — live federated query access",
            Parameters={
                "function": CONNECTOR_ARN
            },
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Project", "Value": "QuickCart-FederatedQuery"},
                {"Key": "SourceType", "Value": "MariaDB"}
            ]
        )
        print(f"✅ Athena data source registered: {CATALOG_NAME}")
        print(f"   Type: LAMBDA")
        print(f"   Function: {CONNECTOR_ARN}")

    except Exception as e:
        if "AlreadyExists" in str(e):
            athena_client.update_data_catalog(
                Name=CATALOG_NAME,
                Type="LAMBDA",
                Description="QuickCart MariaDB — live federated query access",
                Parameters={"function": CONNECTOR_ARN}
            )
            print(f"ℹ️  Data source updated: {CATALOG_NAME}")
        else:
            raise

    # Verify
    response = athena_client.get_data_catalog(Name=CATALOG_NAME)
    catalog = response["DataCatalog"]
    print(f"\n📋 Registered catalog:")
    print(f"   Name: {catalog['Name']}")
    print(f"   Type: {catalog['Type']}")
    print(f"   Parameters: {catalog.get('Parameters', {})}")

    # Show usage instructions
    print(f"\n📝 USAGE:")
    print(f"   Athena SQL:")
    print(f'   SELECT * FROM "{CATALOG_NAME}".quickcart.orders LIMIT 10')
    print(f'   SELECT * FROM "{CATALOG_NAME}".quickcart.customers WHERE tier = \'gold\'')
    print(f"")
    print(f"   Cross-source JOIN (MariaDB + S3 Gold zone):")
    print(f'   SELECT o.order_id, o.total_amount, c.segment')
    print(f'   FROM "{CATALOG_NAME}".quickcart.orders o')
    print(f'   JOIN "quickcart_gold".customer_360 c')
    print(f'     ON o.customer_id = c.customer_id')
    print(f"   WHERE o.order_date = CURRENT_DATE")


def create_federated_workgroup():
    """
    Create a separate Athena workgroup for federated queries.
    
    WHY separate workgroup:
    → Track federated query costs separately
    → Set query timeout limits (prevent runaway queries)
    → Control who can run federated queries via IAM
    → Dashboard workgroup can be restricted to S3 only
    """
    athena_client = boto3.client("athena", region_name=REGION)

    try:
        athena_client.create_work_group(
            Name="federated-adhoc",
            Description="Workgroup for ad-hoc federated queries against live databases",
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": "s3://quickcart-datalake-prod/_system/athena-results/federated/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 10737418240,  # 10 GB max per query
                "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"}
            },
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "QueryType", "Value": "Federated"}
            ]
        )
        print(f"\n✅ Workgroup created: federated-adhoc")
        print(f"   Max scan: 10 GB per query")
        print(f"   CloudWatch metrics: enabled")

    except Exception as e:
        if "AlreadyExists" in str(e):
            print(f"ℹ️  Workgroup already exists: federated-adhoc")
        else:
            raise


if __name__ == "__main__":
    register_data_source()
    create_federated_workgroup()