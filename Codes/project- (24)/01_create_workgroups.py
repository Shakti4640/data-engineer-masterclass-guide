# file: 01_create_workgroups.py
# Creates isolated Athena workgroups for each team
# Each workgroup has its own: result location, scan limit, encryption

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"


def create_workgroup(athena_client, name, description, result_prefix,
                     scan_limit_mb, enforce_limit=True):
    """
    Create Athena workgroup with cost controls
    
    WORKGROUP CONTROLS:
    → ResultConfiguration: where query results are stored
    → BytesScannedCutoffPerQuery: max data scan before auto-cancel
    → EnforceWorkGroupConfiguration: override client settings
    → PublishCloudWatchMetricsEnabled: cost tracking per workgroup
    → EngineVersion: Athena engine version (v3 recommended)
    """
    result_location = f"s3://{BUCKET_NAME}/athena-results/{result_prefix}/"

    workgroup_config = {
        "ResultConfiguration": {
            "OutputLocation": result_location,
            "EncryptionConfiguration": {
                "EncryptionOption": "SSE_S3"  # Encrypt results at rest
            }
        },
        "EnforceWorkGroupConfiguration": enforce_limit,
        "PublishCloudWatchMetricsEnabled": True,
        "EngineVersion": {
            "SelectedEngineVersion": "Athena engine version 3"
        }
    }

    # Add scan limit if specified
    if scan_limit_mb > 0:
        workgroup_config["BytesScannedCutoffPerQuery"] = scan_limit_mb * 1024 * 1024

    try:
        athena_client.create_work_group(
            Name=name,
            Configuration=workgroup_config,
            Description=description,
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Team", "Value": name}
            ]
        )
        print(f"✅ Workgroup created: {name}")
        print(f"   Results: {result_location}")
        print(f"   Scan limit: {scan_limit_mb} MB per query")
        print(f"   Enforce: {enforce_limit}")

    except ClientError as e:
        if "already exists" in str(e).lower():
            # Update existing
            athena_client.update_work_group(
                WorkGroup=name,
                ConfigurationUpdates={
                    "ResultConfigurationUpdates": {
                        "OutputLocation": result_location,
                        "EncryptionConfigurationUpdates": {
                            "EncryptionOption": "SSE_S3"
                        }
                    },
                    "EnforceWorkGroupConfiguration": enforce_limit,
                    "PublishCloudWatchMetricsEnabled": True,
                    "BytesScannedCutoffPerQuery": scan_limit_mb * 1024 * 1024,
                    "EngineVersion": {
                        "SelectedEngineVersion": "Athena engine version 3"
                    }
                }
            )
            print(f"ℹ️  Workgroup updated: {name}")
        else:
            raise


def create_all_workgroups():
    """Create workgroups for all teams"""
    athena_client = boto3.client("athena", region_name=REGION)

    print("=" * 60)
    print("🏢 CREATING ATHENA WORKGROUPS")
    print("=" * 60)

    teams = [
        {
            "name": "finance_team",
            "description": "Finance team — revenue reports, cost analysis",
            "result_prefix": "finance",
            "scan_limit_mb": 1024      # 1 GB max per query
        },
        {
            "name": "marketing_team",
            "description": "Marketing team — cohort analysis, campaign metrics",
            "result_prefix": "marketing",
            "scan_limit_mb": 5120      # 5 GB max per query
        },
        {
            "name": "product_team",
            "description": "Product team — feature usage, funnel analysis",
            "result_prefix": "product",
            "scan_limit_mb": 2048      # 2 GB max per query
        },
        {
            "name": "data_engineering",
            "description": "Data engineering — pipeline development, debugging",
            "result_prefix": "engineering",
            "scan_limit_mb": 51200     # 50 GB max per query
        }
    ]

    for team in teams:
        create_workgroup(
            athena_client=athena_client,
            name=team["name"],
            description=team["description"],
            result_prefix=team["result_prefix"],
            scan_limit_mb=team["scan_limit_mb"]
        )
        print()

    print("=" * 60)
    print("✅ All workgroups created")
    print("\n💡 Assign analysts to workgroups via IAM policy:")
    print('   Condition: {"StringEquals": {"athena:workgroup": "finance_team"}}')


if __name__ == "__main__":
    create_all_workgroups()