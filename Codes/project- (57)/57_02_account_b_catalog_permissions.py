# file: 57_02_account_b_catalog_permissions.py
# Purpose: Add Glue Catalog permissions to Account B's roles
# RUN THIS IN ACCOUNT B

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/catalog_sharing_config.json", "r") as f:
        return json.load(f)


def add_catalog_permissions_to_account_b():
    """
    Add Glue Catalog API permissions to Account B's IAM roles
    
    REMINDER (from Project 56):
    → Cross-account = BOTH sides must allow
    → Account A: Catalog Resource Policy (Step 1) ✅
    → Account B: IAM identity policy (this step) ← REQUIRED
    → Missing this step = AccessDenied even with resource policy
    """
    config = load_config()
    iam_client = boto3.client("iam")

    print("=" * 70)
    print("🔧 ACCOUNT B: ADDING GLUE CATALOG PERMISSIONS")
    print("=" * 70)

    # Build resource ARNs for Account A's Catalog
    catalog_resources = [
        f"arn:aws:glue:{REGION}:{config['account_a_id']}:catalog",
    ]
    for db in config["shared_databases"]:
        catalog_resources.extend([
            f"arn:aws:glue:{REGION}:{config['account_a_id']}:database/{db}",
            f"arn:aws:glue:{REGION}:{config['account_a_id']}:table/{db}/*"
        ])

    # Also include Account B's OWN Catalog (for local tables)
    catalog_resources.extend([
        f"arn:aws:glue:{REGION}:{config['account_b_id']}:catalog",
        f"arn:aws:glue:{REGION}:{config['account_b_id']}:database/*",
        f"arn:aws:glue:{REGION}:{config['account_b_id']}:table/*"
    ])

    catalog_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadAccountACatalog",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                    "glue:GetColumnStatisticsForTable"
                ],
                "Resource": catalog_resources
            },
            {
                "Sid": "FullAccessOwnCatalog",
                "Effect": "Allow",
                "Action": "glue:*",
                "Resource": [
                    f"arn:aws:glue:{REGION}:{config['account_b_id']}:*"
                ]
            }
        ]
    }

    # Apply to each Account B role
    roles = [
        "QuickCart-Analytics-GlueRole",
        "QuickCart-Analytics-AthenaRole"
    ]

    for role_name in roles:
        try:
            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName="CrossAccountCatalogRead",
                PolicyDocument=json.dumps(catalog_policy)
            )
            print(f"  ✅ Catalog policy attached to: {role_name}")
        except iam_client.exceptions.NoSuchEntityException:
            print(f"  ⚠️  Role not found: {role_name}")
            print(f"     → Create it first (Project 56, Step 4)")

    print(f"\n📋 POLICY APPLIED:")
    print(json.dumps(catalog_policy, indent=2))

    print(f"\n{'=' * 70}")
    print(f"📊 ACCOUNT B ROLES CAN NOW:")
    print(f"  ✅ Read Account A's Catalog (quickcart_db)")
    print(f"  ✅ Read Account A's S3 data (from Project 56)")
    print(f"  ✅ Full access to Account B's own Catalog")
    print(f"  ❌ Cannot modify Account A's Catalog")
    print(f"  ❌ Cannot access Account A's internal_db")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    add_catalog_permissions_to_account_b()