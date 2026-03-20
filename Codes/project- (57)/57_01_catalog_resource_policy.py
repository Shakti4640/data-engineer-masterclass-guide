# file: 57_01_catalog_resource_policy.py
# Purpose: Set Glue Catalog Resource Policy in Account A to share with Account B
# RUN THIS IN ACCOUNT A

import boto3
import json

REGION = "us-east-2"
ACCOUNT_A_ID [REDACTED:BANK_ACCOUNT_NUMBER]111"
ACCOUNT_B_ID [REDACTED:BANK_ACCOUNT_NUMBER]222"

# Account B roles that need Catalog access
ACCOUNT_B_ROLES = [
    f"arn:aws:iam::{ACCOUNT_B_ID}:role/QuickCart-Analytics-GlueRole",
    f"arn:aws:iam::{ACCOUNT_B_ID}:role/QuickCart-Analytics-AthenaRole"
]

# Databases to share (whitelist approach)
SHARED_DATABASES = ["quickcart_db", "quickcart_staging_db"]

# Databases to DENY (explicit deny for safety)
DENIED_DATABASES = ["internal_db", "hr_db", "credentials_db"]


def create_catalog_resource_policy():
    """
    Create Glue Catalog Resource Policy that allows Account B
    to READ specific databases and their tables
    
    IMPORTANT DISTINCTIONS:
    → Resource policy is on the CATALOG (not individual databases)
    → Scoping to specific databases done via Resource ARNs in policy
    → One policy per account per region
    → Replaces any existing policy (not additive)
    """
    glue_client = boto3.client("glue", region_name=REGION)

    print("=" * 70)
    print("🔧 SETTING GLUE CATALOG RESOURCE POLICY (Account A)")
    print(f"   Sharing with Account B: {ACCOUNT_B_ID}")
    print(f"   Databases to share: {SHARED_DATABASES}")
    print(f"   Databases to deny:  {DENIED_DATABASES}")
    print("=" * 70)

    # Build Resource ARNs for shared databases
    shared_resources = [
        # Catalog-level (needed for GetDatabases)
        f"arn:aws:glue:{REGION}:{ACCOUNT_A_ID}:catalog"
    ]

    for db in SHARED_DATABASES:
        shared_resources.extend([
            # Database-level (needed for GetDatabase)
            f"arn:aws:glue:{REGION}:{ACCOUNT_A_ID}:database/{db}",
            # Table-level (needed for GetTable, GetTables)
            f"arn:aws:glue:{REGION}:{ACCOUNT_A_ID}:table/{db}/*"
        ])

    # Build Resource ARNs for denied databases
    denied_resources = []
    for db in DENIED_DATABASES:
        denied_resources.extend([
            f"arn:aws:glue:{REGION}:{ACCOUNT_A_ID}:database/{db}",
            f"arn:aws:glue:{REGION}:{ACCOUNT_A_ID}:table/{db}/*"
        ])

    # --- BUILD RESOURCE POLICY ---
    resource_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAccountBCatalogRead",
                "Effect": "Allow",
                "Principal": {
                    "AWS": ACCOUNT_B_ROLES
                },
                "Action": [
                    # Database actions
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    # Table actions
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions",
                    # Partition actions (critical for partitioned tables)
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                    # Column statistics
                    "glue:GetColumnStatisticsForTable"
                ],
                "Resource": shared_resources
            },
            {
                "Sid": "ExplicitDenySensitiveDatabases",
                "Effect": "Deny",
                "Principal": {
                    "AWS": ACCOUNT_B_ROLES
                },
                "Action": "glue:*",
                "Resource": denied_resources
            },
            {
                "Sid": "DenyAllWriteActions",
                "Effect": "Deny",
                "Principal": {
                    "AWS": ACCOUNT_B_ROLES
                },
                "Action": [
                    "glue:CreateDatabase",
                    "glue:UpdateDatabase",
                    "glue:DeleteDatabase",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                    "glue:DeletePartition",
                    "glue:BatchCreatePartition",
                    "glue:BatchDeletePartition"
                ],
                "Resource": "*"
            }
        ]
    }

    # --- APPLY RESOURCE POLICY ---
    try:
        glue_client.put_resource_policy(
            PolicyInJson=json.dumps(resource_policy),
            PolicyHashCondition=None,   # No conditional update (overwrite)
            EnableHybrid="TRUE"         # Allow both resource policy AND Lake Formation
        )
        print(f"\n✅ Catalog Resource Policy applied successfully!")
    except glue_client.exceptions.ConditionCheckFailureException:
        # Policy already exists — need hash for conditional update
        existing = glue_client.get_resource_policy()
        policy_hash = existing.get("PolicyHash", "")
        glue_client.put_resource_policy(
            PolicyInJson=json.dumps(resource_policy),
            PolicyHashCondition=policy_hash,
            EnableHybrid="TRUE"
        )
        print(f"\n✅ Catalog Resource Policy updated (replaced existing)")

    # --- DISPLAY POLICY ---
    print(f"\n📋 RESOURCE POLICY:")
    print(json.dumps(resource_policy, indent=2))

    # --- VERIFY ---
    print(f"\n📌 VERIFYING...")
    verify = glue_client.get_resource_policy()
    if verify.get("PolicyInJson"):
        print(f"  ✅ Policy is active")
        print(f"  Policy hash: {verify.get('PolicyHash', 'N/A')}")
        print(f"  Last updated: {verify.get('UpdateTime', 'N/A')}")
    else:
        print(f"  ❌ Policy not found — check for errors above")

    # --- ACCESS MATRIX ---
    print(f"\n{'=' * 70}")
    print(f"📊 ACCESS MATRIX — Account B's Roles")
    print(f"{'=' * 70}")
    print(f"  {'Action':<35} {'quickcart_db':^14} {'internal_db':^14}")
    print(f"  {'─' * 63}")
    print(f"  {'GetDatabase':<35} {'✅ ALLOW':^14} {'❌ DENY':^14}")
    print(f"  {'GetTable / GetTables':<35} {'✅ ALLOW':^14} {'❌ DENY':^14}")
    print(f"  {'GetPartitions':<35} {'✅ ALLOW':^14} {'❌ DENY':^14}")
    print(f"  {'CreateTable':<35} {'❌ DENY':^14} {'❌ DENY':^14}")
    print(f"  {'UpdateTable':<35} {'❌ DENY':^14} {'❌ DENY':^14}")
    print(f"  {'DeleteTable':<35} {'❌ DENY':^14} {'❌ DENY':^14}")
    print(f"{'=' * 70}")

    # Save for other scripts
    config = {
        "account_a_id": ACCOUNT_A_ID,
        "account_b_id": ACCOUNT_B_ID,
        "shared_databases": SHARED_DATABASES,
        "denied_databases": DENIED_DATABASES,
        "account_b_roles": ACCOUNT_B_ROLES
    }
    with open("/tmp/catalog_sharing_config.json", "w") as f:
        json.dump(config, f, indent=2)

    return resource_policy


if __name__ == "__main__":
    create_catalog_resource_policy()