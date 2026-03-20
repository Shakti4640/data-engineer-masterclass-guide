# file: 71_01_producer_account_setup.py
# Account A (111111111111): Orders Domain — Producer Account Setup
# Creates: S3 bucket, Glue Catalog, Cross-Account IAM Role, Bucket Policy

import boto3
import json

# --- CONFIGURATION ---
PRODUCER_ACCOUNT_ID = "111111111111"
CONSUMER_ANALYTICS_ACCOUNT_ID [REDACTED:BANK_ACCOUNT_NUMBER]333"
CONSUMER_CUSTOMER_ACCOUNT_ID [REDACTED:BANK_ACCOUNT_NUMBER]222"
GOVERNANCE_ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
REGION = "us-east-2"
DOMAIN_NAME = "orders"
BUCKET_NAME = f"{DOMAIN_NAME}-data-prod"
EXTERNAL_ID = "mesh-analytics-2025"

# Accounts allowed to read this domain's Gold data
CONSUMER_ACCOUNT_IDS = [
    CONSUMER_ANALYTICS_ACCOUNT_ID,
    CONSUMER_CUSTOMER_ACCOUNT_ID
]


def create_domain_bucket(s3_client):
    """Create the domain's data lake bucket with standard structure"""
    try:
        s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        print(f"✅ Bucket created: {BUCKET_NAME}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"ℹ️  Bucket already exists: {BUCKET_NAME}")

    # Block public access (Project 1 pattern)
    s3_client.put_public_access_block(
        Bucket=BUCKET_NAME,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True
        }
    )

    # Enable encryption
    s3_client.put_bucket_encryption(
        Bucket=BUCKET_NAME,
        ServerSideEncryptionConfiguration={
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                "BucketKeyEnabled": True
            }]
        }
    )

    # Tag for mesh identification
    s3_client.put_bucket_tagging(
        Bucket=BUCKET_NAME,
        Tagging={
            "TagSet": [
                {"Key": "Domain", "Value": DOMAIN_NAME},
                {"Key": "MeshRole", "Value": "producer"},
                {"Key": "Environment", "Value": "production"},
                {"Key": "DataClassification", "Value": "internal"}
            ]
        }
    )
    print("✅ Bucket configured: encryption, tags, public access blocked")


def set_cross_account_bucket_policy(s3_client):
    """
    Allow consumer accounts to READ Gold layer only
    
    KEY DESIGN DECISIONS:
    → Only gold/* prefix is shared — bronze/silver are internal
    → Only GetObject and ListBucket — no write, no delete
    → Condition: consumer must use the cross-account role (not root)
    """
    consumer_role_arns = [
        f"arn:aws:iam::{acct_id}:role/MeshDataConsumerRole"
        for acct_id in CONSUMER_ACCOUNT_IDS
    ]

    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowCrossAccountGoldRead",
                "Effect": "Allow",
                "Principal": {
                    "AWS": consumer_role_arns
                },
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectVersion"
                ],
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}/gold/*"
            },
            {
                "Sid": "AllowCrossAccountListGoldOnly",
                "Effect": "Allow",
                "Principal": {
                    "AWS": consumer_role_arns
                },
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}",
                "Condition": {
                    "StringLike": {
                        "s3:prefix": "gold/*"
                    }
                }
            }
        ]
    }

    # Must temporarily disable BlockPublicPolicy to set bucket policy
    # (Bucket policy with cross-account principals is NOT public)
    # Actually — cross-account IAM principals are NOT considered "public"
    # So BlockPublicPolicy does NOT block this. Safe to apply.

    s3_client.put_bucket_policy(
        Bucket=BUCKET_NAME,
        Policy=json.dumps(bucket_policy)
    )
    print(f"✅ Bucket policy set: {len(CONSUMER_ACCOUNT_IDS)} consumer accounts can read gold/")


def create_cross_account_reader_role(iam_client):
    """
    Create IAM role that consumer accounts can assume
    
    Cross-account role pattern (Project 56):
    → Trust Policy: WHO can assume (consumer accounts)
    → Permission Policy: WHAT they can do (read Gold S3 + Glue Catalog)
    → ExternalId: prevents confused deputy attack
    """
    role_name = f"MeshProducer-{DOMAIN_NAME}-CrossAcctReader"

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowConsumerAccountsToAssume",
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        f"arn:aws:iam::{acct_id}:root"
                        for acct_id in CONSUMER_ACCOUNT_IDS
                    ]
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": EXTERNAL_ID
                    }
                }
            }
        ]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadGoldS3",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/gold/*"
                ]
            },
            {
                "Sid": "ReadGlueCatalog",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions"
                ],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{PRODUCER_ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{REGION}:{PRODUCER_ACCOUNT_ID}:database/{DOMAIN_NAME}_gold",
                    f"arn:aws:glue:{REGION}:{PRODUCER_ACCOUNT_ID}:table/{DOMAIN_NAME}_gold/*"
                ]
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description=f"Cross-account read access to {DOMAIN_NAME} Gold data products",
            Tags=[
                {"Key": "Domain", "Value": DOMAIN_NAME},
                {"Key": "MeshRole", "Value": "cross-account-reader"},
                {"Key": "ManagedBy", "Value": "data-mesh-platform"}
            ]
        )
        print(f"✅ IAM Role created: {role_name}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  IAM Role already exists: {role_name}")
        # Update trust policy in case consumer list changed
        iam_client.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=json.dumps(trust_policy)
        )
        print(f"✅ Trust policy updated for: {role_name}")

    # Attach inline permission policy
    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName=f"{DOMAIN_NAME}-gold-read-access",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"✅ Permission policy attached to: {role_name}")

    return f"arn:aws:iam::{PRODUCER_ACCOUNT_ID}:role/{role_name}"


def setup_glue_catalog(glue_client):
    """
    Create Glue Catalog database for Gold layer
    + Resource Policy for cross-account access (Project 57 pattern)
    """
    db_name = f"{DOMAIN_NAME}_gold"

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": db_name,
                "Description": f"Gold layer data products for {DOMAIN_NAME} domain",
                "Parameters": {
                    "domain": DOMAIN_NAME,
                    "layer": "gold",
                    "mesh_role": "producer",
                    "owner_account": PRODUCER_ACCOUNT_ID
                }
            }
        )
        print(f"✅ Glue database created: {db_name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Glue database already exists: {db_name}")

    # Set Glue Catalog Resource Policy for cross-account access
    catalog_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowConsumerAccountsCatalogAccess",
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        f"arn:aws:iam::{acct_id}:root"
                        for acct_id in CONSUMER_ACCOUNT_IDS
                    ]
                },
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions"
                ],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{PRODUCER_ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{REGION}:{PRODUCER_ACCOUNT_ID}:database/{db_name}",
                    f"arn:aws:glue:{REGION}:{PRODUCER_ACCOUNT_ID}:table/{db_name}/*"
                ]
            }
        ]
    }

    glue_client.put_resource_policy(
        PolicyInJson=json.dumps(catalog_policy),
        EnableHybrid="TRUE"  # Allow both resource-based and Lake Formation
    )
    print(f"✅ Glue Catalog resource policy set for cross-account access")

    return db_name


def create_sample_gold_table(glue_client, db_name):
    """Register a Gold data product table in the catalog"""
    table_name = "daily_orders"

    try:
        glue_client.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": table_name,
                "Description": "Daily aggregated orders — Gold data product",
                "Owner": "orders-domain-team",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "order_id", "Type": "string", "Comment": "Unique order identifier"},
                        {"Name": "customer_id", "Type": "string", "Comment": "FK to customer domain"},
                        {"Name": "order_total", "Type": "decimal(10,2)", "Comment": "Total order value"},
                        {"Name": "item_count", "Type": "int", "Comment": "Number of items"},
                        {"Name": "status", "Type": "string", "Comment": "Order status"},
                        {"Name": "order_date", "Type": "date", "Comment": "Date order was placed"}
                    ],
                    "Location": f"s3://{BUCKET_NAME}/gold/daily_orders/",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                    "Compressed": True
                },
                "PartitionKeys": [
                    {"Name": "year", "Type": "string"},
                    {"Name": "month", "Type": "string"},
                    {"Name": "day", "Type": "string"}
                ],
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "parquet",
                    "compressionType": "snappy",
                    "data_product_name": "daily_orders",
                    "data_product_version": "v2",
                    "domain": "orders",
                    "sla_freshness_hours": "6",
                    "owner_email": "orders-team@quickcart.com",
                    "quality_threshold": "99"
                }
            }
        )
        print(f"✅ Gold table registered: {db_name}.{table_name}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Table already exists: {db_name}.{table_name}")


def main():
    print("=" * 70)
    print(f"🏗️  SETTING UP PRODUCER ACCOUNT: {DOMAIN_NAME} Domain")
    print(f"   Account: {PRODUCER_ACCOUNT_ID}")
    print("=" * 70)

    s3_client = boto3.client("s3", region_name=REGION)
    iam_client = boto3.client("iam", region_name=REGION)
    glue_client = boto3.client("glue", region_name=REGION)

    # Step 1: Create domain bucket
    print("\n--- Step 1: S3 Bucket ---")
    create_domain_bucket(s3_client)

    # Step 2: Set cross-account bucket policy
    print("\n--- Step 2: Cross-Account Bucket Policy ---")
    set_cross_account_bucket_policy(s3_client)

    # Step 3: Create cross-account IAM role
    print("\n--- Step 3: Cross-Account IAM Role ---")
    role_arn = create_cross_account_reader_role(iam_client)
    print(f"   Role ARN: {role_arn}")

    # Step 4: Setup Glue Catalog
    print("\n--- Step 4: Glue Catalog ---")
    db_name = setup_glue_catalog(glue_client)

    # Step 5: Register Gold table
    print("\n--- Step 5: Gold Data Product Table ---")
    create_sample_gold_table(glue_client, db_name)

    print("\n" + "=" * 70)
    print("✅ PRODUCER ACCOUNT SETUP COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()