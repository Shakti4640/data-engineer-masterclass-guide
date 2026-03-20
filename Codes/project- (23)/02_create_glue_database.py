# file: 02_create_glue_database.py
# Creates the Glue Data Catalog database for all QuickCart tables
# This was done manually in Project 15 — now automated

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"


def create_glue_database():
    """
    Create Glue Database — logical container for catalog tables
    
    NAMING CONVENTION:
    → lowercase, underscores (no hyphens — Athena doesn't like them)
    → format: {company}_{purpose}
    → example: quickcart_datalake
    
    This database was referenced in Project 15 (manual catalog)
    Now crawlers will auto-create tables INSIDE this database
    """
    glue_client = boto3.client("glue", region_name=REGION)

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": DATABASE_NAME,
                "Description": "QuickCart Data Lake — auto-discovered schemas from S3",
                "Parameters": {
                    "created_by": "data_engineering_team",
                    "environment": "production",
                    "source": "s3_crawler_discovery"
                }
            }
        )
        print(f"✅ Database created: {DATABASE_NAME}")

    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Database already exists: {DATABASE_NAME}")

    # Verify
    response = glue_client.get_database(Name=DATABASE_NAME)
    db = response["Database"]
    print(f"\n📋 Database details:")
    print(f"   Name:        {db['Name']}")
    print(f"   Description: {db.get('Description', 'N/A')}")
    print(f"   Created:     {db.get('CreateTime', 'N/A')}")

    return DATABASE_NAME


if __name__ == "__main__":
    create_glue_database()