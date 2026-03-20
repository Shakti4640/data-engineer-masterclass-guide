# file: 44_02_create_glue_catalog_databases.py
# Purpose: Create separate Glue Catalog databases for each zone

import boto3

REGION = "us-east-2"
DATALAKE_BUCKET = "quickcart-datalake-prod"

DATABASES = {
    "quickcart_bronze": {
        "description": "Bronze zone — raw data from source systems (CSV, JSON)",
        "location": f"s3://{DATALAKE_BUCKET}/bronze/"
    },
    "quickcart_silver": {
        "description": "Silver zone — cleansed, typed, deduplicated Parquet",
        "location": f"s3://{DATALAKE_BUCKET}/silver/"
    },
    "quickcart_gold": {
        "description": "Gold zone — business aggregations, consumption-ready",
        "location": f"s3://{DATALAKE_BUCKET}/gold/"
    }
}


def create_catalog_databases():
    """
    Create one Glue Catalog database per zone.
    
    WHY separate databases:
    → Clean Athena namespace: FROM quickcart_gold.revenue_daily
    → Zone-level permissions (Lake Formation — Project 74)
    → Crawlers target one database per zone
    → Easier to manage: DROP DATABASE quickcart_bronze (removes all bronze tables)
    """
    glue_client = boto3.client("glue", region_name=REGION)

    for db_name, db_config in DATABASES.items():
        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": db_name,
                    "Description": db_config["description"],
                    "LocationUri": db_config["location"],
                    "Parameters": {
                        "zone": db_name.split("_")[-1],
                        "created_by": "datalake_setup",
                        "bucket": DATALAKE_BUCKET
                    }
                }
            )
            print(f"✅ Glue Database: {db_name}")
            print(f"   Location: {db_config['location']}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"ℹ️  Already exists: {db_name}")

    # Verify
    print(f"\n📋 All Glue Databases:")
    response = glue_client.get_databases()
    for db in response["DatabaseList"]:
        if db["Name"].startswith("quickcart_"):
            print(f"   {db['Name']}: {db.get('Description', 'N/A')}")


if __name__ == "__main__":
    create_catalog_databases()