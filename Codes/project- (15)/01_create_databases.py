# file: 01_create_databases.py
# Purpose: Create Glue Catalog databases for each data layer
# Run once during infrastructure setup

import boto3
import json
from datetime import datetime
from botocore.exceptions import ClientError

REGION = "us-east-2"
ACCOUNT_ID = [REDACTED:BANK_ACCOUNT_NUMBER]2"
S3_BUCKET = "quickcart-raw-data-prod"

# Database definitions — one per data layer
DATABASES = {
    "quickcart_raw_data": {
        "description": "Raw CSV files from source systems (MariaDB exports, uploads). "
                       "No transformation applied. Bronze layer.",
        "location": f"s3://{S3_BUCKET}/daily_exports/",
        "parameters": {
            "layer": "bronze",
            "source": "mariadb_exports",
            "owner_team": "data-engineering",
            "created_by": "catalog_setup_script"
        }
    },
    "quickcart_curated": {
        "description": "Cleaned and transformed data in Parquet format. "
                       "Schema validated, types enforced. Silver layer.",
        "location": f"s3://{S3_BUCKET}/curated/",
        "parameters": {
            "layer": "silver",
            "format": "parquet",
            "owner_team": "data-engineering",
            "created_by": "catalog_setup_script"
        }
    },
    "quickcart_aggregated": {
        "description": "Pre-computed aggregations for dashboards. "
                       "Gold layer — ready for BI tools.",
        "location": f"s3://{S3_BUCKET}/aggregated/",
        "parameters": {
            "layer": "gold",
            "format": "parquet",
            "owner_team": "analytics",
            "created_by": "catalog_setup_script"
        }
    }
}


def create_database(glue_client, db_name, db_config):
    """Create a Glue Catalog database"""
    
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": db_name,
                "Description": db_config["description"],
                "LocationUri": db_config["location"],
                "Parameters": db_config["parameters"]
            }
        )
        print(f"✅ Database created: {db_name}")
        print(f"   Location: {db_config['location']}")
        print(f"   Layer: {db_config['parameters'].get('layer', 'N/A')}")
        return True
        
    except ClientError as e:
        if "AlreadyExistsException" in str(e):
            print(f"ℹ️  Database already exists: {db_name}")
            
            # Update description and parameters
            glue_client.update_database(
                Name=db_name,
                DatabaseInput={
                    "Name": db_name,
                    "Description": db_config["description"],
                    "LocationUri": db_config["location"],
                    "Parameters": db_config["parameters"]
                }
            )
            print(f"   ✅ Updated metadata")
            return True
        else:
            print(f"❌ Failed to create {db_name}: {e}")
            return False


def list_databases(glue_client):
    """List all databases in catalog"""
    response = glue_client.get_databases()
    
    print(f"\n📋 ALL DATABASES IN CATALOG:")
    print(f"   {'─'*60}")
    
    for db in response["DatabaseList"]:
        name = db["Name"]
        desc = db.get("Description", "No description")[:80]
        location = db.get("LocationUri", "N/A")
        params = db.get("Parameters", {})
        layer = params.get("layer", "N/A")
        
        print(f"\n   📦 {name}")
        print(f"      Description: {desc}")
        print(f"      Location:    {location}")
        print(f"      Layer:       {layer}")


def run_setup():
    """Create all databases"""
    print("=" * 65)
    print("📚 CREATING GLUE CATALOG DATABASES")
    print("=" * 65)
    
    glue_client = boto3.client("glue", region_name=REGION)
    
    for db_name, db_config in DATABASES.items():
        print(f"\n📋 Creating '{db_name}'...")
        create_database(glue_client, db_name, db_config)
    
    # List all databases
    list_databases(glue_client)
    
    # Save config
    config = {
        "region": REGION,
        "account_id": ACCOUNT_ID,
        "s3_bucket": S3_BUCKET,
        "databases": list(DATABASES.keys())
    }
    with open("catalog_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"\n📁 Config saved to catalog_config.json")
    
    print(f"\n{'='*65}")
    print(f"✅ ALL DATABASES CREATED")
    print(f"{'='*65}")


if __name__ == "__main__":
    run_setup()