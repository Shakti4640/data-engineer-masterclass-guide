# file: 02_create_raw_tables.py
# Purpose: Define Glue Catalog tables for S3 CSV files
# These tables match the CSV files created in Project 1

import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"
S3_BUCKET = "quickcart-raw-data-prod"
DATABASE_NAME = "quickcart_raw_data"

# ═══════════════════════════════════════════════════
# TABLE DEFINITIONS
# Each maps to CSV files from Project 1 daily exports
# Column order MUST match CSV column order exactly
# ═══════════════════════════════════════════════════

TABLE_DEFINITIONS = {
    "orders": {
        "description": "Daily order export from MariaDB. One row per order.",
        "s3_location": f"s3://{S3_BUCKET}/daily_exports/orders/",
        "columns": [
            {"Name": "order_id",      "Type": "string",  "Comment": "Unique order identifier (e.g., ORD-20250715-000001)"},
            {"Name": "customer_id",   "Type": "string",  "Comment": "Customer who placed the order"},
            {"Name": "product_id",    "Type": "string",  "Comment": "Primary product in order"},
            {"Name": "quantity",      "Type": "int",     "Comment": "Number of items ordered"},
            {"Name": "unit_price",    "Type": "double",  "Comment": "Price per unit at time of order"},
            {"Name": "total_amount",  "Type": "double",  "Comment": "Total order amount (quantity × unit_price)"},
            {"Name": "status",        "Type": "string",  "Comment": "Order status: pending, shipped, completed, cancelled, refunded"},
            {"Name": "order_date",    "Type": "string",  "Comment": "Date order was placed (YYYY-MM-DD format)"}
        ],
        "serde": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
        "serde_params": {
            "separatorChar": ",",
            "quoteChar": "\"",
            "escapeChar": "\\"
        },
        "table_properties": {
            "skip.header.line.count": "1",
            "classification": "csv",
            "source_system": "mariadb",
            "source_table": "orders",
            "refresh_frequency": "daily",
            "has_header": "true"
        }
    },
    
    "customers": {
        "description": "Daily customer export from MariaDB. One row per customer.",
        "s3_location": f"s3://{S3_BUCKET}/daily_exports/customers/",
        "columns": [
            {"Name": "customer_id",   "Type": "string",  "Comment": "Unique customer identifier"},
            {"Name": "name",          "Type": "string",  "Comment": "Customer full name"},
            {"Name": "email",         "Type": "string",  "Comment": "Customer email address"},
            {"Name": "city",          "Type": "string",  "Comment": "City of residence"},
            {"Name": "state",         "Type": "string",  "Comment": "State/province code"},
            {"Name": "tier",          "Type": "string",  "Comment": "Loyalty tier: bronze, silver, gold, platinum"},
            {"Name": "signup_date",   "Type": "string",  "Comment": "Date customer registered (YYYY-MM-DD)"}
        ],
        "serde": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
        "serde_params": {
            "separatorChar": ",",
            "quoteChar": "\"",
            "escapeChar": "\\"
        },
        "table_properties": {
            "skip.header.line.count": "1",
            "classification": "csv",
            "source_system": "mariadb",
            "source_table": "customers",
            "refresh_frequency": "daily",
            "has_header": "true"
        }
    },
    
    "products": {
        "description": "Daily product catalog export from MariaDB. One row per product.",
        "s3_location": f"s3://{S3_BUCKET}/daily_exports/products/",
        "columns": [
            {"Name": "product_id",      "Type": "string",  "Comment": "Unique product identifier"},
            {"Name": "name",            "Type": "string",  "Comment": "Product display name"},
            {"Name": "category",        "Type": "string",  "Comment": "Product category"},
            {"Name": "price",           "Type": "double",  "Comment": "Current list price"},
            {"Name": "stock_quantity",  "Type": "int",     "Comment": "Current inventory count"},
            {"Name": "is_active",       "Type": "string",  "Comment": "Whether product is currently for sale (true/false)"}
        ],
        "serde": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
        "serde_params": {
            "separatorChar": ",",
            "quoteChar": "\"",
            "escapeChar": "\\"
        },
        "table_properties": {
            "skip.header.line.count": "1",
            "classification": "csv",
            "source_system": "mariadb",
            "source_table": "products",
            "refresh_frequency": "daily",
            "has_header": "true"
        }
    }
}


def create_table(glue_client, table_name, table_def):
    """
    Create a Glue Catalog table definition
    
    KEY COMPONENTS:
    → StorageDescriptor: WHERE and HOW to read the data
    → Columns: WHAT the data looks like
    → SerdeInfo: HOW to parse the raw bytes
    → Parameters: METADATA about the table
    """
    
    table_input = {
        "Name": table_name,
        "Description": table_def["description"],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": table_def["table_properties"],
        
        "StorageDescriptor": {
            # WHERE: S3 location containing the data files
            "Location": table_def["s3_location"],
            
            # WHAT: column definitions
            "Columns": table_def["columns"],
            
            # HOW TO READ: input format for reading files
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            
            # HOW TO WRITE: output format (for Glue ETL writes)
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            
            # HOW TO PARSE: SerDe configuration
            "SerdeInfo": {
                "SerializationLibrary": table_def["serde"],
                "Parameters": table_def["serde_params"]
            },
            
            # COMPRESSION: none for raw CSV
            "Compressed": False,
            
            # STORAGE: not stored as sorted (raw data)
            "StoredAsSubDirectories": False
        }
    }
    
    try:
        glue_client.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput=table_input
        )
        print(f"   ✅ Table created: {table_name}")
        print(f"      Location: {table_def['s3_location']}")
        print(f"      Columns: {len(table_def['columns'])}")
        print(f"      SerDe: {table_def['serde'].split('.')[-1]}")
        return True
        
    except ClientError as e:
        if "AlreadyExistsException" in str(e):
            print(f"   ℹ️  Table already exists: {table_name} — updating...")
            
            # Update existing table
            glue_client.update_table(
                DatabaseName=DATABASE_NAME,
                TableInput=table_input
            )
            print(f"   ✅ Table updated: {table_name}")
            return True
        else:
            print(f"   ❌ Failed: {e}")
            return False


def verify_table(glue_client, table_name):
    """Read table back from catalog and display details"""
    
    try:
        response = glue_client.get_table(
            DatabaseName=DATABASE_NAME,
            Name=table_name
        )
        
        table = response["Table"]
        sd = table["StorageDescriptor"]
        
        print(f"\n   🔍 Verification: {table_name}")
        print(f"      Database:    {DATABASE_NAME}")
        print(f"      Type:        {table.get('TableType', 'N/A')}")
        print(f"      Location:    {sd.get('Location', 'N/A')}")
        print(f"      Format:      {sd['SerdeInfo']['SerializationLibrary'].split('.')[-1]}")
        print(f"      Columns ({len(sd['Columns'])}):")
        
        for col in sd["Columns"]:
            comment = col.get("Comment", "")
            comment_short = comment[:40] + "..." if len(comment) > 40 else comment
            print(f"         {col['Name']:<20} {col['Type']:<10} {comment_short}")
        
        # Show properties
        params = table.get("Parameters", {})
        print(f"      Properties:")
        for k, v in params.items():
            print(f"         {k}: {v}")
        
        return True
        
    except ClientError as e:
        print(f"   ❌ Cannot read table: {e}")
        return False


def run_create_tables():
    """Create all raw data tables"""
    print("=" * 65)
    print("📋 CREATING GLUE CATALOG TABLES (Raw CSV Layer)")
    print(f"   Database: {DATABASE_NAME}")
    print("=" * 65)
    
    glue_client = boto3.client("glue", region_name=REGION)
    
    # Verify database exists
    try:
        glue_client.get_database(Name=DATABASE_NAME)
        print(f"\n✅ Database '{DATABASE_NAME}' exists")
    except ClientError:
        print(f"\n❌ Database '{DATABASE_NAME}' not found — run 01_create_databases.py first")
        return
    
    # Create each table
    for table_name, table_def in TABLE_DEFINITIONS.items():
        print(f"\n📋 Creating table: {table_name}")
        create_table(glue_client, table_name, table_def)
    
    # Verify all tables
    print(f"\n{'='*65}")
    print(f"🔍 VERIFICATION — Reading tables back from catalog")
    print(f"{'='*65}")
    
    for table_name in TABLE_DEFINITIONS:
        verify_table(glue_client, table_name)
    
    # Save table list
    config = {
        "database": DATABASE_NAME,
        "tables": list(TABLE_DEFINITIONS.keys()),
        "region": REGION
    }
    with open("raw_tables_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"\n{'='*65}")
    print(f"✅ ALL TABLES CREATED IN CATALOG")
    print(f"{'='*65}")
    print(f"""
NEXT STEPS:
→ Query with Athena (Project 16):
  SELECT * FROM quickcart_raw_data.orders LIMIT 10;

→ Read with Glue ETL (Project 25):
  df = glueContext.create_dynamic_frame.from_catalog(
      database="quickcart_raw_data",
      table_name="orders"
  )

→ Query from Redshift Spectrum (Project 38):
  CREATE EXTERNAL SCHEMA raw_data FROM DATA CATALOG
  DATABASE 'quickcart_raw_data'
  IAM_ROLE 'arn:aws:iam::...:role/...';
    """)


if __name__ == "__main__":
    run_create_tables()