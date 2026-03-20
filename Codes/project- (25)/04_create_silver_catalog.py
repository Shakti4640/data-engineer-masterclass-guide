# file: 04_create_silver_catalog.py
# Creates Glue Catalog table pointing to Parquet output
# Alternative: run crawler on silver/ prefix (Project 23 pattern)

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"
BUCKET_NAME = "quickcart-raw-data-prod"


def create_silver_table(table_name, s3_location, columns):
    """
    Programmatically create Glue Catalog table for Parquet output
    
    WHY PROGRAMMATIC vs CRAWLER:
    → Programmatic: exact control over types, no inference guessing
    → We KNOW the schema (we defined it in the Glue job)
    → Crawler would work but adds delay and possible type issues
    → Best practice: programmatic for tables YOU create
    → Crawler for tables from EXTERNAL sources you don't control
    """
    glue_client = boto3.client("glue", region_name=REGION)

    table_input = {
        "Name": table_name,
        "Description": f"Silver layer — optimized Parquet from CSV conversion",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "compressionType": "snappy",
            "typeOfData": "file",
            "created_by": "glue_etl_csv_to_parquet"
        },
        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {
                    "serialization.format": "1"
                }
            },
            "Compressed": True
        }
    }

    try:
        glue_client.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput=table_input
        )
        print(f"✅ Table created: {DATABASE_NAME}.{table_name}")

    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_table(
            DatabaseName=DATABASE_NAME,
            TableInput=table_input
        )
        print(f"ℹ️  Table updated: {DATABASE_NAME}.{table_name}")

    print(f"   Location: {s3_location}")
    print(f"   Format:   Parquet (Snappy)")
    print(f"   Columns:  {len(columns)}")


def create_all_silver_tables():
    """Create catalog tables for all silver layer Parquet outputs"""

    print("=" * 60)
    print("📋 CREATING SILVER LAYER CATALOG TABLES")
    print("=" * 60)

    # --- SILVER ORDERS ---
    create_silver_table(
        table_name="silver_orders",
        s3_location=f"s3://{BUCKET_NAME}/silver/orders/",
        columns=[
            {"Name": "order_id",     "Type": "string",  "Comment": "Unique order identifier"},
            {"Name": "customer_id",  "Type": "string",  "Comment": "Customer reference"},
            {"Name": "product_id",   "Type": "string",  "Comment": "Product reference"},
            {"Name": "quantity",     "Type": "int",     "Comment": "Units ordered"},
            {"Name": "unit_price",   "Type": "double",  "Comment": "Price per unit"},
            {"Name": "total_amount", "Type": "double",  "Comment": "Total order value"},
            {"Name": "status",       "Type": "string",  "Comment": "Order status"},
            {"Name": "order_date",   "Type": "date",    "Comment": "Date order was placed"}
        ]
    )

    print()

    # --- SILVER CUSTOMERS ---
    create_silver_table(
        table_name="silver_customers",
        s3_location=f"s3://{BUCKET_NAME}/silver/customers/",
        columns=[
            {"Name": "customer_id",  "Type": "string",  "Comment": "Unique customer ID"},
            {"Name": "name",         "Type": "string",  "Comment": "Customer full name"},
            {"Name": "email",        "Type": "string",  "Comment": "Email address"},
            {"Name": "city",         "Type": "string",  "Comment": "City"},
            {"Name": "state",        "Type": "string",  "Comment": "State/Country"},
            {"Name": "tier",         "Type": "string",  "Comment": "Loyalty tier"},
            {"Name": "signup_date",  "Type": "date",    "Comment": "Registration date"}
        ]
    )

    print()

    # --- SILVER PRODUCTS ---
    create_silver_table(
        table_name="silver_products",
        s3_location=f"s3://{BUCKET_NAME}/silver/products/",
        columns=[
            {"Name": "product_id",     "Type": "string",  "Comment": "Unique product ID"},
            {"Name": "name",           "Type": "string",  "Comment": "Product name"},
            {"Name": "category",       "Type": "string",  "Comment": "Product category"},
            {"Name": "price",          "Type": "double",  "Comment": "Current price"},
            {"Name": "stock_quantity", "Type": "int",     "Comment": "Available inventory"},
            {"Name": "is_active",      "Type": "string",  "Comment": "Active flag (true/false)"}
        ]
    )

    print(f"\n{'=' * 60}")
    print(f"✅ All silver tables created")
    print(f"\n💡 Test in Athena:")
    print(f"   SELECT * FROM quickcart_datalake.silver_orders LIMIT 10;")


if __name__ == "__main__":
    create_all_silver_tables()