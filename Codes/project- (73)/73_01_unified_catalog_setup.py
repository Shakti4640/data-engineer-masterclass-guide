# file: 73_01_unified_catalog_setup.py
# Creates the complete Glue Data Catalog structure for the entire platform
# This is the SINGLE SOURCE OF TRUTH for all schemas

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
BUCKET_NAME = "orders-data-prod"


def create_all_databases(glue_client):
    """
    Create the complete database hierarchy
    
    Naming convention: {domain}_{layer}
    → Consistent across ALL domains
    → Enables automated discovery and governance
    """
    databases = [
        {
            "Name": "orders_bronze",
            "Description": "Raw ingestion — Orders domain. Immutable, partitioned by arrival time.",
            "Parameters": {"domain": "orders", "layer": "bronze", "access_level": "etl_only"}
        },
        {
            "Name": "orders_silver",
            "Description": "Cleaned & deduplicated — Orders domain. Schema-validated, typed.",
            "Parameters": {"domain": "orders", "layer": "silver", "access_level": "etl_and_ds"}
        },
        {
            "Name": "orders_gold",
            "Description": "Business-ready data products — Orders domain. Curated, SLA-backed.",
            "Parameters": {"domain": "orders", "layer": "gold", "access_level": "all_analysts"}
        },
        {
            "Name": "customers_gold",
            "Description": "Business-ready data products — Customer domain.",
            "Parameters": {"domain": "customers", "layer": "gold", "access_level": "all_analysts"}
        },
        {
            "Name": "products_gold",
            "Description": "Business-ready data products — Product domain.",
            "Parameters": {"domain": "products", "layer": "gold", "access_level": "all_analysts"}
        },
        {
            "Name": "analytics_derived",
            "Description": "Derived datasets — cross-domain joins, ML features, aggregations.",
            "Parameters": {"domain": "analytics", "layer": "derived", "access_level": "analytics_team"}
        },
        {
            "Name": "federated_sources",
            "Description": "Federated query connectors — live access to MariaDB, APIs.",
            "Parameters": {"domain": "platform", "layer": "federated", "access_level": "restricted"}
        }
    ]

    print("📚 Creating Glue Catalog databases:")
    for db in databases:
        try:
            glue_client.create_database(DatabaseInput=db)
            print(f"   ✅ {db['Name']}: {db['Description'][:60]}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"   ℹ️  {db['Name']}: already exists")

    return [db["Name"] for db in databases]


def create_gold_iceberg_table(glue_client):
    """
    Create Gold daily_orders table using Apache Iceberg format
    
    WHY ICEBERG (not plain Parquet):
    → Supports UPDATE/DELETE (CDC merge — Project 65)
    → Time travel (query historical snapshots)
    → Automatic compaction (solves Project 62 small files)
    → Schema evolution (add columns safely — Project 45)
    → Partition evolution (change partitioning without rewrite)
    
    Iceberg tables require special table properties
    """
    table_input = {
        "Name": "daily_orders",
        "Description": (
            "Gold data product: Daily orders with full detail. "
            "Iceberg format for ACID transactions and time travel. "
            "SLA: 6h freshness. Owner: orders-team@quickcart.com"
        ),
        "Owner": "orders-domain-team",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "order_id", "Type": "string",
                 "Comment": "Unique order identifier"},
                {"Name": "customer_id", "Type": "string",
                 "Comment": "FK to customers_gold.customer_segments"},
                {"Name": "order_total", "Type": "decimal(10,2)",
                 "Comment": "Total order value in USD"},
                {"Name": "item_count", "Type": "int",
                 "Comment": "Number of line items"},
                {"Name": "status", "Type": "string",
                 "Comment": "pending|shipped|delivered|cancelled|refunded"},
                {"Name": "payment_method", "Type": "string",
                 "Comment": "credit_card|debit_card|paypal|apple_pay"},
                {"Name": "shipping_city", "Type": "string",
                 "Comment": "Delivery city"},
                {"Name": "shipping_country", "Type": "string",
                 "Comment": "2-letter country code"},
                {"Name": "order_date", "Type": "date",
                 "Comment": "Date order was placed"},
                {"Name": "shipped_date", "Type": "date",
                 "Comment": "Date order was shipped (nullable)"},
                {"Name": "created_at", "Type": "timestamp",
                 "Comment": "Source system creation timestamp"},
                {"Name": "updated_at", "Type": "timestamp",
                 "Comment": "Source system last update timestamp"},
                {"Name": "_etl_loaded_at", "Type": "timestamp",
                 "Comment": "When this record was loaded by ETL"}
            ],
            "Location": f"s3://{BUCKET_NAME}/gold/daily_orders/",
            "InputFormat": "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
            "OutputFormat": "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
            },
            "Compressed": True
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            # Iceberg-specific properties
            "table_type": "ICEBERG",
            "metadata_location": f"s3://{BUCKET_NAME}/gold/daily_orders/metadata/v1.metadata.json",
            "classification": "parquet",

            # Data product metadata
            "data_product_name": "daily_orders",
            "data_product_version": "v3",
            "domain": "orders",
            "sla_freshness_hours": "6",
            "owner_email": "orders-team@quickcart.com",
            "quality_threshold": "99"
        }
    }

    try:
        glue_client.create_table(
            DatabaseName="orders_gold",
            TableInput=table_input
        )
        print(f"   ✅ orders_gold.daily_orders (Iceberg)")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"   ℹ️  orders_gold.daily_orders exists")


def create_clickstream_gold_table(glue_client):
    """
    Clickstream Gold table — already created in Project 72
    Just verify it exists and has partition projection
    """
    try:
        response = glue_client.get_table(
            DatabaseName="orders_gold",
            Name="clickstream_events"
        )
        params = response["Table"].get("Parameters", {})
        has_projection = params.get("projection.enabled") == "true"
        print(f"   ✅ orders_gold.clickstream_events "
              f"(partition projection: {'enabled' if has_projection else 'DISABLED — fix this!'})")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"   ⚠️  orders_gold.clickstream_events NOT FOUND")
        print(f"       → Run Project 72 (72_07_create_gold_catalog_table.py) first")


def create_analytics_derived_tables(glue_client):
    """
    Create derived tables for cross-domain analytics
    These are OUTPUT tables from analytics domain joins
    """
    tables = [
        {
            "Name": "orders_with_customer_segments",
            "Description": "Join: daily_orders × customer_segments. Updated daily.",
            "Columns": [
                {"Name": "order_id", "Type": "string"},
                {"Name": "customer_id", "Type": "string"},
                {"Name": "order_total", "Type": "decimal(10,2)"},
                {"Name": "status", "Type": "string"},
                {"Name": "order_date", "Type": "date"},
                {"Name": "customer_tier", "Type": "string"},
                {"Name": "customer_ltv", "Type": "decimal(12,2)"},
                {"Name": "customer_segment", "Type": "string"},
                {"Name": "customer_city", "Type": "string"}
            ],
            "Location": f"s3://analytics-data-prod/derived/orders_with_segments/"
        },
        {
            "Name": "hourly_clickstream_summary",
            "Description": "Hourly aggregation of clickstream events by event_type and device.",
            "Columns": [
                {"Name": "event_hour", "Type": "timestamp"},
                {"Name": "event_type", "Type": "string"},
                {"Name": "device_type", "Type": "string"},
                {"Name": "event_count", "Type": "bigint"},
                {"Name": "unique_users", "Type": "bigint"},
                {"Name": "unique_sessions", "Type": "bigint"},
                {"Name": "avg_cart_value", "Type": "double"},
                {"Name": "total_cart_value", "Type": "double"}
            ],
            "Location": f"s3://analytics-data-prod/derived/hourly_clickstream_summary/"
        },
        {
            "Name": "product_demand_signals",
            "Description": "Join: clickstream × orders × products. ML feature table.",
            "Columns": [
                {"Name": "product_id", "Type": "string"},
                {"Name": "date", "Type": "date"},
                {"Name": "page_views", "Type": "bigint"},
                {"Name": "add_to_carts", "Type": "bigint"},
                {"Name": "purchases", "Type": "bigint"},
                {"Name": "conversion_rate", "Type": "double"},
                {"Name": "avg_time_to_purchase_hours", "Type": "double"},
                {"Name": "primary_device", "Type": "string"},
                {"Name": "primary_country", "Type": "string"}
            ],
            "Location": f"s3://analytics-data-prod/derived/product_demand_signals/"
        }
    ]

    for table_def in tables:
        table_input = {
            "Name": table_def["Name"],
            "Description": table_def["Description"],
            "StorageDescriptor": {
                "Columns": table_def["Columns"],
                "Location": table_def["Location"],
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
                "compressionType": "snappy"
            }
        }

        try:
            glue_client.create_table(
                DatabaseName="analytics_derived",
                TableInput=table_input
            )
            print(f"   ✅ analytics_derived.{table_def['Name']}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"   ℹ️  analytics_derived.{table_def['Name']} exists")


def print_catalog_summary(glue_client):
    """Print complete catalog inventory"""
    print(f"\n{'=' * 70}")
    print("📊 COMPLETE GLUE DATA CATALOG INVENTORY")
    print(f"{'=' * 70}")

    databases = glue_client.get_databases()
    total_tables = 0

    for db in sorted(databases["DatabaseList"], key=lambda x: x["Name"]):
        db_name = db["Name"]
        # Skip default database
        if db_name == "default":
            continue

        tables = glue_client.get_tables(DatabaseName=db_name)
        table_list = tables.get("TableList", [])
        total_tables += len(table_list)

        layer = db.get("Parameters", {}).get("layer", "unknown")
        domain = db.get("Parameters", {}).get("domain", "unknown")

        print(f"\n   📁 {db_name} [{domain}/{layer}] — {len(table_list)} table(s)")
        for table in sorted(table_list, key=lambda x: x["Name"]):
            t_name = table["Name"]
            col_count = len(table["StorageDescriptor"].get("Columns", []))
            part_count = len(table.get("PartitionKeys", []))
            is_iceberg = table.get("Parameters", {}).get("table_type") == "ICEBERG"
            fmt = "Iceberg" if is_iceberg else "Parquet"

            print(f"      └── {t_name} ({col_count} cols, {part_count} partitions, {fmt})")

    print(f"\n   📈 Total: {len(databases['DatabaseList'])-1} databases, {total_tables} tables")
    print(f"{'=' * 70}")


def main():
    print("=" * 70)
    print("📚 UNIFIED GLUE DATA CATALOG SETUP")
    print("   Single source of truth for ALL platform schemas")
    print("=" * 70)

    glue_client = boto3.client("glue", region_name=REGION)

    # Step 1: Create all databases
    print("\n--- Step 1: Databases ---")
    create_all_databases(glue_client)

    # Step 2: Create Gold tables
    print("\n--- Step 2: Gold Data Product Tables ---")
    create_gold_iceberg_table(glue_client)
    create_clickstream_gold_table(glue_client)

    # Step 3: Create Analytics Derived tables
    print("\n--- Step 3: Analytics Derived Tables ---")
    create_analytics_derived_tables(glue_client)

    # Step 4: Print complete inventory
    print_catalog_summary(glue_client)

    print("\n✅ UNIFIED CATALOG SETUP COMPLETE")


if __name__ == "__main__":
    main()