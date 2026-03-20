# file: 72_07_create_gold_catalog_table.py
# Register the Gold clickstream_events table in Glue Catalog
# This enables Athena and Redshift Spectrum queries

import boto3

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
BUCKET_NAME = "orders-data-prod"
GLUE_DB = "orders_gold"
GLUE_TABLE = "clickstream_events"


def create_gold_database(glue_client):
    """Ensure Gold database exists (may already exist from Project 71)"""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": GLUE_DB,
                "Description": "Gold layer data products for orders domain",
                "Parameters": {
                    "domain": "orders",
                    "layer": "gold",
                    "mesh_role": "producer"
                }
            }
        )
        print(f"✅ Glue database created: {GLUE_DB}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Glue database exists: {GLUE_DB}")


def create_gold_clickstream_table(glue_client):
    """
    Register Gold clickstream_events table
    
    KEY DESIGN CHOICES:
    → Parquet + Snappy (optimal for analytical queries)
    → Partitioned by year/month/day/hour (matches ETL output)
    → Partition projection ENABLED (avoids MSCK REPAIR — Project 26)
    → All columns from Gold ETL output (72_04)
    """
    try:
        glue_client.create_table(
            DatabaseName=GLUE_DB,
            TableInput={
                "Name": GLUE_TABLE,
                "Description": (
                    "Real-time clickstream events — Gold data product. "
                    "Updated every 5 minutes via micro-batch pipeline. "
                    "SLA: 15 min freshness. Owner: orders-domain-team."
                ),
                "Owner": "orders-domain-team",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "event_id", "Type": "string",
                         "Comment": "UUID — unique event identifier (deduplicated)"},
                        {"Name": "event_type", "Type": "string",
                         "Comment": "page_view|product_click|search|add_to_cart|checkout_start|checkout_complete"},
                        {"Name": "user_id", "Type": "string",
                         "Comment": "Customer ID — FK to customer domain"},
                        {"Name": "session_id", "Type": "string",
                         "Comment": "Browser session identifier"},
                        {"Name": "event_ts", "Type": "timestamp",
                         "Comment": "Event timestamp (parsed from ISO string)"},
                        {"Name": "event_timestamp", "Type": "string",
                         "Comment": "Original ISO 8601 timestamp string"},
                        {"Name": "page_url", "Type": "string",
                         "Comment": "URL path of the page"},
                        {"Name": "product_id", "Type": "string",
                         "Comment": "Product ID — FK to product domain (nullable)"},
                        {"Name": "search_query", "Type": "string",
                         "Comment": "Search query text (nullable — only for search events)"},
                        {"Name": "device_type", "Type": "string",
                         "Comment": "mobile|desktop|tablet"},
                        {"Name": "browser", "Type": "string",
                         "Comment": "Browser name"},
                        {"Name": "ip_country", "Type": "string",
                         "Comment": "2-letter country code from IP geolocation"},
                        {"Name": "referrer", "Type": "string",
                         "Comment": "Traffic source domain"},
                        {"Name": "cart_value", "Type": "double",
                         "Comment": "Cart total value (nullable — only for cart/checkout events)"},
                        {"Name": "items_in_cart", "Type": "int",
                         "Comment": "Number of items in cart (nullable)"},
                        {"Name": "event_date", "Type": "date",
                         "Comment": "Date extracted from event_ts"},
                        {"Name": "event_hour", "Type": "int",
                         "Comment": "Hour (0-23) extracted from event_ts"},
                        {"Name": "_processed_at", "Type": "timestamp",
                         "Comment": "When this record was processed by Glue ETL"}
                    ],
                    "Location": f"s3://{BUCKET_NAME}/gold/clickstream_events/",
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
                    {"Name": "day", "Type": "string"},
                    {"Name": "hour", "Type": "string"}
                ],
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    # Standard classification
                    "classification": "parquet",
                    "compressionType": "snappy",
                    "typeOfData": "file",

                    # PARTITION PROJECTION (Project 26 pattern)
                    # Athena auto-discovers partitions without MSCK REPAIR
                    "projection.enabled": "true",

                    "projection.year.type": "integer",
                    "projection.year.range": "2025,2030",

                    "projection.month.type": "integer",
                    "projection.month.range": "1,12",
                    "projection.month.digits": "2",

                    "projection.day.type": "integer",
                    "projection.day.range": "1,31",
                    "projection.day.digits": "2",

                    "projection.hour.type": "integer",
                    "projection.hour.range": "0,23",
                    "projection.hour.digits": "2",

                    "storage.location.template": (
                        f"s3://{BUCKET_NAME}/gold/clickstream_events/"
                        "year=${year}/month=${month}/day=${day}/hour=${hour}/"
                    ),

                    # Data Mesh metadata
                    "data_product_name": "clickstream_events",
                    "data_product_version": "v1",
                    "domain": "orders",
                    "delivery_type": "micro-batch",
                    "sla_freshness_minutes": "15",
                    "owner_email": "orders-team@quickcart.com",
                    "quality_threshold": "99"
                }
            }
        )
        print(f"✅ Gold table created: {GLUE_DB}.{GLUE_TABLE}")
        print(f"   Partition projection: ENABLED (no MSCK REPAIR needed)")
        print(f"   Location: s3://{BUCKET_NAME}/gold/clickstream_events/")

    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Gold table exists: {GLUE_DB}.{GLUE_TABLE}")


def verify_table_queryable(athena_client):
    """
    Run a quick Athena query to verify the table is accessible
    Even if empty — confirms catalog registration is correct
    """
    query = f"""
    SELECT COUNT(*) as total_events,
           COUNT(DISTINCT user_id) as unique_users,
           COUNT(DISTINCT session_id) as unique_sessions
    FROM {GLUE_DB}.{GLUE_TABLE}
    WHERE year = '2025'
      AND month = '01'
      AND day = '15'
    """

    print(f"\n🔍 Verifying table with Athena query...")
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": GLUE_DB},
            ResultConfiguration={
                "OutputLocation": f"s3://{BUCKET_NAME}/athena-results/verification/"
            }
        )
        query_id = response["QueryExecutionId"]
        print(f"   Query started: {query_id}")
        print(f"   (Check Athena console for results)")
    except Exception as e:
        print(f"   ⚠️  Query test failed: {e}")
        print(f"   (This is OK if Athena workgroup not configured yet)")


def main():
    print("=" * 70)
    print("📋 REGISTERING GOLD CLICKSTREAM TABLE IN GLUE CATALOG")
    print("=" * 70)

    glue_client = boto3.client("glue", region_name=REGION)
    athena_client = boto3.client("athena", region_name=REGION)

    create_gold_database(glue_client)
    create_gold_clickstream_table(glue_client)
    verify_table_queryable(athena_client)

    print("\n" + "=" * 70)
    print("✅ GOLD TABLE REGISTRATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
