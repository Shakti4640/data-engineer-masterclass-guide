# file: 47_04_setup_dr_region.py
# Purpose: Pre-configure Glue Catalog and Athena in DR region (us-west-2)
# This must be done BEFORE a disaster — warm standby

import boto3

DR_REGION = "us-west-2"
DR_BUCKET = "quickcart-datalake-dr"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]


def create_dr_glue_catalog():
    """
    Create Glue Catalog databases and tables in DR region.
    
    Uses PARTITION PROJECTION (Project 26 concept) instead of Crawlers.
    Why: partition projection doesn't need Crawlers to discover new partitions.
    The moment CRR replicates a new partition to DR, Athena can query it.
    """
    glue_client = boto3.client("glue", region_name=DR_REGION)

    databases = {
        "quickcart_silver_dr": {
            "description": "Silver zone — DR replica from us-east-2",
            "location": f"s3://{DR_BUCKET}/silver/"
        },
        "quickcart_gold_dr": {
            "description": "Gold zone — DR replica from us-east-2",
            "location": f"s3://{DR_BUCKET}/gold/"
        }
    }

    for db_name, db_config in databases.items():
        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": db_name,
                    "Description": db_config["description"],
                    "LocationUri": db_config["location"]
                }
            )
            print(f"✅ DR Glue Database: {db_name}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"ℹ️  Already exists: {db_name}")

    # Create tables with partition projection
    # This eliminates the need for Crawlers in DR region
    # Athena auto-discovers partitions based on projection config
    create_projected_tables(glue_client)


def create_projected_tables(glue_client):
    """
    Create Glue Catalog tables with PARTITION PROJECTION.
    
    WHY partition projection for DR:
    → No Crawler needed — Athena infers partitions from S3 path pattern
    → The moment CRR replicates a new partition, Athena can query it
    → No lag between replication and query-ability
    → Zero operational overhead in DR region
    
    HOW partition projection works:
    → Define: partition key names, types, ranges
    → Define: S3 path pattern using partition keys
    → Athena generates partition list at query time (not stored in catalog)
    → No MSCK REPAIR TABLE needed
    """

    # ━━━ GOLD TABLES ━━━
    gold_tables = {
        "customer_360": {
            "columns": [
                {"Name": "customer_id", "Type": "string"},
                {"Name": "name", "Type": "string"},
                {"Name": "email", "Type": "string"},
                {"Name": "city", "Type": "string"},
                {"Name": "state", "Type": "string"},
                {"Name": "tier", "Type": "string"},
                {"Name": "signup_date", "Type": "date"},
                {"Name": "total_orders", "Type": "int"},
                {"Name": "lifetime_value", "Type": "decimal(12,2)"},
                {"Name": "avg_order_value", "Type": "decimal(10,2)"},
                {"Name": "first_order_date", "Type": "date"},
                {"Name": "last_order_date", "Type": "date"},
                {"Name": "days_since_last_order", "Type": "int"},
                {"Name": "unique_products_ordered", "Type": "int"},
                {"Name": "churn_risk", "Type": "decimal(5,4)"},
                {"Name": "segment", "Type": "string"},
                {"Name": "computed_at", "Type": "timestamp"},
                {"Name": "_etl_loaded_at", "Type": "timestamp"},
                {"Name": "_etl_source_zone", "Type": "string"},
                {"Name": "_etl_process_date", "Type": "string"}
            ],
            "location": f"s3://{DR_BUCKET}/gold/customer_360/"
        },
        "revenue_daily": {
            "columns": [
                {"Name": "order_date", "Type": "date"},
                {"Name": "segment", "Type": "string"},
                {"Name": "order_count", "Type": "bigint"},
                {"Name": "total_revenue", "Type": "decimal(12,2)"},
                {"Name": "avg_order_value", "Type": "decimal(10,2)"},
                {"Name": "unique_customers", "Type": "bigint"},
                {"Name": "computed_at", "Type": "timestamp"}
            ],
            "location": f"s3://{DR_BUCKET}/gold/revenue_daily/"
        },
        "product_performance": {
            "columns": [
                {"Name": "product_id", "Type": "string"},
                {"Name": "name", "Type": "string"},
                {"Name": "category", "Type": "string"},
                {"Name": "price", "Type": "decimal(10,2)"},
                {"Name": "stock_quantity", "Type": "int"},
                {"Name": "is_active", "Type": "boolean"},
                {"Name": "times_ordered", "Type": "bigint"},
                {"Name": "total_units_sold", "Type": "bigint"},
                {"Name": "total_revenue", "Type": "decimal(12,2)"},
                {"Name": "unique_buyers", "Type": "bigint"},
                {"Name": "computed_at", "Type": "timestamp"}
            ],
            "location": f"s3://{DR_BUCKET}/gold/product_performance/"
        }
    }

    # Partition projection parameters (same for all tables)
    partition_projection_params = {
        "projection.enabled": "true",
        "projection.year.type": "integer",
        "projection.year.range": "2024,2030",
        "projection.month.type": "integer",
        "projection.month.range": "1,12",
        "projection.month.digits": "2",
        "projection.day.type": "integer",
        "projection.day.range": "1,31",
        "projection.day.digits": "2",
        "storage.location.template": ""  # Set per table
    }

    partition_keys = [
        {"Name": "year", "Type": "string"},
        {"Name": "month", "Type": "string"},
        {"Name": "day", "Type": "string"}
    ]

    for table_name, table_config in gold_tables.items():
        params = partition_projection_params.copy()
        params["storage.location.template"] = (
            f"{table_config['location']}"
            f"year=${{year}}/month=${{month}}/day=${{day}}/"
        )

        try:
            glue_client.create_table(
                DatabaseName="quickcart_gold_dr",
                TableInput={
                    "Name": table_name,
                    "Description": f"DR replica — {table_name} (partition projected)",
                    "StorageDescriptor": {
                        "Columns": table_config["columns"],
                        "Location": table_config["location"],
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                        }
                    },
                    "PartitionKeys": partition_keys,
                    "Parameters": params,
                    "TableType": "EXTERNAL_TABLE"
                }
            )
            print(f"   ✅ DR Table: quickcart_gold_dr.{table_name}")
            print(f"      Partition projection: enabled (year/month/day)")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"   ℹ️  Already exists: quickcart_gold_dr.{table_name}")

    # ━━━ SILVER TABLES ━━━
    silver_tables = {
        "orders": {
            "columns": [
                {"Name": "order_id", "Type": "string"},
                {"Name": "customer_id", "Type": "string"},
                {"Name": "product_id", "Type": "string"},
                {"Name": "quantity", "Type": "int"},
                {"Name": "unit_price", "Type": "decimal(10,4)"},
                {"Name": "total_amount", "Type": "decimal(12,2)"},
                {"Name": "status", "Type": "string"},
                {"Name": "order_date", "Type": "date"},
                {"Name": "created_at", "Type": "timestamp"},
                {"Name": "updated_at", "Type": "timestamp"},
                {"Name": "_etl_loaded_at", "Type": "timestamp"},
                {"Name": "_etl_source_zone", "Type": "string"},
                {"Name": "_etl_process_date", "Type": "string"}
            ],
            "location": f"s3://{DR_BUCKET}/silver/orders/"
        },
        "customers": {
            "columns": [
                {"Name": "customer_id", "Type": "string"},
                {"Name": "name", "Type": "string"},
                {"Name": "email", "Type": "string"},
                {"Name": "city", "Type": "string"},
                {"Name": "state", "Type": "string"},
                {"Name": "tier", "Type": "string"},
                {"Name": "signup_date", "Type": "date"},
                {"Name": "created_at", "Type": "timestamp"},
                {"Name": "updated_at", "Type": "timestamp"}
            ],
            "location": f"s3://{DR_BUCKET}/silver/customers/"
        },
        "products": {
            "columns": [
                {"Name": "product_id", "Type": "string"},
                {"Name": "name", "Type": "string"},
                {"Name": "category", "Type": "string"},
                {"Name": "price", "Type": "decimal(10,2)"},
                {"Name": "stock_quantity", "Type": "int"},
                {"Name": "is_active", "Type": "boolean"},
                {"Name": "created_at", "Type": "timestamp"},
                {"Name": "updated_at", "Type": "timestamp"}
            ],
            "location": f"s3://{DR_BUCKET}/silver/products/"
        }
    }

    for table_name, table_config in silver_tables.items():
        params = partition_projection_params.copy()
        params["storage.location.template"] = (
            f"{table_config['location']}"
            f"year=${{year}}/month=${{month}}/day=${{day}}/"
        )

        try:
            glue_client.create_table(
                DatabaseName="quickcart_silver_dr",
                TableInput={
                    "Name": table_name,
                    "Description": f"DR replica — {table_name} (partition projected)",
                    "StorageDescriptor": {
                        "Columns": table_config["columns"],
                        "Location": table_config["location"],
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                        }
                    },
                    "PartitionKeys": partition_keys,
                    "Parameters": params,
                    "TableType": "EXTERNAL_TABLE"
                }
            )
            print(f"   ✅ DR Table: quickcart_silver_dr.{table_name}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"   ℹ️  Already exists: quickcart_silver_dr.{table_name}")


def create_dr_athena_workgroup():
    """Create Athena workgroup in DR region for failover queries"""
    athena_client = boto3.client("athena", region_name=DR_REGION)

    workgroups = [
        {
            "name": "dr-dashboards",
            "description": "DR workgroup for dashboard queries — activated during failover",
            "output": f"s3://{DR_BUCKET}/_system/athena-results/dashboards/"
        },
        {
            "name": "dr-adhoc",
            "description": "DR workgroup for ad-hoc queries — activated during failover",
            "output": f"s3://{DR_BUCKET}/_system/athena-results/adhoc/"
        }
    ]

    for wg in workgroups:
        try:
            athena_client.create_work_group(
                Name=wg["name"],
                Description=wg["description"],
                Configuration={
                    "ResultConfiguration": {
                        "OutputLocation": wg["output"],
                        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
                    },
                    "EnforceWorkGroupConfiguration": True,
                    "PublishCloudWatchMetricsEnabled": True,
                    "BytesScannedCutoffPerQuery": 10737418240,
                    "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"}
                },
                Tags=[
                    {"Key": "Environment", "Value": "DR"},
                    {"Key": "Project", "Value": "QuickCart-DR"}
                ]
            )
            print(f"✅ DR Athena Workgroup: {wg['name']}")
        except Exception as e:
            if "AlreadyExists" in str(e):
                print(f"ℹ️  Already exists: {wg['name']}")
            else:
                raise


def test_dr_athena_query():
    """
    Test that DR Athena can query replicated data.
    This is the critical test — validates the entire DR chain works.
    """
    athena_client = boto3.client("athena", region_name=DR_REGION)
    import time

    print(f"\n{'='*60}")
    print(f"🧪 DR ATHENA QUERY TEST")
    print(f"{'='*60}")

    test_queries = [
        {
            "name": "Gold — customer_360 count",
            "sql": "SELECT COUNT(*) as total_customers FROM quickcart_gold_dr.customer_360 WHERE year = '2025'"
        },
        {
            "name": "Gold — revenue by segment",
            "sql": """
                SELECT segment, COUNT(*) as cnt, SUM(lifetime_value) as total_ltv
                FROM quickcart_gold_dr.customer_360
                WHERE year = '2025' AND month = '01'
                GROUP BY segment
                ORDER BY total_ltv DESC
            """
        },
        {
            "name": "Silver — recent orders",
            "sql": "SELECT COUNT(*) as order_count FROM quickcart_silver_dr.orders WHERE year = '2025'"
        }
    ]

    for query_config in test_queries:
        print(f"\n   📋 Test: {query_config['name']}")

        try:
            response = athena_client.start_query_execution(
                QueryString=query_config["sql"],
                WorkGroup="dr-adhoc",
                ResultConfiguration={
                    "OutputLocation": f"s3://{DR_BUCKET}/_system/athena-results/dr-test/"
                }
            )
            execution_id = response["QueryExecutionId"]

            # Wait for completion
            for _ in range(60):
                status = athena_client.get_query_execution(QueryExecutionId=execution_id)
                state = status["QueryExecution"]["Status"]["State"]
                if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    break
                time.sleep(2)

            if state == "SUCCEEDED":
                results = athena_client.get_query_results(
                    QueryExecutionId=execution_id, MaxResults=5
                )
                rows = results["ResultSet"]["Rows"]

                if len(rows) > 1:
                    headers = [c.get("VarCharValue", "") for c in rows[0]["Data"]]
                    values = [c.get("VarCharValue", "NULL") for c in rows[1]["Data"]]
                    print(f"   ✅ SUCCEEDED — {dict(zip(headers, values))}")
                else:
                    print(f"   ✅ SUCCEEDED — (no data rows, but query worked)")
            else:
                error = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                print(f"   ❌ {state}: {error[:200]}")

        except Exception as e:
            print(f"   ❌ Error: {e}")

    print(f"\n{'='*60}")


if __name__ == "__main__":
    print("=" * 60)
    print("🏗️  SETTING UP DR REGION (us-west-2)")
    print("=" * 60)

    create_dr_glue_catalog()
    create_dr_athena_workgroup()
    test_dr_athena_query()

    print(f"\n✅ DR REGION READY")
    print(f"   Glue Catalog: quickcart_silver_dr, quickcart_gold_dr")
    print(f"   Athena Workgroups: dr-dashboards, dr-adhoc")
    print(f"   Partition Projection: enabled (no Crawler needed)")