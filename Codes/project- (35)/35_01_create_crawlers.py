# file: 35_01_create_crawlers.py
# Purpose: Create Glue Crawlers for bronze and silver layers
# Prerequisites: S3 data exists, Glue Catalog databases exist

import boto3
import json

AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

S3_BUCKET = "quickcart-datalake-prod"
GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/GlueMariaDBExtractorRole"

glue = boto3.client("glue", region_name=AWS_REGION)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Create Bronze Layer Crawler
# ═══════════════════════════════════════════════════════════════════
def create_bronze_crawler():
    """
    Crawler A: Discovers schema and partitions in bronze/mariadb/
    
    WHAT CRAWLER DOES:
    → Scans S3 path: bronze/mariadb/orders/, customers/, products/
    → Reads Parquet file headers to infer schema
    → Creates/updates Glue Catalog tables with:
      - Column names and types
      - Partition keys (year, month, day)
      - New partition values
    → After crawl: Athena can query new data immediately
    
    RECRAWL POLICY:
    → CRAWL_NEW_FOLDERS_ONLY: Only scan new partitions (fast)
    → CRAWL_EVERYTHING: Re-scan all data (slow but thorough)
    → We use CRAWL_NEW_FOLDERS_ONLY for nightly incremental
    """
    print("\n🕷️  Creating Bronze Layer Crawler...")

    crawler_name = "crawl-bronze-mariadb"

    crawler_config = {
        "Name": crawler_name,
        "Role": GLUE_ROLE_ARN,
        "DatabaseName": "bronze_mariadb",
        "Description": "Crawl bronze/mariadb/ to discover new partitions",
        "Targets": {
            "S3Targets": [
                {
                    "Path": f"s3://{S3_BUCKET}/bronze/mariadb/orders/",
                    "Exclusions": ["_temporary/**", "_spark_metadata/**"]
                },
                {
                    "Path": f"s3://{S3_BUCKET}/bronze/mariadb/customers/",
                    "Exclusions": ["_temporary/**"]
                },
                {
                    "Path": f"s3://{S3_BUCKET}/bronze/mariadb/products/",
                    "Exclusions": ["_temporary/**"]
                }
            ]
        },
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG"
        },
        "RecrawlPolicy": {
            "RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"
        },
        "Configuration": json.dumps({
            "Version": 1.0,
            "Grouping": {
                "TableGroupingPolicy": "CombineCompatibleSchemas"
            },
            "CrawlerOutput": {
                "Partitions": {
                    "AddOrUpdateBehavior": "InheritFromTable"
                }
            }
        }),
        "Tags": {
            "Layer": "Bronze",
            "Pipeline": "QuickCart-Nightly"
        }
    }

    try:
        glue.create_crawler(**crawler_config)
        print(f"   ✅ Crawler created: {crawler_name}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_crawler(**crawler_config)
        print(f"   ✅ Crawler updated: {crawler_name}")

    return crawler_name


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Create Silver Layer Crawler
# ═══════════════════════════════════════════════════════════════════
def create_silver_crawler():
    """
    Crawler B: Discovers schema and partitions in silver/
    Runs AFTER silver layer transform job
    """
    print("\n🕷️  Creating Silver Layer Crawler...")

    crawler_name = "crawl-silver-quickcart"

    silver_tables = [
        "clean_orders", "clean_customers", "clean_products",
        "order_details", "daily_revenue_summary",
        "monthly_category_summary", "customer_rfm"
    ]

    s3_targets = [
        {
            "Path": f"s3://{S3_BUCKET}/silver/{table}/",
            "Exclusions": ["_temporary/**", "_spark_metadata/**"]
        }
        for table in silver_tables
    ]

    crawler_config = {
        "Name": crawler_name,
        "Role": GLUE_ROLE_ARN,
        "DatabaseName": "silver_quickcart",
        "Description": "Crawl silver/ to register transformed tables",
        "Targets": {
            "S3Targets": s3_targets
        },
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG"
        },
        "RecrawlPolicy": {
            "RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"
        },
        "Configuration": json.dumps({
            "Version": 1.0,
            "Grouping": {
                "TableGroupingPolicy": "CombineCompatibleSchemas"
            },
            "CrawlerOutput": {
                "Partitions": {
                    "AddOrUpdateBehavior": "InheritFromTable"
                }
            }
        }),
        "Tags": {
            "Layer": "Silver",
            "Pipeline": "QuickCart-Nightly"
        }
    }

    try:
        glue.create_crawler(**crawler_config)
        print(f"   ✅ Crawler created: {crawler_name}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_crawler(**crawler_config)
        print(f"   ✅ Crawler updated: {crawler_name}")

    return crawler_name


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Ensure Catalog Databases Exist
# ═══════════════════════════════════════════════════════════════════
def ensure_catalog_databases():
    """Create Glue Catalog databases for bronze and silver layers"""
    print("\n📚 Ensuring Catalog databases exist...")

    databases = {
        "bronze_mariadb": "Bronze layer — raw MariaDB extracts",
        "silver_quickcart": "Silver layer — cleaned, joined, aggregated data"
    }

    for db_name, description in databases.items():
        try:
            glue.create_database(
                DatabaseInput={
                    "Name": db_name,
                    "Description": description
                }
            )
            print(f"   ✅ Created: {db_name}")
        except glue.exceptions.AlreadyExistsException:
            print(f"   ℹ️  Already exists: {db_name}")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 CREATING CRAWLERS FOR WORKFLOW")
    print("=" * 60)

    ensure_catalog_databases()
    bronze_crawler = create_bronze_crawler()
    silver_crawler = create_silver_crawler()

    print("\n" + "=" * 60)
    print("✅ CRAWLERS READY")
    print(f"   Bronze: {bronze_crawler}")
    print(f"   Silver: {silver_crawler}")
    print("=" * 60)