# file: 04_create_and_run_crawlers.py
# Creates multiple crawlers for different data sources
# Then runs them and monitors completion

import boto3
import time
from botocore.exceptions import ClientError

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
DATABASE_NAME = "quickcart_datalake"
CRAWLER_ROLE_ARN = "arn:aws:iam::123456789012:role/GlueCrawlerRole"
CSV_CLASSIFIER = "quickcart-csv-with-header"


def create_crawler(glue_client, crawler_name, s3_targets, table_prefix, 
                   classifiers=None, description=""):
    """
    Create a single Glue Crawler
    
    CONFIGURATION EXPLAINED:
    → Name: unique identifier for this crawler
    → Role: IAM role with S3 + Glue + Logs permissions
    → DatabaseName: where to create discovered tables
    → Targets: S3 paths to scan
    → TablePrefix: prepended to auto-generated table names
    → Classifiers: custom classifiers (checked before built-in)
    → SchemaChangePolicy: how to handle schema evolution
    → RecrawlPolicy: what to re-scan on subsequent runs
    → Configuration: JSON for additional settings
    """
    # --- CRAWLER CONFIGURATION ---
    crawler_config = {
        "Name": crawler_name,
        "Role": CRAWLER_ROLE_ARN,
        "DatabaseName": DATABASE_NAME,
        "Description": description,
        "Targets": {
            "S3Targets": [
                {
                    "Path": target,
                    "Exclusions": [
                        "**.metadata",       # Skip metadata files
                        "**_temporary/**",   # Skip temp files
                        "**_SUCCESS",        # Skip Spark success markers
                        "**.crc"             # Skip checksum files
                    ]
                }
                for target in s3_targets
            ]
        },
        "TablePrefix": table_prefix,
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",    # Apply schema changes
            "DeleteBehavior": "LOG"                    # Log deletions, don't delete tables
        },
        "RecrawlPolicy": {
            "RecrawlBehavior": "CRAWL_EVERYTHING"      # Thorough for first run
        },
        "Configuration": '{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
        "Tags": {
            "Environment": "Production",
            "Team": "Data-Engineering",
            "Source": "S3"
        }
    }

    # Add custom classifiers if specified
    if classifiers:
        crawler_config["Classifiers"] = classifiers

    try:
        glue_client.create_crawler(**crawler_config)
        print(f"✅ Crawler created: {crawler_name}")
        return True

    except glue_client.exceptions.AlreadyExistsException:
        # Update existing crawler
        del crawler_config["Name"]
        del crawler_config["Tags"]
        glue_client.update_crawler(Name=crawler_name, **crawler_config)
        print(f"ℹ️  Crawler updated: {crawler_name}")
        return True

    except ClientError as e:
        print(f"❌ Failed to create crawler {crawler_name}: {e}")
        return False


def start_crawler(glue_client, crawler_name):
    """Start a crawler and return immediately"""
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"🚀 Crawler started: {crawler_name}")
        return True
    except glue_client.exceptions.CrawlerRunningException:
        print(f"⏳ Crawler already running: {crawler_name}")
        return True
    except ClientError as e:
        print(f"❌ Failed to start crawler {crawler_name}: {e}")
        return False


def wait_for_crawler(glue_client, crawler_name, timeout_seconds=600):
    """
    Poll crawler status until completion or timeout
    
    CRAWLER STATES:
    → READY: idle, can be started
    → RUNNING: actively scanning S3 and updating catalog
    → STOPPING: finishing current work
    → State returned by get_crawler() → State field
    
    Last crawl info in: LastCrawl → Status (SUCCEEDED / FAILED / CANCELLED)
    """
    print(f"\n⏳ Waiting for crawler: {crawler_name}")
    start_time = time.time()
    poll_interval = 10  # seconds

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            print(f"❌ Timeout after {timeout_seconds}s — crawler still running")
            return False

        response = glue_client.get_crawler(Name=crawler_name)
        state = response["Crawler"]["State"]

        if state == "READY":
            # Crawler finished — check result
            last_crawl = response["Crawler"].get("LastCrawl", {})
            status = last_crawl.get("Status", "UNKNOWN")
            duration = last_crawl.get("LogGroup", "N/A")
            tables_created = last_crawl.get("TablesCreated", 0)
            tables_updated = last_crawl.get("TablesUpdated", 0)
            tables_deleted = last_crawl.get("TablesDeleted", 0)

            if status == "SUCCEEDED":
                print(f"   ✅ Crawler SUCCEEDED in {round(elapsed)}s")
                print(f"      Tables created: {tables_created}")
                print(f"      Tables updated: {tables_updated}")
                print(f"      Tables deleted: {tables_deleted}")
                return True
            else:
                error_msg = last_crawl.get("ErrorMessage", "No details")
                print(f"   ❌ Crawler FAILED: {status}")
                print(f"      Error: {error_msg}")
                return False
        else:
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)
            print(f"   ⏳ Status: {state} ({mins}m {secs}s elapsed)")
            time.sleep(poll_interval)


def create_all_crawlers():
    """
    Create and run all QuickCart crawlers
    
    CRAWLER STRATEGY:
    → Separate crawlers per data source/format
    → CSV crawlers use custom classifier
    → Parquet crawler uses built-in (Parquet schema is self-describing)
    """
    glue_client = boto3.client("glue", region_name=REGION)

    crawlers = [
        # --- CRAWLER 1: Orders CSV ---
        {
            "name": "quickcart-orders-csv-crawler",
            "targets": [f"s3://{BUCKET_NAME}/daily_exports/orders/"],
            "prefix": "raw_",
            "classifiers": [CSV_CLASSIFIER],
            "description": "Discovers schema of daily order CSV exports"
        },
        # --- CRAWLER 2: Customers CSV ---
        {
            "name": "quickcart-customers-csv-crawler",
            "targets": [f"s3://{BUCKET_NAME}/daily_exports/customers/"],
            "prefix": "raw_",
            "classifiers": [CSV_CLASSIFIER],
            "description": "Discovers schema of daily customer CSV exports"
        },
        # --- CRAWLER 3: Products CSV ---
        {
            "name": "quickcart-products-csv-crawler",
            "targets": [f"s3://{BUCKET_NAME}/daily_exports/products/"],
            "prefix": "raw_",
            "classifiers": [CSV_CLASSIFIER],
            "description": "Discovers schema of daily product CSV exports"
        },
        # --- CRAWLER 4: Parquet from UNLOAD ---
        {
            "name": "quickcart-parquet-orders-crawler",
            "targets": [f"s3://{BUCKET_NAME}/exports/data_science/orders_snapshot/"],
            "prefix": "curated_",
            "classifiers": [],    # No custom classifier — Parquet is self-describing
            "description": "Discovers schema of Parquet exports from Redshift UNLOAD"
        }
    ]

    print("=" * 60)
    print("🕷️  CREATING GLUE CRAWLERS")
    print("=" * 60)

    # --- CREATE ALL CRAWLERS ---
    for crawler in crawlers:
        create_crawler(
            glue_client=glue_client,
            crawler_name=crawler["name"],
            s3_targets=crawler["targets"],
            table_prefix=crawler["prefix"],
            classifiers=crawler["classifiers"],
            description=crawler["description"]
        )

    # --- RUN ALL CRAWLERS ---
    print(f"\n{'=' * 60}")
    print("🚀 STARTING ALL CRAWLERS")
    print("=" * 60)

    for crawler in crawlers:
        start_crawler(glue_client, crawler["name"])
        time.sleep(2)  # Small delay between starts to avoid throttling

    # --- WAIT FOR ALL TO COMPLETE ---
    print(f"\n{'=' * 60}")
    print("⏳ WAITING FOR COMPLETION")
    print("=" * 60)

    results = {}
    for crawler in crawlers:
        success = wait_for_crawler(glue_client, crawler["name"], timeout_seconds=300)
        results[crawler["name"]] = success

    # --- SUMMARY ---
    print(f"\n{'=' * 60}")
    print("📊 CRAWLER EXECUTION SUMMARY")
    print("=" * 60)

    for name, success in results.items():
        status = "✅ SUCCEEDED" if success else "❌ FAILED"
        print(f"   {status} — {name}")

    return results


if __name__ == "__main__":
    create_all_crawlers()