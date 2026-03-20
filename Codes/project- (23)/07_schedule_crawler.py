# file: 07_schedule_crawler.py
# Set up crawlers to run on a schedule
# So new daily files are automatically discovered

import boto3

REGION = "us-east-2"


def update_crawler_schedule(crawler_name, cron_expression):
    """
    Set crawler to run on a cron schedule
    
    GLUE CRON FORMAT (similar to standard cron):
    → cron(Minutes Hours Day-of-month Month Day-of-week Year)
    → cron(0 3 * * ? *)     → Every day at 3:00 AM UTC
    → cron(0 */6 * * ? *)   → Every 6 hours
    → cron(0 3 ? * MON *)   → Every Monday at 3:00 AM
    
    TIMING STRATEGY:
    → Data lands at midnight (Project 1 upload script)
    → Crawler runs at 3:00 AM (gives upload time to complete)
    → Athena queries use updated catalog from 3:00 AM onward
    → Dashboard refreshes at 6:00 AM (data fully available)
    
    WHY NOT EVENT-BASED:
    → Event-based requires EventBridge + Lambda setup
    → For daily batch: cron is simpler and sufficient
    → Event-based better for: streaming or unpredictable arrivals
    → Covered in Project 41 (event-driven architecture)
    """
    glue_client = boto3.client("glue", region_name=REGION)

    glue_client.update_crawler(
        Name=crawler_name,
        Schedule=cron_expression
    )
    print(f"✅ Scheduled: {crawler_name}")
    print(f"   Cron: {cron_expression}")


def schedule_all_crawlers():
    """Set all crawlers to run at 3 AM UTC daily"""
    print("=" * 60)
    print("📅 SCHEDULING CRAWLERS")
    print("=" * 60)

    crawlers = [
        "quickcart-orders-csv-crawler",
        "quickcart-customers-csv-crawler",
        "quickcart-products-csv-crawler",
        "quickcart-parquet-orders-crawler"
    ]

    # Stagger by 15 minutes to avoid resource contention
    schedules = [
        "cron(0 3 * * ? *)",     # 3:00 AM UTC
        "cron(15 3 * * ? *)",    # 3:15 AM UTC
        "cron(30 3 * * ? *)",    # 3:30 AM UTC
        "cron(45 3 * * ? *)"     # 3:45 AM UTC
    ]

    for crawler_name, schedule in zip(crawlers, schedules):
        update_crawler_schedule(crawler_name, schedule)

    print(f"\n📋 Schedule summary:")
    print(f"   03:00 UTC — Orders CSV crawler")
    print(f"   03:15 UTC — Customers CSV crawler")
    print(f"   03:30 UTC — Products CSV crawler")
    print(f"   03:45 UTC — Parquet orders crawler")
    print(f"\n💡 Staggered by 15 min to avoid DPU contention")


def remove_crawler_schedule(crawler_name):
    """Remove schedule — crawler becomes on-demand only"""
    glue_client = boto3.client("glue", region_name=REGION)

    glue_client.update_crawler(
        Name=crawler_name,
        Schedule=""
    )
    print(f"✅ Schedule removed: {crawler_name} (now on-demand only)")


if __name__ == "__main__":
    schedule_all_crawlers()