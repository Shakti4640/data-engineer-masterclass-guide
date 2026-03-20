# file: 08_monitor_crawlers.py
# Check crawler status, history, and set up alerts
# Integrates with SNS notifications (Project 12)

import boto3
from datetime import datetime, timedelta

REGION = "us-east-2"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:123456789012:data-pipeline-alerts"

CRAWLER_NAMES = [
    "quickcart-orders-csv-crawler",
    "quickcart-customers-csv-crawler",
    "quickcart-products-csv-crawler",
    "quickcart-parquet-orders-crawler"
]


def check_all_crawler_status():
    """
    Get current status of all crawlers
    
    STATES:
    → READY: idle, not running
    → RUNNING: currently scanning
    → STOPPING: finishing up
    
    LAST CRAWL STATUS:
    → SUCCEEDED: completed without errors
    → FAILED: encountered errors
    → CANCELLED: manually stopped
    """
    glue_client = boto3.client("glue", region_name=REGION)

    print("=" * 70)
    print("🕷️  CRAWLER STATUS DASHBOARD")
    print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    alerts = []

    for crawler_name in CRAWLER_NAMES:
        try:
            response = glue_client.get_crawler(Name=crawler_name)
            crawler = response["Crawler"]

            state = crawler["State"]
            last_crawl = crawler.get("LastCrawl", {})
            last_status = last_crawl.get("Status", "NEVER_RUN")
            last_time = last_crawl.get("StartTime", None)
            tables_created = last_crawl.get("TablesCreated", 0)
            tables_updated = last_crawl.get("TablesUpdated", 0)
            tables_deleted = last_crawl.get("TablesDeleted", 0)
            error_msg = last_crawl.get("ErrorMessage", "")

            # Determine emoji
            if last_status == "SUCCEEDED":
                emoji = "✅"
            elif last_status == "FAILED":
                emoji = "❌"
                alerts.append(f"FAILED: {crawler_name} — {error_msg}")
            elif last_status == "NEVER_RUN":
                emoji = "⏸️"
            else:
                emoji = "⚠️"

            # Time since last run
            if last_time:
                hours_ago = round((datetime.now(last_time.tzinfo) - last_time).total_seconds() / 3600, 1)
                time_str = f"{hours_ago}h ago"
                
                # Alert if crawler hasn't run in 36 hours (should run daily)
                if hours_ago > 36:
                    alerts.append(f"STALE: {crawler_name} — last run {hours_ago}h ago")
            else:
                time_str = "Never"

            print(f"\n   {emoji} {crawler_name}")
            print(f"      State:          {state}")
            print(f"      Last Status:    {last_status}")
            print(f"      Last Run:       {time_str}")
            print(f"      Tables Created: {tables_created}")
            print(f"      Tables Updated: {tables_updated}")
            print(f"      Tables Deleted: {tables_deleted}")
            if error_msg:
                print(f"      Error:          {error_msg}")

            # Alert if tables were deleted unexpectedly
            if tables_deleted > 0:
                alerts.append(f"DELETED: {crawler_name} deleted {tables_deleted} table(s)")

        except Exception as e:
            print(f"\n   ❌ {crawler_name}")
            print(f"      Error checking status: {e}")
            alerts.append(f"UNREACHABLE: {crawler_name} — {e}")

    # --- SEND ALERTS IF ANY ---
    if alerts:
        print(f"\n{'=' * 70}")
        print(f"🚨 ALERTS ({len(alerts)}):")
        for alert in alerts:
            print(f"   ⚠️  {alert}")

        send_crawler_alert(alerts)
    else:
        print(f"\n{'=' * 70}")
        print(f"✅ All crawlers healthy — no alerts")

    print("=" * 70)
    return alerts


def send_crawler_alert(alerts):
    """Send SNS alert for crawler issues — pattern from Project 12"""
    try:
        sns_client = boto3.client("sns", region_name=REGION)
        
        message = "GLUE CRAWLER ALERTS\n\n"
        for alert in alerts:
            message += f"• {alert}\n"
        message += f"\nTimestamp: {datetime.now().isoformat()}"

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"⚠️ Glue Crawler Alert — {len(alerts)} issue(s)",
            Message=message
        )
        print(f"   📧 Alert sent to SNS topic")
    except Exception as e:
        print(f"   ⚠️  Failed to send SNS alert: {e}")


def get_crawler_run_history(crawler_name, days=7):
    """
    Get recent run history using CloudWatch metrics
    
    NOTE: Glue doesn't have a built-in "run history" API
    → Use CloudWatch metrics for timing data
    → Use CloudWatch Logs for detailed per-run info
    → Log group: /aws-glue/crawlers
    """
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    print(f"\n📊 Run history for: {crawler_name} (last {days} days)")

    response = cloudwatch.get_metric_statistics(
        Namespace="Glue",
        MetricName="glue.crawler.duration",
        Dimensions=[
            {"Name": "CrawlerName", "Value": crawler_name},
            {"Name": "Type", "Value": "gauge"}
        ],
        StartTime=datetime.utcnow() - timedelta(days=days),
        EndTime=datetime.utcnow(),
        Period=86400,   # 1 day
        Statistics=["Average", "Maximum"]
    )

    datapoints = sorted(response["Datapoints"], key=lambda x: x["Timestamp"])

    if not datapoints:
        print(f"   No metrics found (crawler may not have run recently)")
        return

    for dp in datapoints:
        date = dp["Timestamp"].strftime("%Y-%m-%d")
        avg_sec = round(dp["Average"], 1)
        max_sec = round(dp["Maximum"], 1)
        print(f"   {date}: avg {avg_sec}s, max {max_sec}s")


if __name__ == "__main__":
    check_all_crawler_status()

    # Uncomment to see run history
    # for name in CRAWLER_NAMES:
    #     get_crawler_run_history(name)