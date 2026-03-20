# file: lineage/10_scheduled_lineage_refresh.py
# Purpose: Periodically scan CloudTrail + Glue Catalog + Athena views
# to fill gaps in lineage that active emission missed

import boto3
import json
import logging
from datetime import datetime, timezone

from lineage_emitter import LineageEmitter, LineageNode
from sql_lineage_parser import SQLLineageParser, parse_and_emit_view_lineage
from cloudtrail_lineage_collector import CloudTrailLineageCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lineage_refresh")

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"

# Athena views to scan for lineage
VIEWS_TO_SCAN = [
    {"database": "silver", "view_name": "order_metrics"},
    {"database": "silver", "view_name": "customer_segments"},
    {"database": "gold",   "view_name": "daily_revenue_report"},
    {"database": "gold",   "view_name": "product_performance"},
]

# Glue jobs to check via CloudTrail
JOBS_TO_CHECK = [
    "job_extract_orders",
    "job_extract_refunds",
    "job_clean_orders",
    "job_process_refunds",
    "job_build_finance_summary",
]


def refresh_view_lineage():
    """
    Scan all registered Athena views and re-extract lineage
    
    WHY PERIODIC REFRESH:
    → Views can be modified without triggering any event
    → Someone runs ALTER VIEW or CREATE OR REPLACE VIEW
    → Active emission doesn't cover SQL-only changes
    → This cron job catches those changes
    """
    logger.info("═══ REFRESHING ATHENA VIEW LINEAGE ═══")

    athena = boto3.client("athena", region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)

    total_edges = 0

    for view_config in VIEWS_TO_SCAN:
        db = view_config["database"]
        view = view_config["view_name"]

        logger.info(f"Scanning view: {db}.{view}")

        try:
            # Get view definition from Glue Catalog
            response = glue.get_table(
                DatabaseName=db,
                Name=view
            )

            table_def = response.get("Table", {})
            view_text = table_def.get(
                "ViewOriginalText",
                table_def.get("ViewExpandedText", "")
            )

            if not view_text:
                logger.warning(
                    f"  No view text found for {db}.{view} — skipping"
                )
                continue

            # Create emitter for this view
            emitter = LineageEmitter(
                job_name=f"lineage_refresh_view_{db}_{view}",
                job_run_id=f"refresh_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                account_id=ACCOUNT_ID
            )

            # Parse SQL and emit lineage
            edge_count = parse_and_emit_view_lineage(
                database=db,
                view_name=view,
                sql=view_text,
                emitter=emitter
            )

            emitter.finalize()
            total_edges += edge_count
            logger.info(f"  ✅ {edge_count} edges extracted from {db}.{view}")

        except Exception as e:
            logger.error(f"  ❌ Failed to process {db}.{view}: {e}")

    logger.info(f"View lineage refresh complete: {total_edges} total edges")
    return total_edges


def refresh_cloudtrail_lineage():
    """
    Scan CloudTrail for Glue job executions and infer table-level lineage
    
    WHY THIS MATTERS:
    → Some Glue jobs may not have active lineage instrumentation yet
    → New jobs added by other teams might skip lineage emission
    → CloudTrail gives us at least TABLE-LEVEL lineage automatically
    """
    logger.info("═══ REFRESHING CLOUDTRAIL-BASED LINEAGE ═══")

    collector = CloudTrailLineageCollector(account_id=ACCOUNT_ID)
    total_inferred = 0

    for job_name in JOBS_TO_CHECK:
        logger.info(f"Checking CloudTrail for job: {job_name}")

        try:
            result = collector.infer_table_lineage_from_job(
                job_name=job_name,
                hours_back=25  # Last 25 hours (covers daily runs)
            )

            if result["inputs"] or result["outputs"]:
                logger.info(f"  Inputs:  {result['inputs']}")
                logger.info(f"  Outputs: {result['outputs']}")
                total_inferred += len(result["inputs"]) + len(result["outputs"])
            else:
                logger.info(f"  No recent runs found in CloudTrail")

        except Exception as e:
            logger.error(f"  ❌ Failed for {job_name}: {e}")

    logger.info(
        f"CloudTrail lineage refresh complete: "
        f"{total_inferred} table references found"
    )
    return total_inferred


def detect_lineage_gaps():
    """
    Compare active lineage (DynamoDB) with passive lineage (CloudTrail)
    Flag any tables that appear in CloudTrail but NOT in active lineage
    
    These gaps indicate:
    → Jobs that were NOT instrumented with lineage emission
    → Tables accessed by ad-hoc queries (not tracked)
    → New data sources added without lineage registration
    """
    logger.info("═══ DETECTING LINEAGE GAPS ═══")

    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table("DataLineage")

    # Get all known nodes from DynamoDB
    known_nodes = set()
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr("SK").eq("META"),
        ProjectionExpression="PK"
    )

    for item in response.get("Items", []):
        # PK format: NODE#{node_id}
        node_id = item["PK"].replace("NODE#", "")
        # Extract table-level identifier (without column)
        parts = node_id.split(".")
        if len(parts) >= 3:
            # account:service:db.table
            table_ref = ".".join(parts[:2])  # db.table
            known_nodes.add(table_ref)

    # Handle pagination
    while "LastEvaluatedKey" in response:
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr("SK").eq("META"),
            ProjectionExpression="PK",
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        for item in response.get("Items", []):
            node_id = item["PK"].replace("NODE#", "")
            parts = node_id.split(".")
            if len(parts) >= 3:
                table_ref = ".".join(parts[:2])
                known_nodes.add(table_ref)

    logger.info(f"Known tables in lineage graph: {len(known_nodes)}")

    # Get tables from CloudTrail
    collector = CloudTrailLineageCollector(account_id=ACCOUNT_ID)
    cloudtrail_tables = set()

    for job_name in JOBS_TO_CHECK:
        result = collector.infer_table_lineage_from_job(
            job_name=job_name, hours_back=25
        )
        for tbl in result.get("inputs", []) + result.get("outputs", []):
            cloudtrail_tables.add(tbl)

    # Find gaps
    gaps = cloudtrail_tables - known_nodes

    if gaps:
        logger.warning(f"🚨 LINEAGE GAPS DETECTED: {len(gaps)} tables")
        for gap in sorted(gaps):
            logger.warning(f"  → {gap} (in CloudTrail but NOT in lineage graph)")
    else:
        logger.info("✅ No lineage gaps detected — all tables are tracked")

    return gaps


def lambda_handler(event, context):
    """
    Entry point when deployed as Lambda (triggered by EventBridge schedule)
    
    Schedule: every 6 hours
    EventBridge rule (from Project 66 pattern):
    → rate(6 hours)
    → Target: this Lambda function
    """
    logger.info("🔄 LINEAGE REFRESH STARTED")
    logger.info(f"   Triggered at: {datetime.now(timezone.utc).isoformat()}")

    results = {
        "view_edges": refresh_view_lineage(),
        "cloudtrail_refs": refresh_cloudtrail_lineage(),
        "gaps": list(detect_lineage_gaps())
    }

    logger.info("🔄 LINEAGE REFRESH COMPLETE")
    logger.info(f"   Results: {json.dumps(results)}")

    # If gaps found, send alert via SNS (Project 12/54 pattern)
    if results["gaps"]:
        sns = boto3.client("sns", region_name=REGION)
        sns.publish(
            TopicArn="arn:aws:sns:us-east-2:222222222222:data-platform-alerts",
            Subject="⚠️ Lineage Gaps Detected",
            Message=json.dumps({
                "alert": "Tables found in CloudTrail but missing from lineage",
                "gaps": results["gaps"],
                "action": "Instrument these jobs with LineageEmitter",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, indent=2)
        )
        logger.info("📨 Alert sent via SNS for lineage gaps")

    return results


if __name__ == "__main__":
    # Run locally for testing
    lambda_handler({}, None)