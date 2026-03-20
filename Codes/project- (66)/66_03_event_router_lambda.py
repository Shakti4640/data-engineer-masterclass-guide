# file: 66_03_event_router_lambda.py
# Purpose: Lambda that classifies incoming events and updates state tracker
# Invoked by: Step Functions as first step in pipeline

import boto3
import json
import os
from datetime import datetime, timedelta

dynamodb = boto3.resource("dynamodb")
state_table = dynamodb.Table("pipeline_batch_state")

# Source classification rules
SOURCE_RULES = {
    "quickcart-partner-feeds": {
        "source_name": "partner_feed",
        "glue_job": "etl-partner-feed",
        "required": True
    },
    "quickcart-payment-data": {
        "source_name": "payment_data",
        "glue_job": "etl-payment-data",
        "required": True
    },
    "quickcart-marketing-uploads": {
        "source_name": "marketing_campaign",
        "glue_job": "etl-marketing-campaign",
        "required": False  # Optional — don't block gold layer
    }
}

REQUIRED_SOURCES = [
    "partner_feed",
    "payment_data",
    "cdc_mariadb",
    "inventory_api"
]


def lambda_handler(event, context):
    """
    Classify event, update state tracker, return routing info
    
    INPUT: EventBridge event (S3 Object Created, Glue State Change, or Schedule)
    OUTPUT: routing info for Step Function Choice state
    """
    print(f"📨 Event received: {json.dumps(event)[:500]}")

    # Determine event type
    source = event.get("source", "")
    detail_type = event.get("detail-type", "")
    detail = event.get("detail", {})

    batch_date = datetime.utcnow().strftime("%Y-%m-%d")
    ttl = int((datetime.utcnow() + timedelta(days=90)).timestamp())

    result = {
        "batch_date": batch_date,
        "event_type": "unknown",
        "source_name": "unknown",
        "glue_job": None,
        "file_bucket": None,
        "file_key": None,
        "all_sources_complete": False
    }

    # --- S3 FILE ARRIVAL ---
    if source == "aws.s3" and detail_type == "Object Created":
        bucket = detail.get("bucket", {}).get("name", "")
        key = detail.get("object", {}).get("key", "")

        rule = SOURCE_RULES.get(bucket)
        if rule:
            result["event_type"] = "file_arrival"
            result["source_name"] = rule["source_name"]
            result["glue_job"] = rule["glue_job"]
            result["file_bucket"] = bucket
            result["file_key"] = key

            # Update state tracker
            state_table.put_item(Item={
                "batch_date": batch_date,
                "source_name": rule["source_name"],
                "status": "processing",
                "file_key": f"s3://{bucket}/{key}",
                "started_at": datetime.utcnow().isoformat(),
                "ttl": ttl
            })
            print(f"✅ Classified: {rule['source_name']} → {rule['glue_job']}")
        else:
            print(f"⚠️ Unknown bucket: {bucket} — ignoring")
            result["event_type"] = "unknown_source"

    # --- SCHEDULED EVENT (CDC or Inventory) ---
    elif source == "aws.events" or detail_type == "Scheduled Event":
        schedule_source = event.get("resources", [""])[0]

        if "cdc-trigger" in schedule_source:
            result["event_type"] = "scheduled_cdc"
            result["source_name"] = "cdc_mariadb"
            result["glue_job"] = "cdc-mariadb-to-redshift"

            state_table.put_item(Item={
                "batch_date": batch_date,
                "source_name": "cdc_mariadb",
                "status": "processing",
                "started_at": datetime.utcnow().isoformat(),
                "ttl": ttl
            })

        elif "inventory-trigger" in schedule_source:
            result["event_type"] = "scheduled_inventory"
            result["source_name"] = "inventory_api"
            result["glue_job"] = "etl-inventory-api-pull"

            state_table.put_item(Item={
                "batch_date": batch_date,
                "source_name": "inventory_api",
                "status": "processing",
                "started_at": datetime.utcnow().isoformat(),
                "ttl": ttl
            })

    # --- GLUE JOB COMPLETION ---
    elif source == "aws.glue" and detail_type == "Glue Job State Change":
        job_name = detail.get("jobName", "")
        state = detail.get("state", "")

        if state == "SUCCEEDED":
            result["event_type"] = "etl_completed"
            # Find which source this job belongs to
            for bucket, rule in SOURCE_RULES.items():
                if rule["glue_job"] == job_name:
                    result["source_name"] = rule["source_name"]
                    break

            # Mark source as completed in state tracker
            if result["source_name"] != "unknown":
                state_table.update_item(
                    Key={"batch_date": batch_date, "source_name": result["source_name"]},
                    UpdateExpression="SET #s = :s, completed_at = :t",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":s": "completed",
                        ":t": datetime.utcnow().isoformat()
                    }
                )

    # --- CHECK IF ALL REQUIRED SOURCES COMPLETE ---
    result["all_sources_complete"] = check_all_sources_complete(batch_date)

    print(f"📋 Result: {json.dumps(result)}")
    return result


def check_all_sources_complete(batch_date):
    """
    Query DynamoDB: have ALL required sources completed for this batch?
    """
    response = state_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("batch_date").eq(batch_date)
    )

    completed_sources = {
        item["source_name"]
        for item in response.get("Items", [])
        if item.get("status") == "completed"
    }

    all_complete = all(src in completed_sources for src in REQUIRED_SOURCES)

    print(f"📊 Batch {batch_date}: {len(completed_sources)}/{len(REQUIRED_SOURCES)} required sources complete")
    print(f"   Completed: {completed_sources}")
    print(f"   Required: {set(REQUIRED_SOURCES)}")
    print(f"   Missing: {set(REQUIRED_SOURCES) - completed_sources}")
    print(f"   All complete: {all_complete}")

    return all_complete