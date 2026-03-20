# file: lineage/06_cloudtrail_lineage_collector.py
# Purpose: Collect table-level lineage passively from CloudTrail S3/Glue API logs
# Supplements active lineage with data movement events we didn't instrument

import boto3
import json
from datetime import datetime, timedelta, timezone

REGION = "us-east-2"


class CloudTrailLineageCollector:
    """
    Passive lineage collection from CloudTrail
    
    WHAT CLOUDTRAIL CAPTURES:
    → S3: PutObject, GetObject, CopyObject (who wrote/read which key)
    → Glue: StartJobRun, GetTable, CreateTable (job executions, catalog access)
    → Redshift: COPY, UNLOAD (data movement commands via Data API)
    → Athena: StartQueryExecution (SQL queries run)
    
    WHAT IT CANNOT CAPTURE:
    → Column-level transformations (only table/file-level)
    → Internal Spark/PySpark logic
    → SQL transformation semantics (just that a query ran)
    
    USE CASE:
    → Fill gaps where active instrumentation is missing
    → Detect data movement across accounts (cross-account S3 access)
    → Audit trail: prove data was read from source X on date Y
    """

    def __init__(self, account_id):
        self.account_id = account_id
        self.cloudtrail = boto3.client("cloudtrail", region_name=REGION)

    def get_s3_data_events(self, bucket_name, hours_back=24):
        """
        Query CloudTrail for S3 data events on a specific bucket
        
        PREREQUISITE:
        → CloudTrail must have S3 data events enabled
        → This is NOT enabled by default (costs ~$0.10 per 100K events)
        → Enable via: trail configuration → Data events → S3
        
        Returns list of:
        {
            "timestamp": "2025-01-15T02:30:00Z",
            "event_type": "PutObject",
            "s3_key": "daily_exports/orders/orders_20250115.csv",
            "principal": "arn:aws:iam::111:role/GlueRole",
            "source_ip": "10.0.1.50",
            "account_id": "[REDACTED:BANK_ACCOUNT_NUMBER]"
        }
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours_back)

        events = []

        paginator = self.cloudtrail.get_paginator("lookup_events")
        for page in paginator.paginate(
            LookupAttributes=[
                {
                    "AttributeKey": "ResourceType",
                    "AttributeValue": "AWS::S3::Object"
                }
            ],
            StartTime=start_time,
            EndTime=end_time
        ):
            for event in page.get("Events", []):
                cloud_trail_event = json.loads(
                    event.get("CloudTrailEvent", "{}")
                )

                # Filter for our bucket
                request_params = cloud_trail_event.get(
                    "requestParameters", {}
                )
                event_bucket = request_params.get("bucketName", "")

                if event_bucket != bucket_name:
                    continue

                s3_key = request_params.get("key", "")
                event_name = cloud_trail_event.get("eventName", "")
                principal = cloud_trail_event.get(
                    "userIdentity", {}
                ).get("arn", "unknown")
                source_ip = cloud_trail_event.get("sourceIPAddress", "")

                events.append({
                    "timestamp": event["EventTime"].isoformat(),
                    "event_type": event_name,
                    "s3_key": s3_key,
                    "bucket": event_bucket,
                    "principal": principal,
                    "source_ip": source_ip,
                    "account_id": cloud_trail_event.get(
                        "userIdentity", {}
                    ).get("accountId", "unknown")
                })

        return events

    def get_glue_job_events(self, hours_back=24):
        """
        Query CloudTrail for Glue job execution events
        
        Captures:
        → StartJobRun: which job started, with what parameters
        → GetTable: which catalog tables were accessed during job
        → CreatePartition: which partitions were created (writes)
        
        Useful for:
        → Building table-level lineage automatically
        → Detecting jobs that access unexpected tables
        → Cross-referencing with active lineage for completeness
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours_back)

        events = []

        paginator = self.cloudtrail.get_paginator("lookup_events")
        for page in paginator.paginate(
            LookupAttributes=[
                {
                    "AttributeKey": "EventSource",
                    "AttributeValue": "glue.amazonaws.com"
                }
            ],
            StartTime=start_time,
            EndTime=end_time
        ):
            for event in page.get("Events", []):
                ct_event = json.loads(
                    event.get("CloudTrailEvent", "{}")
                )
                event_name = ct_event.get("eventName", "")

                # Only interested in data-related events
                relevant_events = [
                    "StartJobRun", "GetTable", "GetPartitions",
                    "CreatePartition", "UpdateTable", "BatchCreatePartition"
                ]
                if event_name not in relevant_events:
                    continue

                request_params = ct_event.get("requestParameters", {})

                events.append({
                    "timestamp": event["EventTime"].isoformat(),
                    "event_type": event_name,
                    "database": request_params.get(
                        "databaseName", "N/A"
                    ),
                    "table": request_params.get("name",
                        request_params.get("tableName", "N/A")
                    ),
                    "job_name": request_params.get("jobName", "N/A"),
                    "principal": ct_event.get(
                        "userIdentity", {}
                    ).get("arn", "unknown"),
                    "account_id": ct_event.get(
                        "userIdentity", {}
                    ).get("accountId", "unknown")
                })

        return events

    def infer_table_lineage_from_job(self, job_name, hours_back=24):
        """
        Infer table-level lineage for a Glue job from CloudTrail
        
        Logic:
        → Find StartJobRun event for this job
        → Find all GetTable events from the same principal within job duration
        → GetTable calls BEFORE any CreatePartition = INPUT tables
        → CreatePartition / UpdateTable calls = OUTPUT tables
        → This gives us TABLE-LEVEL lineage automatically
        
        LIMITATION:
        → Cannot determine column-level mappings
        → Cannot determine transformation logic
        → For column-level: need active instrumentation (Module 2)
        """
        events = self.get_glue_job_events(hours_back)

        # Find job run events
        job_events = [e for e in events if e["job_name"] == job_name]
        start_events = [
            e for e in job_events if e["event_type"] == "StartJobRun"
        ]

        if not start_events:
            return {"inputs": [], "outputs": [], "note": "No job runs found"}

        # Get table access events from same principal
        read_tables = set()
        write_tables = set()

        for event in job_events:
            table_ref = f"{event['database']}.{event['table']}"
            if table_ref == "N/A.N/A":
                continue

            if event["event_type"] in ("GetTable", "GetPartitions"):
                read_tables.add(table_ref)
            elif event["event_type"] in (
                "CreatePartition", "BatchCreatePartition", "UpdateTable"
            ):
                write_tables.add(table_ref)

        # Tables that were ONLY read (not written) = inputs
        input_tables = read_tables - write_tables
        output_tables = write_tables

        return {
            "job_name": job_name,
            "inputs": list(input_tables),
            "outputs": list(output_tables),
            "inferred_from": "CloudTrail API logs",
            "confidence": "TABLE-LEVEL ONLY (no column mapping)"
        }