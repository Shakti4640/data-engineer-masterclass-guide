# file: 33_03_watermark_s3.py
# Purpose: Lightweight watermark using S3 JSON file (no DynamoDB needed)
# Use when: DynamoDB is overkill, only 1-3 jobs need watermarking

import boto3
import json
from datetime import datetime


class S3WatermarkManager:
    """
    Store watermark state in a simple S3 JSON file
    
    File: s3://bucket/state/watermarks/{job_name}/{source_table}.json
    Content:
    {
        "watermark_value": "2025-01-15 23:59:59",
        "updated_at": "2025-01-16T02:30:00",
        "rows_processed": 200000,
        "job_run_id": "jr_abc123"
    }
    
    TRADE-OFFS vs DynamoDB:
    ┌────────────────────┬───────────────┬─────────────────┐
    │                    │ S3 File       │ DynamoDB        │
    ├────────────────────┼───────────────┼─────────────────┤
    │ Cost               │ ~$0.00001/mo  │ ~$1.00/mo       │
    │ Atomicity          │ No            │ Yes             │
    │ Concurrent access  │ Last-write    │ Conditional     │
    │                    │ wins          │ writes safe     │
    │ Query all states   │ List + read   │ Single scan     │
    │ Setup              │ Zero          │ Create table    │
    │ Latency            │ 50-200ms      │ <10ms           │
    └────────────────────┴───────────────┴─────────────────┘
    
    BEST FOR: Single-job, non-concurrent watermark tracking
    """

    def __init__(self, bucket, prefix, region="us-east-2"):
        self.s3 = boto3.client("s3", region_name=region)
        self.bucket = bucket
        self.prefix = prefix

    def _get_key(self, job_name, source_table):
        return f"{self.prefix}/{job_name}/{source_table}.json"

    def get_watermark(self, job_name, source_table):
        """Read watermark from S3 JSON file"""
        key = self._get_key(job_name, source_table)
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return data.get("watermark_value")
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Warning: Failed to read watermark: {e}")
            return None

    def set_watermark(self, job_name, source_table, watermark_value,
                      rows_processed=0, run_id="unknown"):
        """Write watermark to S3 JSON file"""
        key = self._get_key(job_name, source_table)
        data = {
            "watermark_value": str(watermark_value),
            "updated_at": datetime.now().isoformat(),
            "rows_processed": rows_processed,
            "job_run_id": run_id,
            "job_name": job_name,
            "source_table": source_table
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )

    def get_all_watermarks(self, job_name):
        """List all watermarks for a job"""
        prefix = f"{self.prefix}/{job_name}/"
        response = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix
        )

        watermarks = {}
        if "Contents" in response:
            for obj in response["Contents"]:
                data_response = self.s3.get_object(
                    Bucket=self.bucket, Key=obj["Key"]
                )
                data = json.loads(
                    data_response["Body"].read().decode("utf-8")
                )
                table = data.get("source_table", obj["Key"])
                watermarks[table] = data

        return watermarks

    def reset_watermark(self, job_name, source_table):
        """Delete watermark — next run does full load"""
        key = self._get_key(job_name, source_table)
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
            print(f"Watermark reset for {job_name}/{source_table}")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════
# USAGE EXAMPLE (runs inside Glue job)
# ═══════════════════════════════════════════════════════════════════
"""
# In your Glue ETL script:

wm = S3WatermarkManager(
    bucket="quickcart-datalake-prod",
    prefix="state/watermarks"
)

# Get last watermark
last_wm = wm.get_watermark("my-glue-job", "orders")
# Returns: "2025-01-14 23:59:59" or None

# After successful processing:
wm.set_watermark(
    job_name="my-glue-job",
    source_table="orders",
    watermark_value="2025-01-15 23:59:59",
    rows_processed=200000
)
"""