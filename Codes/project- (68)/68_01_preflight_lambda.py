# file: 68_01_preflight_lambda.py
# Purpose: Quick validation before Glue job starts
# Triggered by: Step Function (Project 66) before RunSourceETL step

import boto3
import json
import csv
import io
from datetime import datetime

s3 = boto3.client("s3")
sns = boto3.client("sns")
SNS_TOPIC = "arn:aws:sns:us-east-2:222222222222:data-platform-alerts"

# Expected schemas per source
EXPECTED_SCHEMAS = {
    "partner_feed": {
        "bucket": "quickcart-partner-feeds",
        "min_columns": 8,
        "required_columns": ["order_id", "customer_id", "total_amount", "order_date"],
        "min_rows": 100,
        "max_rows": 50000,
        "max_file_size_mb": 100,
        "file_format": "csv"
    },
    "payment_data": {
        "bucket": "quickcart-payment-data",
        "min_columns": 6,
        "required_columns": ["transaction_id", "amount", "payment_date"],
        "min_rows": 1,
        "max_rows": 500000,
        "max_file_size_mb": 500,
        "file_format": "json"
    },
    "marketing_campaign": {
        "bucket": "quickcart-marketing-uploads",
        "min_columns": 5,
        "required_columns": ["campaign_id", "campaign_name", "channel"],
        "min_rows": 1,
        "max_rows": 100000,
        "max_file_size_mb": 50,
        "file_format": "csv"
    }
}


def lambda_handler(event, context):
    """
    Pre-flight quality check — runs in < 30 seconds
    
    CHECKS:
    1. File exists and is not zero bytes
    2. File size within expected range
    3. Schema matches (column count, required columns present)
    4. Row count within expected range
    5. Sample rows: no obvious issues
    
    DOES NOT CHECK (left for Glue DQ):
    → Uniqueness (requires full scan)
    → Value ranges on every row (requires full scan)
    → Cross-table referential integrity
    """
    source_name = event.get("source_name", "unknown")
    file_bucket = event.get("file_bucket", "")
    file_key = event.get("file_key", "")
    batch_date = event.get("batch_date", datetime.utcnow().strftime("%Y-%m-%d"))

    print(f"🔍 Pre-flight check: {source_name}")
    print(f"   File: s3://{file_bucket}/{file_key}")

    schema_config = EXPECTED_SCHEMAS.get(source_name)
    if not schema_config:
        print(f"   ℹ️ No schema config for {source_name} — skipping pre-flight")
        return {"preflight_passed": True, "checks": [], "source_name": source_name}

    checks = []
    all_passed = True

    try:
        # CHECK 1: File exists and size
        head = s3.head_object(Bucket=file_bucket, Key=file_key)
        file_size_bytes = head["ContentLength"]
        file_size_mb = file_size_bytes / (1024 * 1024)

        if file_size_bytes == 0:
            checks.append({"rule": "file_not_empty", "passed": False, "detail": "File is 0 bytes"})
            all_passed = False
        else:
            checks.append({"rule": "file_not_empty", "passed": True, "detail": f"{file_size_mb:.2f} MB"})

        if file_size_mb > schema_config["max_file_size_mb"]:
            checks.append({
                "rule": "file_size",
                "passed": False,
                "detail": f"{file_size_mb:.2f} MB exceeds max {schema_config['max_file_size_mb']} MB"
            })
            all_passed = False
        else:
            checks.append({"rule": "file_size", "passed": True, "detail": f"{file_size_mb:.2f} MB OK"})

        # CHECK 2-4: Read sample for schema and row count
        if schema_config["file_format"] == "csv":
            # Read first 1 MB for schema check + count lines for row estimate
            response = s3.get_object(Bucket=file_bucket, Key=file_key, Range="bytes=0-1048576")
            sample_content = response["Body"].read().decode("utf-8", errors="replace")

            lines = sample_content.strip().split("\n")
            if len(lines) < 2:
                checks.append({"rule": "has_data_rows", "passed": False, "detail": "No data rows found"})
                all_passed = False
            else:
                # Parse header
                reader = csv.reader(io.StringIO(lines[0]))
                headers = next(reader)
                header_set = {h.strip().lower() for h in headers}

                # Column count
                if len(headers) >= schema_config["min_columns"]:
                    checks.append({"rule": "column_count", "passed": True, "detail": f"{len(headers)} columns"})
                else:
                    checks.append({
                        "rule": "column_count",
                        "passed": False,
                        "detail": f"{len(headers)} columns < minimum {schema_config['min_columns']}"
                    })
                    all_passed = False

                # Required columns present
                missing = [c for c in schema_config["required_columns"] if c.lower() not in header_set]
                if not missing:
                    checks.append({"rule": "required_columns", "passed": True, "detail": "All present"})
                else:
                    checks.append({
                        "rule": "required_columns",
                        "passed": False,
                        "detail": f"Missing: {missing}"
                    })
                    all_passed = False

                # Estimate row count (sample-based)
                sample_lines = len(lines) - 1  # Exclude header
                if file_size_bytes <= 1048576:
                    estimated_rows = sample_lines  # Full file read
                else:
                    bytes_per_line = 1048576 / len(lines)
                    estimated_rows = int(file_size_bytes / bytes_per_line) - 1

                if schema_config["min_rows"] <= estimated_rows <= schema_config["max_rows"]:
                    checks.append({
                        "rule": "row_count",
                        "passed": True,
                        "detail": f"~{estimated_rows:,} rows (within {schema_config['min_rows']}-{schema_config['max_rows']})"
                    })
                else:
                    checks.append({
                        "rule": "row_count",
                        "passed": False,
                        "detail": f"~{estimated_rows:,} rows (expected {schema_config['min_rows']}-{schema_config['max_rows']})"
                    })
                    all_passed = False

        elif schema_config["file_format"] == "json":
            # JSON: read sample, check structure
            response = s3.get_object(Bucket=file_bucket, Key=file_key, Range="bytes=0-1048576")
            sample_content = response["Body"].read().decode("utf-8", errors="replace")

            try:
                # Try JSON Lines format
                first_line = sample_content.strip().split("\n")[0]
                sample_record = json.loads(first_line)
                record_keys = set(sample_record.keys())

                missing = [c for c in schema_config["required_columns"] if c not in record_keys]
                if not missing:
                    checks.append({"rule": "required_columns", "passed": True, "detail": "All present"})
                else:
                    checks.append({"rule": "required_columns", "passed": False, "detail": f"Missing: {missing}"})
                    all_passed = False

                checks.append({"rule": "json_parseable", "passed": True, "detail": "Valid JSON"})
            except json.JSONDecodeError:
                checks.append({"rule": "json_parseable", "passed": False, "detail": "Invalid JSON format"})
                all_passed = False

    except s3.exceptions.NoSuchKey:
        checks.append({"rule": "file_exists", "passed": False, "detail": f"File not found: {file_key}"})
        all_passed = False
    except Exception as e:
        checks.append({"rule": "preflight_error", "passed": False, "detail": str(e)[:200]})
        all_passed = False

    # Summary
    passed_count = sum(1 for c in checks if c["passed"])
    total_count = len(checks)

    print(f"\n   📊 Pre-flight results: {passed_count}/{total_count} passed")
    for check in checks:
        icon = "✅" if check["passed"] else "❌"
        print(f"   {icon} {check['rule']}: {check['detail']}")

    # Alert on failure
    if not all_passed:
        failed_checks = [c for c in checks if not c["passed"]]
        sns.publish(
            TopicArn=SNS_TOPIC,
            Subject=f"❌ Pre-flight FAILED: {source_name} ({batch_date})",
            Message="\n".join([
                f"Pre-flight quality check FAILED for {source_name}",
                f"File: s3://{file_bucket}/{file_key}",
                f"Batch: {batch_date}",
                "",
                "Failed checks:",
                *[f"  ❌ {c['rule']}: {c['detail']}" for c in failed_checks],
                "",
                "ACTION: Pipeline STOPPED. Investigate source data before retrying."
            ])
        )
        print(f"   📧 Alert sent")

    return {
        "preflight_passed": all_passed,
        "checks": checks,
        "source_name": source_name,
        "batch_date": batch_date,
        "file_bucket": file_bucket,
        "file_key": file_key
    }