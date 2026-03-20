# file: 48_01_s3_select_utility.py
# Purpose: Reusable S3 Select utility for the entire data team
# Wraps boto3 S3 Select API with error handling and convenience functions

import boto3
import csv
import io
import json
import time
from datetime import datetime

REGION = "us-east-2"
DATALAKE_BUCKET = "quickcart-datalake-prod"


def s3_select_csv(bucket, key, sql_expression, 
                  header_info="USE", field_delimiter=",",
                  compression="NONE", scan_range=None,
                  output_format="csv"):
    """
    Run S3 Select SQL on a CSV object in S3.
    
    Parameters:
    → bucket: S3 bucket name
    → key: S3 object key
    → sql_expression: SQL query (FROM s3object)
    → header_info: USE (columns by name), IGNORE, NONE (_1, _2, ...)
    → field_delimiter: CSV delimiter (default comma)
    → compression: NONE, GZIP, BZIP2
    → scan_range: dict {"Start": N, "End": M} for partial file read
    → output_format: "csv" or "json"
    
    Returns:
    → dict with: rows (list of dicts/lists), stats, duration
    """
    s3_client = boto3.client("s3", region_name=REGION)

    start_time = time.time()

    # Build request
    input_serialization = {
        "CSV": {
            "FileHeaderInfo": header_info,
            "FieldDelimiter": field_delimiter,
            "RecordDelimiter": "\n",
            "QuoteCharacter": '"',
            "QuoteEscapeCharacter": '"'
        },
        "CompressionType": compression
    }

    if output_format == "json":
        output_serialization = {"JSON": {"RecordDelimiter": "\n"}}
    else:
        output_serialization = {
            "CSV": {
                "FieldDelimiter": ",",
                "RecordDelimiter": "\n",
                "QuoteCharacter": '"'
            }
        }

    # Build params
    params = {
        "Bucket": bucket,
        "Key": key,
        "ExpressionType": "SQL",
        "Expression": sql_expression,
        "InputSerialization": input_serialization,
        "OutputSerialization": output_serialization
    }

    if scan_range:
        params["ScanRange"] = scan_range

    # Execute
    try:
        response = s3_client.select_object_content(**params)
    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e),
            "rows": [],
            "stats": {},
            "duration": time.time() - start_time
        }

    # Process EventStream
    records_data = []
    stats = {}

    for event in response["Payload"]:
        if "Records" in event:
            records_data.append(event["Records"]["Payload"].decode("utf-8"))
        elif "Stats" in event:
            details = event["Stats"]["Details"]
            stats = {
                "bytes_scanned": details["BytesScanned"],
                "bytes_processed": details["BytesProcessed"],
                "bytes_returned": details["BytesReturned"]
            }
        elif "End" in event:
            pass

    duration = time.time() - start_time

    # Parse records
    all_data = "".join(records_data)
    rows = []

    if output_format == "json" and all_data.strip():
        for line in all_data.strip().split("\n"):
            if line.strip():
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    elif output_format == "csv" and all_data.strip():
        reader = csv.reader(io.StringIO(all_data))
        for row in reader:
            if row:
                rows.append(row)

    return {
        "status": "SUCCESS",
        "rows": rows,
        "row_count": len(rows),
        "stats": stats,
        "duration": duration
    }


def s3_select_parquet(bucket, key, sql_expression):
    """
    Run S3 Select SQL on a Parquet object in S3.
    
    Parquet advantages:
    → Columnar: S3 reads only selected columns
    → Typed: no CAST needed for numeric comparisons
    → Compressed: smaller file = less data scanned = cheaper
    """
    s3_client = boto3.client("s3", region_name=REGION)

    start_time = time.time()

    params = {
        "Bucket": bucket,
        "Key": key,
        "ExpressionType": "SQL",
        "Expression": sql_expression,
        "InputSerialization": {"Parquet": {}},
        "OutputSerialization": {"JSON": {"RecordDelimiter": "\n"}}
    }

    try:
        response = s3_client.select_object_content(**params)
    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e),
            "rows": [],
            "stats": {},
            "duration": time.time() - start_time
        }

    records_data = []
    stats = {}

    for event in response["Payload"]:
        if "Records" in event:
            records_data.append(event["Records"]["Payload"].decode("utf-8"))
        elif "Stats" in event:
            details = event["Stats"]["Details"]
            stats = {
                "bytes_scanned": details["BytesScanned"],
                "bytes_processed": details["BytesProcessed"],
                "bytes_returned": details["BytesReturned"]
            }

    duration = time.time() - start_time
    all_data = "".join(records_data)
    rows = []

    if all_data.strip():
        for line in all_data.strip().split("\n"):
            if line.strip():
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    return {
        "status": "SUCCESS",
        "rows": rows,
        "row_count": len(rows),
        "stats": stats,
        "duration": duration
    }


def print_results(result, title="", max_rows=20):
    """Pretty-print S3 Select results"""
    print(f"\n{'━'*70}")
    if title:
        print(f"📋 {title}")
    print(f"{'━'*70}")

    if result["status"] != "SUCCESS":
        print(f"   ❌ Error: {result.get('error', 'Unknown')}")
        return

    print(f"   ⏱  Duration: {result['duration']:.2f}s")

    stats = result.get("stats", {})
    if stats:
        scanned_mb = stats.get("bytes_scanned", 0) / (1024 * 1024)
        returned_kb = stats.get("bytes_returned", 0) / 1024
        cost_scan = stats.get("bytes_scanned", 0) / (1024 ** 3) * 0.002
        cost_return = stats.get("bytes_returned", 0) / (1024 ** 3) * 0.0007

        print(f"   📊 Scanned: {scanned_mb:.2f} MB")
        print(f"   📊 Returned: {returned_kb:.2f} KB")
        print(f"   💰 Cost: ${cost_scan + cost_return:.6f}")
        print(f"   📊 Reduction: {((1 - returned_kb*1024/(max(scanned_mb,0.001)*1024*1024)) * 100):.1f}% less data transferred")

    rows = result["rows"]
    print(f"   📊 Rows returned: {result['row_count']}")

    if rows:
        # Display first N rows
        display_rows = rows[:max_rows]

        if isinstance(display_rows[0], dict):
            # JSON format
            headers = list(display_rows[0].keys())
            print(f"\n   {' | '.join(h[:18].ljust(18) for h in headers[:5])}")
            print(f"   {'─'*60}")
            for row in display_rows:
                values = [str(row.get(h, ""))[:18].ljust(18) for h in headers[:5]]
                print(f"   {' | '.join(values)}")
        else:
            # CSV format (list of lists)
            for row in display_rows:
                print(f"   {' | '.join(str(v)[:18].ljust(18) for v in row[:5])}")

        if len(rows) > max_rows:
            print(f"   ... ({len(rows) - max_rows} more rows)")

    print(f"{'━'*70}")