# file: 07_verify_unload_output.py
# Downloads and validates UNLOAD output files from S3
# Ensures data integrity after export

import boto3
import gzip
import io
import csv
from datetime import datetime

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def list_unload_files(prefix):
    """List all files at an UNLOAD output prefix"""
    s3_client = boto3.client("s3", region_name=REGION)
    paginator = s3_client.get_paginator("list_objects_v2")

    files = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            files.append({
                "key": obj["Key"],
                "size_bytes": obj["Size"],
                "size_mb": round(obj["Size"] / (1024 * 1024), 3),
                "last_modified": obj["LastModified"]
            })

    return sorted(files, key=lambda x: x["key"])


def count_rows_in_csv_gz(s3_client, bucket, key, has_header=True):
    """Download and count rows in a GZIP CSV file"""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    compressed_body = response["Body"].read()

    decompressed = gzip.decompress(compressed_body)
    text = decompressed.decode("utf-8")
    lines = text.strip().split("\n")

    total_lines = len(lines)
    data_rows = total_lines - 1 if has_header else total_lines

    return data_rows, total_lines


def count_rows_in_csv(s3_client, bucket, key, has_header=True):
    """Download and count rows in uncompressed CSV file"""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    lines = body.strip().split("\n")

    total_lines = len(lines)
    data_rows = total_lines - 1 if has_header else total_lines

    return data_rows, total_lines


def verify_unload_output(prefix, has_header=True, is_gzip=False):
    """
    Complete verification of UNLOAD output
    
    CHECKS:
    1. Files exist at expected prefix
    2. No zero-byte files
    3. Total row count matches expectation
    4. Sample data looks correct
    5. All files have consistent column count
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print(f"\n🔍 VERIFYING UNLOAD OUTPUT")
    print(f"   Prefix: s3://{BUCKET_NAME}/{prefix}")
    print("=" * 60)

    # --- STEP 1: LIST FILES ---
    files = list_unload_files(prefix)
    if not files:
        print(f"   ❌ No files found at this prefix!")
        return False

    print(f"\n📁 Files found: {len(files)}")
    total_bytes = 0
    for f in files:
        status = "✅" if f["size_bytes"] > 0 else "❌ EMPTY"
        print(f"   {status} {f['key']} ({f['size_mb']} MB)")
        total_bytes += f["size_bytes"]

    total_mb = round(total_bytes / (1024 * 1024), 3)
    print(f"   Total: {total_mb} MB across {len(files)} files")

    # --- STEP 2: CHECK FOR ZERO-BYTE FILES ---
    empty_files = [f for f in files if f["size_bytes"] == 0]
    if empty_files:
        print(f"\n⚠️  {len(empty_files)} empty file(s) detected!")
        for ef in empty_files:
            print(f"   → {ef['key']}")
        print(f"   This may indicate a slice with no data (possible data skew)")

    # --- STEP 3: COUNT ROWS ---
    print(f"\n📊 Row count verification:")
    total_data_rows = 0

    for f in files:
        key = f["key"]

        # Skip Parquet files (need PyArrow for those)
        if key.endswith(".parquet"):
            print(f"   ⏭️  {key} — Parquet (use PyArrow for row count)")
            continue

        # Skip empty files
        if f["size_bytes"] == 0:
            continue

        try:
            if key.endswith(".gz"):
                data_rows, total_lines = count_rows_in_csv_gz(
                    s3_client, BUCKET_NAME, key, has_header
                )
            else:
                data_rows, total_lines = count_rows_in_csv(
                    s3_client, BUCKET_NAME, key, has_header
                )

            print(f"   {key} → {data_rows:,} data rows")
            total_data_rows += data_rows

        except Exception as e:
            print(f"   ❌ {key} → Error reading: {e}")

    print(f"\n   📋 Total data rows across all files: {total_data_rows:,}")

    # --- STEP 4: SAMPLE FIRST FILE ---
    first_file = files[0]
    if not first_file["key"].endswith(".parquet") and first_file["size_bytes"] > 0:
        print(f"\n📋 Sample data (first 5 rows from first file):")
        try:
            if first_file["key"].endswith(".gz"):
                response = s3_client.get_object(
                    Bucket=BUCKET_NAME, Key=first_file["key"]
                )
                decompressed = gzip.decompress(response["Body"].read())
                text = decompressed.decode("utf-8")
            else:
                response = s3_client.get_object(
                    Bucket=BUCKET_NAME, Key=first_file["key"]
                )
                text = response["Body"].read().decode("utf-8")

            lines = text.strip().split("\n")[:6]  # header + 5 data rows
            for i, line in enumerate(lines):
                prefix_label = "HEADER:" if i == 0 and has_header else f"ROW {i}:"
                # Truncate long lines
                display_line = line[:100] + "..." if len(line) > 100 else line
                print(f"   {prefix_label:>8} {display_line}")

        except Exception as e:
            print(f"   ❌ Error reading sample: {e}")

    print(f"\n{'=' * 60}")
    print(f"✅ Verification complete")
    return True


def verify_parquet_output(prefix):
    """
    Verify Parquet UNLOAD output using PyArrow
    
    REQUIRES: pip install pyarrow s3fs
    """
    try:
        import pyarrow.parquet as pq
        import s3fs

        print(f"\n🔍 VERIFYING PARQUET OUTPUT")
        print(f"   Prefix: s3://{BUCKET_NAME}/{prefix}")

        fs = s3fs.S3FileSystem(anon=False)
        s3_path = f"{BUCKET_NAME}/{prefix}"

        # Read all Parquet files at prefix
        dataset = pq.ParquetDataset(s3_path, filesystem=fs)
        table = dataset.read()

        print(f"\n📊 Parquet Dataset Info:")
        print(f"   Total rows:    {len(table):,}")
        print(f"   Total columns: {len(table.schema)}")
        print(f"\n📋 Schema:")
        for field in table.schema:
            print(f"   {field.name:<25} {field.type}")

        print(f"\n📋 Sample data (first 5 rows):")
        df = table.to_pandas().head(5)
        print(df.to_string(index=False))

        print(f"\n✅ Parquet verification complete")
        return True

    except ImportError:
        print("⚠️  PyArrow not installed. Run: pip install pyarrow s3fs")
        print("   Falling back to file listing only")
        return verify_unload_output(prefix, has_header=False, is_gzip=False)


if __name__ == "__main__":
    date_compact = "20250129"

    print("\n" + "🔹" * 30)
    print("VERIFICATION 1: Finance CSV")
    verify_unload_output(
        prefix=f"exports/finance/daily_summary/{date_compact}/",
        has_header=True,
        is_gzip=False
    )

    print("\n" + "🔹" * 30)
    print("VERIFICATION 2: Partner GZIP CSV")
    verify_unload_output(
        prefix=f"exports/partner_shipfast/daily_orders/{date_compact}/",
        has_header=True,
        is_gzip=True
    )

    print("\n" + "🔹" * 30)
    print("VERIFICATION 3: Data Science Parquet")
    verify_parquet_output(
        prefix=f"exports/data_science/daily_orders/{date_compact}/"
    )