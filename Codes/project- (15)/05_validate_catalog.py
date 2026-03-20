# file: 05_validate_catalog.py
# Purpose: Verify catalog tables actually match the data in S3
# Catches: wrong paths, missing files, column count mismatches

import boto3
import json
import csv
import io
from botocore.exceptions import ClientError

REGION = "us-east-2"


def load_config():
    with open("catalog_config.json", "r") as f:
        return json.load(f)


def validate_table(glue_client, s3_client, database_name, table_name):
    """
    Validate a catalog table against actual S3 data
    
    CHECKS:
    1. S3 location exists and has files
    2. Files are readable
    3. Column count in file matches catalog
    4. Header names match catalog column names (if header exists)
    """
    print(f"\n🔍 Validating: {database_name}.{table_name}")
    print(f"   {'─'*55}")
    
    checks = {"passed": 0, "failed": 0, "warnings": 0}
    
    # Get table from catalog
    try:
        response = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        table = response["Table"]
        sd = table["StorageDescriptor"]
    except ClientError as e:
        print(f"   ❌ Table not found in catalog: {e}")
        checks["failed"] += 1
        return checks
    
    location = sd.get("Location", "")
    catalog_columns = sd.get("Columns", [])
    params = table.get("Parameters", {})
    has_header = params.get("has_header", "false") == "true"
    skip_header = params.get("skip.header.line.count", "0") != "0"
    
    # ── CHECK 1: S3 location format ──
    if location.startswith("s3://") and location.endswith("/"):
        print(f"   ✅ Location format correct: {location}")
        checks["passed"] += 1
    else:
        if not location.endswith("/"):
            print(f"   ⚠️  Location missing trailing slash: {location}")
            checks["warnings"] += 1
        if not location.startswith("s3://"):
            print(f"   ❌ Location not S3 path: {location}")
            checks["failed"] += 1
            return checks
    
    # Parse S3 bucket and prefix
    path = location.replace("s3://", "")
    bucket = path.split("/")[0]
    prefix = "/".join(path.split("/")[1:])
    
    # ── CHECK 2: S3 path has files ──
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=10
        )
        
        objects = response.get("Contents", [])
        # Filter out "directory" markers (0-byte objects)
        files = [obj for obj in objects if obj["Size"] > 0]
        
        if files:
            total_size = sum(f["Size"] for f in files)
            size_mb = round(total_size / (1024 * 1024), 2)
            print(f"   ✅ S3 has {len(files)} file(s) ({size_mb} MB)")
            checks["passed"] += 1
        else:
            print(f"   ❌ S3 location is empty — no files found")
            print(f"      Bucket: {bucket}")
            print(f"      Prefix: {prefix}")
            checks["failed"] += 1
            return checks
            
    except ClientError as e:
        print(f"   ❌ Cannot access S3: {e}")
        checks["failed"] += 1
        return checks
    
    # ── CHECK 3: Read first file and verify structure ──
    first_file = files[0]
    file_key = first_file["Key"]
    
    try:
        obj_response = s3_client.get_object(
            Bucket=bucket,
            Key=file_key,
            Range="bytes=0-10240"      # Read first 10 KB only
        )
        
        content = obj_response["Body"].read().decode("utf-8", errors="replace")
        lines = content.strip().split("\n")
        
        if len(lines) < 2:
            print(f"   ⚠️  File has only {len(lines)} line(s) — too few to validate")
            checks["warnings"] += 1
            return checks
        
        # Parse first line (header or first data row)
        reader = csv.reader(io.StringIO(lines[0]))
        first_row = next(reader)
        
        # Parse second line (data row)
        reader2 = csv.reader(io.StringIO(lines[1]))
        second_row = next(reader2)
        
        file_col_count = len(first_row)
        catalog_col_count = len(catalog_columns)
        
        print(f"   File: {file_key.split('/')[-1]}")
        
        # ── CHECK 3a: Column count match ──
        if file_col_count == catalog_col_count:
            print(f"   ✅ Column count matches: {file_col_count}")
            checks["passed"] += 1
        else:
            print(f"   ❌ Column count MISMATCH:")
            print(f"      Catalog: {catalog_col_count} columns")
            print(f"      File:    {file_col_count} columns")
            checks["failed"] += 1
        
        # ── CHECK 3b: Header names match (if file has header) ──
        if has_header or skip_header:
            catalog_names = [col["Name"] for col in catalog_columns]
            file_names = [h.strip().lower().replace(" ", "_") for h in first_row]
            
            if file_names == catalog_names:
                print(f"   ✅ Header names match catalog columns")
                checks["passed"] += 1
            else:
                print(f"   ⚠️  Header name differences:")
                for i, (fn, cn) in enumerate(zip(file_names, catalog_names)):
                    if fn != cn:
                        print(f"      Col {i}: file='{fn}' vs catalog='{cn}'")
                checks["warnings"] += 1
        
        # ── CHECK 3c: Data row has reasonable values ──
        data_row = second_row if (has_header or skip_header) else first_row
        
        empty_count = sum(1 for v in data_row if v.strip() == "")
        if empty_count == 0:
            print(f"   ✅ Sample data row has no empty values")
            checks["passed"] += 1
        elif empty_count < len(data_row) / 2:
            print(f"   ⚠️  Sample data row has {empty_count} empty values")
            checks["warnings"] += 1
        else:
            print(f"   ❌ Sample data row mostly empty ({empty_count}/{len(data_row)})")
            checks["failed"] += 1
        
    except (ClientError, csv.Error, StopIteration) as e:
        print(f"   ⚠️  Cannot parse file content: {e}")
        checks["warnings"] += 1
    
    return checks


def validate_all_tables():
    """Validate all tables in catalog"""
    config = load_config()
    glue_client = boto3.client("glue", region_name=config["region"])
    s3_client = boto3.client("s3", region_name=config["region"])
    
    print("=" * 65)
    print("🔍 CATALOG VALIDATION — Verifying catalog matches S3 data")
    print("=" * 65)
    
    total_checks = {"passed": 0, "failed": 0, "warnings": 0}
    
    for db_name in config["databases"]:
        print(f"\n📦 Database: {db_name}")
        
        try:
            tables_response = glue_client.get_tables(DatabaseName=db_name)
            tables = tables_response.get("TableList", [])
            
            if not tables:
                print(f"   (no tables)")
                continue
            
            for table in tables:
                table_name = table["Name"]
                result = validate_table(
                    glue_client, s3_client, db_name, table_name
                )
                
                for k, v in result.items():
                    total_checks[k] += v
                    
        except ClientError as e:
            print(f"   ❌ Error listing tables: {e}")
            total_checks["failed"] += 1
    
    # Summary
    total = sum(total_checks.values())
    print(f"\n{'='*65}")
    print(f"📊 VALIDATION SUMMARY")
    print(f"   ✅ Passed:   {total_checks['passed']}")
    print(f"   ⚠️  Warnings: {total_checks['warnings']}")
    print(f"   ❌ Failed:   {total_checks['failed']}")
    
    if total_checks["failed"] == 0:
        print(f"   🟢 CATALOG IS VALID")
    else:
        print(f"   🔴 CATALOG HAS ISSUES — fix before using with Athena/Glue")
    print(f"{'='*65}")
    
    return total_checks["failed"] == 0


if __name__ == "__main__":
    validate_all_tables()