# file: 42_03_cdc_extract_mariadb_to_s3.py
# Purpose: Extract only changed rows from MariaDB and upload to S3
# This replaces the full dump script from Project 20/29

import boto3
import pymysql
import csv
import io
import time
from datetime import datetime, timedelta
from decimal import Decimal

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
DYNAMODB_TABLE = "quickcart_cdc_watermarks"
OVERLAP_MINUTES = 5

MARIADB_CONFIG = {
    "host": "10.0.1.50",
    "port": 3306,
    "user": "quickcart_app",
    "password": "secure_password_here",
    "database": "quickcart"
}

CDC_ENTITIES = {
    "orders": {
        "pk": "order_id",
        "columns": [
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "status", "order_date", "created_at", "updated_at"
        ]
    },
    "customers": {
        "pk": "customer_id",
        "columns": [
            "customer_id", "name", "email",
            "city", "state", "tier",
            "signup_date", "created_at", "updated_at"
        ]
    },
    "products": {
        "pk": "product_id",
        "columns": [
            "product_id", "name", "category",
            "price", "stock_quantity", "is_active",
            "created_at", "updated_at"
        ]
    }
}


def get_watermark(entity_name):
    """Read last watermark from DynamoDB with strong consistency"""
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(DYNAMODB_TABLE)

    response = table.get_item(
        Key={"entity_name": entity_name},
        ConsistentRead=True
    )

    if "Item" not in response:
        raise ValueError(f"No watermark for entity: {entity_name}")

    return response["Item"]["last_watermark"]


def update_watermark(entity_name, new_watermark, rows_extracted, duration_seconds):
    """
    Update watermark in DynamoDB AFTER successful extraction + load.
    
    CRITICAL: Update watermark ONLY after data is safely in S3.
    If extraction fails, watermark stays old → next run re-extracts.
    This is the "at-least-once" delivery guarantee.
    """
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(DYNAMODB_TABLE)

    table.put_item(
        Item={
            "entity_name": entity_name,
            "last_watermark": new_watermark,
            "last_run_at": datetime.utcnow().isoformat(),
            "rows_extracted": rows_extracted,
            "extraction_duration_seconds": Decimal(str(round(duration_seconds, 2))),
            "status": "SUCCESS"
        }
    )
    print(f"   ✅ Watermark updated: {entity_name} → {new_watermark}")


def extract_cdc_rows(entity_name, entity_config):
    """
    Extract only changed rows from MariaDB since last watermark.
    
    FLOW:
    1. Read watermark from DynamoDB
    2. Subtract overlap window (5 minutes)
    3. Query MariaDB: WHERE updated_at > adjusted_watermark
    4. Write results to CSV in memory
    5. Upload CSV to S3
    6. Update watermark to MAX(updated_at) from extracted rows
    
    RETURNS: (s3_key, rows_extracted) or (None, 0) if no changes
    """
    start_time = time.time()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STEP 1: Get watermark
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    raw_watermark = get_watermark(entity_name)
    watermark_dt = datetime.fromisoformat(raw_watermark)

    # Apply overlap window (subtract 5 minutes for safety)
    adjusted_watermark = watermark_dt - timedelta(minutes=OVERLAP_MINUTES)
    adjusted_watermark_str = adjusted_watermark.strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n   📋 Watermark: {raw_watermark}")
    print(f"   📋 Adjusted (−{OVERLAP_MINUTES}min): {adjusted_watermark_str}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STEP 2: Query MariaDB for changed rows
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    columns = entity_config["columns"]
    columns_str = ", ".join(columns)

    cdc_query = f"""
        SELECT {columns_str}
        FROM {entity_name}
        WHERE updated_at > %s
        ORDER BY updated_at ASC
    """

    conn = pymysql.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()

    print(f"   🔍 Executing CDC query: WHERE updated_at > '{adjusted_watermark_str}'")
    cursor.execute(cdc_query, (adjusted_watermark_str,))
    rows = cursor.fetchall()
    row_count = len(rows)

    print(f"   📊 Rows extracted: {row_count:,}")

    if row_count == 0:
        print(f"   ℹ️  No changes detected — skipping upload")
        cursor.close()
        conn.close()
        return None, 0

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STEP 3: Find new watermark
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Watermark = MAX(updated_at) from extracted rows
    # Since ORDER BY updated_at ASC, last row has the max
    updated_at_index = columns.index("updated_at")
    max_updated_at = rows[-1][updated_at_index]  # Last row = max timestamp

    if isinstance(max_updated_at, datetime):
        new_watermark = max_updated_at.isoformat()
    else:
        new_watermark = str(max_updated_at)

    print(f"   📋 New watermark will be: {new_watermark}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STEP 4: Write CSV to memory buffer
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    # Header row
    writer.writerow(columns)

    # Data rows — convert datetime objects to strings
    for row in rows:
        clean_row = []
        for val in row:
            if isinstance(val, datetime):
                clean_row.append(val.isoformat())
            elif isinstance(val, Decimal):
                clean_row.append(str(val))
            elif val is None:
                clean_row.append("")
            else:
                clean_row.append(val)
        writer.writerow(clean_row)

    csv_content = csv_buffer.getvalue()
    csv_size_kb = round(len(csv_content.encode("utf-8")) / 1024, 1)
    print(f"   📦 CSV size: {csv_size_kb} KB")

    cursor.close()
    conn.close()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STEP 5: Upload to S3
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    today = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%H%M%S")

    # S3 key structure: CDC files go to a separate prefix
    # This separates them from full dumps (Project 20)
    s3_key = (
        f"cdc_exports/{entity_name}/"
        f"dt={today}/"
        f"{entity_name}_cdc_{today}_{timestamp}.csv"
    )

    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
        Metadata={
            "cdc-type": "timestamp-watermark",
            "entity": entity_name,
            "watermark-from": adjusted_watermark_str,
            "watermark-to": new_watermark,
            "rows-extracted": str(row_count),
            "extraction-date": today
        }
    )
    print(f"   ✅ Uploaded: s3://{BUCKET_NAME}/{s3_key}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STEP 6: Update watermark (ONLY after successful upload)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    duration = time.time() - start_time
    update_watermark(entity_name, new_watermark, row_count, duration)

    return s3_key, row_count


def run_cdc_extraction():
    """Main entry: extract CDC for all entities"""
    print("=" * 70)
    print(f"🔄 CDC EXTRACTION — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    results = {}

    for entity_name, entity_config in CDC_ENTITIES.items():
        print(f"\n{'─'*50}")
        print(f"📋 Entity: {entity_name}")
        print(f"{'─'*50}")

        try:
            s3_key, row_count = extract_cdc_rows(entity_name, entity_config)
            results[entity_name] = {
                "status": "SUCCESS",
                "rows": row_count,
                "s3_key": s3_key
            }
        except Exception as e:
            print(f"   ❌ FAILED: {e}")
            results[entity_name] = {
                "status": "FAILED",
                "error": str(e)
            }

    # Summary
    print(f"\n{'='*70}")
    print(f"📊 CDC EXTRACTION SUMMARY")
    print(f"{'='*70}")
    for entity, result in results.items():
        status_icon = "✅" if result["status"] == "SUCCESS" else "❌"
        if result["status"] == "SUCCESS":
            print(f"  {status_icon} {entity}: {result['rows']:,} rows → {result.get('s3_key', 'N/A')}")
        else:
            print(f"  {status_icon} {entity}: FAILED — {result.get('error', 'Unknown')}")

    return results


if __name__ == "__main__":
    run_cdc_extraction()