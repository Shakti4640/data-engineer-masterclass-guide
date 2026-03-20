# file: 42_06_watermark_utility.py
# Purpose: View, reset, and manage CDC watermarks
# Essential for debugging and recovery

import boto3
from datetime import datetime, timedelta

REGION = "us-east-2"
TABLE_NAME = "quickcart_cdc_watermarks"


def list_all_watermarks():
    """Display current watermarks for all entities"""
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    response = table.scan()
    items = response.get("Items", [])

    print(f"\n📋 CDC WATERMARKS ({TABLE_NAME})")
    print("─" * 80)
    print(f"{'Entity':<15} {'Last Watermark':<25} {'Last Run':<25} {'Rows':<10} {'Status'}")
    print("─" * 80)

    for item in sorted(items, key=lambda x: x["entity_name"]):
        print(
            f"{item['entity_name']:<15} "
            f"{item.get('last_watermark', 'N/A'):<25} "
            f"{item.get('last_run_at', 'N/A'):<25} "
            f"{str(item.get('rows_extracted', 'N/A')):<10} "
            f"{item.get('status', 'N/A')}"
        )

    print("─" * 80)


def reset_watermark(entity_name, new_watermark=None):
    """
    Reset watermark for an entity.
    
    USE CASES:
    → Watermark corrupted → reset to safe point
    → Need to reprocess last N days → set watermark back
    → Initial load needed → set to epoch
    → Skip historical data → set to now
    """
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    if new_watermark is None:
        new_watermark = "1970-01-01T00:00:00"

    # Read current
    response = table.get_item(Key={"entity_name": entity_name}, ConsistentRead=True)
    current = response.get("Item", {}).get("last_watermark", "N/A")

    print(f"\n⚠️  WATERMARK RESET: {entity_name}")
    print(f"   Current: {current}")
    print(f"   New:     {new_watermark}")

    table.update_item(
        Key={"entity_name": entity_name},
        UpdateExpression="SET last_watermark = :w, #s = :s, last_run_at = :r",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":w": new_watermark,
            ":s": "MANUAL_RESET",
            ":r": datetime.utcnow().isoformat()
        }
    )

    print(f"   ✅ Watermark reset successfully")
    print(f"   ℹ️  Next CDC run will extract all rows since {new_watermark}")


def rewind_watermark(entity_name, hours_back):
    """
    Rewind watermark by N hours.
    
    USE CASE: "We suspect some rows were missed 6 hours ago.
    Rewind by 8 hours to re-extract and re-merge them."
    
    SAFE because: MERGE is idempotent (duplicate rows are handled)
    """
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    response = table.get_item(Key={"entity_name": entity_name}, ConsistentRead=True)
    current = response.get("Item", {}).get("last_watermark", "N/A")

    current_dt = datetime.fromisoformat(current)
    new_dt = current_dt - timedelta(hours=hours_back)
    new_watermark = new_dt.isoformat()

    print(f"\n⏪ WATERMARK REWIND: {entity_name}")
    print(f"   Current:  {current}")
    print(f"   Rewind:   {hours_back} hours back")
    print(f"   New:      {new_watermark}")

    reset_watermark(entity_name, new_watermark)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        list_all_watermarks()
        print("\nUsage:")
        print("  python 42_06_watermark_utility.py list")
        print("  python 42_06_watermark_utility.py reset orders 2025-01-10T00:00:00")
        print("  python 42_06_watermark_utility.py rewind orders 24")
        sys.exit(0)

    action = sys.argv[1]

    if action == "list":
        list_all_watermarks()

    elif action == "reset":
        entity = sys.argv[2]
        watermark = sys.argv[3] if len(sys.argv) > 3 else None
        reset_watermark(entity, watermark)

    elif action == "rewind":
        entity = sys.argv[2]
        hours = int(sys.argv[3])
        rewind_watermark(entity, hours)

    else:
        print(f"Unknown action: {action}")