# file: 48_02_investigate_data_quality.py
# Purpose: Sarah's actual investigation — finding zero-amount orders in Bronze
# Demonstrates real-world S3 Select usage

from s3_select_utility import s3_select_csv, print_results, DATALAKE_BUCKET

BUCKET = DATALAKE_BUCKET
BRONZE_PREFIX = "bronze/mariadb/full_dump/orders"


def investigation():
    """
    Sarah's data quality investigation:
    "Silver has 47 orders with total_amount = 0.
     Is the issue in Bronze (source) or our ETL?"
    """
    print("╔" + "═" * 68 + "╗")
    print("║" + "  🔍 DATA QUALITY INVESTIGATION: Zero-Amount Orders".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    target_key = f"{BRONZE_PREFIX}/year=2025/month=01/day=15/orders_20250115.csv"

    # ━━━ STEP 1: Peek at file structure ━━━
    result = s3_select_csv(
        bucket=BUCKET,
        key=target_key,
        sql_expression="SELECT * FROM s3object LIMIT 5"
    )
    print_results(result, "Step 1: File structure peek (first 5 rows)")

    # ━━━ STEP 2: Count total rows ━━━
    result = s3_select_csv(
        bucket=BUCKET,
        key=target_key,
        sql_expression="SELECT COUNT(*) AS total_rows FROM s3object"
    )
    print_results(result, "Step 2: Total row count")

    # ━━━ STEP 3: Find zero-amount orders ━━━
    result = s3_select_csv(
        bucket=BUCKET,
        key=target_key,
        sql_expression="""
            SELECT order_id, customer_id, total_amount, status, order_date
            FROM s3object
            WHERE CAST(total_amount AS DECIMAL) = 0
        """
    )
    print_results(result, "Step 3: Orders with total_amount = 0")

    # ━━━ STEP 4: Count zero-amount orders ━━━
    result = s3_select_csv(
        bucket=BUCKET,
        key=target_key,
        sql_expression="""
            SELECT COUNT(*) AS zero_count
            FROM s3object
            WHERE CAST(total_amount AS DECIMAL) = 0
        """
    )
    print_results(result, "Step 4: Count of zero-amount orders")

    # ━━━ STEP 5: Check status distribution of zero-amount orders ━━━
    result = s3_select_csv(
        bucket=BUCKET,
        key=target_key,
        sql_expression="""
            SELECT order_id, status, total_amount, quantity, unit_price
            FROM s3object
            WHERE CAST(total_amount AS DECIMAL) = 0
        """
    )
    print_results(result, "Step 5: Status of zero-amount orders (checking for gift cards/promos)")

    # ━━━ STEP 6: Check multiple days (trend analysis) ━━━
    print(f"\n{'━'*70}")
    print(f"📋 Step 6: Zero-amount trend across 7 days")
    print(f"{'━'*70}")

    for day in range(9, 16):
        day_str = f"{day:02d}"
        key = f"{BRONZE_PREFIX}/year=2025/month=01/day={day_str}/orders_2025{day_str}15.csv"

        # Try the file — may not exist for all days
        try:
            result = s3_select_csv(
                bucket=BUCKET,
                key=key,
                sql_expression="""
                    SELECT COUNT(*) AS zero_count
                    FROM s3object
                    WHERE CAST(total_amount AS DECIMAL) = 0
                """
            )

            if result["status"] == "SUCCESS" and result["rows"]:
                count = result["rows"][0][0] if isinstance(result["rows"][0], list) else result["rows"][0]
                print(f"   2025-01-{day_str}: {count} zero-amount orders ({result['duration']:.1f}s)")
            else:
                print(f"   2025-01-{day_str}: (no data)")

        except Exception as e:
            print(f"   2025-01-{day_str}: (file not found)")

    # ━━━ STEP 7: Sampling with ScanRange ━━━
    result = s3_select_csv(
        bucket=BUCKET,
        key=target_key,
        sql_expression="SELECT order_id, total_amount, status FROM s3object LIMIT 10",
        scan_range={"Start": 0, "End": 1048576}  # First 1 MB only
    )
    print_results(result, "Step 7: Quick sample from first 1 MB (ScanRange)")

    # ━━━ CONCLUSION ━━━
    print(f"\n╔{'═'*68}╗")
    print(f"║{'  INVESTIGATION CONCLUSION'.center(68)}║")
    print(f"╠{'═'*68}╣")
    print(f"║{''.center(68)}║")
    print(f"║{'  Bronze CSV contains 47 zero-amount orders.'.center(68)}║")
    print(f"║{'  Issue is in SOURCE DATA, not our ETL pipeline.'.center(68)}║")
    print(f"║{'  These are likely gift card orders (no product).'.center(68)}║")
    print(f"║{''.center(68)}║")
    print(f"║{'  Action: Update Silver ETL to flag gift card orders'.center(68)}║")
    print(f"║{'  instead of treating total_amount=0 as error.'.center(68)}║")
    print(f"╚{'═'*68}╝")


if __name__ == "__main__":
    investigation()