# file: 37_05_view_health_check.py
# Purpose: Validate all views are working and measure query performance
# Run: Weekly or after any schema changes

import boto3
import time
from datetime import datetime

AWS_REGION = "us-east-2"
S3_BUCKET = "quickcart-datalake-prod"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

VIEWS_DB = "reporting_views"

athena = boto3.client("athena", region_name=AWS_REGION)
glue = boto3.client("glue", region_name=AWS_REGION)


def test_view(view_name, database=VIEWS_DB):
    """Test a single view — verify it returns data and measure performance"""
    query = f"SELECT COUNT(*) AS cnt FROM {database}.{view_name}"

    start = time.time()

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    qid = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    elapsed = time.time() - start

    result = {
        "view": view_name,
        "status": state,
        "elapsed_s": round(elapsed, 2),
        "scanned_mb": 0,
        "row_count": 0,
        "cost": 0,
        "error": None
    }

    if state == "SUCCEEDED":
        stats = status["QueryExecution"].get("Statistics", {})
        scanned = stats.get("DataScannedInBytes", 0)
        result["scanned_mb"] = round(scanned / (1024 * 1024), 2)
        result["cost"] = round(scanned / (1024**4) * 5, 6)

        # Get row count
        query_results = athena.get_query_results(QueryExecutionId=qid)
        rows = query_results["ResultSet"]["Rows"]
        if len(rows) > 1:
            result["row_count"] = int(rows[1]["Data"][0]["VarCharValue"])
    else:
        error = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        result["error"] = error[:200]

    return result


def run_full_health_check():
    """Test all views and report status"""
    print("=" * 80)
    print("🏥 VIEW HEALTH CHECK")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Discover all views
    response = glue.get_tables(DatabaseName=VIEWS_DB)
    views = [
        t["Name"] for t in response.get("TableList", [])
        if t.get("TableType") == "VIRTUAL_VIEW"
    ]

    if not views:
        print("   ❌ No views found in reporting_views database")
        return

    print(f"\n   Testing {len(views)} views...")
    print(f"   {'View':<40} {'Status':>8} {'Rows':>12} {'Scan MB':>10} {'Cost':>10} {'Time':>8}")
    print(f"   {'-'*40} {'-'*8} {'-'*12} {'-'*10} {'-'*10} {'-'*8}")

    total_cost = 0
    passed = 0
    failed = 0
    results = []

    for view_name in sorted(views):
        result = test_view(view_name)
        results.append(result)

        icon = "✅" if result["status"] == "SUCCEEDED" and result["row_count"] > 0 else "❌"

        if result["status"] == "SUCCEEDED":
            if result["row_count"] > 0:
                passed += 1
            else:
                failed += 1
                icon = "⚠️"
            total_cost += result["cost"]
        else:
            failed += 1

        print(
            f"   {icon} {view_name:<38} {result['status']:>8} "
            f"{result['row_count']:>12,} {result['scanned_mb']:>10.2f} "
            f"${result['cost']:>8.6f} {result['elapsed_s']:>6.1f}s"
        )

        if result["error"]:
            print(f"      ERROR: {result['error']}")

    # Summary
    print(f"\n   {'=' * 80}")
    print(f"   SUMMARY:")
    print(f"   ✅ Passed: {passed}")
    print(f"   ❌ Failed: {failed}")
    print(f"   Total health check cost: ${total_cost:.6f}")

    if failed > 0:
        print(f"\n   ⚠️  {failed} views need attention:")
        for r in results:
            if r["status"] != "SUCCEEDED" or r["row_count"] == 0:
                print(f"      → {r['view']}: {r.get('error', 'Empty result set')}")

    # Workgroup usage report
    print(f"\n\n📊 WORKGROUP USAGE REPORT")
    print("-" * 60)

    workgroups_to_check = [
        "wg-revenue-team", "wg-marketing-team",
        "wg-operations-team", "wg-junior-analysts",
        "wg-data-engineering", "wg-gold-only"
    ]

    for wg_name in workgroups_to_check:
        try:
            wg = athena.get_work_group(WorkGroup=wg_name)
            config = wg["WorkGroup"]["Configuration"]
            state = wg["WorkGroup"]["State"]
            limit_gb = config.get("BytesScannedCutoffPerQuery", 0) / (1024**3)
            enforce = config.get("EnforceWorkGroupConfiguration", False)

            icon = "🟢" if state == "ENABLED" else "🔴"
            print(f"   {icon} {wg_name:<25} | Limit: {limit_gb:.0f} GB | Enforced: {enforce}")
        except Exception:
            print(f"   ⚪ {wg_name:<25} | NOT FOUND")

    print("=" * 80)

    return results


if __name__ == "__main__":
    run_full_health_check()
    print("\n🏁 DONE")