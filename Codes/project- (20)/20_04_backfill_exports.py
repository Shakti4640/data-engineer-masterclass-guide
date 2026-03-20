# file: 20_04_backfill_exports.py
# Purpose: Export multiple historical dates at once
# Use case: First-time setup, catch up after downtime, re-export after fix

import sys
import time
from datetime import date, datetime, timedelta
from importlib import import_module

exporter = import_module("20_01_export_mariadb_to_s3")


def backfill_exports(start_date, end_date, exports=None):
    """
    Export data for a range of dates
    
    Parameters:
    → start_date: first date to export (inclusive)
    → end_date: last date to export (inclusive)
    → exports: list of export names to run (None = all)
    
    Example:
    → backfill_exports('2025-01-01', '2025-01-15')
    → Exports 15 days of data
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    total_days = (end_date - start_date).days + 1

    print("=" * 70)
    print(f"🔄 BACKFILL EXPORT")
    print(f"   Date range: {start_date} to {end_date} ({total_days} days)")
    print(f"   Exports:    {exports or 'ALL'}")
    print("=" * 70)

    overall_start = time.time()
    day_results = {}

    current_date = start_date
    day_num = 0

    while current_date <= end_date:
        day_num += 1
        print(f"\n{'━' * 70}")
        print(f"📅 DAY {day_num}/{total_days}: {current_date}")
        print(f"{'━' * 70}")

        if exports:
            # Run only specified exports
            day_result = {}
            for export_name in exports:
                result = exporter.run_single_export(export_name, current_date)
                day_result[export_name] = result
        else:
            # Run all exports
            day_result = exporter.run_all_exports(export_date=current_date)

        day_results[str(current_date)] = day_result
        current_date += timedelta(days=1)

    # ── BACKFILL SUMMARY ──
    overall_duration = time.time() - overall_start
    total_rows = 0
    total_failures = 0

    print("\n" + "=" * 70)
    print("📊 BACKFILL SUMMARY")
    print("=" * 70)

    for date_str, results in day_results.items():
        day_rows = 0
        day_failures = 0

        for name, result in results.items():
            if result.get("success"):
                day_rows += result.get("rows_exported", 0)
            else:
                day_failures += 1

        status = "✅" if day_failures == 0 else "❌"
        print(f"   {status} {date_str}: {day_rows:>8,} rows exported"
              f"{'  (' + str(day_failures) + ' failed)' if day_failures else ''}")

        total_rows += day_rows
        total_failures += day_failures

    print(f"\n   {'─' * 50}")
    print(f"   Total days:    {total_days}")
    print(f"   Total rows:    {total_rows:,}")
    print(f"   Total failures: {total_failures}")
    print(f"   Duration:      {overall_duration/60:.1f} minutes")
    print(f"   Avg per day:   {overall_duration/total_days:.1f} seconds")

    if total_failures > 0:
        print(f"\n   ⚠️  {total_failures} exports failed — check logs above")
    else:
        print(f"\n   ✅ ALL {total_days} days exported successfully")

    return day_results


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python 20_04_backfill_exports.py START_DATE END_DATE [export_name]")
        print("Example: python 20_04_backfill_exports.py 2025-01-01 2025-01-15")
        print("Example: python 20_04_backfill_exports.py 2025-01-01 2025-01-07 orders_full")
        sys.exit(1)

    start = sys.argv[1]
    end = sys.argv[2]
    specific_exports = [sys.argv[3]] if len(sys.argv) > 3 else None

    backfill_exports(start, end, exports=specific_exports)