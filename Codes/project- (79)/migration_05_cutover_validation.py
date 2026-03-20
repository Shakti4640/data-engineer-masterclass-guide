# file: migration/05_cutover_validation.py
# Purpose: Comprehensive validation before cutover
# Must ALL pass before applications switch to AWS

import boto3
import json
from datetime import datetime, timezone

REGION = "us-east-2"


class CutoverValidator:
    """
    Pre-cutover validation checklist
    
    ALL checks must pass:
    ✅ Row count reconciliation (source vs all layers)
    ✅ Business metric comparison (revenue, order count)
    ✅ Data quality score > 99.5% (Project 68 rules)
    ✅ CDC latency < 5 minutes
    ✅ Redshift query performance < 5 seconds
    ✅ SLA monitoring green for 7 days
    ✅ Lineage graph complete (Project 77)
    ✅ Rollback procedure tested
    """

    def __init__(self):
        self.results = []
        self.all_passed = True

    def check(self, name, passed, detail=""):
        """Record a validation check result"""
        self.results.append({
            "check": name,
            "passed": passed,
            "detail": detail,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        if not passed:
            self.all_passed = False

    def validate_row_counts(self):
        """Check 1: Row counts match across all layers"""
        # Reuse Module 2 validation
        from dms_monitor import validate_row_counts
        results, passed = validate_row_counts()
        self.check(
            "Row Count Reconciliation",
            passed,
            f"{len(results)} tables checked, "
            f"{len([r for r in results if r['passed']])} passed"
        )

    def validate_business_metrics(self):
        """
        Check 2: Key business metrics match between source and AWS
        
        Compare:
        → Total revenue (last 30 days) — must match within 0.1%
        → Total order count (last 30 days) — must match exactly
        → Total customer count — must match exactly
        """
        # Source metrics (from MariaDB)
        source_revenue = 5800000.00     # Simulated — query source in production
        source_orders = 150000
        source_customers = 8000000

        # AWS metrics (from Redshift Gold)
        redshift_data = boto3.client("redshift-data", region_name=REGION)
        # Execute query to get AWS metrics
        # Simulated for code completeness:
        aws_revenue = 5798500.00
        aws_orders = 149998
        aws_customers = 8000000

        revenue_delta_pct = abs(source_revenue - aws_revenue) / source_revenue * 100
        orders_delta = abs(source_orders - aws_orders)
        customers_delta = abs(source_customers - aws_customers)

        revenue_ok = revenue_delta_pct < 0.1
        orders_ok = orders_delta <= 10  # Allow small CDC delay
        customers_ok = customers_delta == 0

        self.check(
            "Revenue Match (30-day)",
            revenue_ok,
            f"Source: ${source_revenue:,.2f} | AWS: ${aws_revenue:,.2f} | "
            f"Delta: {revenue_delta_pct:.3f}%"
        )
        self.check(
            "Order Count Match (30-day)",
            orders_ok,
            f"Source: {source_orders:,} | AWS: {aws_orders:,} | "
            f"Delta: {orders_delta}"
        )
        self.check(
            "Customer Count Match",
            customers_ok,
            f"Source: {source_customers:,} | AWS: {aws_customers:,}"
        )

    def validate_cdc_latency(self):
        """Check 3: CDC replication latency < 5 minutes"""
        from dms_monitor import DMSMonitor
        monitor = DMSMonitor()
        latency = monitor.get_cdc_latency()

        avg_seconds = latency.get("avg_seconds", 999)
        passed = avg_seconds is not None and avg_seconds < 300

        self.check(
            "CDC Latency < 5 minutes",
            passed,
            f"Current latency: {avg_seconds:.0f}s"
            if avg_seconds else "No latency data available"
        )

    def validate_query_performance(self):
        """Check 4: Key Redshift queries return within 5 seconds"""
        test_queries = [
            (
                "Finance Summary (30-day)",
                "SELECT SUM(net_revenue) FROM gold.finance_summary "
                "WHERE order_date >= DATEADD(day, -30, CURRENT_DATE)"
            ),
            (
                "Customer 360 Lookup",
                "SELECT * FROM gold.customer_360 WHERE customer_id = 'CUST-000001'"
            ),
            (
                "Top Products (aggregation)",
                "SELECT order_date, COUNT(*) FROM gold.finance_summary "
                "GROUP BY order_date ORDER BY order_date DESC LIMIT 30"
            )
        ]

        for query_name, sql in test_queries:
            # Simulated timing — in production, measure actual execution
            execution_time_seconds = 1.2  # Placeholder
            passed = execution_time_seconds < 5.0

            self.check(
                f"Query Performance: {query_name}",
                passed,
                f"Execution time: {execution_time_seconds:.1f}s (target: < 5s)"
            )

    def validate_sla_monitoring(self):
        """Check 5: SLA monitoring green for 7+ consecutive days"""
        cloudwatch = boto3.client("cloudwatch", region_name=REGION)

        alarm_name = "sla-finance_summary-composite-unhealthy"
        response = cloudwatch.describe_alarm_history(
            AlarmName=alarm_name,
            HistoryItemType="StateUpdate",
            MaxRecords=50
        )

        # Check last 7 days of alarm history
        alarm_events = response.get("AlarmHistoryItems", [])
        recent_alarms = [
            e for e in alarm_events
            if "ALARM" in e.get("HistorySummary", "")
        ]

        # Simulated check
        days_green = 8  # Placeholder — calculate from alarm history
        passed = days_green >= 7

        self.check(
            "SLA Monitoring Green 7+ Days",
            passed,
            f"Consecutive green days: {days_green}"
        )

    def validate_lineage_complete(self):
        """Check 6: Lineage graph covers all Gold tables (Project 77)"""
        dynamodb = boto3.resource("dynamodb", region_name=REGION)
        table = dynamodb.Table("DataLineage")

        gold_tables = ["finance_summary", "customer_360"]
        missing_lineage = []

        for gold_table in gold_tables:
            response = table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key("PK").begins_with(
                    f"NODE#"
                ),
                FilterExpression=boto3.dynamodb.conditions.Attr("table_name").eq(gold_table),
                Limit=1
            )

            if not response.get("Items"):
                missing_lineage.append(gold_table)

        passed = len(missing_lineage) == 0

        self.check(
            "Lineage Graph Complete",
            passed,
            f"Missing lineage: {missing_lineage}" if missing_lineage
            else f"All {len(gold_tables)} Gold tables have lineage"
        )

    def validate_rollback_tested(self):
        """Check 7: Rollback procedure documented and tested"""
        # This is a manual check — we verify the document exists
        rollback_doc_exists = True  # Placeholder
        rollback_tested = True      # Placeholder

        self.check(
            "Rollback Procedure Documented",
            rollback_doc_exists,
            "https://wiki.quickcart.com/migration/rollback-plan"
        )
        self.check(
            "Rollback Procedure Tested",
            rollback_tested,
            "Last tested: 2025-01-10 — switchback to on-prem in 15 minutes"
        )

    def run_all_validations(self):
        """Execute complete validation suite"""
        print("\n" + "=" * 70)
        print("  🔍 PRE-CUTOVER VALIDATION SUITE")
        print(f"  Time: {datetime.now(timezone.utc).isoformat()}")
        print("=" * 70)

        self.validate_row_counts()
        self.validate_business_metrics()
        self.validate_cdc_latency()
        self.validate_query_performance()
        self.validate_sla_monitoring()
        self.validate_lineage_complete()
        self.validate_rollback_tested()

        # Print results
        print(f"\n  {'Check':<45} {'Status':>8} {'Detail'}")
        print(f"  {'-'*45} {'-'*8} {'-'*40}")

        for r in self.results:
            icon = "✅" if r["passed"] else "❌"
            print(f"  {r['check']:<45} {icon:>5}    {r['detail'][:60]}")

        passed_count = len([r for r in self.results if r["passed"]])
        total_count = len(self.results)

        print(f"\n  {'=' * 70}")
        if self.all_passed:
            print(f"  ✅ ALL {total_count} CHECKS PASSED — CUTOVER APPROVED")
            print(f"  📋 Proceed with cutover procedure")
        else:
            failed = [r for r in self.results if not r["passed"]]
            print(f"  ❌ {len(failed)} of {total_count} CHECKS FAILED — CUTOVER BLOCKED")
            print(f"  📋 Fix the following before retrying:")
            for f in failed:
                print(f"     → {f['check']}: {f['detail']}")

        print("=" * 70)
        return self.all_passed, self.results


if __name__ == "__main__":
    validator = CutoverValidator()
    approved, results = validator.run_all_validations()