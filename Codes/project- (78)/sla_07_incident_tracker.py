# file: sla/07_incident_tracker.py
# Purpose: DynamoDB table for incident tracking + SLA calculation engine

import boto3
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal

REGION = "us-east-2"
INCIDENT_TABLE = "SLAIncidentTracker"


def create_incident_table():
    """Create DynamoDB table for incident lifecycle tracking"""
    dynamodb = boto3.client("dynamodb", region_name=REGION)

    try:
        dynamodb.create_table(
            TableName=INCIDENT_TABLE,
            KeySchema=[
                {"AttributeName": "incident_id", "KeyType": "HASH"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "incident_id", "AttributeType": "S"},
                {"AttributeName": "month_key", "AttributeType": "S"},
                {"AttributeName": "started_at", "AttributeType": "S"},
                {"AttributeName": "pipeline", "AttributeType": "S"}
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI-MonthPipeline",
                    "KeySchema": [
                        {"AttributeName": "month_key", "KeyType": "HASH"},
                        {"AttributeName": "started_at", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                },
                {
                    "IndexName": "GSI-PipelineTime",
                    "KeySchema": [
                        {"AttributeName": "pipeline", "KeyType": "HASH"},
                        {"AttributeName": "started_at", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "Purpose", "Value": "SLATracking"},
                {"Key": "Team", "Value": "DataPlatform"}
            ]
        )
        print(f"✅ DynamoDB table '{INCIDENT_TABLE}' created")

        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=INCIDENT_TABLE)
        print(f"✅ Table is ACTIVE")

    except dynamodb.exceptions.ResourceInUseException:
        print(f"ℹ️  Table '{INCIDENT_TABLE}' already exists")


class SLACalculator:
    """
    Calculate SLA metrics from incident data
    
    KEY METRICS:
    → Uptime Percentage (monthly, quarterly, yearly)
    → MTTR (Mean Time To Recovery)
    → Incident Count
    → Auto-Recovery Rate
    → SLA Compliance (yes/no against target)
    """

    def __init__(self):
        self.dynamodb = boto3.resource("dynamodb", region_name=REGION)
        self.table = self.dynamodb.Table(INCIDENT_TABLE)

    def get_monthly_incidents(self, year, month, pipeline=None):
        """Get all incidents for a given month"""
        month_key = f"{year}-{month:02d}"

        kwargs = {
            "IndexName": "GSI-MonthPipeline",
            "KeyConditionExpression": (
                boto3.dynamodb.conditions.Key("month_key").eq(month_key)
            )
        }

        if pipeline:
            kwargs["FilterExpression"] = (
                boto3.dynamodb.conditions.Attr("pipeline").eq(pipeline)
            )

        response = self.table.query(**kwargs)
        return response.get("Items", [])

    def calculate_monthly_sla(self, year, month, pipeline=None):
        """
        Calculate uptime percentage for a given month
        
        FORMULA:
        total_minutes = days_in_month × 24 × 60
        downtime_minutes = sum of all incident durations
        uptime_percent = (total_minutes - downtime_minutes) / total_minutes × 100
        """
        import calendar
        days_in_month = calendar.monthrange(year, month)[1]
        total_minutes = days_in_month * 24 * 60

        incidents = self.get_monthly_incidents(year, month, pipeline)

        # Calculate total downtime
        total_downtime = 0
        resolved_count = 0
        auto_recovered_count = 0
        mttr_values = []

        for incident in incidents:
            mttr = incident.get("mttr_minutes")
            if mttr is not None:
                mttr_numeric = float(mttr)
                total_downtime += mttr_numeric
                mttr_values.append(mttr_numeric)
                resolved_count += 1

                if incident.get("resolution") == "AUTO_RECOVERED":
                    auto_recovered_count += 1
            else:
                # Unresolved incident — count from start to end of month
                # or to now if current month
                started = datetime.fromisoformat(
                    incident["started_at"].replace("Z", "+00:00")
                )
                now = datetime.now(timezone.utc)
                month_end = datetime(
                    year, month, days_in_month, 23, 59, 59,
                    tzinfo=timezone.utc
                )
                end = min(now, month_end)
                duration = (end - started).total_seconds() / 60
                total_downtime += duration

        uptime_percent = (
            (total_minutes - total_downtime) / total_minutes * 100
        )
        avg_mttr = (
            sum(mttr_values) / len(mttr_values)
            if mttr_values else 0
        )
        auto_recovery_rate = (
            auto_recovered_count / resolved_count * 100
            if resolved_count > 0 else 0
        )

        return {
            "period": f"{year}-{month:02d}",
            "pipeline": pipeline or "ALL",
            "total_minutes": total_minutes,
            "downtime_minutes": round(total_downtime, 1),
            "uptime_percent": round(uptime_percent, 4),
            "incident_count": len(incidents),
            "resolved_count": resolved_count,
            "unresolved_count": len(incidents) - resolved_count,
            "auto_recovered_count": auto_recovered_count,
            "auto_recovery_rate_percent": round(auto_recovery_rate, 1),
            "avg_mttr_minutes": round(avg_mttr, 1),
            "max_mttr_minutes": round(max(mttr_values), 1) if mttr_values else 0,
            "min_mttr_minutes": round(min(mttr_values), 1) if mttr_values else 0,
            "sla_target_percent": 99.9,
            "sla_met": uptime_percent >= 99.9,
            "sla_margin_minutes": round(
                (uptime_percent - 99.9) / 100 * total_minutes, 1
            )
        }

    def generate_board_report(self, year, quarter):
        """
        Generate quarterly SLA report for board presentation
        
        Returns structured data for:
        → Executive summary (1 number: quarterly uptime %)
        → Trend (monthly breakdown)
        → Top incidents (longest MTTR)
        → Improvement metrics (MTTR trend, auto-recovery trend)
        """
        months = {
            1: [1, 2, 3],
            2: [4, 5, 6],
            3: [7, 8, 9],
            4: [10, 11, 12]
        }[quarter]

        monthly_reports = []
        all_incidents = []

        for month in months:
            report = self.calculate_monthly_sla(year, month)
            monthly_reports.append(report)

            incidents = self.get_monthly_incidents(year, month)
            all_incidents.extend(incidents)

        # Quarterly aggregate
        total_minutes = sum(r["total_minutes"] for r in monthly_reports)
        total_downtime = sum(r["downtime_minutes"] for r in monthly_reports)
        quarterly_uptime = (
            (total_minutes - total_downtime) / total_minutes * 100
            if total_minutes > 0 else 100
        )

        # Top 5 longest incidents
        sorted_incidents = sorted(
            [i for i in all_incidents if i.get("mttr_minutes")],
            key=lambda x: float(x.get("mttr_minutes", 0)),
            reverse=True
        )[:5]

        return {
            "report_type": "QUARTERLY_SLA_BOARD_REPORT",
            "period": f"Q{quarter} {year}",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "executive_summary": {
                "quarterly_uptime_percent": round(quarterly_uptime, 4),
                "sla_target_percent": 99.9,
                "sla_met": quarterly_uptime >= 99.9,
                "total_incidents": len(all_incidents),
                "auto_recovery_rate": round(
                    sum(r["auto_recovery_rate_percent"]
                        for r in monthly_reports)
                    / len(monthly_reports), 1
                ) if monthly_reports else 0
            },
            "monthly_trend": monthly_reports,
            "top_incidents": [
                {
                    "incident_id": i["incident_id"],
                    "pipeline": i["pipeline"],
                    "mttr_minutes": float(i.get("mttr_minutes", 0)),
                    "resolution": i.get("resolution", "UNKNOWN"),
                    "error_type": i.get("error_type", "UNKNOWN")
                }
                for i in sorted_incidents
            ]
        }


def print_sla_report(report):
    """Pretty-print SLA report for console output"""
    print("\n" + "=" * 70)
    print(f"  📊 SLA REPORT: {report.get('period', report.get('pipeline', 'ALL'))}")
    print("=" * 70)

    if "executive_summary" in report:
        # Quarterly board report
        es = report["executive_summary"]
        icon = "✅" if es["sla_met"] else "❌"
        print(f"\n  {icon} Quarterly Uptime: {es['quarterly_uptime_percent']}%")
        print(f"     SLA Target: {es['sla_target_percent']}%")
        print(f"     Total Incidents: {es['total_incidents']}")
        print(f"     Auto-Recovery Rate: {es['auto_recovery_rate']}%")

        print(f"\n  📈 Monthly Trend:")
        for m in report["monthly_trend"]:
            icon = "✅" if m["sla_met"] else "❌"
            print(
                f"     {icon} {m['period']}: {m['uptime_percent']}% "
                f"({m['incident_count']} incidents, "
                f"avg MTTR {m['avg_mttr_minutes']}min)"
            )

        if report["top_incidents"]:
            print(f"\n  🔴 Top Incidents by Duration:")
            for inc in report["top_incidents"]:
                print(
                    f"     {inc['incident_id']}: "
                    f"{inc['pipeline']} — {inc['mttr_minutes']}min "
                    f"({inc['resolution']})"
                )
    else:
        # Monthly report
        icon = "✅" if report["sla_met"] else "❌"
        print(f"\n  {icon} Uptime: {report['uptime_percent']}%")
        print(f"     SLA Target: {report['sla_target_percent']}%")
        print(f"     SLA Margin: {report['sla_margin_minutes']} minutes")
        print(f"\n     Total Downtime: {report['downtime_minutes']} minutes")
        print(f"     Incidents: {report['incident_count']}")
        print(f"     Auto-Recovered: {report['auto_recovered_count']}")
        print(f"     Auto-Recovery Rate: {report['auto_recovery_rate_percent']}%")
        print(f"\n     Avg MTTR: {report['avg_mttr_minutes']} minutes")
        print(f"     Max MTTR: {report['max_mttr_minutes']} minutes")
        print(f"     Min MTTR: {report['min_mttr_minutes']} minutes")

    print("=" * 70)


if __name__ == "__main__":
    create_incident_table()

    calculator = SLACalculator()

    # Monthly report
    now = datetime.now()
    monthly = calculator.calculate_monthly_sla(now.year, now.month)
    print_sla_report(monthly)

    # Quarterly board report
    quarter = (now.month - 1) // 3 + 1
    quarterly = calculator.generate_board_report(now.year, quarter)
    print_sla_report(quarterly)
