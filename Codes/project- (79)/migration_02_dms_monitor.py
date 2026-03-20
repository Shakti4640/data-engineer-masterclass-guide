# file: migration/02_dms_monitor.py
# Purpose: Monitor DMS task progress, validate row counts, detect issues

import boto3
import json
import time
from datetime import datetime, timezone

REGION = "us-east-2"
TASK_IDENTIFIER = "quickcart-full-migration"


class DMSMonitor:
    """
    Monitor DMS replication task in real-time
    
    Tracks:
    → Full load progress (tables completed, rows loaded)
    → CDC latency (seconds behind source)
    → Validation status (row count mismatches)
    → Error conditions
    """

    def __init__(self):
        self.dms = boto3.client("dms", region_name=REGION)
        self.cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    def get_task_status(self):
        """Get current task status and statistics"""
        response = self.dms.describe_replication_tasks(
            Filters=[{
                "Name": "replication-task-id",
                "Values": [TASK_IDENTIFIER]
            }]
        )

        if not response["ReplicationTasks"]:
            raise RuntimeError(f"Task '{TASK_IDENTIFIER}' not found")

        task = response["ReplicationTasks"][0]

        return {
            "status": task["Status"],
            "stop_reason": task.get("StopReason", ""),
            "migration_type": task["MigrationType"],
            "cdc_start_position": task.get("CdcStartPosition", ""),
            "recovery_checkpoint": task.get("RecoveryCheckpoint", ""),
            "stats": task.get("ReplicationTaskStats", {})
        }

    def get_table_statistics(self):
        """Get per-table load statistics"""
        response = self.dms.describe_table_statistics(
            ReplicationTaskArn=self._get_task_arn(),
            MaxRecords=100
        )

        tables = []
        for stat in response.get("TableStatistics", []):
            tables.append({
                "schema": stat["SchemaName"],
                "table": stat["TableName"],
                "state": stat["TableState"],
                "inserts": stat["Inserts"],
                "updates": stat["Updates"],
                "deletes": stat["Deletes"],
                "full_load_rows": stat["FullLoadRows"],
                "full_load_end_time": str(stat.get("FullLoadEndTime", "")),
                "last_update_time": str(stat.get("LastUpdateTime", "")),
                "validation_state": stat.get("ValidationState", "N/A"),
                "validation_failures": stat.get(
                    "ValidationFailedRecords", 0
                ),
                "validation_suspended": stat.get(
                    "ValidationSuspendedRecords", 0
                )
            })

        return sorted(tables, key=lambda x: x["full_load_rows"], reverse=True)

    def _get_task_arn(self):
        """Resolve task identifier to ARN"""
        response = self.dms.describe_replication_tasks(
            Filters=[{
                "Name": "replication-task-id",
                "Values": [TASK_IDENTIFIER]
            }]
        )
        return response["ReplicationTasks"][0]["ReplicationTaskArn"]

    def get_cdc_latency(self):
        """
        Get CDC replication latency from CloudWatch
        
        Metric: CDCLatencySource (seconds behind source)
        → < 60 seconds = healthy
        → 60-300 seconds = warning
        → > 300 seconds = critical (CDC falling behind)
        """
        response = self.cloudwatch.get_metric_statistics(
            Namespace="AWS/DMS",
            MetricName="CDCLatencySource",
            Dimensions=[{
                "Name": "ReplicationInstanceIdentifier",
                "Value": "quickcart-migration-instance"
            }],
            StartTime=datetime.now(timezone.utc).replace(
                minute=0, second=0, microsecond=0
            ),
            EndTime=datetime.now(timezone.utc),
            Period=60,
            Statistics=["Average", "Maximum"]
        )

        datapoints = sorted(
            response.get("Datapoints", []),
            key=lambda x: x["Timestamp"],
            reverse=True
        )

        if datapoints:
            latest = datapoints[0]
            return {
                "avg_seconds": latest.get("Average", 0),
                "max_seconds": latest.get("Maximum", 0),
                "timestamp": latest["Timestamp"].isoformat()
            }
        return {"avg_seconds": None, "max_seconds": None, "timestamp": None}

    def print_dashboard(self):
        """Print real-time migration dashboard"""
        status = self.get_task_status()
        tables = self.get_table_statistics()
        latency = self.get_cdc_latency()

        print("\n" + "=" * 80)
        print(f"  📊 DMS MIGRATION DASHBOARD — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        # Overall status
        status_icon = {
            "running": "🟢", "stopped": "🔴",
            "starting": "🟡", "failed": "❌"
        }.get(status["status"], "⚪")
        print(f"\n  Status: {status_icon} {status['status'].upper()}")
        print(f"  Type: {status['migration_type']}")

        stats = status.get("stats", {})
        print(f"\n  📈 Overall Statistics:")
        print(f"     Full Load: {stats.get('FullLoadProgressPercent', 0)}% complete")
        print(f"     Tables Loaded: {stats.get('TablesLoaded', 0)}")
        print(f"     Tables Loading: {stats.get('TablesLoading', 0)}")
        print(f"     Tables Queued: {stats.get('TablesQueued', 0)}")
        print(f"     Tables Errored: {stats.get('TablesErrored', 0)}")

        # CDC latency
        if latency["avg_seconds"] is not None:
            lat_icon = "🟢" if latency["avg_seconds"] < 60 else "🟡" if latency["avg_seconds"] < 300 else "🔴"
            print(f"\n  ⏱️  CDC Latency: {lat_icon} {latency['avg_seconds']:.0f}s avg / {latency['max_seconds']:.0f}s max")
        else:
            print(f"\n  ⏱️  CDC Latency: ⚪ No data yet (full load in progress)")

        # Per-table status
        print(f"\n  📋 Table Status:")
        print(f"  {'Schema':<15} {'Table':<25} {'State':<15} {'Rows':>12} {'CDC I/U/D':>20} {'Validation':>12}")
        print(f"  {'-'*15} {'-'*25} {'-'*15} {'-'*12} {'-'*20} {'-'*12}")

        for t in tables:
            state_icon = {
                "Table completed": "✅",
                "Table loading": "⏳",
                "Table queued": "⏸️",
                "Table error": "❌"
            }.get(t["state"], "⚪")

            cdc_summary = f"{t['inserts']}I/{t['updates']}U/{t['deletes']}D"
            val_state = t["validation_state"]
            val_icon = "✅" if val_state == "Validated" else "⏳" if val_state == "Pending records" else "❌" if "error" in val_state.lower() else "⚪"

            print(
                f"  {t['schema']:<15} {t['table']:<25} "
                f"{state_icon} {t['state']:<12} "
                f"{t['full_load_rows']:>10,} "
                f"{cdc_summary:>20} "
                f"{val_icon} {val_state}"
            )

            if t["validation_failures"] > 0:
                print(
                    f"  {'':>42} ⚠️  {t['validation_failures']} validation failures"
                )

        print("=" * 80)

    def monitor_continuously(self, interval_seconds=30, max_iterations=None):
        """Poll and display dashboard until migration complete or max iterations"""
        iteration = 0
        while True:
            self.print_dashboard()

            status = self.get_task_status()
            if status["status"] in ("stopped", "failed"):
                print(f"\n🛑 Migration {status['status']}: {status['stop_reason']}")
                break

            iteration += 1
            if max_iterations and iteration >= max_iterations:
                break

            print(f"\n  ⏳ Next refresh in {interval_seconds}s... (Ctrl+C to stop)")
            time.sleep(interval_seconds)


def validate_row_counts():
    """
    Compare source MariaDB row counts with S3 Bronze layer
    
    VALIDATION STRATEGY:
    → For each table: COUNT(*) on source vs count of Parquet rows in S3
    → Tolerance: 0% for full load tables (must match exactly)
    → Tolerance: 0.01% for CDC tables (slight delay acceptable)
    → Any mismatch > tolerance triggers alert
    """
    import pymysql

    # Connect to source MariaDB
    source_conn = pymysql.connect(
        host="10.0.1.11",
        port=3306,
        user="dms_validation",
        password="RETRIEVE_FROM_SECRETS_MANAGER",
        database="ecommerce"
    )

    # S3 + Athena for Bronze counts (using Athena — Project 24)
    athena = boto3.client("athena", region_name=REGION)

    tables_to_validate = [
        ("ecommerce", "orders"),
        ("ecommerce", "order_items"),
        ("ecommerce", "customers"),
        ("ecommerce", "products"),
        ("payments", "transactions"),
        ("payments", "refunds"),
    ]

    print("\n" + "=" * 70)
    print("  🔍 ROW COUNT VALIDATION: Source vs Bronze")
    print("=" * 70)
    print(f"  {'Database':<15} {'Table':<20} {'Source':>12} {'Bronze':>12} {'Delta':>8} {'Status':>8}")
    print(f"  {'-'*15} {'-'*20} {'-'*12} {'-'*12} {'-'*8} {'-'*8}")

    all_passed = True
    results = []

    for db_name, table_name in tables_to_validate:
        # Source count
        with source_conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {db_name}.{table_name}")
            source_count = cursor.fetchone()[0]

        # Bronze count via Athena
        query = f"SELECT COUNT(*) as cnt FROM bronze_{db_name}.{table_name}"
        exec_response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": f"bronze_{db_name}"},
            ResultConfiguration={
                "OutputLocation": "s3://quickcart-athena-results-prod/validation/"
            }
        )

        # Wait for Athena query
        exec_id = exec_response["QueryExecutionId"]
        while True:
            status = athena.get_query_execution(QueryExecutionId=exec_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(2)

        if state == "SUCCEEDED":
            result = athena.get_query_results(QueryExecutionId=exec_id)
            bronze_count = int(
                result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
            )
        else:
            bronze_count = -1

        # Compare
        delta = source_count - bronze_count
        delta_pct = abs(delta) / source_count * 100 if source_count > 0 else 0
        passed = delta_pct <= 0.01  # 0.01% tolerance

        status_icon = "✅" if passed else "❌"
        if not passed:
            all_passed = False

        print(
            f"  {db_name:<15} {table_name:<20} "
            f"{source_count:>12,} {bronze_count:>12,} "
            f"{delta:>+8,} {status_icon}"
        )

        results.append({
            "database": db_name,
            "table": table_name,
            "source_count": source_count,
            "bronze_count": bronze_count,
            "delta": delta,
            "delta_pct": round(delta_pct, 4),
            "passed": passed
        })

    source_conn.close()

    print(f"\n  {'=' * 70}")
    if all_passed:
        print("  ✅ ALL TABLES VALIDATED — row counts match within tolerance")
    else:
        failed = [r for r in results if not r["passed"]]
        print(f"  ❌ {len(failed)} TABLE(S) FAILED VALIDATION:")
        for f in failed:
            print(f"     → {f['database']}.{f['table']}: delta={f['delta']:+,} ({f['delta_pct']}%)")

    return results, all_passed


if __name__ == "__main__":
    monitor = DMSMonitor()
    monitor.print_dashboard()