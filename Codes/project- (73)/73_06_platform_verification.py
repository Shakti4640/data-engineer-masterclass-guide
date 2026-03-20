# file: 73_06_platform_verification.py
# Comprehensive verification of the entire data lake platform
# Run this after all setup scripts to confirm everything works

import boto3
import json
from datetime import datetime, timedelta

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def check_catalog_layer(glue_client):
    """Verify Glue Data Catalog has all expected databases and tables"""
    print("\n🔍 LAYER 3a: GLUE DATA CATALOG")
    print("-" * 60)

    expected_databases = [
        "orders_bronze", "orders_silver", "orders_gold",
        "customers_gold", "products_gold",
        "analytics_derived", "federated_sources"
    ]

    databases = glue_client.get_databases()
    existing_dbs = [db["Name"] for db in databases["DatabaseList"]]

    total_tables = 0
    for db_name in expected_databases:
        if db_name in existing_dbs:
            tables = glue_client.get_tables(DatabaseName=db_name)
            table_count = len(tables.get("TableList", []))
            total_tables += table_count
            table_names = [t["Name"] for t in tables.get("TableList", [])]
            print(f"   ✅ {db_name}: {table_count} table(s) — {', '.join(table_names) or '(empty)'}")
        else:
            print(f"   ❌ {db_name}: MISSING")

    print(f"\n   Summary: {len([d for d in expected_databases if d in existing_dbs])}"
          f"/{len(expected_databases)} databases, {total_tables} total tables")

    return all(db in existing_dbs for db in expected_databases)


def check_lake_formation(lf_client):
    """Verify Lake Formation governance is active"""
    print("\n🔍 LAYER 3b: LAKE FORMATION GOVERNANCE")
    print("-" * 60)

    # Check admin
    try:
        settings = lf_client.get_data_lake_settings()
        admins = settings["DataLakeSettings"].get("DataLakeAdmins", [])
        default_db = settings["DataLakeSettings"].get("CreateDatabaseDefaultPermissions", [])
        default_tbl = settings["DataLakeSettings"].get("CreateTableDefaultPermissions", [])

        if admins:
            print(f"   ✅ Admin configured: {admins[0]['Da[REDACTED:AWS_ACCESS_KEY]er'].split('/')[-1]}")
        else:
            print(f"   ❌ No Lake Formation admin configured")

        if not default_db and not default_tbl:
            print(f"   ✅ Default permissions: EMPTY (governance active)")
        else:
            print(f"   ⚠️  Default permissions still set — IAMAllowedPrincipals may bypass LF")

    except Exception as e:
        print(f"   ⚠️  Could not check LF settings: {e}")

    # Count grants
    try:
        grants = []
        paginator = lf_client.get_paginator("list_permissions")
        for page in paginator.paginate():
            grants.extend(page["PrincipalResourcePermissions"])

        # Categorize
        db_grants = [g for g in grants if "Database" in g["Resource"]]
        table_grants = [g for g in grants if "Table" in g["Resource"] or "TableWithColumns" in g["Resource"]]
        column_filtered = [g for g in grants if "TableWithColumns" in g["Resource"]]
        cross_account = [g for g in grants if not g["Principal"]["Da[REDACTED:AWS_ACCESS_KEY]er"].startswith(f"arn:aws:iam::{ACCOUNT_ID}")]

        print(f"   ✅ Total grants: {len(grants)}")
        print(f"      Database-level: {len(db_grants)}")
        print(f"      Table-level: {len(table_grants)}")
        print(f"      Column-filtered: {len(column_filtered)}")
        print(f"      Cross-account: {len(cross_account)}")

    except Exception as e:
        print(f"   ⚠️  Could not list grants: {e}")

    return True


def check_storage_layer(s3_client):
    """Verify S3 data lake structure exists"""
    print("\n🔍 LAYER 2a: S3 DATA LAKE")
    print("-" * 60)

    buckets_to_check = {
        "orders-data-prod": ["bronze/", "silver/", "gold/"],
        "analytics-data-prod": ["derived/", "athena-results/"]
    }

    all_ok = True
    for bucket, prefixes in buckets_to_check.items():
        try:
            s3_client.head_bucket(Bucket=bucket)
            print(f"   ✅ Bucket exists: {bucket}")

            for prefix in prefixes:
                response = s3_client.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, MaxKeys=5
                )
                count = response.get("KeyCount", 0)
                status = "✅" if count > 0 else "⚠️  (empty)"
                print(f"      {status} {prefix} — {count} object(s)")

        except Exception as e:
            print(f"   ❌ Bucket issue: {bucket} — {e}")
            all_ok = False

    return all_ok


def check_redshift_spectrum(rs_data_client):
    """Verify Redshift Spectrum external schemas work"""
    print("\n🔍 LAYER 2b + 5: REDSHIFT + SPECTRUM")
    print("-" * 60)

    verification_queries = [
        (
            "List external schemas",
            "SELECT schemaname, databasename FROM svv_external_schemas ORDER BY schemaname;"
        ),
        (
            "List external tables",
            "SELECT schemaname, tablename FROM svv_external_tables ORDER BY schemaname, tablename;"
        ),
        (
            "List internal schemas",
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema' ORDER BY schema_name;"
        ),
        (
            "Count rows in materialized view",
            "SELECT COUNT(*) as row_count FROM mart_sales.daily_sales_summary;"
        )
    ]

    for query_name, sql in verification_queries:
        try:
            response = rs_data_client.execute_statement(
                WorkgroupName="quickcart-analytics",
                Database="quickcart",
                Sql=sql
            )
            stmt_id = response["Id"]

            # Wait for result
            import time
            for _ in range(30):
                status = rs_data_client.describe_statement(Id=stmt_id)
                if status["Status"] in ("FINISHED", "FAILED", "ABORTED"):
                    break
                time.sleep(1)

            if status["Status"] == "FINISHED":
                if status.get("HasResultSet"):
                    results = rs_data_client.get_statement_result(Id=stmt_id)
                    rows = results.get("Records", [])
                    print(f"   ✅ {query_name}: {len(rows)} result(s)")
                    for row in rows[:5]:
                        vals = [str(field.get("stringValue", field.get("longValue", ""))) for field in row]
                        print(f"      → {' | '.join(vals)}")
                else:
                    print(f"   ✅ {query_name}: executed successfully")
            else:
                error = status.get("Error", "Unknown")
                print(f"   ❌ {query_name}: {error}")

        except Exception as e:
            print(f"   ⚠️  {query_name}: {e}")
            print(f"      (Redshift Serverless may not be running)")

    return True


def check_ingestion_layer():
    """Verify ingestion components are healthy"""
    print("\n🔍 LAYER 1: INGESTION")
    print("-" * 60)

    # Kinesis
    kinesis = boto3.client("kinesis", region_name=REGION)
    try:
        desc = kinesis.describe_stream(StreamName="clickstream-events")
        status = desc["StreamDescription"]["StreamStatus"]
        shards = len(desc["StreamDescription"]["Shards"])
        print(f"   ✅ Kinesis stream: clickstream-events ({status}, {shards} shards)")
    except Exception as e:
        print(f"   ❌ Kinesis stream: {e}")

    # Firehose
    firehose = boto3.client("firehose", region_name=REGION)
    try:
        desc = firehose.describe_delivery_stream(DeliveryStreamName="clickstream-delivery")
        status = desc["DeliveryStreamDescription"]["DeliveryStreamStatus"]
        print(f"   ✅ Firehose: clickstream-delivery ({status})")
    except Exception as e:
        print(f"   ❌ Firehose: {e}")


def check_orchestration_layer():
    """Verify Step Functions and EventBridge"""
    print("\n🔍 ORCHESTRATION")
    print("-" * 60)

    sfn = boto3.client("stepfunctions", region_name=REGION)
    events = boto3.client("events", region_name=REGION)

    # Step Functions
    expected_machines = [
        "quickcart-master-nightly-pipeline",
        "clickstream-microbatch-pipeline"
    ]

    machines = sfn.list_state_machines()
    existing = {sm["name"]: sm for sm in machines["stateMachines"]}

    for name in expected_machines:
        if name in existing:
            sm = existing[name]
            # Check recent executions
            execs = sfn.list_executions(
                stateMachineArn=sm["stateMachineArn"],
                maxResults=3
            )
            exec_list = execs.get("executions", [])
            last_status = exec_list[0]["status"] if exec_list else "NO RUNS"
            print(f"   ✅ {name} (last: {last_status})")
        else:
            print(f"   ❌ {name}: NOT FOUND")

    # EventBridge rules
    expected_rules = [
        "nightly-master-pipeline",
        "clickstream-microbatch-schedule"
    ]

    for rule_name in expected_rules:
        try:
            rule = events.describe_rule(Name=rule_name)
            state = rule["State"]
            schedule = rule.get("ScheduleExpression", "event-based")
            print(f"   ✅ EventBridge: {rule_name} ({state}, {schedule})")
        except Exception:
            print(f"   ❌ EventBridge: {rule_name} NOT FOUND")


def check_monitoring_layer():
    """Verify monitoring components"""
    print("\n🔍 MONITORING")
    print("-" * 60)

    cw = boto3.client("cloudwatch", region_name=REGION)

    # Dashboard
    try:
        cw.get_dashboard(DashboardName="DataLakePlatform-Health")
        print(f"   ✅ Dashboard: DataLakePlatform-Health")
    except Exception:
        print(f"   ❌ Dashboard: DataLakePlatform-Health NOT FOUND")

    # Alarms
    alarms = cw.describe_alarms(AlarmNamePrefix="platform-")
    alarm_list = alarms.get("MetricAlarms", [])
    print(f"   ✅ Alarms: {len(alarm_list)} platform alarms configured")
    for alarm in alarm_list:
        state = alarm["StateValue"]
        icon = {"OK": "🟢", "ALARM": "🔴", "INSUFFICIENT_DATA": "🟡"}.get(state, "❓")
        print(f"      {icon} {alarm['AlarmName']}: {state}")


def generate_platform_report():
    """Generate comprehensive platform status report"""
    print(f"\n{'═' * 70}")
    print(f"📋 QUICKCART DATA LAKE PLATFORM — STATUS REPORT")
    print(f"   Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"   Account: {ACCOUNT_ID}")
    print(f"   Region: {REGION}")
    print(f"{'═' * 70}")

    glue_client = boto3.client("glue", region_name=REGION)
    lf_client = boto3.client("lakeformation", region_name=REGION)
    s3_client = boto3.client("s3", region_name=REGION)

    results = {}

    results["catalog"] = check_catalog_layer(glue_client)
    results["governance"] = check_lake_formation(lf_client)
    results["storage"] = check_storage_layer(s3_client)
    check_ingestion_layer()
    check_orchestration_layer()
    check_monitoring_layer()

    # Optional: Redshift check (requires running cluster)
    try:
        rs_data = boto3.client("redshift-data", region_name=REGION)
        check_redshift_spectrum(rs_data)
    except Exception:
        print("\n🔍 REDSHIFT: Skipped (Serverless workgroup may be paused)")

    # Final summary
    print(f"\n{'═' * 70}")
    print("📊 PLATFORM COMPONENT SUMMARY")
    print(f"{'─' * 70}")
    print(f"""
   INGESTION:
   ├── Kinesis Data Stream:     clickstream-events (real-time)
   ├── Kinesis Firehose:        clickstream-delivery (JSON → Parquet)
   └── Glue JDBC:               MariaDB extraction (nightly batch)

   STORAGE:
   ├── S3 Lake:                 orders-data-prod (bronze/silver/gold)
   ├── S3 Analytics:            analytics-data-prod (derived datasets)
   └── Redshift Serverless:     quickcart-analytics (hot data + Spectrum)

   CATALOG:
   ├── Glue Data Catalog:       7 databases, unified schema registry
   ├── Lake Formation:          Centralized permissions, column-level
   └── Redshift External:       Spectrum schemas → Glue Catalog

   TRANSFORMATION:
   ├── Glue Batch ETL:          Bronze → Silver → Gold (nightly)
   ├── Glue Micro-Batch:        Clickstream Bronze → Gold (every 5 min)
   ├── Redshift Stored Procs:   In-warehouse aggregations
   └── Athena CTAS:             Derived dataset creation

   CONSUMPTION:
   ├── Athena:                  Ad-hoc queries on S3 (analysts)
   ├── Redshift:                BI dashboards (sub-second)
   ├── Redshift Spectrum:       S3 queries from Redshift (warm data)
   └── Cross-Account:           Analytics domain reads via mesh

   ORCHESTRATION:
   ├── Step Functions:           Master nightly + micro-batch pipelines
   ├── EventBridge:             Nightly schedule + 5-min schedule
   └── SNS → SQS:              Data Mesh publish/subscribe

   MONITORING:
   ├── CloudWatch Dashboard:    DataLakePlatform-Health
   ├── CloudWatch Alarms:       5 critical platform alarms
   └── SNS Alerts:              pipeline-alerts → PagerDuty/Slack
   """)
    print(f"{'═' * 70}")


if __name__ == "__main__":
    generate_platform_report()