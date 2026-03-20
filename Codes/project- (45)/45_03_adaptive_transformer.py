# file: 45_03_adaptive_transformer.py
# Purpose: Apply schema adaptations inside the Bronze→Silver ETL
# Integrates with Project 44's Bronze→Silver job

import boto3
import json
from datetime import datetime
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, TimestampType


REGION = "us-east-2"
DATALAKE_BUCKET = "quickcart-datalake-prod"
SNS_TOPIC_ARN = None  # Set at runtime


def apply_schema_adaptations(df, entity_name, baseline_schema, report):
    """
    Apply all detected schema changes to the DataFrame.
    
    Called AFTER detect_schema_drift, BEFORE writing to Silver.
    
    Parameters:
    → df: PySpark DataFrame (raw Bronze data)
    → entity_name: string
    → baseline_schema: dict from registry
    → report: dict from detect_schema_drift
    
    Returns:
    → adapted_df: DataFrame with schema changes applied
    → should_continue: bool (False if pipeline should fail)
    """
    alias_map = baseline_schema.get("alias_map", {})
    baseline_columns = {col["name"]: col for col in baseline_schema["columns"]}

    print(f"\n🔧 Applying schema adaptations for: {entity_name}")

    should_continue = True
    adaptation_log = []

    for change in report["changes"]:
        change_type = change["change_type"]
        column = change["column"]
        handling = change["handling"]

        # ━━━ HANDLE COLUMN RENAMED ━━━
        if change_type == "COLUMN_RENAMED":
            old_name = change["old_value"]
            new_name = change["new_value"]

            if new_name in df.columns:
                df = df.withColumnRenamed(new_name, old_name)
                adaptation_log.append(f"RENAMED: {new_name} → {old_name}")
                print(f"   ✅ Renamed: {new_name} → {old_name}")

        # ━━━ HANDLE COLUMN ADDED ━━━
        elif change_type == "COLUMN_ADDED":
            # New column from source — keep it in Silver as-is
            # It will be a string type initially (from CSV)
            # Future schema update will assign proper type
            adaptation_log.append(f"ADDED: {column} (kept as-is, nullable)")
            print(f"   ✅ New column included: {column} (nullable)")

        # ━━━ HANDLE COLUMN REMOVED ━━━
        elif change_type == "COLUMN_REMOVED":
            if handling == "FILL_NULL":
                # Optional column missing — add as NULL
                baseline_col = baseline_columns.get(column, {})
                col_type = baseline_col.get("type", "string")
                spark_type = map_type_string_to_spark(col_type)

                df = df.withColumn(column, F.lit(None).cast(spark_type))
                adaptation_log.append(f"REMOVED (nullable): {column} → filled with NULL")
                print(f"   ⚠️  Missing optional column: {column} → filled NULL")

            elif handling == "FAIL_PIPELINE":
                # Required column missing — cannot continue
                print(f"   ❌ REQUIRED column missing: {column} → PIPELINE WILL FAIL")
                adaptation_log.append(f"REMOVED (required): {column} → PIPELINE FAIL")
                should_continue = False

        # ━━━ HANDLE TYPE WIDENED ━━━
        elif change_type == "TYPE_WIDENED":
            # Safe cast — use the WIDER type
            new_type = change["new_value"]
            spark_type = map_type_string_to_spark(new_type)

            if column in df.columns:
                df = df.withColumn(column, F.col(column).cast(spark_type))
                adaptation_log.append(f"TYPE_WIDENED: {column} → {new_type}")
                print(f"   ✅ Type widened: {column} → {new_type}")

        # ━━━ HANDLE TYPE CHANGED (potentially unsafe) ━━━
        elif change_type == "TYPE_CHANGED":
            old_type = change["old_value"]
            new_type = change["new_value"]

            if column in df.columns:
                # Attempt cast, count failures
                spark_type = map_type_string_to_spark(old_type)

                # Cast to baseline type (what Silver expects)
                df_casted = df.withColumn(
                    f"_{column}_cast_attempt",
                    F.col(column).cast(spark_type)
                )

                # Count cast failures (NULL after cast where original was not NULL)
                cast_failures = df_casted.filter(
                    F.col(f"_{column}_cast_attempt").isNull() &
                    F.col(column).isNotNull()
                ).count()

                total_rows = df.count()
                failure_pct = (cast_failures / max(total_rows, 1)) * 100

                if failure_pct > 1.0:
                    # More than 1% cast failures — dangerous
                    print(f"   ❌ Type change: {column} ({old_type} → {new_type})")
                    print(f"      Cast failures: {cast_failures}/{total_rows} ({failure_pct:.2f}%)")
                    print(f"      Exceeds 1% threshold — keeping source type, alerting team")
                    adaptation_log.append(
                        f"TYPE_CHANGED (UNSAFE): {column} {old_type}→{new_type}, "
                        f"{cast_failures} failures ({failure_pct:.2f}%)"
                    )
                    # Keep the column as-is (source type) — don't fail pipeline
                    df = df.drop(f"_{column}_cast_attempt")
                else:
                    # Cast succeeds for >99% — apply the cast
                    print(f"   ⚠️  Type change: {column} ({old_type} → {new_type})")
                    print(f"      Cast failures: {cast_failures}/{total_rows} ({failure_pct:.2f}%) — acceptable")
                    df = df.withColumn(column, F.col(f"_{column}_cast_attempt"))
                    df = df.drop(f"_{column}_cast_attempt")
                    adaptation_log.append(
                        f"TYPE_CHANGED (cast applied): {column} {old_type}→{new_type}, "
                        f"{cast_failures} failures"
                    )

        # ━━━ HANDLE NULLABLE CHANGED ━━━
        elif change_type == "NULLABLE_CHANGED":
            adaptation_log.append(f"NULLABLE_CHANGED: {column} — logged, no action needed")
            print(f"   ℹ️  Nullable changed: {column} — logged")

    # ━━━ SUMMARY ━━━
    print(f"\n   📊 Adaptations applied: {len(adaptation_log)}")
    for entry in adaptation_log:
        print(f"      → {entry}")

    return df, should_continue


def map_type_string_to_spark(type_string):
    """Map type string from schema registry to PySpark type"""
    from pyspark.sql.types import (
        StringType, IntegerType, LongType, DoubleType, FloatType,
        DecimalType, DateType, TimestampType, BooleanType
    )

    t = type_string.lower().strip()

    if t == "string" or t.startswith("varchar"):
        return StringType()
    elif t == "integer" or t == "int":
        return IntegerType()
    elif t == "bigint" or t == "long":
        return LongType()
    elif t == "double":
        return DoubleType()
    elif t == "float":
        return FloatType()
    elif t.startswith("decimal"):
        # Parse decimal(precision, scale)
        try:
            parts = t.replace("decimal(", "").replace(")", "").split(",")
            precision = int(parts[0].strip())
            scale = int(parts[1].strip())
            return DecimalType(precision, scale)
        except (ValueError, IndexError):
            return DecimalType(10, 2)
    elif t == "date":
        return DateType()
    elif t == "timestamp":
        return TimestampType()
    elif t == "boolean":
        return BooleanType()
    else:
        return StringType()


def save_drift_report(report, entity_name, process_date):
    """
    Save schema drift report to S3 for historical tracking.
    
    Path: _system/schema-reports/{entity}/{date}.json
    Queryable via Athena for drift trend analysis.
    """
    s3_client = boto3.client("s3", region_name=REGION)

    report_key = f"_system/schema-reports/{entity_name}/{process_date}.json"

    s3_client.put_object(
        Bucket=DATALAKE_BUCKET,
        Key=report_key,
        Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "entity": entity_name,
            "process-date": process_date,
            "total-changes": str(report["total_changes"]),
            "severity": report["overall_severity"]
        }
    )

    print(f"   📄 Drift report saved: s3://{DATALAKE_BUCKET}/{report_key}")
    return report_key


def send_drift_alert(report, entity_name):
    """
    Send schema drift notification via SNS.
    
    ALWAYS sends — even for auto-handled changes.
    Severity determines urgency:
    → LOW: informational email
    → MEDIUM: email + Slack
    → HIGH: email + Slack + escalation
    → CRITICAL: all of the above + PagerDuty
    """
    sns_client = boto3.client("sns", region_name=REGION)

    severity = report["overall_severity"]
    total = report["total_changes"]
    auto = report["auto_handled"]
    manual = report["manual_required"]
    action = report["pipeline_action"]

    severity_emoji = {
        "NONE": "✅",
        "LOW": "ℹ️",
        "MEDIUM": "⚠️",
        "HIGH": "🔴",
        "CRITICAL": "🚨"
    }

    emoji = severity_emoji.get(severity, "❓")
    subject = f"{emoji} Schema Drift: {entity_name} — {severity} ({total} changes)"

    message_lines = [
        f"Schema Drift Report — {entity_name}",
        f"Detected: {report['detected_at']}",
        f"Severity: {severity}",
        f"Pipeline Action: {action}",
        "",
        f"Changes: {total} total",
        f"  Auto-handled: {auto}",
        f"  Manual required: {manual}",
        "",
        "Details:",
        "-" * 50
    ]

    for change in report["changes"]:
        message_lines.append(
            f"  [{change['severity']}] {change['change_type']}: {change['column']}"
        )
        message_lines.append(f"    Handling: {change['handling']}")
        if change.get("details"):
            message_lines.append(f"    Details: {change['details']}")
        message_lines.append("")

    if manual > 0:
        message_lines.extend([
            "⚠️  ACTION REQUIRED:",
            "  Some changes could not be auto-handled.",
            "  Data engineer must review and update the schema registry.",
            f"  Entity: {entity_name}",
            "  Steps:",
            "    1. Review changes above",
            "    2. Update alias_map if columns were renamed",
            "    3. Update schema registry with new baseline",
            "    4. Re-run pipeline"
        ])

    # Determine SNS topic (could route to different topics by severity)
    topic_arn = SNS_TOPIC_ARN
    if topic_arn:
        try:
            sns_client.publish(
                TopicArn=topic_arn,
                Subject=subject[:100],
                Message="\n".join(message_lines)
            )
            print(f"   📧 Drift alert sent: {subject}")
        except Exception as e:
            print(f"   ⚠️  Failed to send drift alert: {e}")
    else:
        print(f"   ℹ️  SNS topic not configured — alert logged but not sent")
        print(f"   📧 Would send: {subject}")