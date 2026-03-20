# file: 68_03_glue_etl_with_dq.py
# This is the Glue ETL script with DATA QUALITY gates
# Replaces or augments existing ETL jobs from previous projects
# Upload to: s3://quickcart-analytics-scripts/glue/etl_with_dq.py

import sys
import json
import math
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from awsglue.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_name",
    "file_bucket",
    "file_key",
    "batch_date",
    "dq_severity"    # HARD_FAIL or SOFT_WARN
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

spark.conf.set("spark.sql.shuffle.partitions", "10")

SOURCE_NAME = args["source_name"]
FILE_BUCKET = args["file_bucket"]
FILE_KEY = args["file_key"]
BATCH_DATE = args["batch_date"]
DQ_SEVERITY = args.get("dq_severity", "HARD_FAIL")

LAKE_BUCKET = "quickcart-analytics-datalake"
QUARANTINE_BUCKET = "quickcart-analytics-quarantine"
SNS_TOPIC = "arn:aws:sns:us-east-2:222222222222:data-platform-alerts"

RUN_ID = f"dq_{SOURCE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


# Import DQDL rules (in practice, these would be loaded from config)
DQDL_RULES = {
    "partner_feed": """
        Rules = [
            RowCount between 2000 and 15000,
            IsUnique "order_id",
            IsComplete "order_id",
            IsComplete "customer_id",
            Completeness "total_amount" >= 0.99,
            ColumnValues "total_amount" > 0,
            ColumnValues "total_amount" < 50000
        ]
    """,
    "payment_data": """
        Rules = [
            RowCount > 0,
            IsComplete "transaction_id",
            IsUnique "transaction_id",
            ColumnValues "amount" > 0
        ]
    """,
    "marketing_campaign": """
        Rules = [
            RowCount > 0,
            IsComplete "campaign_id",
            IsComplete "campaign_name"
        ]
    """
}


def read_source_data():
    """Read source file from S3"""
    source_path = f"s3://{FILE_BUCKET}/{FILE_KEY}"
    logger.info(f"📥 Reading: {source_path}")

    if FILE_KEY.endswith(".csv"):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
    elif FILE_KEY.endswith(".json"):
        df = spark.read.json(source_path)
    elif FILE_KEY.endswith(".parquet"):
        df = spark.read.parquet(source_path)
    else:
        df = spark.read.option("header", "true").csv(source_path)

    row_count = df.count()
    col_count = len(df.columns)
    logger.info(f"   Rows: {row_count:,}, Columns: {col_count}")
    logger.info(f"   Schema: {df.columns}")

    return df


def run_data_quality_check(df, source_name):
    """
    Execute DQDL rules against the DataFrame
    
    RETURNS:
    → dq_results: dict with pass/fail per rule
    → good_df: rows that passed all rules
    → bad_df: rows that failed one or more rules
    → overall_pass: boolean — did all rules pass?
    """
    logger.info(f"🔍 Running Data Quality checks for: {source_name}")

    ruleset_string = DQDL_RULES.get(source_name)
    if not ruleset_string:
        logger.info(f"   ℹ️ No DQDL rules for {source_name} — skipping DQ")
        return {"overall_pass": True, "rules": []}, df, None

    # Convert to DynamicFrame for Glue DQ
    dyf = DynamicFrame.fromDF(df, glueContext, f"dq_input_{source_name}")

    try:
        # EvaluateDataQuality returns rule outcomes
        dq_results_dyf = EvaluateDataQuality().process_rows(
            frame=dyf,
            ruleset=ruleset_string,
            publishing_options={
                "dataQualityEvaluationContext": f"dq_{source_name}",
                "enableDataQualityCloudWatchMetrics": True,
                "enableDataQualityResultsPublishing": True
            }
        )

        # Get rule outcomes
        rule_outcomes_df = dq_results_dyf["ruleOutcomes"].toDF()

        # Parse results
        rules_result = []
        overall_pass = True

        if rule_outcomes_df.count() > 0:
            outcomes = rule_outcomes_df.collect()
            for row in outcomes:
                rule_name = row["Rule"] if "Rule" in rule_outcomes_df.columns else "unknown"
                outcome = row["Outcome"] if "Outcome" in rule_outcomes_df.columns else "unknown"
                passed = outcome == "Passed"

                rules_result.append({
                    "rule": str(rule_name),
                    "passed": passed,
                    "outcome": str(outcome)
                })

                if not passed:
                    overall_pass = False
                    icon = "❌"
                else:
                    icon = "✅"

                logger.info(f"   {icon} {rule_name}: {outcome}")

        # Get row-level results if available
        good_df = df  # Default: all rows pass
        bad_df = None

        if "rowLevelOutcomes" in dq_results_dyf:
            row_outcomes = dq_results_dyf["rowLevelOutcomes"].toDF()
            if "DataQualityEvaluationResult" in row_outcomes.columns:
                good_df = row_outcomes.filter(
                    F.col("DataQualityEvaluationResult") == "Passed"
                ).drop("DataQualityEvaluationResult")

                bad_df = row_outcomes.filter(
                    F.col("DataQualityEvaluationResult") == "Failed"
                )

                good_count = good_df.count()
                bad_count = bad_df.count() if bad_df else 0
                logger.info(f"\n   📊 Row-level results:")
                logger.info(f"      Good rows: {good_count:,}")
                logger.info(f"      Bad rows: {bad_count:,}")

        return {"overall_pass": overall_pass, "rules": rules_result}, good_df, bad_df

    except Exception as e:
        logger.error(f"   ❌ DQ evaluation error: {str(e)}")
        # On DQ framework error: fall back to custom checks
        logger.info(f"   Falling back to custom PySpark validation...")
        return run_custom_quality_checks(df, source_name)


def run_custom_quality_checks(df, source_name):
    """
    Fallback: custom PySpark quality checks
    Used when DQDL is unavailable or for complex rules
    """
    logger.info(f"🔍 Running custom quality checks for: {source_name}")

    rules_result = []
    overall_pass = True
    total_rows = df.count()

    # Rule 1: Row count
    if source_name == "partner_feed":
        passed = 2000 <= total_rows <= 15000
        rules_result.append({
            "rule": "row_count_range",
            "passed": passed,
            "detail": f"{total_rows:,} rows (expected 2000-15000)"
        })
        if not passed:
            overall_pass = False

    # Rule 2: Uniqueness on primary key
    pk_col = {
        "partner_feed": "order_id",
        "payment_data": "transaction_id",
        "marketing_campaign": "campaign_id"
    }.get(source_name)

    if pk_col and pk_col in df.columns:
        distinct_count = df.select(pk_col).distinct().count()
        uniqueness = distinct_count / max(total_rows, 1)
        passed = uniqueness >= 1.0
        rules_result.append({
            "rule": f"uniqueness_{pk_col}",
            "passed": passed,
            "detail": f"Uniqueness: {uniqueness:.4f} ({distinct_count:,} distinct / {total_rows:,} total)"
        })
        if not passed:
            overall_pass = False
            # Identify duplicates for quarantine
            dup_ids = df.groupBy(pk_col).count().filter(F.col("count") > 1)
            dup_count = dup_ids.count()
            logger.info(f"   ⚠️ Found {dup_count:,} duplicate {pk_col} values")

    # Rule 3: Completeness on critical columns
    critical_cols = {
        "partner_feed": ["order_id", "customer_id", "total_amount"],
        "payment_data": ["transaction_id", "amount"],
        "marketing_campaign": ["campaign_id", "campaign_name"]
    }.get(source_name, [])

    for col in critical_cols:
        if col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            completeness = 1.0 - (null_count / max(total_rows, 1))
            passed = completeness >= 0.99
            rules_result.append({
                "rule": f"completeness_{col}",
                "passed": passed,
                "detail": f"Completeness: {completeness:.4f} ({null_count:,} nulls)"
            })
            if not passed:
                overall_pass = False

    # Rule 4: Value range checks
    if source_name == "partner_feed" and "total_amount" in df.columns:
        negative_count = df.filter(F.col("total_amount") <= 0).count()
        passed = negative_count == 0
        rules_result.append({
            "rule": "positive_amounts",
            "passed": passed,
            "detail": f"{negative_count:,} non-positive amounts"
        })
        if not passed:
            overall_pass = False

    # Log all results
    for rule in rules_result:
        icon = "✅" if rule["passed"] else "❌"
        logger.info(f"   {icon} {rule['rule']}: {rule.get('detail', rule.get('outcome', ''))}")

    # Separate good and bad rows
    bad_df = None
    good_df = df

    if not overall_pass and pk_col and pk_col in df.columns:
        # Mark duplicate rows as bad
        window = F.row_number().over(
            F.Window.partitionBy(pk_col).orderBy(F.lit(1))
        )
        df_with_rn = df.withColumn("_rn", window)
        good_df = df_with_rn.filter(F.col("_rn") == 1).drop("_rn")
        bad_df = df_with_rn.filter(F.col("_rn") > 1).drop("_rn")

    return {"overall_pass": overall_pass, "rules": rules_result}, good_df, bad_df


def quarantine_bad_rows(bad_df, source_name):
    """
    Write failed rows to quarantine bucket for investigation
    
    PATH: s3://quarantine/{source}/{batch_date}/{run_id}/
    """
    if bad_df is None or bad_df.count() == 0:
        logger.info("   ℹ️ No bad rows to quarantine")
        return

    bad_count = bad_df.count()
    quarantine_path = f"s3://{QUARANTINE_BUCKET}/{source_name}/{BATCH_DATE}/{RUN_ID}/"

    logger.info(f"🔒 Quarantining {bad_count:,} bad rows to: {quarantine_path}")

    # Add quarantine metadata
    bad_df_with_meta = bad_df \
        .withColumn("quarantine_reason", F.lit("data_quality_failure")) \
        .withColumn("quarantine_timestamp", F.current_timestamp()) \
        .withColumn("quarantine_batch", F.lit(BATCH_DATE)) \
        .withColumn("quarantine_run_id", F.lit(RUN_ID))

    bad_df_with_meta.coalesce(1).write \
        .mode("overwrite") \
        .format("parquet") \
        .save(quarantine_path)

    logger.info(f"   ✅ {bad_count:,} rows quarantined")


def send_quality_alert(dq_results, source_name, severity):
    """Send SNS alert on quality failure"""
    sns_client = boto3.client("sns")

    failed_rules = [r for r in dq_results["rules"] if not r["passed"]]

    subject = f"{'❌' if severity == 'HARD_FAIL' else '⚠️'} Data Quality {severity}: {source_name} ({BATCH_DATE})"

    message_lines = [
        f"DATA QUALITY {'FAILURE' if severity == 'HARD_FAIL' else 'WARNING'}",
        f"Source: {source_name}",
        f"Batch: {BATCH_DATE}",
        f"Run: {RUN_ID}",
        f"Severity: {severity}",
        "",
        f"Failed rules ({len(failed_rules)}):",
        *[f"  ❌ {r['rule']}: {r.get('detail', r.get('outcome', 'FAILED'))}" for r in failed_rules],
        "",
        f"Action: {'Pipeline STOPPED. Manual review required.' if severity == 'HARD_FAIL' else 'Pipeline continued with warnings. Review quarantine bucket.'}",
        f"Quarantine: s3://{QUARANTINE_BUCKET}/{source_name}/{BATCH_DATE}/{RUN_ID}/"
    ]

    sns_client.publish(
        TopicArn=SNS_TOPIC,
        Subject=subject,
        Message="\n".join(message_lines)
    )
    logger.info(f"   📧 Quality alert sent")


def write_to_silver(good_df, source_name):
    """Write quality-approved data to silver layer"""
    silver_path = f"s3://{LAKE_BUCKET}/silver/{source_name}/"

    logger.info(f"📤 Writing to silver: {silver_path}")

    # Add ETL metadata
    df_with_meta = good_df \
        .withColumn("etl_loaded_at", F.current_timestamp()) \
        .withColumn("etl_source", F.lit(f"dq_validated_{source_name}")) \
        .withColumn("etl_batch_date", F.lit(BATCH_DATE)) \
        .withColumn("etl_run_id", F.lit(RUN_ID)) \
        .withColumn("dq_status", F.lit("passed"))

    # Smart coalesce (Project 62)
    row_count = df_with_meta.count()
    estimated_mb = (row_count * 500) / (1024 * 1024)
    target_files = max(1, math.ceil(estimated_mb / 128))

    df_coalesced = df_with_meta.coalesce(target_files)

    df_coalesced.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(silver_path)

    logger.info(f"   ✅ Written {row_count:,} quality-approved rows to silver")
    logger.info(f"   Files: {target_files}")

    return row_count


def publish_quality_metrics(dq_results, source_name, good_count, bad_count):
    """
    Publish quality metrics to CloudWatch for dashboarding
    
    METRICS:
    → DQ/PassRate: percentage of rules that passed (0-100)
    → DQ/GoodRows: count of rows that passed quality
    → DQ/BadRows: count of rows that failed quality
    → DQ/RulesPassed: count of rules that passed
    → DQ/RulesFailed: count of rules that failed
    """
    cw = boto3.client("cloudwatch")
    timestamp = datetime.utcnow()

    rules = dq_results.get("rules", [])
    passed_rules = sum(1 for r in rules if r["passed"])
    failed_rules = sum(1 for r in rules if not r["passed"])
    total_rules = len(rules)
    pass_rate = (passed_rules / max(total_rules, 1)) * 100

    metrics = [
        {
            "MetricName": "PassRate",
            "Value": pass_rate,
            "Unit": "Percent"
        },
        {
            "MetricName": "GoodRows",
            "Value": good_count,
            "Unit": "Count"
        },
        {
            "MetricName": "BadRows",
            "Value": bad_count,
            "Unit": "Count"
        },
        {
            "MetricName": "RulesPassed",
            "Value": passed_rules,
            "Unit": "Count"
        },
        {
            "MetricName": "RulesFailed",
            "Value": failed_rules,
            "Unit": "Count"
        }
    ]

    cw.put_metric_data(
        Namespace="QuickCart/DataQuality",
        MetricData=[
            {
                **metric,
                "Dimensions": [
                    {"Name": "SourceName", "Value": source_name},
                    {"Name": "BatchDate", "Value": BATCH_DATE}
                ],
                "Timestamp": timestamp
            }
            for metric in metrics
        ]
    )

    logger.info(f"   📊 CloudWatch metrics published:")
    logger.info(f"      Pass rate: {pass_rate:.1f}%")
    logger.info(f"      Good rows: {good_count:,}")
    logger.info(f"      Bad rows: {bad_count:,}")
    logger.info(f"      Rules: {passed_rules} passed / {failed_rules} failed")


def store_dq_results_to_s3(dq_results, source_name, good_count, bad_count):
    """
    Persist quality results as JSON for historical tracking
    
    PATH: s3://lake/dq_results/{source}/{batch_date}/{run_id}.json
    
    Used for:
    → Historical quality trending
    → Audit trail (compliance)
    → Quality SLA reporting
    """
    s3_client = boto3.client("s3")

    result_record = {
        "run_id": RUN_ID,
        "batch_date": BATCH_DATE,
        "source_name": source_name,
        "timestamp": datetime.utcnow().isoformat(),
        "overall_pass": dq_results["overall_pass"],
        "good_rows": good_count,
        "bad_rows": bad_count,
        "total_rows": good_count + bad_count,
        "pass_rate": round(good_count / max(good_count + bad_count, 1) * 100, 2),
        "rules": dq_results["rules"],
        "severity": DQ_SEVERITY
    }

    result_key = f"dq_results/{source_name}/{BATCH_DATE}/{RUN_ID}.json"

    s3_client.put_object(
        Bucket=LAKE_BUCKET,
        Key=result_key,
        Body=json.dumps(result_record, indent=2, default=str),
        ContentType="application/json"
    )

    logger.info(f"   📋 DQ results saved to s3://{LAKE_BUCKET}/{result_key}")


def run_etl_with_quality():
    """
    Main ETL orchestrator WITH quality gates
    
    FLOW:
    1. Read source data from S3
    2. Run data quality checks (DQDL or custom PySpark)
    3. Based on results:
       → ALL PASS: write good data to silver layer
       → SOFT_WARN failure: write good data, quarantine bad, alert
       → HARD_FAIL failure: quarantine ALL data, alert, STOP pipeline
    4. Publish quality metrics
    5. Store quality results for audit
    """
    logger.info("=" * 70)
    logger.info(f"🚀 ETL WITH DATA QUALITY — {SOURCE_NAME}")
    logger.info(f"   Run ID: {RUN_ID}")
    logger.info(f"   Batch: {BATCH_DATE}")
    logger.info(f"   Source: s3://{FILE_BUCKET}/{FILE_KEY}")
    logger.info(f"   Severity: {DQ_SEVERITY}")
    logger.info("=" * 70)

    # STEP 1: Read source data
    df = read_source_data()
    total_rows = df.count()

    if total_rows == 0:
        logger.error("❌ Source file has 0 rows — aborting")
        send_quality_alert(
            {"overall_pass": False, "rules": [{"rule": "not_empty", "passed": False, "detail": "0 rows"}]},
            SOURCE_NAME, "HARD_FAIL"
        )
        raise Exception(f"Source file is empty: s3://{FILE_BUCKET}/{FILE_KEY}")

    # STEP 2: Run quality checks
    dq_results, good_df, bad_df = run_data_quality_check(df, SOURCE_NAME)

    good_count = good_df.count() if good_df is not None else 0
    bad_count = bad_df.count() if bad_df is not None else 0

    # STEP 3: Handle results based on severity
    if dq_results["overall_pass"]:
        # ALL RULES PASSED — proceed normally
        logger.info(f"\n✅ ALL QUALITY CHECKS PASSED")
        written_count = write_to_silver(good_df, SOURCE_NAME)

    elif DQ_SEVERITY == "SOFT_WARN":
        # SOME RULES FAILED but severity is SOFT — continue with good rows
        logger.info(f"\n⚠️ QUALITY WARNINGS — continuing with good rows")

        # Quarantine bad rows
        quarantine_bad_rows(bad_df, SOURCE_NAME)

        # Write good rows to silver
        if good_count > 0:
            written_count = write_to_silver(good_df, SOURCE_NAME)
        else:
            logger.error("❌ No good rows after quality filtering!")
            written_count = 0

        # Alert
        send_quality_alert(dq_results, SOURCE_NAME, "SOFT_WARN")

    else:
        # HARD_FAIL — stop pipeline, quarantine everything
        logger.error(f"\n❌ QUALITY CHECK FAILED (HARD_FAIL) — STOPPING PIPELINE")

        # Quarantine ALL data (not just bad rows)
        quarantine_bad_rows(df, SOURCE_NAME)

        # Alert
        send_quality_alert(dq_results, SOURCE_NAME, "HARD_FAIL")

        # Publish metrics before failing
        publish_quality_metrics(dq_results, SOURCE_NAME, 0, total_rows)
        store_dq_results_to_s3(dq_results, SOURCE_NAME, 0, total_rows)

        # Raise exception to stop Step Function pipeline
        failed_rules = [r for r in dq_results["rules"] if not r["passed"]]
        raise Exception(
            f"Data quality HARD_FAIL for {SOURCE_NAME}: "
            f"{len(failed_rules)} rules failed — "
            f"{[r['rule'] for r in failed_rules]}"
        )

    # STEP 4: Publish quality metrics
    publish_quality_metrics(dq_results, SOURCE_NAME, good_count, bad_count)

    # STEP 5: Store quality results for audit
    store_dq_results_to_s3(dq_results, SOURCE_NAME, good_count, bad_count)

    # STEP 6: Summary
    logger.info(f"\n{'='*70}")
    logger.info(f"📊 ETL + DQ SUMMARY")
    logger.info(f"   Source: {SOURCE_NAME}")
    logger.info(f"   Total input rows: {total_rows:,}")
    logger.info(f"   Good rows (to silver): {good_count:,}")
    logger.info(f"   Bad rows (quarantined): {bad_count:,}")
    logger.info(f"   Quality pass rate: {good_count / max(total_rows, 1) * 100:.1f}%")
    logger.info(f"   Overall DQ: {'PASS ✅' if dq_results['overall_pass'] else 'WARN ⚠️'}")
    logger.info(f"{'='*70}")


# --- EXECUTE ---
run_etl_with_quality()
job.commit()