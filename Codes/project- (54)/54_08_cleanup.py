# file: 54_08_cleanup.py
# Purpose: Remove all monitoring infrastructure

import boto3
import json

REGION = "us-east-2"
DASHBOARD_NAME = "QuickCart-Pipeline-Health"


def cleanup_all():
    """Remove all alarms, metric filters, SNS topics, dashboard"""
    cw_client = boto3.client("cloudwatch", region_name=REGION)
    logs_client = boto3.client("logs", region_name=REGION)
    sns_client = boto3.client("sns", region_name=REGION)

    print("=" * 70)
    print("🧹 CLEANING UP PROJECT 54 — MONITORING INFRASTRUCTURE")
    print("=" * 70)

    # --- Step 1: Delete composite alarms FIRST ---
    print("\n📌 Step 1: Deleting composite alarms...")
    composite_names = [
        "Pipeline-CRITICAL-DataFlowBroken",
        "Pipeline-WARNING-Degraded",
        "Pipeline-SLA-Breach"
    ]
    for name in composite_names:
        try:
            cw_client.delete_alarms(AlarmNames=[name])
            print(f"  ✅ Deleted composite: {name}")
        except Exception as e:
            print(f"  ⚠️  {name}: {e}")

    # --- Step 2: Delete metric alarms ---
    print("\n📌 Step 2: Deleting metric alarms...")
    metric_alarm_names = [
        "Pipeline-GlueETL-JobFailed",
        "Pipeline-GlueCrawler-Failed",
        "Pipeline-SQS-DLQ-NotEmpty",
        "Pipeline-DataBrew-JobFailed",
        "Pipeline-Worker-HeartbeatMissing",
        "Pipeline-S3Upload-NotReceived"
    ]
    try:
        cw_client.delete_alarms(AlarmNames=metric_alarm_names)
        for name in metric_alarm_names:
            print(f"  ✅ Deleted alarm: {name}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 3: Delete metric filters ---
    print("\n📌 Step 3: Deleting metric filters...")
    filter_configs = [
        ("/aws-glue/jobs/error", "GlueETL-ErrorCount"),
        ("/aws-glue/jobs/error", "GlueETL-OOMErrors"),
        ("/aws-glue/jobs/output", "GlueETL-JobDuration"),
        ("/quickcart/sqs-workers", "SQSWorker-ProcessingErrors"),
        ("/aws-glue-databrew/jobs", "DataBrew-StepFailures")
    ]
    for log_group, filter_name in filter_configs:
        try:
            logs_client.delete_metric_filter(
                logGroupName=log_group,
                filterName=filter_name
            )
            print(f"  ✅ Deleted filter: {filter_name}")
        except Exception as e:
            print(f"  ⚠️  {filter_name}: {e}")

    # --- Step 4: Delete log groups (optional — may want to keep) ---
    print("\n📌 Step 4: Deleting test log groups...")
    test_log_groups = ["/quickcart/sqs-workers"]
    for lg in test_log_groups:
        try:
            logs_client.delete_log_group(logGroupName=lg)
            print(f"  ✅ Deleted log group: {lg}")
        except Exception as e:
            print(f"  ⚠️  {lg}: {e}")

    # --- Step 5: Delete SNS topics ---
    print("\n📌 Step 5: Deleting SNS notification topics...")
    topic_names = [
        "quickcart-pipeline-warnings",
        "quickcart-pipeline-critical",
        "quickcart-pipeline-sla-breach"
    ]
    topics = sns_client.list_topics()
    for topic in topics.get("Topics", []):
        topic_arn = topic["TopicArn"]
        for name in topic_names:
            if name in topic_arn:
                # Delete subscriptions first
                subs = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
                for sub in subs.get("Subscriptions", []):
                    if sub["SubscriptionArn"] != "PendingConfirmation":
                        try:
                            sns_client.unsubscribe(SubscriptionArn=sub["SubscriptionArn"])
                        except Exception:
                            pass

                sns_client.delete_topic(TopicArn=topic_arn)
                print(f"  ✅ Deleted topic: {name}")

    # --- Step 6: Delete dashboard ---
    print("\n📌 Step 6: Deleting dashboard...")
    try:
        cw_client.delete_dashboards(DashboardNames=[DASHBOARD_NAME])
        print(f"  ✅ Deleted dashboard: {DASHBOARD_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 7: Delete test DLQ ---
    print("\n📌 Step 7: Deleting test DLQ...")
    sqs_client = boto3.client("sqs", region_name=REGION)
    try:
        dlq_url = sqs_client.get_queue_url(QueueName="quickcart-etl-queue-dlq")["QueueUrl"]
        sqs_client.delete_queue(QueueUrl=dlq_url)
        print(f"  ✅ Deleted test DLQ")
    except Exception as e:
        print(f"  ⚠️  {e}")

    print("\n" + "=" * 70)
    print("🎉 ALL PROJECT 54 RESOURCES CLEANED UP")
    print("=" * 70)


if __name__ == "__main__":
    cleanup_all()