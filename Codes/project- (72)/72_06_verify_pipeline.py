# file: 72_06_verify_pipeline.py
# Verify the entire streaming pipeline is working end-to-end

import boto3
import json
import time
from datetime import datetime, timedelta

REGION = "us-east-2"
STREAM_NAME = "clickstream-events"
FIREHOSE_NAME = "clickstream-delivery"
BUCKET_NAME = "orders-data-prod"
STATE_MACHINE_NAME = "clickstream-microbatch-pipeline"


def check_kinesis_stream(kinesis_client):
    """Verify stream is healthy and receiving data"""
    print("\n🔍 KINESIS DATA STREAM")
    print("-" * 50)

    desc = kinesis_client.describe_stream(StreamName=STREAM_NAME)
    stream = desc["StreamDescription"]

    print(f"   Status: {stream['StreamStatus']}")
    print(f"   Shards: {len(stream['Shards'])}")
    print(f"   Retention: {stream['RetentionPeriodHours']} hours")

    # Check recent metrics
    cw = boto3.client("cloudwatch", region_name=REGION)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=15)

    metrics = cw.get_metric_statistics(
        Namespace="AWS/Kinesis",
        MetricName="IncomingRecords",
        Dimensions=[{"Name": "StreamName", "Value": STREAM_NAME}],
        StartTime=start_time,
        EndTime=end_time,
        Period=300,
        Statistics=["Sum"]
    )

    if metrics["Datapoints"]:
        total = sum(dp["Sum"] for dp in metrics["Datapoints"])
        print(f"   Records (last 15 min): {total:,.0f}")
    else:
        print(f"   Records (last 15 min): No data yet")

    return stream["StreamStatus"] == "ACTIVE"


def check_firehose(firehose_client):
    """Verify Firehose is delivering to S3"""
    print("\n🔍 KINESIS FIREHOSE")
    print("-" * 50)

    desc = firehose_client.describe_delivery_stream(
        DeliveryStreamName=FIREHOSE_NAME
    )
    stream = desc["DeliveryStreamDescription"]

    print(f"   Status: {stream['DeliveryStreamStatus']}")
    print(f"   Source: {stream.get('Source', {}).get('KinesisStreamSourceDescription', {}).get('KinesisStreamARN', 'N/A')}")

    # Check delivery metrics
    cw = boto3.client("cloudwatch", region_name=REGION)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=15)

    for metric_name in ["DeliveryToS3.Records", "DeliveryToS3.DataFreshness"]:
        metrics = cw.get_metric_statistics(
            Namespace="AWS/Firehose",
            MetricName=metric_name,
            Dimensions=[{"Name": "DeliveryStreamName", "Value": FIREHOSE_NAME}],
            StartTime=start_time,
            EndTime=end_time,
            Period=300,
            Statistics=["Sum", "Average"]
        )
        if metrics["Datapoints"]:
            if "Sum" in metrics["Datapoints"][0]:
                val = sum(dp.get("Sum", 0) for dp in metrics["Datapoints"])
                print(f"   {metric_name}: {val:,.0f}")
            if "Average" in metrics["Datapoints"][0]:
                val = sum(dp.get("Average", 0) for dp in metrics["Datapoints"]) / len(metrics["Datapoints"])
                print(f"   {metric_name} (avg): {val:.1f}")

    return stream["DeliveryStreamStatus"] == "ACTIVE"


def check_s3_bronze(s3_client):
    """Verify Firehose is writing Parquet files to Bronze"""
    print("\n🔍 S3 BRONZE LAYER")
    print("-" * 50)

    now = datetime.utcnow()
    prefix = (
        f"bronze/clickstream/"
        f"year={now.strftime('%Y')}/month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/hour={now.strftime('%H')}/"
    )

    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix,
        MaxKeys=20
    )

    files = response.get("Contents", [])
    print(f"   Prefix: {prefix}")
    print(f"   Files found: {len(files)}")

    if files:
        total_size = sum(f["Size"] for f in files)
        latest = max(f["LastModified"] for f in files)
        age_seconds = (datetime.utcnow().replace(tzinfo=latest.tzinfo) - latest).total_seconds() if latest.tzinfo else 0

        print(f"   Total size: {total_size / 1024:.1f} KB")
        print(f"   Latest file: {latest.strftime('%H:%M:%S')}")
        print(f"   Age: {age_seconds:.0f} seconds")

        # Show first few files
        for f in files[:5]:
            size_kb = f["Size"] / 1024
            print(f"     → {f['Key'].split('/')[-1]} ({size_kb:.1f} KB)")
    else:
        print(f"   ⚠️  No files yet — wait for Firehose buffer flush (60s)")

    return len(files) > 0


def check_s3_gold(s3_client):
    """Verify Glue ETL is writing to Gold layer"""
    print("\n🔍 S3 GOLD LAYER")
    print("-" * 50)

    now = datetime.utcnow()
    prefix = (
        f"gold/clickstream_events/"
        f"year={now.strftime('%Y')}/month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
    )

    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix,
        MaxKeys=20
    )

    files = response.get("Contents", [])
    print(f"   Prefix: {prefix}")
    print(f"   Files found: {len(files)}")

    if files:
        total_size = sum(f["Size"] for f in files)
        print(f"   Total size: {total_size / (1024*1024):.2f} MB")
        for f in files[:5]:
            size_kb = f["Size"] / 1024
            print(f"     → {f['Key'].split('/')[-1]} ({size_kb:.1f} KB)")
    else:
        print(f"   ⚠️  No Gold files yet — wait for Glue micro-batch to run")

    return len(files) > 0


def check_step_functions(sfn_client):
    """Verify Step Functions executions"""
    print("\n🔍 STEP FUNCTIONS")
    print("-" * 50)

    # Find state machine ARN
    machines = sfn_client.list_state_machines()
    sm_arn = None
    for sm in machines["stateMachines"]:
        if sm["name"] == STATE_MACHINE_NAME:
            sm_arn = sm["stateMachineArn"]
            break

    if not sm_arn:
        print(f"   ❌ State machine not found: {STATE_MACHINE_NAME}")
        return False

    # List recent executions
    executions = sfn_client.list_executions(
        stateMachineArn=sm_arn,
        maxResults=5
    )

    execs = executions.get("executions", [])
    print(f"   State Machine: {STATE_MACHINE_NAME}")
    print(f"   Recent executions: {len(execs)}")

    for ex in execs:
        status = ex["status"]
        start = ex["startDate"].strftime("%Y-%m-%d %H:%M:%S")
        icon = {"SUCCEEDED": "✅", "RUNNING": "🔄", "FAILED": "❌"}.get(status, "❓")
        duration = ""
        if "stopDate" in ex:
            dur = (ex["stopDate"] - ex["startDate"]).total_seconds()
            duration = f" ({dur:.0f}s)"
        print(f"     {icon} {status} — started {start}{duration}")

    return any(ex["status"] == "SUCCEEDED" for ex in execs)


def check_end_to_end_latency():
    """Calculate actual end-to-end latency from metrics"""
    print("\n🔍 END-TO-END LATENCY")
    print("-" * 50)

    cw = boto3.client("cloudwatch", region_name=REGION)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=1)

    metrics = cw.get_metric_statistics(
        Namespace="QuickCart/DataMesh/Clickstream",
        MetricName="ProcessingDurationSeconds",
        Dimensions=[{"Name": "Pipeline", "Value": "clickstream-microbatch"}],
        StartTime=start_time,
        EndTime=end_time,
        Period=300,
        Statistics=["Average", "Maximum"]
    )

    if metrics["Datapoints"]:
        avg_duration = sum(dp["Average"] for dp in metrics["Datapoints"]) / len(metrics["Datapoints"])
        max_duration = max(dp["Maximum"] for dp in metrics["Datapoints"])

        # Total latency = Firehose buffer (60s) + Glue processing
        total_avg = 60 + avg_duration  # Firehose buffer + processing
        total_max = 60 + max_duration

        print(f"   Firehose buffer:      60s (configured)")
        print(f"   Glue ETL (avg):       {avg_duration:.0f}s")
        print(f"   Glue ETL (max):       {max_duration:.0f}s")
        print(f"   Total latency (avg):  {total_avg:.0f}s ({total_avg/60:.1f} min)")
        print(f"   Total latency (max):  {total_max:.0f}s ({total_max/60:.1f} min)")
        print(f"   SLA (15 min):         {'✅ MET' if total_max < 900 else '❌ BREACHED'}")
    else:
        print("   No latency data yet — pipeline hasn't completed a cycle")


def run_full_verification():
    """Run all verification checks"""
    kinesis_client = boto3.client("kinesis", region_name=REGION)
    firehose_client = boto3.client("firehose", region_name=REGION)
    s3_client = boto3.client("s3", region_name=REGION)
    sfn_client = boto3.client("stepfunctions", region_name=REGION)

    print("=" * 70)
    print("🔬 END-TO-END PIPELINE VERIFICATION")
    print(f"   Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)

    results = {}
    results["kinesis"] = check_kinesis_stream(kinesis_client)
    results["firehose"] = check_firehose(firehose_client)
    results["s3_bronze"] = check_s3_bronze(s3_client)
    results["s3_gold"] = check_s3_gold(s3_client)
    results["step_functions"] = check_step_functions(sfn_client)
    check_end_to_end_latency()

    # Summary
    print("\n" + "=" * 70)
    print("📊 VERIFICATION SUMMARY")
    print("-" * 50)
    all_pass = True
    for component, status in results.items():
        icon = "✅" if status else "❌"
        print(f"   {icon} {component}")
        if not status:
            all_pass = False

    if all_pass:
        print("\n   🎉 ALL CHECKS PASSED — Pipeline is healthy!")
    else:
        print("\n   ⚠️  Some checks failed — review above for details")
    print("=" * 70)


if __name__ == "__main__":
    run_full_verification()