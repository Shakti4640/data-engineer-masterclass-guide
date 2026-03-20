# file: 62_06_deploy_monitoring.py
# Run from: Account B (222222222222)
# Purpose: Deploy the small file monitoring Lambda + CloudWatch rule

import boto3
import json
import zipfile
import io

REGION = "us-east-2"
ACCOUNT_B_ID[REDACTED:BANK_ACCOUNT_NUMBER]2222"
LAMBDA_NAME = "small-file-monitor"
LAMBDA_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_B_ID}:role/LambdaSmallFileMonitorRole"


def create_lambda_role():
    """
    IAM Role for the monitoring Lambda
    
    NEEDS:
    → S3: ListBucket (to count files)
    → CloudWatch: PutMetricData (to publish metrics)
    → SNS: Publish (to send alerts)
    → Glue: StartJobRun (to trigger compaction)
    → CloudWatch Logs: write Lambda logs
    """
    iam = boto3.client("iam")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3List",
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": ["arn:aws:s3:::quickcart-analytics-datalake"]
            },
            {
                "Sid": "CloudWatchMetrics",
                "Effect": "Allow",
                "Action": ["cloudwatch:PutMetricData"],
                "Resource": ["*"]
            },
            {
                "Sid": "SNSPublish",
                "Effect": "Allow",
                "Action": ["sns:Publish"],
                "Resource": [
                    f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts"
                ]
            },
            {
                "Sid": "GlueTriggerCompaction",
                "Effect": "Allow",
                "Action": ["glue:StartJobRun"],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{ACCOUNT_B_ID}:job/s3-small-file-compaction"
                ]
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }

    try:
        iam.create_role(
            RoleName="LambdaSmallFileMonitorRole",
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for small file monitoring Lambda"
        )
        print("✅ IAM Role created")
    except iam.exceptions.EntityAlreadyExistsException:
        print("ℹ️  Role already exists")

    iam.put_role_policy(
        RoleName="LambdaSmallFileMonitorRole",
        PolicyName="SmallFileMonitorPolicy",
        PolicyDocument=json.dumps(policy)
    )
    print("✅ Policy attached")


def create_lambda_function():
    """
    Create Lambda function from the monitoring script
    """
    lam = boto3.client("lambda", region_name=REGION)

    # Package the Lambda code
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("62_05_small_file_monitor.py", "lambda_function.py")
    zip_buffer.seek(0)

    try:
        lam.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Role=LAMBDA_ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_buffer.read()},
            Description="Monitors S3 data lake for small file problems",
            Timeout=300,  # 5 minutes
            MemorySize=256,
            Tags={
                "Environment": "Production",
                "Purpose": "SmallFileMonitoring",
                "ManagedBy": "DataEngineering"
            }
        )
        print(f"✅ Lambda created: {LAMBDA_NAME}")
    except lam.exceptions.ResourceConflictException:
        print(f"ℹ️  Lambda already exists — updating code")
        zip_buffer.seek(0)
        lam.update_function_code(
            FunctionName=LAMBDA_NAME,
            ZipFile=zip_buffer.read()
        )
        print(f"✅ Lambda code updated")


def create_daily_trigger():
    """
    CloudWatch Events rule to run monitor daily at 6 AM UTC
    → After all nightly ETL jobs have completed (2-5 AM window)
    """
    events = boto3.client("events", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)

    # Create rule
    rule_name = "daily-small-file-check"
    events.put_rule(
        Name=rule_name,
        ScheduleExpression="cron(0 6 * * ? *)",  # Daily at 6 AM UTC
        State="ENABLED",
        Description="Daily check for small files in data lake"
    )
    print(f"✅ EventBridge rule created: {rule_name}")

    # Add Lambda as target
    lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_B_ID}:function:{LAMBDA_NAME}"

    events.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "small-file-monitor-target",
                "Arn": lambda_arn
            }
        ]
    )
    print(f"✅ Lambda target added to rule")

    # Grant EventBridge permission to invoke Lambda
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId="AllowEventBridgeInvoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_B_ID}:rule/{rule_name}"
        )
        print(f"✅ Lambda invoke permission granted to EventBridge")
    except lam.exceptions.ResourceConflictException:
        print(f"ℹ️  Permission already exists")


def create_cloudwatch_alarm():
    """
    CloudWatch alarm that fires when average file size drops below threshold
    
    ALARM CHAIN:
    → Custom metric: QuickCart/DataLake → AvgFileSizeMB
    → Threshold: < 10 MB for 2 consecutive datapoints
    → Action: SNS notification
    
    WHY ALARM + LAMBDA:
    → Lambda: detailed analysis + auto-compaction trigger
    → Alarm: simple threshold alert — catches issues between Lambda runs
    """
    cw = boto3.client("cloudwatch", region_name=REGION)

    tables = ["silver_orders", "silver_customers", "silver_products"]

    for table in tables:
        alarm_name = f"small-files-{table}"

        cw.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmDescription=f"Average file size for {table} dropped below 10 MB",
            Namespace="QuickCart/DataLake",
            MetricName="AvgFileSizeMB",
            Dimensions=[
                {"Name": "TableName", "Value": table}
            ],
            Statistic="Average",
            Period=86400,  # 24 hours
            EvaluationPeriods=2,
            Threshold=10.0,
            ComparisonOperator="LessThanThreshold",
            AlarmActions=[
                f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts"
            ],
            TreatMissingData="notBreaching",
            Tags=[
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print(f"✅ Alarm created: {alarm_name}")


if __name__ == "__main__":
    print("=" * 60)
    print("📡 DEPLOYING SMALL FILE MONITORING")
    print("=" * 60)

    create_lambda_role()
    print()
    create_lambda_function()
    print()
    create_daily_trigger()
    print()
    create_cloudwatch_alarm()

    print("\n" + "=" * 60)
    print("✅ MONITORING STACK DEPLOYED")
    print("   Lambda: daily at 6 AM UTC")
    print("   Alarms: per-table threshold monitoring")
    print("   Auto-compaction: triggered when thresholds exceeded")
    print("=" * 60)