# file: 67_01_pause_resume_schedule.py
# Run from: Account B (222222222222)
# Purpose: Automated pause/resume to cut provisioned cluster costs by 58%

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
CLUSTER_ID = "quickcart-analytics-cluster"


def create_pause_resume_lambda():
    """
    Lambda that pauses or resumes the Redshift cluster
    Invoked by EventBridge scheduled rules
    """
    lambda_code = '''
import boto3
import time

redshift = boto3.client("redshift")
CLUSTER_ID = "quickcart-analytics-cluster"

def lambda_handler(event, context):
    action = event.get("action", "status")
    
    # Get current status
    desc = redshift.describe_clusters(ClusterIdentifier=CLUSTER_ID)
    current_status = desc["Clusters"][0]["ClusterStatus"]
    print(f"Current status: {current_status}")
    
    if action == "pause":
        if current_status == "available":
            redshift.pause_cluster(ClusterIdentifier=CLUSTER_ID)
            print(f"✅ Pause initiated for {CLUSTER_ID}")
            return {"status": "pausing", "previous": current_status}
        else:
            print(f"ℹ️ Cannot pause — current status: {current_status}")
            return {"status": current_status, "action": "skipped"}
    
    elif action == "resume":
        if current_status == "paused":
            redshift.resume_cluster(ClusterIdentifier=CLUSTER_ID)
            print(f"✅ Resume initiated for {CLUSTER_ID}")
            
            # Wait for cluster to be available (max 5 minutes)
            max_wait = 300
            elapsed = 0
            while elapsed < max_wait:
                time.sleep(15)
                elapsed += 15
                desc = redshift.describe_clusters(ClusterIdentifier=CLUSTER_ID)
                status = desc["Clusters"][0]["ClusterStatus"]
                print(f"Status: {status} ({elapsed}s)")
                if status == "available":
                    print(f"✅ Cluster available after {elapsed}s")
                    return {"status": "available", "resume_time_s": elapsed}
            
            print(f"⚠️ Cluster still resuming after {max_wait}s")
            return {"status": "resuming", "waited": max_wait}
        else:
            print(f"ℹ️ Cannot resume — current status: {current_status}")
            return {"status": current_status, "action": "skipped"}
    
    elif action == "status":
        return {"status": current_status}
    
    else:
        return {"error": f"Unknown action: {action}"}
'''

    lam = boto3.client("lambda", region_name=REGION)
    iam = boto3.client("iam")

    # IAM Role
    role_name = "LambdaRedshiftPauseResumeRole"
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]
    }
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["redshift:PauseCluster", "redshift:ResumeCluster", "redshift:DescribeClusters"],
                "Resource": f"arn:aws:redshift:{REGION}:{ACCOUNT_B_ID}:cluster:{CLUSTER_ID}"
            },
            {
                "Effect": "Allow",
                "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }

    try:
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust))
    except iam.exceptions.EntityAlreadyExistsException:
        pass
    iam.put_role_policy(RoleName=role_name, PolicyName="RedshiftPauseResume", PolicyDocument=json.dumps(policy))

    role_arn = f"arn:aws:iam::{ACCOUNT_B_ID}:role/{role_name}"
    time.sleep(10)

    import zipfile, io
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        zf.writestr("lambda_function.py", lambda_code)
    zip_buf.seek(0)

    try:
        lam.create_function(
            FunctionName="redshift-pause-resume",
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_buf.read()},
            Timeout=360,
            MemorySize=128,
            Description="Pause/Resume Redshift cluster on schedule"
        )
        print("✅ Lambda created: redshift-pause-resume")
    except lam.exceptions.ResourceConflictException:
        zip_buf.seek(0)
        lam.update_function_code(FunctionName="redshift-pause-resume", ZipFile=zip_buf.read())
        print("✅ Lambda updated: redshift-pause-resume")


def create_schedules():
    """
    EventBridge rules for pause/resume schedule
    
    WEEKDAY SCHEDULE:
    → Resume: 1:30 AM (30 min before ETL at 2:00 AM)
    → Pause: 6:00 PM (after business hours)
    
    WEEKEND SCHEDULE:
    → Pause: Friday 6:00 PM
    → Resume: Monday 1:30 AM
    → Saturday/Sunday: fully paused ($0)
    """
    events = boto3.client("events", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_B_ID}:function:redshift-pause-resume"

    schedules = [
        {
            "name": "redshift-resume-weekday",
            "description": "Resume Redshift at 1:30 AM Mon-Fri (before ETL)",
            "schedule": "cron(30 1 ? * MON-FRI *)",
            "input": json.dumps({"action": "resume"})
        },
        {
            "name": "redshift-pause-weekday",
            "description": "Pause Redshift at 6:00 PM Mon-Fri (after business)",
            "schedule": "cron(0 18 ? * MON-FRI *)",
            "input": json.dumps({"action": "pause"})
        },
        {
            "name": "redshift-pause-friday",
            "description": "Pause Redshift Friday 6 PM (weekend off)",
            "schedule": "cron(0 18 ? * FRI *)",
            "input": json.dumps({"action": "pause"})
        }
    ]

    for sched in schedules:
        events.put_rule(
            Name=sched["name"],
            Description=sched["description"],
            ScheduleExpression=sched["schedule"],
            State="ENABLED"
        )

        events.put_targets(
            Rule=sched["name"],
            Targets=[{
                "Id": f"target-{sched['name']}",
                "Arn": lambda_arn,
                "Input": sched["input"]
            }]
        )

        # Grant EventBridge permission to invoke Lambda
        try:
            lam.add_permission(
                FunctionName="redshift-pause-resume",
                StatementId=f"allow-{sched['name']}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_B_ID}:rule/{sched['name']}"
            )
        except lam.exceptions.ResourceConflictException:
            pass

        print(f"✅ Schedule: {sched['name']} ({sched['schedule']})")


def calculate_savings():
    """
    Show projected savings from pause/resume
    """
    print(f"\n{'='*60}")
    print("💰 PROJECTED SAVINGS FROM PAUSE/RESUME")
    print(f"{'='*60}")

    node_cost_hr = 0.25  # dc2.large per hour
    nodes = 2
    hourly_cost = node_cost_hr * nodes  # $0.50/hr

    # Current: 24/7
    current_monthly = hourly_cost * 730
    print(f"   Current (24/7): ${current_monthly:.2f}/month")

    # With pause/resume
    weekday_hours = 16.5  # 1:30 AM to 6:00 PM
    weekday_days = 22
    weekend_hours = 0  # Fully paused
    weekend_days = 8

    active_hours = (weekday_hours * weekday_days) + (weekend_hours * weekend_days)
    new_monthly = hourly_cost * active_hours
    savings = current_monthly - new_monthly
    savings_pct = (savings / current_monthly) * 100

    print(f"   With Pause/Resume: ${new_monthly:.2f}/month")
    print(f"   Active hours/month: {active_hours:.0f} (vs 730)")
    print(f"   💰 Monthly savings: ${savings:.2f} ({savings_pct:.0f}%)")
    print(f"   💰 Annual savings: ${savings * 12:.2f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    print("=" * 60)
    print("⏸️  DEPLOYING REDSHIFT PAUSE/RESUME AUTOMATION")
    print("=" * 60)

    create_pause_resume_lambda()
    print()
    create_schedules()
    calculate_savings()