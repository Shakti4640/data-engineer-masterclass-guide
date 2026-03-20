# file: 52_08_cleanup.py
# Purpose: Tear down all auto-scaling infrastructure

import boto3
import json
import time

REGION = "us-east-2"
ASG_NAME = "quickcart-etl-workers-asg"
LAUNCH_TEMPLATE_NAME = "quickcart-etl-worker-lt"
ROLE_NAME = "QuickCart-SQS-Worker-Role"
INSTANCE_PROFILE_NAME = "QuickCart-SQS-Worker-Profile"


def cleanup_all():
    """Remove ASG, Launch Template, IAM resources, CloudWatch alarms"""
    asg_client = boto3.client("autoscaling", region_name=REGION)
    ec2_client = boto3.client("ec2", region_name=REGION)
    cw_client = boto3.client("cloudwatch", region_name=REGION)
    iam_client = boto3.client("iam")

    print("=" * 70)
    print("🧹 CLEANING UP PROJECT 52 INFRASTRUCTURE")
    print("=" * 70)

    # --- Step 1: Set ASG to 0 instances ---
    print("\n📌 Step 1: Scaling ASG to 0...")
    try:
        asg_client.update_auto_scaling_group(
            AutoScalingGroupName=ASG_NAME,
            MinSize=0,
            MaxSize=0,
            DesiredCapacity=0
        )
        print(f"  ✅ ASG scaled to 0 — waiting for instances to terminate...")
        time.sleep(30)  # Wait for terminations to start
    except Exception as e:
        print(f"  ⚠️  Could not update ASG: {e}")

    # --- Step 2: Delete lifecycle hooks ---
    print("\n📌 Step 2: Removing lifecycle hooks...")
    try:
        asg_client.delete_lifecycle_hook(
            LifecycleHookName="ETL-Worker-Graceful-Shutdown",
            AutoScalingGroupName=ASG_NAME
        )
        print(f"  ✅ Lifecycle hook deleted")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 3: Delete ASG ---
    print("\n📌 Step 3: Deleting Auto Scaling Group...")
    try:
        asg_client.delete_auto_scaling_group(
            AutoScalingGroupName=ASG_NAME,
            ForceDelete=True    # Terminate remaining instances
        )
        print(f"  ✅ ASG deleted: {ASG_NAME}")
        print(f"  ⏳ Waiting 60 seconds for instances to fully terminate...")
        time.sleep(60)
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 4: Delete Launch Template ---
    print("\n📌 Step 4: Deleting Launch Template...")
    try:
        ec2_client.delete_launch_template(
            LaunchTemplateName=LAUNCH_TEMPLATE_NAME
        )
        print(f"  ✅ Launch Template deleted: {LAUNCH_TEMPLATE_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 5: Delete CloudWatch Alarms ---
    print("\n📌 Step 5: Deleting CloudWatch Alarms...")
    alarm_names = [
        f"{ASG_NAME}-Queue-High",
        f"{ASG_NAME}-Queue-Low"
    ]
    try:
        cw_client.delete_alarms(AlarmNames=alarm_names)
        for name in alarm_names:
            print(f"  ✅ Alarm deleted: {name}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 6: Delete ASG notification topic ---
    print("\n📌 Step 6: Deleting notification topic...")
    sns_client = boto3.client("sns", region_name=REGION)
    try:
        topics = sns_client.list_topics()
        for topic in topics.get("Topics", []):
            if "quickcart-asg-notifications" in topic["TopicArn"]:
                sns_client.delete_topic(TopicArn=topic["TopicArn"])
                print(f"  ✅ Topic deleted: quickcart-asg-notifications")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 7: Delete IAM resources ---
    print("\n📌 Step 7: Deleting IAM resources...")
    try:
        iam_client.remove_role_from_instance_profile(
            InstanceProfileName=INSTANCE_PROFILE_NAME,
            RoleName=ROLE_NAME
        )
        iam_client.delete_instance_profile(
            InstanceProfileName=INSTANCE_PROFILE_NAME
        )
        print(f"  ✅ Instance profile deleted: {INSTANCE_PROFILE_NAME}")

        iam_client.delete_role_policy(
            RoleName=ROLE_NAME,
            PolicyName="SQSWorkerPermissions"
        )
        iam_client.delete_role(RoleName=ROLE_NAME)
        print(f"  ✅ IAM role deleted: {ROLE_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 8: Delete Security Group ---
    print("\n📌 Step 8: Deleting security group...")
    try:
        sgs = ec2_client.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": ["quickcart-sqs-worker-sg"]}]
        )
        for sg in sgs["SecurityGroups"]:
            ec2_client.delete_security_group(GroupId=sg["GroupId"])
            print(f"  ✅ Security group deleted: {sg['GroupId']}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    print("\n" + "=" * 70)
    print("🎉 ALL PROJECT 52 RESOURCES CLEANED UP")
    print("=" * 70)


if __name__ == "__main__":
    cleanup_all()