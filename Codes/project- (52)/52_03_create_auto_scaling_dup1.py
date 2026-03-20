# file: 52_03_create_auto_scaling.py
# Purpose: Create Launch Template + ASG + Step Scaling Policies

import boto3
import json
import base64
import time

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# --- CONFIGURATION ---
ASG_NAME = "quickcart-etl-workers-asg"
LAUNCH_TEMPLATE_NAME = "quickcart-etl-worker-lt"
INSTANCE_TYPE = "t3.medium"
MIN_SIZE = 1
MAX_SIZE = 20
DESIRED_CAPACITY = 1
INSTANCE_PROFILE_NAME = "QuickCart-SQS-Worker-Profile"

# From Project 51 infrastructure
QUEUE_NAME = "quickcart-etl-queue"


def get_latest_amazon_linux_ami():
    """Get latest Amazon Linux 2023 AMI ID"""
    ec2_client = boto3.client("ec2", region_name=REGION)

    response = ec2_client.describe_images(
        Owners=["amazon"],
        Filters=[
            {"Name": "name", "Values": ["al2023-ami-2023.*-x86_64"]},
            {"Name": "state", "Values": ["available"]},
            {"Name": "architecture", "Values": ["x86_64"]}
        ]
    )

    # Sort by creation date, get newest
    images = sorted(
        response["Images"],
        key=lambda x: x["CreationDate"],
        reverse=True
    )

    if not images:
        raise Exception("No Amazon Linux 2023 AMI found")

    ami_id = images[0]["ImageId"]
    print(f"✅ Latest AMI: {ami_id} ({images[0]['Name']})")
    return ami_id


def get_default_vpc_subnets():
    """Get subnet IDs from default VPC for ASG placement"""
    ec2_client = boto3.client("ec2", region_name=REGION)

    # Get default VPC
    vpcs = ec2_client.describe_vpcs(
        Filters=[{"Name": "is-default", "Values": ["true"]}]
    )
    if not vpcs["Vpcs"]:
        raise Exception("No default VPC found")

    vpc_id = vpcs["Vpcs"][0]["VpcId"]

    # Get subnets in default VPC
    subnets = ec2_client.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )
    subnet_ids = [s["SubnetId"] for s in subnets["Subnets"]]

    print(f"✅ VPC: {vpc_id} — Subnets: {subnet_ids}")
    return vpc_id, subnet_ids


def get_or_create_security_group(vpc_id):
    """Create security group for worker instances"""
    ec2_client = boto3.client("ec2", region_name=REGION)
    sg_name = "quickcart-sqs-worker-sg"

    try:
        response = ec2_client.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [sg_name]},
                {"Name": "vpc-id", "Values": [vpc_id]}
            ]
        )
        if response["SecurityGroups"]:
            sg_id = response["SecurityGroups"][0]["GroupId"]
            print(f"ℹ️  Security Group exists: {sg_id}")
            return sg_id
    except Exception:
        pass

    response = ec2_client.create_security_group(
        GroupName=sg_name,
        Description="SG for QuickCart SQS worker instances",
        VpcId=vpc_id,
        TagSpecifications=[{
            "ResourceType": "security-group",
            "Tags": [
                {"Key": "Name", "Value": sg_name},
                {"Key": "Team", "Value": "Data-Engineering"}
            ]
        }]
    )
    sg_id = response["GroupId"]

    # Allow outbound HTTPS (for SQS, S3, CloudWatch API calls)
    # Default SG already allows all outbound, but be explicit
    print(f"✅ Security Group: {sg_id}")
    return sg_id


def get_queue_url():
    """Get SQS queue URL for worker configuration"""
    sqs_client = boto3.client("sqs", region_name=REGION)
    response = sqs_client.get_queue_url(QueueName=QUEUE_NAME)
    return response["QueueUrl"]


def build_user_data(queue_url):
    """
    Build User Data script that runs on FIRST BOOT of every instance
    
    THIS IS THE BOOTSTRAP:
    → Installs Python dependencies
    → Downloads worker script from S3
    → Creates systemd service (survives reboot)
    → Starts worker automatically
    
    WHY systemd:
    → Worker restarts automatically if it crashes
    → Logs go to journald (queryable)
    → Proper signal handling (SIGTERM from ASG)
    """
    # Get instance ID for unique worker naming
    user_data_script = f"""#!/bin/bash
set -euxo pipefail

# --- LOG EVERYTHING ---
exec > /var/log/user-data.log 2>&1
echo "=== User Data script started at $(date) ==="

# --- INSTALL DEPENDENCIES ---
dnf update -y
dnf install -y python3 python3-pip
pip3 install boto3

# --- GET INSTANCE METADATA ---
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \\
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \\
    http://169.254.169.254/latest/meta-data/instance-id)

echo "Instance ID: $INSTANCE_ID"

# --- CREATE WORKER SCRIPT ---
# In production: download from S3 instead of inline
# aws s3 cp s3://quickcart-scripts/sqs_worker.py /opt/worker/sqs_worker.py
mkdir -p /opt/worker

cat > /opt/worker/sqs_worker.py << 'WORKER_EOF'
{open("52_02_sqs_worker.py").read() if os.path.exists("52_02_sqs_worker.py") else "# Worker script placeholder"}
WORKER_EOF

# --- CREATE SYSTEMD SERVICE ---
cat > /etc/systemd/system/sqs-worker.service << EOF
[Unit]
Description=QuickCart SQS Worker
After=network.target

[Service]
Type=simple
User=root
Environment=SQS_QUEUE_URL={queue_url}
Environment=AWS_REGION={REGION}
Environment=WORKER_ID=$INSTANCE_ID
ExecStart=/usr/bin/python3 /opt/worker/sqs_worker.py
Restart=always
RestartSec=10
KillSignal=SIGTERM
TimeoutStopSec=300

[Install]
WantedBy=multi-user.target
EOF

# --- START WORKER ---
systemctl daemon-reload
systemctl enable sqs-worker
systemctl start sqs-worker

echo "=== User Data script completed at $(date) ==="
echo "=== Worker service started ==="
"""
    return base64.b64encode(user_data_script.encode("utf-8")).decode("utf-8")


def create_launch_template(ami_id, sg_id, queue_url):
    """Create Launch Template — blueprint for every worker instance"""
    ec2_client = boto3.client("ec2", region_name=REGION)

    user_data_b64 = build_user_data(queue_url)

    try:
        response = ec2_client.create_launch_template(
            LaunchTemplateName=LAUNCH_TEMPLATE_NAME,
            VersionDescription="v1 - Initial ETL worker",
            LaunchTemplateData={
                "ImageId": ami_id,
                "InstanceType": INSTANCE_TYPE,
                "IamInstanceProfile": {
                    "Name": INSTANCE_PROFILE_NAME
                },
                "SecurityGroupIds": [sg_id],
                "UserData": user_data_b64,
                "Monitoring": {
                    "Enabled": True     # Detailed monitoring (1-min intervals)
                },
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": "QuickCart-ETL-Worker"},
                            {"Key": "Team", "Value": "Data-Engineering"},
                            {"Key": "AutoScaling", "Value": "true"},
                            {"Key": "QueueName", "Value": QUEUE_NAME}
                        ]
                    }
                ],
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/xvda",
                        "Ebs": {
                            "VolumeSize": 20,       # 20 GB root volume
                            "VolumeType": "gp3",
                            "Encrypted": True
                        }
                    }
                ]
            },
            TagSpecifications=[
                {
                    "ResourceType": "launch-template",
                    "Tags": [
                        {"Key": "Team", "Value": "Data-Engineering"},
                        {"Key": "Purpose", "Value": "SQS-ETL-Worker"}
                    ]
                }
            ]
        )
        lt_id = response["LaunchTemplate"]["LaunchTemplateId"]
        print(f"✅ Launch Template created: {lt_id} ({LAUNCH_TEMPLATE_NAME})")
        return lt_id

    except ec2_client.exceptions.ClientError as e:
        if "already exists" in str(e):
            # Get existing
            response = ec2_client.describe_launch_templates(
                LaunchTemplateNames=[LAUNCH_TEMPLATE_NAME]
            )
            lt_id = response["LaunchTemplates"][0]["LaunchTemplateId"]
            print(f"ℹ️  Launch Template already exists: {lt_id}")
            return lt_id
        raise


def create_auto_scaling_group(lt_id, subnet_ids):
    """
    Create Auto Scaling Group with:
    → Launch Template reference
    → Min/Max/Desired capacity
    → Multi-AZ placement
    → Health checks
    → Termination policies
    """
    asg_client = boto3.client("autoscaling", region_name=REGION)

    try:
        asg_client.create_auto_scaling_group(
            AutoScalingGroupName=ASG_NAME,
            LaunchTemplate={
                "LaunchTemplateId": lt_id,
                "Version": "$Latest"    # Always use latest template version
            },
            MinSize=MIN_SIZE,
            MaxSize=MAX_SIZE,
            DesiredCapacity=DESIRED_CAPACITY,
            VPCZoneIdentifier=",".join(subnet_ids),
            HealthCheckType="EC2",
            HealthCheckGracePeriod=300,     # 5 min for User Data to complete
            TerminationPolicies=[
                "OldestInstance",            # Terminate oldest first during scale-in
                "Default"
            ],
            NewInstancesProtectedFromScaleIn=False,
            Tags=[
                {
                    "Key": "Name",
                    "Value": "QuickCart-ETL-Worker",
                    "PropagateAtLaunch": True
                },
                {
                    "Key": "Environment",
                    "Value": "Production",
                    "PropagateAtLaunch": True
                },
                {
                    "Key": "Team",
                    "Value": "Data-Engineering",
                    "PropagateAtLaunch": True
                },
                {
                    "Key": "QueueName",
                    "Value": QUEUE_NAME,
                    "PropagateAtLaunch": True
                }
            ]
        )
        print(f"✅ Auto Scaling Group created: {ASG_NAME}")
        print(f"   Min: {MIN_SIZE} | Max: {MAX_SIZE} | Desired: {DESIRED_CAPACITY}")
        print(f"   Subnets: {subnet_ids}")

    except asg_client.exceptions.AlreadyExistsFault:
        print(f"ℹ️  ASG already exists: {ASG_NAME}")

    return ASG_NAME


def create_step_scaling_policies(asg_name):
    """
    Create STEP SCALING policies:
    
    SCALE OUT (add workers when queue grows):
    → 1,000 - 5,000 msgs    → add 2 instances
    → 5,000 - 20,000 msgs   → add 5 instances
    → 20,000+ msgs           → add 10 instances
    
    SCALE IN (remove workers when queue empties):
    → < 100 msgs             → remove 2 instances
    → 0 msgs                 → remove 4 instances (back toward minimum)
    
    WHY STEP SCALING over Simple Scaling:
    → Proportional response: small spike = small scale, big spike = big scale
    → No cooldown between steps: if queue keeps growing, keeps scaling
    → Evaluates continuously: adjusts as conditions change
    """
    asg_client = boto3.client("autoscaling", region_name=REGION)

    # ═══════════════════════════════════════
    # SCALE-OUT POLICY (add instances)
    # ═══════════════════════════════════════
    scale_out_response = asg_client.put_scaling_policy(
        AutoScalingGroupName=asg_name,
        PolicyName="ETL-Queue-ScaleOut-Steps",
        PolicyType="StepScaling",
        AdjustmentType="ChangeInCapacity",    # Add/remove N instances
        MetricAggregationType="Average",
        StepAdjustments=[
            {
                # 1,000 <= queue < 5,000 → add 2
                "MetricIntervalLowerBound": 0,      # alarm threshold + 0 = 1000
                "MetricIntervalUpperBound": 4000,    # alarm threshold + 4000 = 5000
                "ScalingAdjustment": 2
            },
            {
                # 5,000 <= queue < 20,000 → add 5
                "MetricIntervalLowerBound": 4000,
                "MetricIntervalUpperBound": 19000,
                "ScalingAdjustment": 5
            },
            {
                # 20,000+ → add 10
                "MetricIntervalLowerBound": 19000,
                # No upper bound = infinity
                "ScalingAdjustment": 10
            }
        ],
        # Estimated warm-up time for new instances
        # ASG won't count new instance in metrics until warm-up completes
        EstimatedInstanceWarmup=180     # 3 minutes for User Data to finish
    )
    scale_out_arn = scale_out_response["PolicyARN"]
    print(f"✅ Scale-Out Policy created")
    print(f"   1K-5K msgs → +2 | 5K-20K → +5 | 20K+ → +10")
    print(f"   ARN: {scale_out_arn}")

    # ═══════════════════════════════════════
    # SCALE-IN POLICY (remove instances)
    # ═══════════════════════════════════════
    scale_in_response = asg_client.put_scaling_policy(
        AutoScalingGroupName=asg_name,
        PolicyName="ETL-Queue-ScaleIn-Steps",
        PolicyType="StepScaling",
        AdjustmentType="ChangeInCapacity",
        MetricAggregationType="Average",
        StepAdjustments=[
            {
                # queue < 100 (alarm threshold) → remove 2
                # Bounds are NEGATIVE because alarm is "below threshold"
                "MetricIntervalUpperBound": 0,       # alarm threshold - 0 = 100
                "MetricIntervalLowerBound": -100,    # alarm threshold - 100 = 0
                "ScalingAdjustment": -2
            },
            {
                # queue = 0 → remove 4 (aggressive scale-in)
                "MetricIntervalUpperBound": -100,    # below 0 (effectively 0)
                "ScalingAdjustment": -4
            }
        ]
    )
    scale_in_arn = scale_in_response["PolicyARN"]
    print(f"✅ Scale-In Policy created")
    print(f"   <100 msgs → -2 | 0 msgs → -4")
    print(f"   ARN: {scale_in_arn}")

    return scale_out_arn, scale_in_arn


def create_cloudwatch_alarms(scale_out_arn, scale_in_arn):
    """
    Create CloudWatch Alarms that TRIGGER scaling policies
    
    THE CHAIN:
    SQS metric → CloudWatch Alarm → ASG Scaling Policy → Launch/Terminate EC2
    
    CRITICAL SETTINGS:
    → EvaluationPeriods: how many consecutive periods before alarm fires
    → Period: metric evaluation window (seconds)
    → Scale-out: 1 period of 60s (react fast to spikes)
    → Scale-in: 5 periods of 180s = 15 min sustained low (avoid thrashing)
    """
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    # ═══════════════════════════════════════
    # HIGH ALARM → triggers scale OUT
    # ═══════════════════════════════════════
    cw_client.put_metric_alarm(
        AlarmName=f"{ASG_NAME}-Queue-High",
        AlarmDescription=(
            "ETL Queue depth exceeds 1,000 messages. "
            "Trigger scale-out to add worker instances."
        ),
        Namespace="AWS/SQS",
        MetricName="ApproximateNumberOfMessagesVisible",
        Dimensions=[
            {"Name": "QueueName", "Value": QUEUE_NAME}
        ],
        Statistic="Average",
        Period=60,                      # Check every 1 minute
        EvaluationPeriods=1,            # Fire after 1 breach (fast response)
        DatapointsToAlarm=1,            # 1 out of 1 must breach
        Threshold=1000,                 # 1,000 messages
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[scale_out_arn],   # Trigger scale-out policy
        Tags=[
            {"Key": "Team", "Value": "Data-Engineering"},
            {"Key": "Purpose", "Value": "AutoScaling-ScaleOut"}
        ]
    )
    print(f"\n✅ CloudWatch Alarm: {ASG_NAME}-Queue-High")
    print(f"   Trigger: queue >= 1,000 msgs (1 × 60s evaluation)")
    print(f"   Action: scale OUT")

    # ═══════════════════════════════════════
    # LOW ALARM → triggers scale IN
    # ═══════════════════════════════════════
    cw_client.put_metric_alarm(
        AlarmName=f"{ASG_NAME}-Queue-Low",
        AlarmDescription=(
            "ETL Queue depth below 100 messages for 15 minutes. "
            "Trigger scale-in to remove idle worker instances."
        ),
        Namespace="AWS/SQS",
        MetricName="ApproximateNumberOfMessagesVisible",
        Dimensions=[
            {"Name": "QueueName", "Value": QUEUE_NAME}
        ],
        Statistic="Average",
        Period=180,                     # Check every 3 minutes
        EvaluationPeriods=5,            # Must be low for 5 × 3 min = 15 min
        DatapointsToAlarm=5,            # All 5 must breach (conservative)
        Threshold=100,                  # 100 messages
        ComparisonOperator="LessThanThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[scale_in_arn],    # Trigger scale-in policy
        Tags=[
            {"Key": "Team", "Value": "Data-Engineering"},
            {"Key": "Purpose", "Value": "AutoScaling-ScaleIn"}
        ]
    )
    print(f"\n✅ CloudWatch Alarm: {ASG_NAME}-Queue-Low")
    print(f"   Trigger: queue < 100 msgs (5 × 180s = 15 min sustained)")
    print(f"   Action: scale IN")


def enable_asg_notifications():
    """
    Get notified via SNS when ASG launches/terminates instances
    
    Reuses SNS from Project 12
    Useful for:
    → Debugging scaling behavior
    → Cost tracking (know when instances start/stop)
    → Audit trail
    """
    asg_client = boto3.client("autoscaling", region_name=REGION)
    sns_client = boto3.client("sns", region_name=REGION)

    # Create notification topic
    topic_response = sns_client.create_topic(
        Name="quickcart-asg-notifications"
    )
    notification_topic_arn = topic_response["TopicArn"]

    # Enable ASG notifications
    asg_client.put_notification_configuration(
        AutoScalingGroupName=ASG_NAME,
        TopicARN=notification_topic_arn,
        NotificationTypes=[
            "autoscaling:EC2_INSTANCE_LAUNCH",
            "autoscaling:EC2_INSTANCE_LAUNCH_ERROR",
            "autoscaling:EC2_INSTANCE_TERMINATE",
            "autoscaling:EC2_INSTANCE_TERMINATE_ERROR"
        ]
    )

    print(f"\n✅ ASG Notifications enabled")
    print(f"   Topic: {notification_topic_arn}")
    print(f"   Events: launch, launch_error, terminate, terminate_error")
    print(f"   → Subscribe your email to this topic for alerts")

    return notification_topic_arn


def setup_complete_auto_scaling():
    """
    Master orchestrator: create all auto-scaling components
    """
    print("=" * 70)
    print("🚀 SETTING UP SQS-BASED AUTO SCALING")
    print("=" * 70)

    # Step 1: Get prerequisites
    print("\n📌 Step 1: Getting prerequisites...")
    ami_id = get_latest_amazon_linux_ami()
    vpc_id, subnet_ids = get_default_vpc_subnets()
    sg_id = get_or_create_security_group(vpc_id)
    queue_url = get_queue_url()
    print(f"✅ Queue URL: {queue_url}")

    # Step 2: Create Launch Template
    print("\n📌 Step 2: Creating Launch Template...")
    lt_id = create_launch_template(ami_id, sg_id, queue_url)

    # Step 3: Create Auto Scaling Group
    print("\n📌 Step 3: Creating Auto Scaling Group...")
    asg_name = create_auto_scaling_group(lt_id, subnet_ids)

    # Step 4: Create Scaling Policies
    print("\n📌 Step 4: Creating Step Scaling Policies...")
    scale_out_arn, scale_in_arn = create_step_scaling_policies(asg_name)

    # Step 5: Create CloudWatch Alarms
    print("\n📌 Step 5: Creating CloudWatch Alarms...")
    create_cloudwatch_alarms(scale_out_arn, scale_in_arn)

    # Step 6: Enable ASG Notifications
    print("\n📌 Step 6: Enabling ASG Notifications...")
    notification_topic_arn = enable_asg_notifications()

    # --- SUMMARY ---
    print("\n" + "=" * 70)
    print("📊 AUTO SCALING SETUP COMPLETE")
    print("=" * 70)
    print(f"  Launch Template:     {LAUNCH_TEMPLATE_NAME} ({lt_id})")
    print(f"  ASG:                 {ASG_NAME}")
    print(f"  Min/Max/Desired:     {MIN_SIZE}/{MAX_SIZE}/{DESIRED_CAPACITY}")
    print(f"  Instance Type:       {INSTANCE_TYPE}")
    print(f"  Queue:               {QUEUE_NAME}")
    print(f"  Scale-Out Trigger:   >= 1,000 messages")
    print(f"  Scale-In Trigger:    < 100 messages for 15 min")
    print(f"  Notification Topic:  quickcart-asg-notifications")
    print("=" * 70)

    # Save config for other scripts
    config = {
        "asg_name": asg_name,
        "launch_template_id": lt_id,
        "scale_out_arn": scale_out_arn,
        "scale_in_arn": scale_in_arn,
        "queue_name": QUEUE_NAME,
        "queue_url": queue_url,
        "notification_topic_arn": notification_topic_arn
    }
    with open("/tmp/asg_config.json", "w") as f:
        json.dump(config, f, indent=2)
    print(f"\n💾 Config saved to /tmp/asg_config.json")


if __name__ == "__main__":
    setup_complete_auto_scaling()