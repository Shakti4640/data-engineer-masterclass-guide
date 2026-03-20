# file: 52_07_lifecycle_hook.py
# Purpose: Add lifecycle hook so workers finish processing before termination

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/asg_config.json", "r") as f:
        return json.load(f)


def create_termination_lifecycle_hook():
    """
    Lifecycle Hook: pause instance termination to allow graceful shutdown
    
    WITHOUT hook:
    → ASG decides to terminate → instance killed immediately
    → Worker mid-processing a message → message lost until visibility timeout
    → If processing takes 5 min and timeout is 5 min → barely safe
    
    WITH hook:
    → ASG decides to terminate → sends SIGTERM → enters "Terminating:Wait"
    → Worker receives SIGTERM → finishes current batch → calls CompleteLifecycleAction
    → THEN instance terminates
    → Guaranteed no mid-processing termination
    
    TIMEOUT:
    → If worker doesn't call CompleteLifecycleAction within timeout
    → ASG proceeds with termination anyway (prevents stuck instances)
    → Default: 3600 seconds (1 hour) — we use 300 (5 minutes)
    """
    config = load_config()
    asg_client = boto3.client("autoscaling", region_name=REGION)

    asg_client.put_lifecycle_hook(
        LifecycleHookName="ETL-Worker-Graceful-Shutdown",
        AutoScalingGroupName=config["asg_name"],
        LifecycleTransition="autoscaling:EC2_INSTANCE_TERMINATING",
        HeartbeatTimeout=300,           # 5 minutes to finish work
        DefaultResult="CONTINUE",       # If timeout: proceed with termination
        NotificationTargetARN=config.get("notification_topic_arn", ""),
        RoleARN=""  # SNS notification role (optional)
    )

    print("=" * 70)
    print("🔄 LIFECYCLE HOOK CREATED")
    print("=" * 70)
    print(f"  Hook Name:     ETL-Worker-Graceful-Shutdown")
    print(f"  Transition:    EC2_INSTANCE_TERMINATING")
    print(f"  Timeout:       300 seconds (5 minutes)")
    print(f"  Default:       CONTINUE (terminate even if hook not completed)")
    print(f"")
    print(f"  FLOW:")
    print(f"  1. ASG decides to terminate instance")
    print(f"  2. Instance enters 'Terminating:Wait' state")
    print(f"  3. Worker receives SIGTERM (handled in 52_02)")
    print(f"  4. Worker finishes current batch")
    print(f"  5. Worker calls complete_lifecycle_action()")
    print(f"  6. Instance enters 'Terminating:Proceed' → terminated")
    print("=" * 70)


def complete_lifecycle_action_example():
    """
    This would be called by the worker script when it finishes cleanup
    
    In production: add this to the signal handler in 52_02_sqs_worker.py
    """
    config = load_config()
    asg_client = boto3.client("autoscaling", region_name=REGION)

    # In real code: INSTANCE_ID comes from instance metadata
    # TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" ...)
    # INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" ...)
    instance_id = "i-0example1234567890"  # Placeholder

    print("\n📌 Example: Completing lifecycle action")
    print(f"   (Would be called by worker on instance {instance_id})")

    # This is what the worker would call:
    """
    asg_client.complete_lifecycle_action(
        LifecycleHookName="ETL-Worker-Graceful-Shutdown",
        AutoScalingGroupName=config["asg_name"],
        LifecycleActionResult="CONTINUE",   # Proceed with termination
        InstanceId=instance_id
    )
    """

    print("   Worker would call: complete_lifecycle_action()")
    print("   → Instance proceeds to termination")
    print("   → All messages properly processed or returned to queue")


if __name__ == "__main__":
    create_termination_lifecycle_hook()
    complete_lifecycle_action_example()