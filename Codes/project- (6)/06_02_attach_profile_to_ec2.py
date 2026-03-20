# file: 06_02_attach_profile_to_ec2.py
# Purpose: Associate Instance Profile with an existing EC2 instance

import boto3
import time
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
INSTANCE_ID = "i-0abc123def4567890"     # Replace with your EC2 instance ID
INSTANCE_PROFILE_NAME = "QuickCart-EC2-S3ReadWrite-Prod-Profile"
REGION = "us-east-2"


def remove_existing_profile(ec2_client, instance_id):
    """
    EC2 can only have ONE Instance Profile at a time
    Must remove existing one before attaching new one
    """
    try:
        response = ec2_client.describe_iam_instance_profile_associations(
            Filters=[
                {
                    "Name": "instance-id",
                    "Values": [instance_id]
                },
                {
                    "Name": "state",
                    "Values": ["associated"]
                }
            ]
        )

        associations = response.get("IamInstanceProfileAssociations", [])

        if associations:
            assoc_id = associations[0]["AssociationId"]
            old_profile = associations[0]["IamInstanceProfile"]["Arn"]
            print(f"⚠️  Existing profile found: {old_profile}")
            print(f"   Removing association: {assoc_id}")

            ec2_client.disassociate_iam_instance_profile(
                AssociationId=assoc_id
            )
            print(f"   ✅ Old profile removed")
            time.sleep(5)  # Wait for disassociation to propagate
        else:
            print(f"ℹ️  No existing Instance Profile on {instance_id}")

    except ClientError as e:
        print(f"   Warning during profile check: {e}")


def attach_instance_profile(instance_id, profile_name, region):
    ec2_client = boto3.client("ec2", region_name=region)

    # --- Remove existing profile if any ---
    remove_existing_profile(ec2_client, instance_id)

    # --- Attach new Instance Profile ---
    print(f"\n📎 Attaching '{profile_name}' to instance {instance_id}")

    try:
        ec2_client.associate_iam_instance_profile(
            IamInstanceProfile={
                "Name": profile_name
                # Can also use "Arn": "arn:aws:iam::123456:instance-profile/..."
            },
            InstanceId=instance_id
        )
        print(f"✅ Instance Profile attached successfully")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "IncorrectInstanceState":
            print(f"❌ Instance must be in 'running' or 'stopped' state")
        elif "already has an instance profile" in str(e):
            print(f"❌ Instance already has a profile — remove it first")
        else:
            raise

    # --- Verify attachment ---
    print(f"\n🔍 Verifying...")
    time.sleep(3)

    response = ec2_client.describe_iam_instance_profile_associations(
        Filters=[
            {"Name": "instance-id", "Values": [instance_id]},
            {"Name": "state", "Values": ["associated", "associating"]}
        ]
    )

    if response["IamInstanceProfileAssociations"]:
        assoc = response["IamInstanceProfileAssociations"][0]
        print(f"   State: {assoc['State']}")
        print(f"   Profile ARN: {assoc['IamInstanceProfile']['Arn']}")
        print(f"\n✅ EC2 instance is now using IAM Role for credentials")
    else:
        print(f"   ⚠️  Association not yet visible — wait 30 seconds and check again")


def enforce_imdsv2(instance_id, region):
    """
    CRITICAL SECURITY STEP:
    Enforce IMDSv2 to prevent SSRF-based credential theft
    """
    ec2_client = boto3.client("ec2", region_name=region)

    print(f"\n🔒 Enforcing IMDSv2 on {instance_id}...")

    ec2_client.modify_instance_metadata_options(
        InstanceId=instance_id,
        HttpTokens="required",       # Forces IMDSv2 (session-based)
        HttpEndpoint="enabled",       # Keep metadata service on
        HttpPutResponseHopLimit=1     # Prevent container escape attacks
        # Hop limit = 1: only the instance itself can reach IMDS
        # Hop limit = 2: containers on the instance can also reach IMDS
    )
    print(f"✅ IMDSv2 enforced — IMDSv1 disabled")
    print(f"   HttpTokens: required")
    print(f"   HttpPutResponseHopLimit: 1")


if __name__ == "__main__":
    attach_instance_profile(INSTANCE_ID, INSTANCE_PROFILE_NAME, REGION)
    enforce_imdsv2(INSTANCE_ID, REGION)
    print(f"\n🎯 NEXT STEPS (run ON the EC2 instance):")
    print(f"   1. rm ~/.aws/credentials   (remove old static keys)")
    print(f"   2. aws sts get-caller-identity   (verify role is active)")
    print(f"   3. python 02_upload_daily_csvs.py   (test upload with role)")