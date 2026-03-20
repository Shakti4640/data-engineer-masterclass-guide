# file: 47_01_create_dr_infrastructure.py
# Purpose: Create DR bucket in us-west-2, IAM role for CRR, enable versioning

import boto3
import json
import time

PRIMARY_REGION = "us-east-2"
DR_REGION = "us-west-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

PRIMARY_BUCKET = "quickcart-datalake-prod"
DR_BUCKET = "quickcart-datalake-dr"
CRR_ROLE_NAME = "quickcart-s3-crr-role"


def create_dr_bucket():
    """Create the DR bucket in us-west-2 with matching configuration"""
    s3_client = boto3.client("s3", region_name=DR_REGION)

    try:
        s3_client.create_bucket(
            Bucket=DR_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": DR_REGION}
        )
        print(f"✅ DR bucket created: {DR_BUCKET} (region: {DR_REGION})")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"ℹ️  DR bucket already exists: {DR_BUCKET}")

    # Enable versioning (REQUIRED for CRR destination)
    s3_client.put_bucket_versioning(
        Bucket=DR_BUCKET,
        VersioningConfiguration={"Status": "Enabled"}
    )
    print(f"   ✅ Versioning enabled")

    # Block public access
    s3_client.put_public_access_block(
        Bucket=DR_BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True
        }
    )
    print(f"   ✅ Public access blocked")

    # Encryption
    s3_client.put_bucket_encryption(
        Bucket=DR_BUCKET,
        ServerSideEncryptionConfiguration={
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                "BucketKeyEnabled": True
            }]
        }
    )
    print(f"   ✅ Encryption enabled (AES256)")

    # Tags
    s3_client.put_bucket_tagging(
        Bucket=DR_BUCKET,
        Tagging={
            "TagSet": [
                {"Key": "Environment", "Value": "DR"},
                {"Key": "PrimaryBucket", "Value": PRIMARY_BUCKET},
                {"Key": "PrimaryRegion", "Value": PRIMARY_REGION},
                {"Key": "Project", "Value": "QuickCart-DR"},
                {"Key": "CostCenter", "Value": "ENG-001"}
            ]
        }
    )
    print(f"   ✅ Tagged as DR")

    # Lifecycle rules (match primary for replicated zones)
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=DR_BUCKET,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "silver-dr-lifecycle",
                    "Filter": {"Prefix": "silver/"},
                    "Status": "Enabled",
                    "Transitions": [
                        {"Days": 60, "StorageClass": "STANDARD_IA"}
                    ],
                    "Expiration": {"Days": 180},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 30}
                },
                {
                    "ID": "gold-dr-lifecycle",
                    "Filter": {"Prefix": "gold/"},
                    "Status": "Enabled",
                    "Transitions": [
                        {"Days": 90, "StorageClass": "STANDARD_IA"}
                    ],
                    "Expiration": {"Days": 365},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 30}
                }
            ]
        }
    )
    print(f"   ✅ Lifecycle rules configured")


def verify_primary_versioning():
    """Ensure primary bucket has versioning enabled (required for CRR)"""
    s3_client = boto3.client("s3", region_name=PRIMARY_REGION)

    response = s3_client.get_bucket_versioning(Bucket=PRIMARY_BUCKET)
    status = response.get("Status", "Disabled")

    if status == "Enabled":
        print(f"✅ Primary bucket versioning: Enabled")
    else:
        print(f"⚠️  Primary bucket versioning: {status} — enabling now")
        s3_client.put_bucket_versioning(
            Bucket=PRIMARY_BUCKET,
            VersioningConfiguration={"Status": "Enabled"}
        )
        print(f"   ✅ Versioning enabled on primary")


def create_crr_iam_role():
    """
    Create IAM role that S3 assumes to perform cross-region replication.
    
    This role needs:
    → Read from source bucket (us-east-2)
    → Write to destination bucket (us-west-2)
    → Trusted by: s3.amazonaws.com
    """
    iam_client = boto3.client("iam")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadSourceBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:GetReplicationConfiguration",
                    "s3:ListBucket"
                ],
                "Resource": f"arn:aws:s3:::{PRIMARY_BUCKET}"
            },
            {
                "Sid": "ReadSourceObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObjectVersionForReplication",
                    "s3:GetObjectVersionAcl",
                    "s3:GetObjectVersionTagging"
                ],
                "Resource": f"arn:aws:s3:::{PRIMARY_BUCKET}/*"
            },
            {
                "Sid": "WriteDestinationBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:ReplicateObject",
                    "s3:ReplicateDelete",
                    "s3:ReplicateTags",
                    "s3:ObjectOwnerOverrideToBucketOwner"
                ],
                "Resource": f"arn:aws:s3:::{DR_BUCKET}/*"
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=CRR_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="S3 CRR role — replicates from primary to DR bucket"
        )
        print(f"✅ CRR IAM role created: {CRR_ROLE_NAME}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  CRR IAM role exists: {CRR_ROLE_NAME}")

    iam_client.put_role_policy(
        RoleName=CRR_ROLE_NAME,
        PolicyName="S3CRRPermissions",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"   ✅ Permissions attached")

    time.sleep(10)  # IAM propagation
    return f"arn:aws:iam::{ACCOUNT_ID}:role/{CRR_ROLE_NAME}"


if __name__ == "__main__":
    print("=" * 60)
    print("🏗️  CREATING DR INFRASTRUCTURE")
    print("=" * 60)

    verify_primary_versioning()
    create_dr_bucket()
    role_arn = create_crr_iam_role()

    print(f"\n✅ DR infrastructure ready")
    print(f"   Primary: {PRIMARY_BUCKET} ({PRIMARY_REGION})")
    print(f"   DR: {DR_BUCKET} ({DR_REGION})")
    print(f"   CRR Role: {role_arn}")