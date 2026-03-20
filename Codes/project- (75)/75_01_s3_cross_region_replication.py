# file: 75_01_s3_cross_region_replication.py
# Setup S3 CRR from us-east-2 → us-west-2 for data lake DR

import boto3
import json
import time

PRIMARY_REGION = "us-east-2"
DR_REGION = "us-west-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"

# Buckets to replicate
REPLICATION_PAIRS = [
    {
        "source": "orders-data-prod",
        "destination": "orders-data-dr",
        "prefix_filter": "gold/",  # Only replicate Gold layer
        "priority": 1
    },
    {
        "source": "analytics-data-prod",
        "destination": "analytics-data-dr",
        "prefix_filter": "derived/",
        "priority": 2
    }
]


def create_dr_buckets(s3_dr_client):
    """Create destination buckets in DR region with versioning"""
    print("📦 Creating DR buckets in us-west-2:")

    for pair in REPLICATION_PAIRS:
        bucket_name = pair["destination"]
        try:
            s3_dr_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": DR_REGION}
            )
            print(f"   ✅ Created: {bucket_name}")
        except s3_dr_client.exceptions.BucketAlreadyOwnedByYou:
            print(f"   ℹ️  Exists: {bucket_name}")

        # Enable versioning (REQUIRED for CRR destination)
        s3_dr_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"}
        )

        # Block public access
        s3_dr_client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True
            }
        )

        # Encryption
        s3_dr_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [{
                    "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                    "BucketKeyEnabled": True
                }]
            }
        )

        # Tags
        s3_dr_client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={
                "TagSet": [
                    {"Key": "Purpose", "Value": "disaster-recovery"},
                    {"Key": "SourceBucket", "Value": pair["source"]},
                    {"Key": "SourceRegion", "Value": PRIMARY_REGION},
                    {"Key": "Environment", "Value": "dr"}
                ]
            }
        )
        print(f"      Versioning: enabled, Encryption: AES256, Public: blocked")


def enable_source_versioning(s3_primary_client):
    """Ensure source buckets have versioning enabled (required for CRR)"""
    print("\n🔄 Enabling versioning on source buckets:")
    for pair in REPLICATION_PAIRS:
        s3_primary_client.put_bucket_versioning(
            Bucket=pair["source"],
            VersioningConfiguration={"Status": "Enabled"}
        )
        print(f"   ✅ {pair['source']}: versioning enabled")


def create_replication_role(iam_client):
    """
    IAM role for S3 to perform cross-region replication
    
    S3 assumes this role to:
    → Read objects from source bucket
    → Write objects to destination bucket
    → Replicate object tags and metadata
    """
    role_name = "S3CrossRegionReplicationRole"

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    # Build resource lists for all replication pairs
    source_resources = []
    dest_resources = []
    for pair in REPLICATION_PAIRS:
        source_resources.extend([
            f"arn:aws:s3:::{pair['source']}",
            f"arn:aws:s3:::{pair['source']}/*"
        ])
        dest_resources.extend([
            f"arn:aws:s3:::{pair['destination']}",
            f"arn:aws:s3:::{pair['destination']}/*"
        ])

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadSource",
                "Effect": "Allow",
                "Action": [
                    "s3:GetReplicationConfiguration",
                    "s3:ListBucket",
                    "s3:GetObjectVersionForReplication",
                    "s3:GetObjectVersionAcl",
                    "s3:GetObjectVersionTagging"
                ],
                "Resource": source_resources
            },
            {
                "Sid": "WriteDestination",
                "Effect": "Allow",
                "Action": [
                    "s3:ReplicateObject",
                    "s3:ReplicateDelete",
                    "s3:ReplicateTags",
                    "s3:ObjectOwnerOverrideToBucketOwner"
                ],
                "Resource": dest_resources
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="S3 CRR role for disaster recovery replication"
        )
        print(f"\n✅ IAM role created: {role_name}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"\nℹ️  IAM role exists: {role_name}")

    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName="s3-crr-policy",
        PolicyDocument=json.dumps(permission_policy)
    )

    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"
    print(f"   ARN: {role_arn}")

    # Wait for propagation
    time.sleep(5)
    return role_arn


def configure_replication_rules(s3_primary_client, role_arn):
    """
    Configure CRR rules on source buckets
    
    KEY CONFIGURATION:
    → Filter by prefix: only Gold layer (saves 60% cost)
    → Replication Time Control (RTC): guaranteed 15-min SLA
    → Delete marker replication: enabled (DR stays in sync on deletes)
    → Metrics: enabled (monitor replication lag)
    """
    print("\n📋 Configuring replication rules:")

    for pair in REPLICATION_PAIRS:
        replication_config = {
            "Role": role_arn,
            "Rules": [
                {
                    "ID": f"dr-{pair['destination']}",
                    "Priority": pair["priority"],
                    "Status": "Enabled",
                    "Filter": {
                        "Prefix": pair["prefix_filter"]
                    },
                    "Destination": {
                        "Bucket": f"arn:aws:s3:::{pair['destination']}",
                        "StorageClass": "STANDARD",
                        "Metrics": {
                            "Status": "Enabled",
                            "EventThreshold": {
                                "Minutes": 15
                            }
                        },
                        "ReplicationTime": {
                            "Status": "Enabled",
                            "Time": {
                                "Minutes": 15
                            }
                        }
                    },
                    "DeleteMarkerReplication": {
                        "Status": "Enabled"
                    }
                }
            ]
        }

        s3_primary_client.put_bucket_replication(
            Bucket=pair["source"],
            ReplicationConfiguration=replication_config
        )
        print(f"   ✅ {pair['source']} → {pair['destination']}")
        print(f"      Prefix: {pair['prefix_filter']}")
        print(f"      RTC: 15 min SLA")
        print(f"      Delete markers: replicated")


def create_replication_alarms(cw_client):
    """CloudWatch alarms for replication health"""
    alert_topic = f"arn:aws:sns:{PRIMARY_REGION}:{ACCOUNT_ID}:pipeline-alerts"

    for pair in REPLICATION_PAIRS:
        # Alarm: replication lag > 30 minutes
        cw_client.put_metric_alarm(
            AlarmName=f"dr-replication-lag-{pair['source']}",
            AlarmDescription=f"S3 CRR lag exceeds 30 min for {pair['source']}",
            Namespace="AWS/S3",
            MetricName="ReplicationLatency",
            Dimensions=[
                {"Name": "SourceBucket", "Value": pair["source"]},
                {"Name": "DestinationBucket", "Value": pair["destination"]},
                {"Name": "RuleId", "Value": f"dr-{pair['destination']}"}
            ],
            Statistic="Maximum",
            Period=300,
            EvaluationPeriods=2,
            Threshold=1800,  # 30 minutes in seconds
            ComparisonOperator="GreaterThanThreshold",
            AlarmActions=[alert_topic],
            Tags=[{"Key": "Purpose", "Value": "disaster-recovery"}]
        )
        print(f"   ✅ Alarm: replication lag for {pair['source']} (threshold: 30 min)")

        # Alarm: replication failures
        cw_client.put_metric_alarm(
            AlarmName=f"dr-replication-failures-{pair['source']}",
            AlarmDescription=f"S3 CRR failures for {pair['source']}",
            Namespace="AWS/S3",
            MetricName="OperationsFailedReplication",
            Dimensions=[
                {"Name": "SourceBucket", "Value": pair["source"]},
                {"Name": "DestinationBucket", "Value": pair["destination"]},
                {"Name": "RuleId", "Value": f"dr-{pair['destination']}"}
            ],
            Statistic="Sum",
            Period=300,
            EvaluationPeriods=1,
            Threshold=100,
            ComparisonOperator="GreaterThanThreshold",
            AlarmActions=[alert_topic],
            Tags=[{"Key": "Purpose", "Value": "disaster-recovery"}]
        )

    print(f"   ✅ All replication alarms configured")


def main():
    print("=" * 70)
    print("🔄 S3 CROSS-REGION REPLICATION: us-east-2 → us-west-2")
    print("=" * 70)

    s3_primary = boto3.client("s3", region_name=PRIMARY_REGION)
    s3_dr = boto3.client("s3", region_name=DR_REGION)
    iam_client = boto3.client("iam")
    cw_client = boto3.client("cloudwatch", region_name=PRIMARY_REGION)

    # Step 1: Create DR buckets
    print("\n--- Step 1: DR Buckets ---")
    create_dr_buckets(s3_dr)

    # Step 2: Enable versioning on source
    print("\n--- Step 2: Source Versioning ---")
    enable_source_versioning(s3_primary)

    # Step 3: Replication IAM role
    print("\n--- Step 3: Replication IAM Role ---")
    role_arn = create_replication_role(iam_client)

    # Step 4: Configure replication rules
    print("\n--- Step 4: Replication Rules ---")
    configure_replication_rules(s3_primary, role_arn)

    # Step 5: Monitoring alarms
    print("\n--- Step 5: Replication Alarms ---")
    create_replication_alarms(cw_client)

    print("\n" + "=" * 70)
    print("✅ S3 CRR SETUP COMPLETE")
    print(f"   Gold data replicating to us-west-2 with 15-min RTC SLA")
    print("=" * 70)


if __name__ == "__main__":
    main()