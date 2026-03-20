# file: 47_02_configure_crr.py
# Purpose: Configure S3 Cross-Region Replication rules on the primary bucket

import boto3
import json

PRIMARY_REGION = "us-east-2"
DR_REGION = "us-west-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

PRIMARY_BUCKET = "quickcart-datalake-prod"
DR_BUCKET = "quickcart-datalake-dr"
CRR_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/quickcart-s3-crr-role"


def configure_replication():
    """
    Configure CRR rules on the primary bucket.
    
    Two rules:
    1. Gold zone → CRR with RTC (guaranteed 15-min SLA)
    2. Silver zone → CRR standard (best-effort, usually minutes)
    
    NOT replicated:
    → bronze/ (regenerable, saves cost)
    → _system/ (operational files)
    """
    s3_client = boto3.client("s3", region_name=PRIMARY_REGION)

    replication_config = {
        "Role": CRR_ROLE_ARN,
        "Rules": [
            {
                # ━━━ RULE 1: Gold zone — CRR with RTC ━━━
                "ID": "replicate-gold-with-rtc",
                "Status": "Enabled",
                "Priority": 1,
                "Filter": {
                    "Prefix": "gold/"
                },
                "Destination": {
                    "Bucket": f"arn:aws:s3:::{DR_BUCKET}",
                    "StorageClass": "STANDARD",
                    # Enable replication metrics for monitoring
                    "Metrics": {
                        "Status": "Enabled",
                        "EventThreshold": {
                            "Minutes": 15
                        }
                    },
                    # Enable Replication Time Control (15-min SLA)
                    "ReplicationTime": {
                        "Status": "Enabled",
                        "Time": {
                            "Minutes": 15
                        }
                    }
                },
                # Do NOT replicate delete markers (safety net)
                "DeleteMarkerReplication": {
                    "Status": "Disabled"
                }
            },
            {
                # ━━━ RULE 2: Silver zone — Standard CRR ━━━
                "ID": "replicate-silver-standard",
                "Status": "Enabled",
                "Priority": 2,
                "Filter": {
                    "Prefix": "silver/"
                },
                "Destination": {
                    "Bucket": f"arn:aws:s3:::{DR_BUCKET}",
                    "StorageClass": "STANDARD",
                    # Metrics enabled but no RTC (saves cost)
                    "Metrics": {
                        "Status": "Enabled",
                        "EventThreshold": {
                            "Minutes": 15
                        }
                    }
                },
                "DeleteMarkerReplication": {
                    "Status": "Disabled"
                }
            }
        ]
    }

    s3_client.put_bucket_replication(
        Bucket=PRIMARY_BUCKET,
        ReplicationConfiguration=replication_config
    )

    print(f"✅ CRR configured on {PRIMARY_BUCKET}")
    print(f"   Rule 1: gold/ → {DR_BUCKET} (CRR + RTC, 15-min SLA)")
    print(f"   Rule 2: silver/ → {DR_BUCKET} (CRR standard)")
    print(f"   Delete markers: NOT replicated (safety net)")


def verify_replication_config():
    """Verify the replication configuration"""
    s3_client = boto3.client("s3", region_name=PRIMARY_REGION)

    response = s3_client.get_bucket_replication(Bucket=PRIMARY_BUCKET)
    config = response["ReplicationConfiguration"]

    print(f"\n📋 REPLICATION CONFIGURATION:")
    print(f"   Role: {config['Role']}")
    print(f"   Rules:")
    for rule in config["Rules"]:
        rtc = rule["Destination"].get("ReplicationTime", {}).get("Status", "Disabled")
        metrics = rule["Destination"].get("Metrics", {}).get("Status", "Disabled")
        delete_rep = rule.get("DeleteMarkerReplication", {}).get("Status", "Disabled")

        print(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"   ID: {rule['ID']}")
        print(f"   Status: {rule['Status']}")
        print(f"   Prefix: {rule['Filter'].get('Prefix', '*')}")
        print(f"   Destination: {rule['Destination']['Bucket']}")
        print(f"   Storage Class: {rule['Destination']['StorageClass']}")
        print(f"   RTC: {rtc}")
        print(f"   Metrics: {metrics}")
        print(f"   Delete Replication: {delete_rep}")


if __name__ == "__main__":
    configure_replication()
    verify_replication_config()