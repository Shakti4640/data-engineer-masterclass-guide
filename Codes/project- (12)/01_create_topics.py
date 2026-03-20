# file: 01_create_topics.py
# Purpose: Create SNS topics for pipeline notifications
# Run once during infrastructure setup

import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"ACCOUNT_ID [REDACTED:BANK_ACCOUNT_NUMBER]012"      # Replace with your account ID

# Topic definitions
TOPICS = {
    "success": {
        "name": "quickcart-prod-pipeline-success",
        "display_name": "QuickCart Pipeline Success",
        "description": "Notifications when ETL pipelines complete successfully"
    },
    "failure": {
        "name": "quickcart-prod-pipeline-failure",
        "display_name": "QuickCart Pipeline FAILURE",
        "description": "CRITICAL: Notifications when ETL pipelines fail"
    }
}


def create_topic(sns_client, topic_config):
    """
    Create SNS topic with encryption and tags
    
    IDEMPOTENT: calling create_topic with same name returns existing topic ARN
    """
    topic_name = topic_config["name"]
    
    try:
        response = sns_client.create_topic(
            Name=topic_name,
            Attributes={
                # Display name: shown in SMS messages (first 10 chars)
                "DisplayName": topic_config["display_name"],
                
                # Server-side encryption using AWS managed key
                # Messages encrypted at rest in SNS
                "KmsMasterKeyId": "alias/aws/sns"
            },
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Team", "Value": "Data-Engineering"},
                {"Key": "Project", "Value": "QuickCart"},
                {"Key": "Purpose", "Value": topic_config["description"]},
                {"Key": "CostCenter", "Value": "ENG-001"}
            ]
        )
        
        topic_arn = response["TopicArn"]
        print(f"✅ Topic created: {topic_name}")
        print(f"   ARN: {topic_arn}")
        
        return topic_arn
        
    except ClientError as e:
        print(f"❌ Failed to create topic '{topic_name}': {e}")
        raise


def set_topic_policy(sns_client, topic_arn, topic_name):
    """
    Set resource policy on topic
    
    WHY:
    → Controls WHO can publish to this topic
    → Controls WHO can subscribe
    → Default: only topic owner (this account) can publish/subscribe
    → We explicitly define allowed publishers for clarity
    """
    policy = {
        "Version": "2012-10-17",
        "Id": f"{topic_name}-policy",
        "Statement": [
            {
                # Allow this account to do everything with the topic
                "Sid": "AllowAccountAccess",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{ACCOUNT_ID}:root"
                },
                "Action": [
                    "sns:Publish",
                    "sns:Subscribe",
                    "sns:GetTopicAttributes",
                    "sns:SetTopicAttributes"
                ],
                "Resource": topic_arn
            },
            {
                # Allow CloudWatch Alarms to publish (Project 54)
                "Sid": "AllowCloudWatchAlarms",
                "Effect": "Allow",
                "Principal": {
                    "Service": "cloudwatch.amazonaws.com"
                },
                "Action": "sns:Publish",
                "Resource": topic_arn,
                "Condition": {
                    "StringEquals": {
                        "aws:SourceAccount": ACCOUNT_ID
                    }
                }
            },
            {
                # Allow S3 Event Notifications (Project 17)
                "Sid": "AllowS3Events",
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": "sns:Publish",
                "Resource": topic_arn,
                "Condition": {
                    "StringEquals": {
                        "aws:SourceAccount": ACCOUNT_ID
                    }
                }
            }
        ]
    }
    
    sns_client.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(policy)
    )
    print(f"   ✅ Topic policy set (allows CloudWatch, S3 events)")


def run_setup():
    """Create all topics"""
    print("=" * 60)
    print("🔔 CREATING SNS NOTIFICATION TOPICS")
    print("=" * 60)
    
    sns_client = boto3.client("sns", region_name=REGION)
    
    topic_arns = {}
    
    for key, config in TOPICS.items():
        print(f"\n📋 Creating '{key}' topic...")
        arn = create_topic(sns_client, config)
        set_topic_policy(sns_client, arn, config["name"])
        topic_arns[key] = arn
    
    # Save ARNs for use by other scripts
    print(f"\n{'='*60}")
    print(f"✅ ALL TOPICS CREATED")
    print(f"{'='*60}")
    print(f"\nTOPIC ARNs (save these for other scripts):")
    for key, arn in topic_arns.items():
        print(f"   {key}: {arn}")
    
    # Write to config file for other scripts
    config = {
        "region": REGION,
        "topics": topic_arns
    }
    with open("sns_config.json", "w") as f:
        json.dump(config, f, indent=2)
    print(f"\n📁 Config saved to sns_config.json")
    
    return topic_arns


if __name__ == "__main__":
    run_setup()