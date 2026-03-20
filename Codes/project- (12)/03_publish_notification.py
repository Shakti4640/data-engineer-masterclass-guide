# file: 03_publish_notification.py
# Purpose: Reusable notification publisher for ALL pipelines
# This module is imported by every ETL script (Projects 21, 32, 55, etc.)

import boto3
import json
import time
from datetime import datetime, timezone
from botocore.exceptions import ClientError

REGION = "us-east-2"


class PipelineNotifier:
    """
    Reusable notification publisher for pipeline events
    
    USAGE:
        notifier = PipelineNotifier()
        
        # On success:
        notifier.notify_success(
            pipeline_name="daily_etl",
            duration_seconds=1234,
            rows_processed=500000,
            details={"tables_loaded": ["orders", "customers"]}
        )
        
        # On failure:
        notifier.notify_failure(
            pipeline_name="daily_etl",
            error_message="S3 Access Denied",
            error_type="ClientError",
            details={"failed_step": "COPY orders"}
        )
    """
    
    def __init__(self, config_path="sns_config.json"):
        """Initialize with topic ARNs from config"""
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
            self.sns_client = boto3.client("sns", region_name=config["region"])
            self.success_topic = config["topics"]["success"]
            self.failure_topic = config["topics"]["failure"]
            self.initialized = True
        except (FileNotFoundError, KeyError) as e:
            print(f"⚠️  SNS config not found: {e}")
            print(f"   Notifications will be printed to console only")
            self.initialized = False
    
    def _build_message(self, pipeline_name, status, **kwargs):
        """
        Build structured JSON message
        
        WHY JSON:
        → Human-readable in email
        → Machine-parseable by SQS consumers (Project 14)
        → Lambda can process and route (future)
        → Consistent format across all pipelines
        """
        message = {
            "pipeline_name": pipeline_name,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": "production",
            "account_id": REGION,
        }
        
        # Add optional fields
        if "duration_seconds" in kwargs:
            duration = kwargs["duration_seconds"]
            minutes = int(duration // 60)
            seconds = int(duration % 60)
            message["duration_seconds"] = duration
            message["duration_human"] = f"{minutes}m {seconds}s"
        
        if "rows_processed" in kwargs:
            message["rows_processed"] = kwargs["rows_processed"]
        
        if "error_message" in kwargs:
            message["error_message"] = kwargs["error_message"]
        
        if "error_type" in kwargs:
            message["error_type"] = kwargs["error_type"]
        
        if "details" in kwargs:
            message["details"] = kwargs["details"]
        
        return message
    
    def _publish(self, topic_arn, subject, message_dict, severity):
        """
        Publish message to SNS topic
        
        MESSAGE ATTRIBUTES:
        → Used by filter policies (Project 51)
        → SQS consumers can filter on attributes without parsing body
        → Must be set at publish time — can't add later
        """
        message_body = json.dumps(message_dict, indent=2, default=str)
        
        if not self.initialized:
            print(f"\n📢 [CONSOLE NOTIFICATION] {subject}")
            print(message_body)
            return None
        
        try:
            response = self.sns_client.publish(
                TopicArn=topic_arn,
                Subject=subject[:100],          # SNS subject max: 100 chars
                Message=message_body,
                MessageAttributes={
                    "severity": {
                        "DataType": "String",
                        "StringValue": severity
                    },
                    "pipeline": {
                        "DataType": "String",
                        "StringValue": message_dict.get("pipeline_name", "unknown")
                    },
                    "status": {
                        "DataType": "String",
                        "StringValue": message_dict.get("status", "unknown")
                    },
                    "environment": {
                        "DataType": "String",
                        "StringValue": "production"
                    }
                }
            )
            
            message_id = response["MessageId"]
            print(f"   📨 Published to SNS: {message_id}")
            return message_id
            
        except ClientError as e:
            # Notification failure should NOT crash the pipeline
            # Log the error but don't raise
            print(f"   ⚠️  SNS publish failed: {e}")
            print(f"   ⚠️  Notification not delivered but pipeline continues")
            return None
    
    def notify_success(self, pipeline_name, duration_seconds=None,
                       rows_processed=None, details=None):
        """
        Notify on pipeline SUCCESS
        → Publishes to success topic
        → Informational — data team knows data is fresh
        """
        message = self._build_message(
            pipeline_name=pipeline_name,
            status="SUCCESS",
            duration_seconds=duration_seconds,
            rows_processed=rows_processed,
            details=details
        )
        
        # Build human-readable subject
        duration_str = ""
        if duration_seconds:
            minutes = int(duration_seconds // 60)
            duration_str = f" ({minutes}m)"
        
        rows_str = ""
        if rows_processed:
            rows_str = f" — {rows_processed:,} rows"
        
        subject = f"✅ {pipeline_name} SUCCEEDED{duration_str}{rows_str}"
        
        print(f"\n{'='*60}")
        print(f"📨 SENDING SUCCESS NOTIFICATION")
        print(f"   Pipeline: {pipeline_name}")
        print(f"   Subject:  {subject}")
        
        return self._publish(
            topic_arn=self.success_topic,
            subject=subject,
            message_dict=message,
            severity="info"
        )
    
    def notify_failure(self, pipeline_name, error_message,
                       error_type=None, details=None):
        """
        Notify on pipeline FAILURE
        → Publishes to failure topic
        → CRITICAL — on-call must respond
        """
        message = self._build_message(
            pipeline_name=pipeline_name,
            status="FAILURE",
            error_message=error_message,
            error_type=error_type,
            details=details
        )
        
        # Truncate error for subject line
        short_error = error_message[:50] + "..." if len(error_message) > 50 else error_message
        subject = f"🔴 {pipeline_name} FAILED — {short_error}"
        
        print(f"\n{'='*60}")
        print(f"🚨 SENDING FAILURE NOTIFICATION")
        print(f"   Pipeline: {pipeline_name}")
        print(f"   Error:    {error_message}")
        print(f"   Subject:  {subject}")
        
        return self._publish(
            topic_arn=self.failure_topic,
            subject=subject,
            message_dict=message,
            severity="critical"
        )
    
    def notify_warning(self, pipeline_name, warning_message, details=None):
        """
        Notify on pipeline WARNING (degraded but not failed)
        → Publishes to success topic with warning severity
        → Data team should investigate but not urgent
        """
        message = self._build_message(
            pipeline_name=pipeline_name,
            status="WARNING",
            error_message=warning_message,
            details=details
        )
        
        subject = f"⚠️ {pipeline_name} WARNING — {warning_message[:60]}"
        
        print(f"\n{'='*60}")
        print(f"⚠️  SENDING WARNING NOTIFICATION")
        print(f"   Pipeline: {pipeline_name}")
        print(f"   Warning:  {warning_message}")
        
        return self._publish(
            topic_arn=self.success_topic,     # Warnings go to success topic
            subject=subject,
            message_dict=message,
            severity="warning"
        )


def run_demo():
    """Demonstrate all notification types"""
    print("=" * 60)
    print("🔔 SNS NOTIFICATION DEMONSTRATION")
    print("=" * 60)
    
    notifier = PipelineNotifier()
    
    # --- DEMO 1: Success notification ---
    notifier.notify_success(
        pipeline_name="daily_etl_mariadb_to_redshift",
        duration_seconds=847,
        rows_processed=523847,
        details={
            "tables_loaded": ["fact_orders", "fact_order_items", "dim_customers"],
            "staging_rows": {"stg_orders": 12500, "stg_items": 37500},
            "scd2_changes": {"new": 15, "changed": 8, "unchanged": 477},
            "redshift_cluster": "quickcart-analytics"
        }
    )
    
    # --- DEMO 2: Failure notification ---
    notifier.notify_failure(
        pipeline_name="daily_etl_mariadb_to_redshift",
        error_message="Access Denied: Role RedshiftS3AccessRole cannot access s3://quickcart-raw-data-prod/staging/",
        error_type="ClientError",
        details={
            "failed_step": "COPY stg_orders FROM S3",
            "failed_at": "2025-07-15T02:15:23Z",
            "last_successful_run": "2025-07-14T02:45:00Z",
            "action_required": "Check IAM role permissions for Redshift"
        }
    )
    
    # --- DEMO 3: Warning notification ---
    notifier.notify_warning(
        pipeline_name="daily_etl_mariadb_to_redshift",
        warning_message="Row count mismatch: source=12500, target=12498 (2 rows lost)",
        details={
            "table": "stg_orders",
            "source_count": 12500,
            "target_count": 12498,
            "tolerance": "0.1%",
            "within_tolerance": True
        }
    )
    
    print(f"\n{'='*60}")
    print(f"✅ ALL NOTIFICATIONS SENT")
    print(f"{'='*60}")
    print(f"""
CHECK YOUR INBOX:
→ Success topic subscribers should receive 2 emails (success + warning)
→ Failure topic subscribers should receive 1 email + 1 SMS

EMAIL SUBJECT LINES:
→ ✅ daily_etl_mariadb_to_redshift SUCCEEDED (14m) — 523,847 rows
→ 🔴 daily_etl_mariadb_to_redshift FAILED — Access Denied: Role RedshiftS...
→ ⚠️ daily_etl_mariadb_to_redshift WARNING — Row count mismatch: source=12...

NEXT: Integrate PipelineNotifier into actual ETL scripts:
  from publish_notification import PipelineNotifier
  notifier = PipelineNotifier()
  
  try:
      run_etl()
      notifier.notify_success(...)
  except Exception as e:
      notifier.notify_failure(..., error_message=str(e))
    """)


# --- CLI USAGE ---
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Quick test: publish one message to each topic
        notifier = PipelineNotifier()
        notifier.notify_success(
            pipeline_name="TEST_NOTIFICATION",
            duration_seconds=1,
            rows_processed=0,
            details={"message": "This is a test notification. Ignore if received."}
        )
        notifier.notify_failure(
            pipeline_name="TEST_NOTIFICATION",
            error_message="This is a test failure notification. No action needed.",
            details={"message": "Test only — no real failure occurred."}
        )
    else:
        run_demo()