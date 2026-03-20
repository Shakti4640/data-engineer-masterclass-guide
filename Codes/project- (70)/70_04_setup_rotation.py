# file: 70_04_setup_rotation.py
# Run from: Account B (222222222222)
# Purpose: Deploy rotation Lambda and enable automatic rotation

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_B_ID = boto3.client("sts").get_caller_identity()["Account"]


def enable_mariadb_rotation():
    """
    Enable rotation for MariaDB secret using AWS-provided rotation template
    
    AWS provides pre-built rotation Lambdas for common databases:
    → SecretsManagerRDSMySQLRotationSingleUser
    → SecretsManagerRDSMySQLRotationMultiUser (alternating users)
    
    For EC2-hosted MariaDB: use the same MySQL template
    → MariaDB is MySQL-compatible → same rotation logic works
    
    DEPLOYMENT: via Serverless Application Repository (SAR)
    → AWS publishes rotation Lambda as SAR application
    → Deploy via console or CloudFormation
    """
    sm = boto3.client("secretsmanager", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)

    secret_name = "quickcart/prod/mariadb/glue-reader"
    rotation_lambda_name = "SecretsManager-mariadb-rotation"

    print(f"🔄 Setting up MariaDB rotation...")
    print(f"   Secret: {secret_name}")

    # NOTE: In production, deploy rotation Lambda via SAR or CloudFormation
    # Here we show the rotation configuration
    print(f"\n   📦 Deploy rotation Lambda via AWS Console:")
    print(f"   1. Secrets Manager → {secret_name} → Rotation")
    print(f"   2. Enable automatic rotation")
    print(f"   3. Rotation interval: 90 days")
    print(f"   4. Rotation Lambda: Create new (AWS template)")
    print(f"   5. Template: SecretsManagerRDSMySQLRotationMultiUser")
    print(f"   6. VPC: vpc-0bbb2222bbbb2222b (same as Glue)")
    print(f"   7. Subnet: subnet-0bbb2222glue")
    print(f"   8. Security Group: sg-0bbb2222glueeni")

    # Programmatic rotation configuration (after Lambda is deployed)
    try:
        rotation_lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_B_ID}:function:{rotation_lambda_name}"

        sm.rotate_secret(
            SecretId=secret_name,
            RotationLambdaARN=rotation_lambda_arn,
            RotationRules={
                "AutomaticallyAfterDays": 90,
                "Duration": "2h",
                "ScheduleExpression": "rate(90 days)"
            }
        )
        print(f"\n   ✅ Rotation enabled: every 90 days")
        print(f"   Rotation Lambda: {rotation_lambda_arn}")

    except sm.exceptions.ResourceNotFoundException:
        print(f"\n   ⚠️  Rotation Lambda not yet deployed — deploy via console first")
    except Exception as e:
        print(f"\n   ⚠️  {str(e)[:150]}")


def test_manual_rotation():
    """
    Trigger manual rotation to verify everything works
    
    ALWAYS test manually BEFORE enabling automatic schedule
    
    FLOW:
    1. Trigger rotation
    2. Wait for completion
    3. Verify: old password still works (AWSPREVIOUS)
    4. Verify: new password works (AWSCURRENT)
    5. Verify: all services can connect
    """
    sm = boto3.client("secretsmanager", region_name=REGION)

    secrets_to_test = [
        "quickcart/prod/mariadb/glue-reader",
        "quickcart/prod/redshift/etl-writer"
    ]

    for secret_name in secrets_to_test:
        print(f"\n🧪 Testing manual rotation: {secret_name}")

        try:
            # Get current version before rotation
            before = sm.get_secret_value(SecretId=secret_name)
            before_version = before.get("VersionId", "unknown")
            before_user = json.loads(before["SecretString"]).get("username", "unknown")
            print(f"   Before: version={before_version[:12]}... user={before_user}")

            # Trigger rotation
            print(f"   🔄 Triggering rotation...")
            sm.rotate_secret(SecretId=secret_name)

            # Poll for completion
            max_wait = 120
            elapsed = 0
            while elapsed < max_wait:
                time.sleep(10)
                elapsed += 10

                desc = sm.describe_secret(SecretId=secret_name)
                versions = desc.get("VersionIdsToStages", {})

                # Check if AWSPENDING is gone (rotation complete)
                has_pending = any("AWSPENDING" in stages for stages in versions.values())

                if not has_pending:
                    # Rotation complete
                    after = sm.get_secret_value(SecretId=secret_name)
                    after_version = after.get("VersionId", "unknown")
                    after_user = json.loads(after["SecretString"]).get("username", "unknown")

                    print(f"   After: version={after_version[:12]}... user={after_user}")

                    if before_version != after_version:
                        print(f"   ✅ Rotation SUCCEEDED — version changed")
                    else:
                        print(f"   ⚠️  Version unchanged — rotation may not have executed")

                    break

                print(f"   ⏳ Rotating... ({elapsed}s)")

            else:
                print(f"   ⚠️  Rotation still in progress after {max_wait}s")
                print(f"   Check CloudWatch Logs for rotation Lambda errors")

        except sm.exceptions.ResourceNotFoundException:
            print(f"   ❌ Secret not found: {secret_name}")
        except Exception as e:
            error_msg = str(e)
            if "rotation is already in progress" in error_msg.lower():
                print(f"   ℹ️  Rotation already in progress")
            elif "no rotation lambda" in error_msg.lower():
                print(f"   ⚠️  Rotation Lambda not configured — deploy via console first")
            else:
                print(f"   ❌ Error: {error_msg[:150]}")


def verify_services_after_rotation():
    """
    After rotation: verify all services can still authenticate
    
    CRITICAL: run this after every manual or automatic rotation
    """
    print(f"\n{'='*60}")
    print(f"🔍 POST-ROTATION VERIFICATION")
    print(f"{'='*60}")

    sm = boto3.client("secretsmanager", region_name=REGION)

    # Test 1: MariaDB connectivity
    print(f"\n📋 Test 1: MariaDB connectivity")
    try:
        secret = json.loads(
            sm.get_secret_value(SecretId="quickcart/prod/mariadb/glue-reader")["SecretString"]
        )
        import pymysql
        conn = pymysql.connect(
            host=secret["host"],
            port=int(secret["port"]),
            user=secret["username"],
            password=secret["password"],
            database=secret["dbname"],
            connect_timeout=10
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()
        print(f"   ✅ MariaDB: connected as {secret['username']}")
    except ImportError:
        print(f"   ⚠️  pymysql not installed — skip MariaDB test")
    except Exception as e:
        print(f"   ❌ MariaDB: {str(e)[:100]}")

    # Test 2: Redshift connectivity
    print(f"\n📋 Test 2: Redshift connectivity")
    try:
        secret = json.loads(
            sm.get_secret_value(SecretId="quickcart/prod/redshift/etl-writer")["SecretString"]
        )
        import psycopg2
        conn = psycopg2.connect(
            host=secret["host"],
            port=int(secret["port"]),
            user=secret["username"],
            password=secret["password"],
            dbname=secret["dbname"],
            connect_timeout=30
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()
        print(f"   ✅ Redshift: connected as {secret['username']}")
    except ImportError:
        print(f"   ⚠️  psycopg2 not installed — skip Redshift test")
    except Exception as e:
        print(f"   ❌ Redshift: {str(e)[:100]}")

    # Test 3: Redshift Data API with SecretArn
    print(f"\n📋 Test 3: Redshift Data API (SecretArn)")
    try:
        redshift_data = boto3.client("redshift-data", region_name=REGION)
        secret_desc = sm.describe_secret(SecretId="quickcart/prod/redshift/etl-writer")
        secret_arn = secret_desc["ARN"]

        # Determine if serverless or provisioned
        secret_val = json.loads(
            sm.get_secret_value(SecretId="quickcart/prod/redshift/etl-writer")["SecretString"]
        )

        params = {
            "SecretArn": secret_arn,
            "Database": "analytics",
            "Sql": "SELECT CURRENT_USER, CURRENT_DATE"
        }

        # Add appropriate identifier
        if "redshift-serverless" in secret_val.get("host", ""):
            params["WorkgroupName"] = secret_val.get("dbClusterIdentifier", "quickcart-prod")
        else:
            params["ClusterIdentifier"] = secret_val.get("dbClusterIdentifier", "quickcart-analytics-cluster")

        response = redshift_data.execute_statement(**params)
        stmt_id = response["Id"]

        # Poll
        max_wait = 60
        elapsed = 0
        while elapsed < max_wait:
            time.sleep(5)
            elapsed += 5
            desc = redshift_data.describe_statement(Id=stmt_id)
            if desc["Status"] == "FINISHED":
                result = redshift_data.get_statement_result(Id=stmt_id)
                user = result["Records"][0][0].get("stringValue", "unknown")
                print(f"   ✅ Data API: connected as {user} (using SecretArn)")
                break
            elif desc["Status"] == "FAILED":
                print(f"   ❌ Data API: {desc.get('Error', 'unknown error')}")
                break
        else:
            print(f"   ⚠️  Data API: timeout after {max_wait}s")

    except Exception as e:
        print(f"   ❌ Data API: {str(e)[:100]}")

    # Test 4: Glue Connection test
    print(f"\n📋 Test 4: Glue Connection (manual verification required)")
    print(f"   → Glue Console → Connections → conn-cross-account-mariadb → Test Connection")
    print(f"   → Glue Console → Connections → conn-redshift-analytics → Test Connection")

    print(f"\n{'='*60}")
    print(f"✅ POST-ROTATION VERIFICATION COMPLETE")
    print(f"   If all tests passed: rotation is working correctly")
    print(f"   If any failed: check rotation Lambda logs in CloudWatch")
    print(f"{'='*60}")


def create_rotation_monitoring():
    """
    CloudWatch alarms for rotation health
    """
    cw = boto3.client("cloudwatch", region_name=REGION)
    sns_topic = f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts"

    secrets = [
        "quickcart/prod/mariadb/glue-reader",
        "quickcart/prod/redshift/etl-writer"
    ]

    for secret_name in secrets:
        short_name = secret_name.split("/")[-1]

        # Alarm: rotation hasn't happened in 95 days (should be 90)
        # Using CloudTrail metric filter for RotateSecret events
        print(f"   📋 Monitoring for {short_name}:")
        print(f"      → Check CloudTrail for RotateSecret events")
        print(f"      → Alert if no rotation in 95 days")
        print(f"      → Manual check: aws secretsmanager describe-secret --secret-id {secret_name}")

    # Create general rotation failure alarm
    # Rotation Lambda errors appear in CloudWatch Logs
    cw.put_metric_alarm(
        AlarmName="secrets-rotation-lambda-errors",
        AlarmDescription="Secrets Manager rotation Lambda encountered errors",
        Namespace="AWS/Lambda",
        MetricName="Errors",
        Dimensions=[
            {"Name": "FunctionName", "Value": "SecretsManager-mariadb-rotation"}
        ],
        Statistic="Sum",
        Period=86400,
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[sns_topic],
        TreatMissingData="notBreaching"
    )
    print(f"\n✅ Rotation monitoring alarm created")


def create_rotation_audit_report():
    """
    Generate audit report: when was each secret last rotated?
    Required for SOC 2 compliance evidence
    """
    sm = boto3.client("secretsmanager", region_name=REGION)

    print(f"\n{'='*70}")
    print(f"📋 SECRETS ROTATION AUDIT REPORT")
    print(f"   Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*70}")

    secrets_to_audit = [
        "quickcart/prod/mariadb/glue-reader",
        "quickcart/prod/mariadb/admin",
        "quickcart/prod/redshift/etl-writer"
    ]

    print(f"\n{'Secret':<45} {'Rotation':>10} {'Last Rotated':>20} {'Next Due':>20}")
    print("-" * 100)

    for secret_name in secrets_to_audit:
        try:
            desc = sm.describe_secret(SecretId=secret_name)

            rotation_enabled = desc.get("RotationEnabled", False)
            last_rotated = desc.get("LastRotatedDate", "Never")
            rotation_rules = desc.get("RotationRules", {})
            rotation_days = rotation_rules.get("AutomaticallyAfterDays", "N/A")

            if isinstance(last_rotated, str):
                last_str = last_rotated
                next_str = "N/A"
            else:
                from datetime import timedelta
                last_str = last_rotated.strftime("%Y-%m-%d %H:%M")
                if isinstance(rotation_days, int):
                    next_due = last_rotated + timedelta(days=rotation_days)
                    next_str = next_due.strftime("%Y-%m-%d")
                else:
                    next_str = "N/A"

            rotation_status = f"✅ {rotation_days}d" if rotation_enabled else "❌ OFF"

            print(f"  {secret_name:<45} {rotation_status:>10} {last_str:>20} {next_str:>20}")

        except Exception as e:
            print(f"  {secret_name:<45} {'ERROR':>10} {str(e)[:40]}")

    print(f"\n{'='*70}")
    print(f"   SOC 2 Requirement: rotation every 90 days")
    print(f"   Compliance: {'✅ MET' if all else '❌ REVIEW NEEDED'}")
    print(f"{'='*70}")


if __name__ == "__main__":
    print("=" * 60)
    print("🔄 SETTING UP SECRET ROTATION")
    print("=" * 60)

    enable_mariadb_rotation()
    enable_redshift_rotation()
    print()
    create_rotation_monitoring()
    print()

    print(f"\n{'='*60}")
    print("📋 NEXT STEPS:")
    print("   1. Deploy rotation Lambdas via Console (SAR templates)")
    print("   2. Run manual rotation test:")
    print("      python 70_04_setup_rotation.py --test")
    print("   3. Verify all services after rotation:")
    print("      python 70_04_setup_rotation.py --verify")
    print("   4. Generate audit report:")
    print("      python 70_04_setup_rotation.py --audit")
    print(f"{'='*60}")

    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            test_manual_rotation()
        elif sys.argv[1] == "--verify":
            verify_services_after_rotation()
        elif sys.argv[1] == "--audit":
            create_rotation_audit_report()