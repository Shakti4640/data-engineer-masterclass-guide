# file: 41_08_diagnose_pipeline.py
# Purpose: When pipeline doesn't trigger, systematically find what's broken

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
BUCKET_NAME = "quickcart-raw-data-prod"
RULE_NAME = "quickcart-s3-file-arrival-trigger"
STATE_MACHINE_ARN = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:quickcart-etl-pipeline"
EVENTBRIDGE_ROLE = "quickcart-eventbridge-to-stepfn-role"
STEPFN_ROLE = "quickcart-stepfn-etl-pipeline-role"


def diagnose():
    """
    Systematic checklist — check each link in the chain:
    S3 → EventBridge → Rule → Target → Step Functions → Glue
    
    WHEN TO USE THIS:
    → File uploaded but no execution starts
    → Execution starts but fails immediately
    → Pipeline worked yesterday but broke today
    """
    print("=" * 70)
    print("🔧 PIPELINE DIAGNOSTIC CHECK")
    print("=" * 70)

    issues_found = 0

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CHECK 1: EventBridge enabled on S3 bucket?
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("\n🔍 CHECK 1: S3 EventBridge Configuration")
    s3_client = boto3.client("s3", region_name=REGION)

    try:
        config = s3_client.get_bucket_notification_configuration(Bucket=BUCKET_NAME)
        if "EventBridgeConfiguration" in config:
            print(f"   ✅ EventBridge enabled on '{BUCKET_NAME}'")
        else:
            print(f"   ❌ EventBridge NOT enabled on '{BUCKET_NAME}'")
            print(f"      FIX: Run 41_01_enable_eventbridge_on_s3.py")
            issues_found += 1
    except Exception as e:
        print(f"   ❌ Cannot check bucket: {e}")
        issues_found += 1

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CHECK 2: EventBridge rule exists and enabled?
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("\n🔍 CHECK 2: EventBridge Rule")
    events_client = boto3.client("events", region_name=REGION)

    try:
        rule = events_client.describe_rule(Name=RULE_NAME)
        if rule["State"] == "ENABLED":
            print(f"   ✅ Rule '{RULE_NAME}' exists and is ENABLED")
        else:
            print(f"   ⚠️  Rule exists but is DISABLED")
            print(f"      FIX: events_client.enable_rule(Name='{RULE_NAME}')")
            issues_found += 1

        # Verify pattern matches our bucket
        pattern = json.loads(rule["EventPattern"])
        bucket_filter = pattern.get("detail", {}).get("bucket", {}).get("name", [])
        if BUCKET_NAME in bucket_filter:
            print(f"   ✅ Pattern filters for bucket '{BUCKET_NAME}'")
        else:
            print(f"   ❌ Pattern does NOT filter for '{BUCKET_NAME}'")
            print(f"      Current filter: {bucket_filter}")
            issues_found += 1

    except events_client.exceptions.ResourceNotFoundException:
        print(f"   ❌ Rule '{RULE_NAME}' does NOT exist")
        print(f"      FIX: Run 41_04_create_eventbridge_rule.py")
        issues_found += 1

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CHECK 3: EventBridge rule has correct target?
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("\n🔍 CHECK 3: EventBridge Target")

    try:
        targets = events_client.list_targets_by_rule(Rule=RULE_NAME)
        if targets["Targets"]:
            target = targets["Targets"][0]
            target_arn = target["Arn"]
            target_role = target.get("RoleArn", "N/A")
            print(f"   ✅ Target ARN: {target_arn}")
            print(f"   ✅ Target Role: {target_role}")

            if "stateMachine" not in target_arn:
                print(f"   ⚠️  Target does not look like a Step Functions ARN")
                issues_found += 1
        else:
            print(f"   ❌ Rule has NO targets")
            print(f"      FIX: Run 41_04_create_eventbridge_rule.py")
            issues_found += 1
    except Exception as e:
        print(f"   ❌ Cannot check targets: {e}")
        issues_found += 1

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CHECK 4: IAM roles exist and have right policies?
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("\n🔍 CHECK 4: IAM Roles")
    iam_client = boto3.client("iam")

    for role_name in [EVENTBRIDGE_ROLE, STEPFN_ROLE]:
        try:
            role = iam_client.get_role(RoleName=role_name)
            trust = role["Role"]["AssumeRolePolicyDocument"]
            principals = []
            for stmt in trust.get("Statement", []):
                p = stmt.get("Principal", {}).get("Service", "")
                if isinstance(p, list):
                    principals.extend(p)
                else:
                    principals.append(p)
            print(f"   ✅ Role '{role_name}' exists")
            print(f"      Trust: {principals}")

            # Check inline policies
            policies = iam_client.list_role_policies(RoleName=role_name)
            for policy_name in policies["PolicyNames"]:
                print(f"      Policy: {policy_name}")

        except iam_client.exceptions.NoSuchEntityException:
            print(f"   ❌ Role '{role_name}' does NOT exist")
            print(f"      FIX: Run 41_02_create_iam_roles.py")
            issues_found += 1

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CHECK 5: State Machine exists?
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("\n🔍 CHECK 5: Step Functions State Machine")
    sfn_client = boto3.client("stepfunctions", region_name=REGION)

    try:
        sm = sfn_client.describe_state_machine(stateMachineArn=STATE_MACHINE_ARN)
        print(f"   ✅ State Machine: {sm['name']}")
        print(f"      Status: {sm['status']}")
        print(f"      Type: {sm['type']}")
        print(f"      Role: {sm['roleArn']}")
    except sfn_client.exceptions.StateMachineDoesNotExist:
        print(f"   ❌ State Machine does NOT exist: {STATE_MACHINE_ARN}")
        print(f"      FIX: Run 41_03_create_state_machine.py")
        issues_found += 1

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CHECK 6: Glue resources exist?
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("\n🔍 CHECK 6: Glue Resources")
    glue_client = boto3.client("glue", region_name=REGION)

    for entity in ["orders", "customers", "products"]:
        crawler_name = f"quickcart-{entity}-crawler"
        job_name = f"quickcart-{entity}-etl"

        try:
            glue_client.get_crawler(Name=crawler_name)
            print(f"   ✅ Crawler: {crawler_name}")
        except glue_client.exceptions.EntityNotFoundException:
            print(f"   ❌ Crawler NOT found: {crawler_name}")
            issues_found += 1

        try:
            glue_client.get_job(JobName=job_name)
            print(f"   ✅ ETL Job: {job_name}")
        except glue_client.exceptions.EntityNotFoundException:
            print(f"   ❌ ETL Job NOT found: {job_name}")
            issues_found += 1

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CHECK 7: Recent executions?
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("\n🔍 CHECK 7: Recent Executions")
    try:
        execs = sfn_client.list_executions(
            stateMachineArn=STATE_MACHINE_ARN,
            maxResults=5
        )
        if execs["executions"]:
            for ex in execs["executions"]:
                print(f"   📋 {ex['name']}")
                print(f"      Status: {ex['status']}")
                print(f"      Started: {ex['startDate']}")
        else:
            print(f"   ℹ️  No executions found (pipeline never triggered)")
    except Exception as e:
        print(f"   ❌ Cannot list executions: {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SUMMARY
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("\n" + "=" * 70)
    if issues_found == 0:
        print(f"✅ ALL CHECKS PASSED — pipeline should be functional")
        print(f"   If still not working, check CloudWatch Logs for EventBridge")
    else:
        print(f"❌ {issues_found} ISSUE(S) FOUND — fix them in order above")
    print("=" * 70)


if __name__ == "__main__":
    diagnose()