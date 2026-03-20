# file: scripts/rollback.py
# Emergency rollback: revert Glue jobs to previous commit SHA
# Can be run manually or triggered by failed smoke test

import boto3
import argparse
import json
import sys

REGION = "us-east-2"


def get_previous_commit_sha(cfn_client, stack_name):
    """
    Find the PREVIOUS commit SHA from CloudFormation stack events
    → Look for the last successful UPDATE_COMPLETE event
    → Extract the CommitSha parameter from that deployment
    """
    try:
        paginator = cfn_client.get_paginator("describe_stack_events")
        
        previous_sha = None
        current_sha = None
        
        for page in paginator.paginate(StackName=stack_name):
            for event in page["StackEvents"]:
                if (event["ResourceType"] == "AWS::CloudFormation::Stack" and
                    event["ResourceStatus"] == "UPDATE_COMPLETE"):
                    
                    # Get parameters from this deployment
                    stack = cfn_client.describe_stacks(StackName=stack_name)
                    params = stack["Stacks"][0].get("Parameters", [])
                    
                    for param in params:
                        if param["ParameterKey"] == "CommitSha":
                            if current_sha is None:
                                current_sha = param["ParameterValue"]
                            else:
                                previous_sha = param["ParameterValue"]
                                return previous_sha, current_sha
        
        return previous_sha, current_sha
        
    except Exception as e:
# file: scripts/rollback.py (CONTINUED)

        print(f"   ⚠️  Could not find previous SHA: {e}")
        return None, None


def rollback_glue_jobs(environment, target_sha=None):
    """
    Rollback all Glue jobs to a previous commit SHA
    
    HOW IT WORKS:
    → Previous scripts still exist in S3: s3://bucket/glue-scripts/{old_sha}/
    → CloudFormation update changes ScriptLocation to old SHA
    → Glue job immediately points to old (working) script
    → No re-upload needed — old artifacts preserved
    
    This is WHY we use commit SHA in S3 paths:
    → Every deployment creates new S3 prefix
    → Old versions never deleted
    → Rollback = change pointer, not restore data
    """
    cfn_client = boto3.client("cloudformation", region_name=REGION)
    stack_name = f"quickcart-glue-jobs-{environment}"

    print("=" * 60)
    print(f"⏪ ROLLBACK — {environment.upper()}")
    print(f"   Stack: {stack_name}")
    print("=" * 60)

    # Find previous SHA if not provided
    if target_sha is None:
        print("\n--- Finding previous deployment ---")
        previous_sha, current_sha = get_previous_commit_sha(cfn_client, stack_name)

        if previous_sha is None:
            print("   ❌ Cannot find previous commit SHA")
            print("   → Provide target SHA manually: --target-sha <sha>")
            sys.exit(1)

        print(f"   Current SHA:  {current_sha}")
        print(f"   Rollback SHA: {previous_sha}")
        target_sha = previous_sha
    else:
        print(f"\n   Target SHA: {target_sha}")

    # Verify target scripts exist in S3
    print("\n--- Verifying rollback artifacts ---")
    s3_client = boto3.client("s3", region_name=REGION)
    artifacts_bucket = f"quickcart-artifacts-{environment}"

    try:
        response = s3_client.list_objects_v2(
            Bucket=artifacts_bucket,
            Prefix=f"glue-scripts/{target_sha}/",
            MaxKeys=10
        )
        script_count = response.get("KeyCount", 0)
        if script_count == 0:
            print(f"   ❌ No scripts found at s3://{artifacts_bucket}/glue-scripts/{target_sha}/")
            print(f"   → Cannot rollback to this SHA")
            sys.exit(1)
        print(f"   ✅ Found {script_count} scripts for SHA {target_sha}")
    except Exception as e:
        print(f"   ❌ S3 check failed: {e}")
        sys.exit(1)

    # Execute rollback via CloudFormation update
    print("\n--- Executing rollback ---")
    try:
        # Get current parameters
        stack = cfn_client.describe_stacks(StackName=stack_name)
        current_params = stack["Stacks"][0].get("Parameters", [])

        # Update CommitSha parameter, keep everything else
        new_params = []
        for param in current_params:
            if param["ParameterKey"] == "CommitSha":
                new_params.append({
                    "ParameterKey": "CommitSha",
                    "ParameterValue": target_sha
                })
            else:
                new_params.append({
                    "ParameterKey": param["ParameterKey"],
                    "UsePreviousValue": True
                })

        cfn_client.update_stack(
            StackName=stack_name,
            UsePreviousTemplate=True,
            Parameters=new_params,
            Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
            Tags=[
                {"Key": "RollbackFrom", "Value": str(get_previous_commit_sha(cfn_client, stack_name)[1] or "unknown")},
                {"Key": "RollbackTo", "Value": target_sha},
                {"Key": "RollbackReason", "Value": "automated-or-manual"}
            ]
        )
        print(f"   ✅ CloudFormation update initiated")

        # Wait for completion
        print("   Waiting for rollback to complete...", end="", flush=True)
        waiter = cfn_client.get_waiter("stack_update_complete")
        waiter.wait(
            StackName=stack_name,
            WaiterConfig={"Delay": 10, "MaxAttempts": 60}
        )
        print(" ✅")

        print(f"\n   ✅ ROLLBACK COMPLETE")
        print(f"   All Glue jobs now pointing to: {target_sha}")

    except cfn_client.exceptions.ClientError as e:
        if "No updates" in str(e):
            print(f"   ℹ️  No changes needed — already at {target_sha}")
        else:
            print(f"   ❌ Rollback failed: {e}")
            sys.exit(1)


def rollback_redshift_migration(environment, target_version):
    """
    Rollback Redshift to a specific migration version
    
    Strategy: create a NEW migration that reverses changes
    → We don't DELETE migration records
    → We ADD a rollback migration
    → This preserves audit trail
    """
    print(f"\n--- Redshift Rollback to version {target_version} ---")
    print(f"   ⚠️  Redshift rollback requires manual migration file:")
    print(f"   1. Create: redshift/migrations/NNN_rollback_to_v{target_version}.sql")
    print(f"   2. Include reverse DDL (DROP COLUMN, etc.)")
    print(f"   3. Push through normal CI/CD pipeline")
    print(f"   4. Migration runner will apply it automatically")
    print(f"\n   WHY NOT automatic:")
    print(f"   → DDL rollbacks are destructive (data loss possible)")
    print(f"   → Each rollback is unique — can't generalize")
    print(f"   → Human review required for safety")


def verify_rollback(environment):
    """Post-rollback verification"""
    print("\n--- Post-Rollback Verification ---")

    glue_client = boto3.client("glue", region_name=REGION)

    job_names = [
        f"extract-mariadb-orders-{environment}",
        f"transform-bronze-to-silver-{environment}",
        f"transform-silver-to-gold-{environment}",
        f"clickstream-microbatch-etl-{environment}"
    ]

    for job_name in job_names:
        try:
            response = glue_client.get_job(JobName=job_name)
            script_location = response["Job"]["Command"]["ScriptLocation"]
            # Extract SHA from path
            parts = script_location.split("/")
            sha_index = parts.index("glue-scripts") + 1 if "glue-scripts" in parts else -1
            sha = parts[sha_index] if sha_index > 0 and sha_index < len(parts) else "unknown"

            print(f"   ✅ {job_name}: SHA={sha}")
        except Exception as e:
            print(f"   ❌ {job_name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Rollback data platform deployment")
    parser.add_argument("--environment", required=True, choices=["dev", "staging", "prod"])
    parser.add_argument("--target-sha", help="Specific commit SHA to rollback to")
    parser.add_argument("--redshift-version", type=int, help="Redshift migration version to rollback to")
    parser.add_argument("--verify-only", action="store_true", help="Only verify current state")
    args = parser.parse_args()

    if args.verify_only:
        verify_rollback(args.environment)
        return

    # Rollback Glue jobs
    rollback_glue_jobs(args.environment, args.target_sha)

    # Rollback Redshift (if requested)
    if args.redshift_version:
        rollback_redshift_migration(args.environment, args.redshift_version)

    # Verify
    verify_rollback(args.environment)

    # Notify
    print(f"\n{'=' * 60}")
    print(f"✅ ROLLBACK COMPLETE — {args.environment.upper()}")
    print(f"   Next steps:")
    print(f"   1. Verify dashboards are working")
    print(f"   2. Create incident report PR")
    print(f"   3. Fix the bug on a feature branch")
    print(f"   4. Deploy fix through normal CI/CD pipeline")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
