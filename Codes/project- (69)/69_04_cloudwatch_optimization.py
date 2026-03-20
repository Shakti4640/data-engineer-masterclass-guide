# file: 69_04_cloudwatch_optimization.py
# Run from: Account B (222222222222)
# Purpose: Reduce CloudWatch Logs costs
# SAVINGS: ~$40/month

import boto3

REGION = "us-east-2"
logs = boto3.client("logs", region_name=REGION)


def set_log_retention():
    """
    Set retention on all log groups
    
    DEFAULT: logs kept FOREVER (infinite retention)
    → 80 GB of Glue logs at $0.50/GB ingestion + $0.03/GB storage
    → Old logs rarely accessed → wasting money
    
    POLICY:
    → Glue job logs: 30 days
    → Lambda logs: 14 days
    → Step Function logs: 30 days
    → VPC Flow Logs: 7 days
    → Other: 30 days default
    """
    print("📋 Setting log retention policies...")

    retention_policies = {
        "/aws-glue/": 30,
        "/aws/lambda/": 14,
        "/aws/states/": 30,
        "/aws/redshift/": 30,
        "vpc-flow-logs": 7
    }

    # List all log groups
    paginator = logs.get_paginator("describe_log_groups")
    total_groups = 0
    updated_groups = 0

    for page in paginator.paginate():
        for group in page["logGroups"]:
            group_name = group["logGroupName"]
            current_retention = group.get("retentionInDays", "∞")
            stored_bytes = group.get("storedBytes", 0)
            stored_gb = stored_bytes / (1024 ** 3)

            # Determine target retention
            target_retention = 30  # default
            for prefix, days in retention_policies.items():
                if prefix in group_name:
                    target_retention = days
                    break

            total_groups += 1

            # Only update if current retention is longer or infinite
            if current_retention == "∞" or (isinstance(current_retention, int) and current_retention > target_retention):
                try:
                    logs.put_retention_policy(
                        logGroupName=group_name,
                        retentionInDays=target_retention
                    )
                    updated_groups += 1
                    print(f"   ✅ {group_name}: {current_retention} → {target_retention} days ({stored_gb:.2f} GB)")
                except Exception as e:
                    print(f"   ❌ {group_name}: {str(e)[:80]}")
            else:
                if stored_gb > 0.01:
                    print(f"   ℹ️  {group_name}: already {current_retention} days ({stored_gb:.2f} GB)")

    print(f"\n   Total groups: {total_groups}")
    print(f"   Updated: {updated_groups}")


def reduce_glue_log_verbosity():
    """
    Reduce Glue job logging from continuous to standard
    
    CONTINUOUS LOGGING: every Spark log line → CloudWatch
    → Very verbose: ~1 GB per job run
    → Cost: $0.50/GB ingestion
    → 30 runs/month: 30 GB = $15/month just for log ingestion
    
    STANDARD LOGGING: only job output + errors
    → ~50 MB per job run
    → 30 runs/month: 1.5 GB = $0.75/month
    """
    glue_client = boto3.client("glue", region_name=REGION)

    print(f"\n📋 Reducing Glue log verbosity...")

    response = glue_client.get_jobs()
    jobs = response.get("Jobs", [])

    for job_def in jobs:
        job_name = job_def["Name"]
        default_args = job_def.get("DefaultArguments", {})

        # Check if continuous logging is enabled
        if default_args.get("--enable-continuous-cloudwatch-log") == "true":
            # Disable continuous logging (keep standard logging)
            default_args.pop("--enable-continuous-cloudwatch-log", None)

            # Keep metrics enabled (lightweight)
            default_args["--enable-metrics"] = "true"

            try:
                job_update = {
                    "Role": job_def["Role"],
                    "Command": job_def["Command"],
                    "DefaultArguments": default_args
                }
                for key in ["NumberOfWorkers", "WorkerType", "GlueVersion",
                            "Connections", "Timeout", "MaxRetries"]:
                    if key in job_def:
                        job_update[key] = job_def[key]

                glue_client.update_job(JobName=job_name, JobUpdate=job_update)
                print(f"   ✅ {job_name}: continuous logging DISABLED (standard logging kept)")

            except Exception as e:
                print(f"   ❌ {job_name}: {str(e)[:80]}")
        else:
            print(f"   ℹ️  {job_name}: continuous logging already off")


if __name__ == "__main__":
    print("=" * 60)
    print("📊 CLOUDWATCH LOG OPTIMIZATION")
    print("=" * 60)

    set_log_retention()
    print()
    reduce_glue_log_verbosity()

    print(f"\n💰 Estimated savings: ~$40/month")
    print(f"   Log retention: eliminates old log storage costs")
    print(f"   Log verbosity: reduces ingestion from ~30 GB to ~1.5 GB/month")