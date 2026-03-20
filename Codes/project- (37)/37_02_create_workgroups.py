# file: 37_02_create_workgroups.py
# Purpose: Create Athena workgroups with per-team cost controls
# Run from: Your local machine

import boto3

AWS_REGION = "us-east-2"
S3_BUCKET = "quickcart-datalake-prod"

athena = boto3.client("athena", region_name=AWS_REGION)


def create_workgroup(name, description, scan_limit_bytes, result_prefix,
                     enforce=True, publish_metrics=True):
    """
    Create Athena workgroup with cost controls
    
    WORKGROUP CONTROLS:
    → BytesScannedCutoffPerQuery: max scan per query (cost limit)
    → EnforceWorkGroupConfiguration: analyst can't override settings
    → ResultConfiguration: where results are stored
    → PublishCloudWatchMetricsEnabled: monitoring
    """
    result_location = f"s3://{S3_BUCKET}/athena-results/{result_prefix}/"

    print(f"\n⚙️  Creating workgroup: {name}")
    print(f"   Scan limit: {scan_limit_bytes / (1024**3):.1f} GB/query")
    print(f"   Results: {result_location}")

    try:
        athena.create_work_group(
            Name=name,
            Description=description,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": result_location,
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_S3"
                    }
                },
                "EnforceWorkGroupConfiguration": enforce,
                "PublishCloudWatchMetricsEnabled": publish_metrics,
                "BytesScannedCutoffPerQuery": int(scan_limit_bytes),
                "EngineVersion": {
                    "SelectedEngineVersion": "Athena engine version 3"
                }
            },
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Team", "Value": name.replace("wg-", "")},
                {"Key": "ManagedBy", "Value": "DataEngineering"}
            ]
        )
        print(f"   ✅ Created: {name}")
    except athena.exceptions.InvalidRequestException as e:
        if "already exists" in str(e).lower():
            athena.update_work_group(
                WorkGroup=name,
                Description=description,
                ConfigurationUpdates={
                    "ResultConfigurationUpdates": {
                        "OutputLocation": result_location,
                        "EncryptionConfigurationUpdates": {
                            "EncryptionOption": "SSE_S3"
                        }
                    },
                    "EnforceWorkGroupConfiguration": enforce,
                    "PublishCloudWatchMetricsEnabled": publish_metrics,
                    "BytesScannedCutoffPerQuery": int(scan_limit_bytes)
                }
            )
            print(f"   ✅ Updated: {name}")
        else:
            print(f"   ❌ Error: {e}")


def create_all_workgroups():
    """Create workgroups for all teams"""
    print("=" * 60)
    print("🚀 CREATING ATHENA WORKGROUPS")
    print("=" * 60)

    workgroups = [
        {
            "name": "wg-revenue-team",
            "description": "Revenue analytics team — senior analysts",
            "scan_limit_bytes": 50 * 1024**3,    # 50 GB
            "result_prefix": "revenue-team"
        },
        {
            "name": "wg-marketing-team",
            "description": "Marketing analytics — campaign analysis",
            "scan_limit_bytes": 50 * 1024**3,    # 50 GB
            "result_prefix": "marketing-team"
        },
        {
            "name": "wg-operations-team",
            "description": "Operations team — daily monitoring",
            "scan_limit_bytes": 20 * 1024**3,    # 20 GB
            "result_prefix": "operations-team"
        },
        {
            "name": "wg-junior-analysts",
            "description": "Junior analysts — strict cost limits, learning environment",
            "scan_limit_bytes": 1 * 1024**3,     # 1 GB — strict!
            "result_prefix": "junior-analysts"
        },
        {
            "name": "wg-data-engineering",
            "description": "Data engineering — pipeline development and debugging",
            "scan_limit_bytes": 100 * 1024**3,   # 100 GB
            "result_prefix": "data-engineering"
        },
        {
            "name": "wg-gold-only",
            "description": "Gold layer queries only — extremely cheap, no scan limits needed",
            "scan_limit_bytes": 1 * 1024**3,     # 1 GB (gold tables are tiny)
            "result_prefix": "gold-queries"
        }
    ]

    for wg in workgroups:
        create_workgroup(**wg)

    # Print summary
    print(f"\n{'=' * 60}")
    print("📊 WORKGROUP SUMMARY")
    print(f"{'=' * 60}")
    print(f"   {'Workgroup':<25} {'Scan Limit':>12} {'Monthly Est':>12}")
    print(f"   {'-'*25} {'-'*12} {'-'*12}")

    for wg in workgroups:
        limit_gb = wg["scan_limit_bytes"] / (1024**3)
        # Estimate: 20 queries/day × 30 days × average 50% of limit
        monthly_est = 20 * 30 * (limit_gb * 0.5) * 5 / 1024  # $5/TB
        print(f"   {wg['name']:<25} {limit_gb:>9.0f} GB   ${monthly_est:>8.0f}")

    print(f"\n   To assign analyst to workgroup:")
    print(f"   → Athena Console: specify workgroup when running queries")
    print(f"   → API: WorkGroup parameter in start_query_execution()")
    print(f"   → IAM: Restrict user to specific workgroup via IAM policy")


if __name__ == "__main__":
    create_all_workgroups()
    print("\n🏁 DONE")