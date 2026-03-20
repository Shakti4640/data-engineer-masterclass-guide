# file: 07_results_lifecycle.py
# Athena saves query results as CSV in S3
# Without cleanup: thousands of result files accumulate
# This sets up automatic deletion after 7 days

import boto3

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def setup_results_lifecycle():
    """
    Set S3 lifecycle policy to auto-delete Athena query results
    
    WHY:
    → 200 queries/day = 200 result CSVs + 200 metadata files = 400 objects/day
    → 400 × 30 days = 12,000 objects/month
    → Each result: 1 KB – 100 MB depending on query
    → Storage cost is small but clutter is real
    → 7-day retention: analysts can re-download recent results
    → Beyond 7 days: re-run the query if needed
    
    NOTE: This does NOT affect the source data tables
    → Only affects athena-results/ prefix
    → Source data under daily_exports/ and exports/ is untouched
    """
    s3_client = boto3.client("s3", region_name=REGION)

    lifecycle_config = {
        "Rules": [
            {
                "ID": "DeleteAthenaResults-Finance-7days",
                "Filter": {
                    "Prefix": "athena-results/finance/"
                },
                "Status": "Enabled",
                "Expiration": {
                    "Days": 7
                },
                "AbortIncompleteMultipartUpload": {
                    "DaysAfterInitiation": 1
                }
            },
            {
                "ID": "DeleteAthenaResults-Marketing-7days",
                "Filter": {
                    "Prefix": "athena-results/marketing/"
                },
                "Status": "Enabled",
                "Expiration": {
                    "Days": 7
                },
                "AbortIncompleteMultipartUpload": {
                    "DaysAfterInitiation": 1
                }
            },
            {
                "ID": "DeleteAthenaResults-Product-7days",
                "Filter": {
                    "Prefix": "athena-results/product/"
                },
                "Status": "Enabled",
                "Expiration": {
                    "Days": 7
                },
                "AbortIncompleteMultipartUpload": {
                    "DaysAfterInitiation": 1
                }
            },
            {
                "ID": "DeleteAthenaResults-Engineering-14days",
                "Filter": {
                    "Prefix": "athena-results/engineering/"
                },
                "Status": "Enabled",
                "Expiration": {
                    "Days": 14     # Engineering gets 14 days (debugging)
                },
                "AbortIncompleteMultipartUpload": {
                    "DaysAfterInitiation": 1
                }
            }
        ]
    }

    # NOTE: put_bucket_lifecycle_configuration REPLACES existing rules
    # Must include existing rules if any (from Project 4 lifecycle)
    # For safety: get existing rules first, merge, then put

    try:
        existing = s3_client.get_bucket_lifecycle_configuration(Bucket=BUCKET_NAME)
        existing_rules = existing.get("Rules", [])
        
        # Remove old athena rules if they exist
        existing_rules = [
            r for r in existing_rules 
            if not r.get("ID", "").startswith("DeleteAthenaResults")
        ]
        
        # Merge
        all_rules = existing_rules + lifecycle_config["Rules"]
        
    except s3_client.exceptions.ClientError:
        # No existing lifecycle config
        all_rules = lifecycle_config["Rules"]

    s3_client.put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME,
        LifecycleConfiguration={"Rules": all_rules}
    )

    print("✅ Athena result lifecycle policies set:")
    for rule in lifecycle_config["Rules"]:
        prefix = rule["Filter"]["Prefix"]
        days = rule["Expiration"]["Days"]
        print(f"   {prefix} → auto-delete after {days} days")

    print(f"\n💡 Source data is NOT affected")
    print(f"   Only athena-results/ prefix has expiration")


if __name__ == "__main__":
    setup_results_lifecycle()