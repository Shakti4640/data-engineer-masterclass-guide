# file: 06_06_deactivate_old_keys.py
# Purpose: Safely deactivate old IAM user access keys after confirming role works

import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
# The IAM user whose keys were hardcoded on EC2 instances
OLD_IAM_USERNAME = "quickcart-data-uploader"  # Replace with actual username


def deactivate_old_keys(username):
    """
    SAFE DECOMMISSION PROCESS:
    Step 1: List all access keys for the user
    Step 2: Deactivate them (not delete — reversible)
    Step 3: Wait 7 days observing CloudTrail for any usage
    Step 4: If no usage → delete permanently
    
    WHY NOT DELETE IMMEDIATELY:
    → There might be a forgotten script somewhere still using these keys
    → Deactivation is REVERSIBLE — deletion is NOT
    → 7-day observation catches edge cases (weekly cron jobs, etc.)
    """
    iam_client = boto3.client("iam")

    print("=" * 60)
    print(f"🔑 ACCESS KEY DECOMMISSION — User: {username}")
    print("=" * 60)

    # ================================================================
    # STEP 1: List all access keys for this IAM user
    # ================================================================
    try:
        response = iam_client.list_access_keys(UserName=username)
        keys = response["AccessKeyMetadata"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            print(f"\n❌ IAM User '{username}' does not exist")
            return
        raise

    if not keys:
        print(f"\nℹ️  No access keys found for user '{username}'")
        return

    print(f"\n📋 Found {len(keys)} access key(s):")
    print("-" * 70)
    print(f"  {'Access Key ID':<25} {'Status':<12} {'Created':<22} {'Age (days)'}")
    print("-" * 70)

    for key in keys:
        key_id = key["AccessKeyId"]
        status = key["Status"]
        created = key["CreateDate"]
        age_days = (datetime.now(created.tzinfo) - created).days

        # Mask the key for display (show first 8 and last 4 only)
        masked_key = f"{key_id[:8]}...{key_id[-4:]}"
        print(f"  {masked_key:<25} {status:<12} {created.strftime('%Y-%m-%d %H:%M'):<22} {age_days} days")

        # Flag keys older than 90 days
        if age_days > 90:
            print(f"  ⚠️  Key is {age_days} days old — exceeds 90-day rotation policy")

    # ================================================================
    # STEP 2: Get last-used information for each key
    # ================================================================
    print(f"\n📊 Last Usage Details:")
    print("-" * 70)

    for key in keys:
        key_id = key["AccessKeyId"]
        masked_key = f"{key_id[:8]}...{key_id[-4:]}"

        try:
            usage = iam_client.get_access_key_last_used(AccessKeyId=key_id)
            last_used_info = usage["AccessKeyLastUsed"]

            if "LastUsedDate" in last_used_info:
                last_used = last_used_info["LastUsedDate"]
                service = last_used_info.get("ServiceName", "Unknown")
                region = last_used_info.get("Region", "Unknown")
                days_since = (datetime.now(last_used.tzinfo) - last_used).days

                print(f"  {masked_key}:")
                print(f"    Last Used:   {last_used.strftime('%Y-%m-%d %H:%M')} ({days_since} days ago)")
                print(f"    Service:     {service}")
                print(f"    Region:      {region}")

                if days_since < 7:
                    print(f"    ⚠️  RECENTLY USED — verify all scripts are migrated before deactivating")
            else:
                print(f"  {masked_key}: NEVER USED")

        except ClientError as e:
            print(f"  {masked_key}: Error checking usage — {e}")

    # ================================================================
    # STEP 3: Deactivate keys (safe — reversible)
    # ================================================================
    print(f"\n🔒 DEACTIVATING KEYS:")
    print("-" * 70)

    for key in keys:
        key_id = key["AccessKeyId"]
        masked_key = f"{key_id[:8]}...{key_id[-4:]}"
        status = key["Status"]

        if status == "Inactive":
            print(f"  {masked_key}: Already inactive — skipping")
            continue

        try:
            iam_client.update_access_key(
                UserName=username,
                AccessKeyId=key_id,
                Status="Inactive"
            )
            print(f"  {masked_key}: ✅ DEACTIVATED")
            print(f"    → Any script using this key will now get AccessDenied")
            print(f"    → Can reactivate with: Status='Active' if needed")

        except ClientError as e:
            print(f"  {masked_key}: ❌ Failed to deactivate — {e}")

    # ================================================================
    # STEP 4: Print monitoring instructions
    # ================================================================
    print(f"\n{'=' * 60}")
    print(f"📋 POST-DEACTIVATION CHECKLIST")
    print(f"{'=' * 60}")
    print(f"""
  1. MONITOR for 7 days:
     → Check CloudTrail for AccessDenied events from this user
     → If any service breaks → reactivate temporarily → fix → deactivate

  2. After 7 days with no issues, DELETE the keys permanently:
     → Run: 06_07_delete_old_keys.py

  3. Consider DELETING the IAM user entirely:
     → If no other purpose, the user is now useless
     → Less users = smaller attack surface

  4. Document the migration:
     → Note in runbook: "EC2 uses IAM Role since {date}"
     → Update architecture diagrams
    """)

    return keys


def reactivate_key_emergency(username, key_id):
    """
    EMERGENCY ROLLBACK: Reactivate a key if something breaks
    Only use during the 7-day observation period
    """
    iam_client = boto3.client("iam")

    print(f"\n🚨 EMERGENCY REACTIVATION")
    print(f"   User: {username}")
    print(f"   Key:  {key_id[:8]}...{key_id[-4:]}")

    iam_client.update_access_key(
        UserName=username,
        AccessKeyId=key_id,
        Status="Active"
    )
    print(f"   ✅ Key reactivated")
    print(f"   ⚠️  This is temporary — fix the root cause and deactivate again")


if __name__ == "__main__":
    deactivate_old_keys(OLD_IAM_USERNAME)