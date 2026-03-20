# file: 06_07_delete_old_keys.py
# Purpose: Permanently delete deactivated keys after 7-day observation
# Run ONLY after confirming no breakage during observation period

import boto3
from datetime import datetime
from botocore.exceptions import ClientError

OLD_IAM_USERNAME = "quickcart-data-uploader"


def delete_old_keys(username):
    """
    PERMANENT deletion — cannot be undone
    Only run after 7+ days of deactivation with zero issues
    """
    iam_client = boto3.client("iam")

    print("=" * 60)
    print(f"🗑️  PERMANENT KEY DELETION — User: {username}")
    print("=" * 60)

    # --- List all keys ---
    response = iam_client.list_access_keys(UserName=username)
    keys = response["AccessKeyMetadata"]

    if not keys:
        print(f"\nℹ️  No keys found for '{username}'")
        return

    for key in keys:
        key_id = key["AccessKeyId"]
        status = key["Status"]
        masked_key = f"{key_id[:8]}...{key_id[-4:]}"

        if status == "Active":
            print(f"\n  ⚠️  {masked_key} is ACTIVE — skipping")
            print(f"     → Deactivate first (06_06), wait 7 days, then delete")
            continue

        # --- Safety confirmation ---
        print(f"\n  🗑️  Deleting {masked_key} (currently {status})")

        try:
            iam_client.delete_access_key(
                UserName=username,
                AccessKeyId=key_id
            )
            print(f"     ✅ PERMANENTLY DELETED")
            print(f"     → This key can never be recovered")
            print(f"     → Any reference to this key is now permanently broken")

        except ClientError as e:
            print(f"     ❌ Failed: {e}")

    # --- Check if user should be deleted entirely ---
    print(f"\n{'=' * 60}")
    print(f"📋 NEXT STEP: Consider deleting IAM user '{username}' entirely")
    print(f"   → User has no access keys now")
    print(f"   → If user has no console access or other policies → DELETE USER")
    print(f"   → Fewer IAM users = smaller attack surface")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    delete_old_keys(OLD_IAM_USERNAME)