# file: 23_create_key_pair.py
# Purpose: Create SSH key pair for EC2 access
# DEPENDS ON: None (parallel start with S3 projects)

import boto3
import os
import stat
from botocore.exceptions import ClientError

REGION = "us-east-2"
KEY_NAME = "quickcart-data-team-key"
KEY_DIR = os.path.expanduser("~/.ssh")
KEY_PATH = os.path.join(KEY_DIR, f"{KEY_NAME}.pem")


def create_key_pair():
    ec2_client = boto3.client("ec2", region_name=REGION)

    # --- CHECK IF KEY ALREADY EXISTS ---
    try:
        ec2_client.describe_key_pairs(KeyNames=[KEY_NAME])
        print(f"ℹ️  Key pair '{KEY_NAME}' already exists in AWS")
        if os.path.exists(KEY_PATH):
            print(f"   Local key file exists: {KEY_PATH}")
            return KEY_NAME
        else:
            print(f"   ⚠️  Local key file NOT found at {KEY_PATH}")
            print(f"   → If lost, delete key pair and recreate")
            return KEY_NAME
    except ClientError as e:
        if e.response["Error"]["Code"] != "InvalidKeyPair.NotFound":
            raise

    # --- CREATE KEY PAIR ---
    print(f"🔑 Creating key pair: {KEY_NAME}")

    response = ec2_client.create_key_pair(
        KeyName=KEY_NAME,
        KeyType="ed25519",     # Modern, secure, shorter keys
        KeyFormat="pem"
    )

    private_key = response["KeyMaterial"]

    # --- SAVE PRIVATE KEY LOCALLY ---
    os.makedirs(KEY_DIR, exist_ok=True)

    with open(KEY_PATH, "w") as f:
        f.write(private_key)

    # --- SET CORRECT PERMISSIONS ---
    # chmod 400 — owner read only
    # SSH refuses keys with permissions too open
    os.chmod(KEY_PATH, stat.S_IRUSR)

    print(f"✅ Key pair created: {KEY_NAME}")
    print(f"   Private key saved: {KEY_PATH}")
    print(f"   Permissions: 400 (owner read only)")
    print(f"   Key fingerprint: {response['KeyFingerprint']}")

    print(f"\n⚠️  IMPORTANT:")
    print(f"   → This private key is shown ONCE — AWS does not store it")
    print(f"   → If lost, you must create a new key pair")
    print(f"   → NEVER commit this file to Git")
    print(f"   → NEVER share via email or Slack")

    return KEY_NAME


if __name__ == "__main__":
    print("=" * 60)
    print("🔑 CREATING EC2 KEY PAIR")
    print("=" * 60)
    create_key_pair()