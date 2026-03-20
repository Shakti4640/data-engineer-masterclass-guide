# file: 28_deploy_scripts_to_ec2.py
# Purpose: Copy our Project 1-4 scripts to the EC2 instance
# DEPENDS ON: Steps 3, 5 (instance running and verified)

import boto3
import subprocess
import os

REGION = "us-east-2"
INSTANCE_NAME = "quickcart-data-worker"
KEY_PATH = os.path.expanduser("~/.ssh/quickcart-data-team-key.pem")
SSH_USER = "ubuntu"
REMOTE_SCRIPTS_DIR = "/home/ubuntu/quickcart/scripts"

# Scripts from Projects 1-4 to deploy
SCRIPTS_TO_DEPLOY = [
    "02_upload_daily_csvs.py",
    "03_generate_test_data.py",
    "04_verify_uploads.py",
    "05_download_from_s3.py",
    "17_analyze_storage.py",
    "22_storage_cost_monitor.py",
]


def get_instance_public_ip(ec2_client, instance_name):
    """Same as in verify script — get public IP by name"""
    response = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [instance_name]},
            {"Name": "instance-state-name", "Values": ["running"]}
        ]
    )
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            public_ip = instance.get("PublicIpAddress")
            if public_ip:
                return public_ip
    return None


def scp_file(local_path, remote_path, public_ip):
    """Copy a file to EC2 via SCP"""
    scp_cmd = [
        "scp",
        "-i", KEY_PATH,
        "-o", "StrictHostKeyChecking=no",
        local_path,
        f"{SSH_USER}@{public_ip}:{remote_path}"
    ]

    try:
        result = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=30)
        return result.returncode == 0, result.stderr.strip()
    except subprocess.TimeoutExpired:
        return False, "SCP timed out"


def deploy_scripts():
    ec2_client = boto3.client("ec2", region_name=REGION)
    public_ip = get_instance_public_ip(ec2_client, INSTANCE_NAME)

    if not public_ip:
        print(f"❌ No running instance found: {INSTANCE_NAME}")
        return

    print("=" * 60)
    print(f"📦 DEPLOYING SCRIPTS TO EC2")
    print(f"   Instance: {INSTANCE_NAME}")
    print(f"   IP: {public_ip}")
    print(f"   Remote dir: {REMOTE_SCRIPTS_DIR}")
    print("=" * 60)

    deployed = 0
    failed = 0

    for script_name in SCRIPTS_TO_DEPLOY:
        local_path = os.path.join(".", script_name)

        if not os.path.exists(local_path):
            print(f"   ⚠️  Skipping (not found locally): {script_name}")
            failed += 1
            continue

        remote_path = f"{REMOTE_SCRIPTS_DIR}/{script_name}"
        success, error = scp_file(local_path, remote_path, public_ip)

        if success:
            print(f"   ✅ Deployed: {script_name}")
            deployed += 1
        else:
            print(f"   ❌ Failed: {script_name} — {error}")
            failed += 1

    # --- SET UP CRON JOB FOR DAILY UPLOAD ---
    print(f"\n📅 SETTING UP CRON JOB FOR DAILY UPLOAD")
    cron_command = (
        f'(crontab -l 2>/dev/null; echo "0 0 * * * '
        f'cd {REMOTE_SCRIPTS_DIR} && /usr/bin/python3 02_upload_daily_csvs.py '
        f'>> /home/ubuntu/quickcart/logs/upload_cron.log 2>&1") | sort -u | crontab -'
    )

    ssh_cmd = [
        "ssh", "-i", KEY_PATH,
        "-o", "StrictHostKeyChecking=no",
        f"{SSH_USER}@{public_ip}",
        cron_command
    ]

    result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
    if result.returncode == 0:
        print(f"   ✅ Cron job set: daily at midnight UTC")
        print(f"   → Runs: python3 02_upload_daily_csvs.py")
        print(f"   → Logs: /home/ubuntu/quickcart/logs/upload_cron.log")
    else:
        print(f"   ❌ Cron setup failed: {result.stderr}")

    # --- VERIFY CRON ---
    verify_cmd = [
        "ssh", "-i", KEY_PATH,
        "-o", "StrictHostKeyChecking=no",
        f"{SSH_USER}@{public_ip}",
        "crontab -l"
    ]
    result = subprocess.run(verify_cmd, capture_output=True, text=True, timeout=10)
    if result.returncode == 0:
        print(f"\n📋 Current crontab:")
        for line in result.stdout.strip().split("\n"):
            print(f"   {line}")

    # --- SUMMARY ---
    print(f"\n{'=' * 60}")
    print(f"📊 DEPLOYMENT SUMMARY")
    print(f"   ✅ Deployed: {deployed} scripts")
    print(f"   ❌ Failed: {failed} scripts")
    print(f"   📅 Cron: daily upload at midnight UTC")
    print(f"\n   ⚠️  IMPORTANT: Scripts currently use hardcoded credentials")
    print(f"   → Project 6 will fix this with IAM Roles")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    deploy_scripts()