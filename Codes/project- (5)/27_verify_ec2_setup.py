# file: 27_verify_ec2_setup.py
# Purpose: SSH into instance programmatically and verify setup
# DEPENDS ON: Step 3 (instance launched and user data completed)
# NOTE: In production, use SSM Run Command instead of paramiko SSH

import boto3
import subprocess
import os
import sys

REGION = "us-east-2"
INSTANCE_NAME = "quickcart-data-worker"
KEY_PATH = os.path.expanduser("~/.ssh/quickcart-data-team-key.pem")
SSH_USER = "ubuntu"


def get_instance_public_ip(ec2_client, instance_name):
    """Get public IP of running instance by name"""
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


def run_ssh_command(public_ip, command, key_path=KEY_PATH, user=SSH_USER):
    """
    Execute command on EC2 via SSH
    
    NOTE: This uses subprocess + system SSH client
    → Requires: ssh command available locally
    → Alternative: paramiko library (pure Python SSH)
    → Better alternative: AWS SSM Run Command (no SSH port needed)
    """
    ssh_cmd = [
        "ssh",
        "-i", key_path,
        "-o", "StrictHostKeyChecking=no",   # Skip host key verification (first connect)
        "-o", "ConnectTimeout=10",
        f"{user}@{public_ip}",
        command
    ]

    try:
        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, result.stderr.strip()

    except subprocess.TimeoutExpired:
        return False, "SSH command timed out"
    except FileNotFoundError:
        return False, "SSH client not found — install OpenSSH"


def verify_setup():
    """Run verification checks on the instance"""
    ec2_client = boto3.client("ec2", region_name=REGION)

    # --- GET IP ---
    public_ip = get_instance_public_ip(ec2_client, INSTANCE_NAME)
    if not public_ip:
        print(f"❌ No running instance found: {INSTANCE_NAME}")
        return

    print("=" * 65)
    print(f"🔍 VERIFYING EC2 SETUP: {INSTANCE_NAME}")
    print(f"   IP: {public_ip}")
    print("=" * 65)

    # --- VERIFICATION CHECKS ---
    checks = [
        ("OS Version",           "cat /etc/os-release | grep PRETTY_NAME"),
        ("Python Version",       "python3 --version"),
        ("Pip Version",          "pip3 --version"),
        ("AWS CLI Version",      "aws --version"),
        ("boto3 Version",        "python3 -c 'import boto3; print(boto3.__version__)'"),
        ("pandas Version",       "python3 -c 'import pandas; print(pandas.__version__)'"),
        ("pymysql Version",      "python3 -c 'import pymysql; print(pymysql.__version__)'"),
        ("psycopg2 Available",   "python3 -c 'import psycopg2; print(psycopg2.__version__)'"),
        ("Disk Space",           "df -h / | tail -1"),
        ("Memory",               "free -h | head -2"),
        ("Swap",                 "swapon --show"),
        ("CPU Count",            "nproc"),
        ("Uptime",               "uptime"),
        ("Project Directory",    "ls -la /home/ubuntu/quickcart/"),
        ("MySQL Client",         "mysql --version"),
        ("PostgreSQL Client",    "psql --version"),
        ("User Data Log Tail",   "tail -5 /var/log/user-data.log"),
    ]

    passed = 0
    failed = 0

    for check_name, command in checks:
        success, output = run_ssh_command(public_ip, command)

        if success:
            # Truncate long outputs
            display_output = output[:70] + "..." if len(output) > 70 else output
            print(f"   ✅ {check_name:<22} → {display_output}")
            passed += 1
        else:
            print(f"   ❌ {check_name:<22} → FAILED: {output[:50]}")
            failed += 1

    # --- SUMMARY ---
    print("\n" + "-" * 65)
    print(f"   ✅ Passed: {passed}/{passed + failed}")
    print(f"   ❌ Failed: {failed}/{passed + failed}")

    if failed == 0:
        print("\n   🎉 INSTANCE FULLY CONFIGURED AND READY")
    else:
        print("\n   ⚠️  Some checks failed — user data may still be running")
        print("   → Wait 3-5 minutes and re-run this script")
        print(f"   → Check logs: ssh -i {KEY_PATH} {SSH_USER}@{public_ip} "
              f"'tail -50 /var/log/user-data.log'")

    print("=" * 65)


def generate_ssh_cheatsheet():
    """Print useful SSH commands for the team"""
    ec2_client = boto3.client("ec2", region_name=REGION)
    public_ip = get_instance_public_ip(ec2_client, INSTANCE_NAME)

    if not public_ip:
        public_ip = "<PUBLIC_IP>"

    print(f"\n📋 SSH CHEAT SHEET FOR {INSTANCE_NAME}")
    print("=" * 70)
    print(f"  # Connect to instance")
    print(f"  ssh -i {KEY_PATH} {SSH_USER}@{public_ip}")
    print()
    print(f"  # Copy file FROM local TO instance")
    print(f"  scp -i {KEY_PATH} local_file.py {SSH_USER}@{public_ip}:/home/ubuntu/quickcart/scripts/")
    print()
    print(f"  # Copy file FROM instance TO local")
    print(f"  scp -i {KEY_PATH} {SSH_USER}@{public_ip}:/home/ubuntu/quickcart/logs/upload.log ./")
    print()
    print(f"  # Copy entire directory FROM local TO instance")
    print(f"  scp -i {KEY_PATH} -r ./scripts/ {SSH_USER}@{public_ip}:/home/ubuntu/quickcart/scripts/")
    print()
    print(f"  # Run a command without interactive SSH")
    print(f"  ssh -i {KEY_PATH} {SSH_USER}@{public_ip} 'python3 /home/ubuntu/quickcart/scripts/upload.py'")
    print()
    print(f"  # Watch live logs")
    print(f"  ssh -i {KEY_PATH} {SSH_USER}@{public_ip} 'tail -f /var/log/user-data.log'")
    print()
    print(f"  # Port forwarding (e.g., for MariaDB on port 3306 — Project 8)")
    print(f"  ssh -i {KEY_PATH} -L 3306:localhost:3306 {SSH_USER}@{public_ip}")
    print("=" * 70)


if __name__ == "__main__":
    verify_setup()
    generate_ssh_cheatsheet()