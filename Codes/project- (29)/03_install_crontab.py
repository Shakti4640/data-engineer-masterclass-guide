# file: 03_install_crontab.py
# Programmatically installs crontab entry
# Also verifies the installation

import subprocess
import os


def install_crontab():
    """Install nightly export crontab entry"""
    script_path = "/opt/scripts/mariadb-export/02_run_mariadb_export.sh"
    log_dir = "/var/log/mariadb-export"

    # Cron expression: 2:00 AM UTC every day
    cron_entry = f"0 2 * * * {script_path}"

    print("=" * 60)
    print("⏰ INSTALLING CRONTAB ENTRY")
    print("=" * 60)

    # Ensure script is executable
    if os.path.exists(script_path):
        os.chmod(script_path, 0o755)
        print(f"✅ Script is executable: {script_path}")
    else:
        print(f"⚠️  Script not found at {script_path}")
        print(f"   Copy scripts to /opt/scripts/mariadb-export/ first")

    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)
    print(f"✅ Log directory: {log_dir}")

    # Get current crontab
    try:
        result = subprocess.run(
            ["crontab", "-l"],
            capture_output=True, text=True
        )
        current_crontab = result.stdout
    except Exception:
        current_crontab = ""

    # Check if entry already exists
    if script_path in current_crontab:
        print(f"ℹ️  Crontab entry already exists")
        print(f"   {cron_entry}")
        return

    # Add new entry
    new_crontab = current_crontab.rstrip() + "\n" + cron_entry + "\n"

    process = subprocess.Popen(
        ["crontab", "-"],
        stdin=subprocess.PIPE, text=True
    )
    process.communicate(input=new_crontab)

    if process.returncode == 0:
        print(f"✅ Crontab entry installed:")
        print(f"   {cron_entry}")
        print(f"\n   Schedule: Every day at 2:00 AM UTC")
        print(f"   Script:   {script_path}")
    else:
        print(f"❌ Failed to install crontab entry")

    # Verify
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    print(f"\n📋 Current crontab:")
    for line in result.stdout.strip().split("\n"):
        print(f"   {line}")


def verify_cron_running():
    """Verify crond is running"""
    result = subprocess.run(
        ["systemctl", "is-active", "crond"],
        capture_output=True, text=True
    )
    status = result.stdout.strip()

    if status == "active":
        print(f"\n✅ crond service: active")
    else:
        print(f"\n⚠️  crond service: {status}")
        print(f"   Start with: sudo systemctl start crond")
        print(f"   Enable on boot: sudo systemctl enable crond")


if __name__ == "__main__":
    install_crontab()
    verify_cron_running()