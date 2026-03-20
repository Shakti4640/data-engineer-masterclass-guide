# file: 20_03_scheduled_export.py
# Purpose: Run exports on schedule — designed for cron or systemd timer
# Combines: Project 19 load + Project 20 export in sequence
# Cron entry: 0 3 * * * python /path/to/20_03_scheduled_export.py

import sys
import os
import time
import logging
from datetime import date, datetime, timedelta

# ── LOGGING SETUP ──
# Log to file for cron jobs (no terminal to see stdout)
LOG_DIR = "/tmp/quickcart_logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"export_{date.today().strftime('%Y%m%d')}.log")
        ),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def run_scheduled_export():
    """
    Production scheduled export workflow:
    1. Determine which date to export
    2. Run all exports
    3. Log results
    4. Exit with appropriate code for cron monitoring
    """
    # ── DETERMINE EXPORT DATE ──
    # Usually export YESTERDAY's data (today's data may be incomplete)
    if len(sys.argv) > 1:
        export_date_str = sys.argv[1]
        export_date = datetime.strptime(export_date_str, "%Y-%m-%d").date()
    else:
        export_date = date.today() - timedelta(days=1)

    logger.info("=" * 70)
    logger.info(f"SCHEDULED EXPORT — {export_date}")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    start_time = time.time()

    try:
        # Import and run exports
        from importlib import import_module
        exporter = import_module("20_01_export_mariadb_to_s3")

        results = exporter.run_all_exports(export_date=export_date)

        # ── EVALUATE RESULTS ──
        failed_exports = [
            name for name, result in results.items()
            if not result["success"]
        ]

        duration = time.time() - start_time
        total_rows = sum(
            r.get("rows_exported", 0) for r in results.values()
            if r.get("success")
        )

        if failed_exports:
            logger.error(f"FAILED EXPORTS: {', '.join(failed_exports)}")
            logger.info(f"Duration: {duration:.1f}s | Rows: {total_rows:,}")
            logger.info("EXIT CODE: 1 (partial failure)")
            return 1
        else:
            logger.info(f"ALL EXPORTS SUCCEEDED")
            logger.info(f"Duration: {duration:.1f}s | Total rows: {total_rows:,}")
            logger.info("EXIT CODE: 0 (success)")
            return 0

    except Exception as e:
        logger.exception(f"FATAL ERROR: {e}")
        logger.info("EXIT CODE: 2 (fatal error)")
        return 2


def setup_cron_instructions():
    """Print instructions for setting up cron scheduling"""
    script_path = os.path.abspath(__file__)

    print("\n" + "=" * 70)
    print("📅 CRON SETUP INSTRUCTIONS")
    print("=" * 70)
    print(f"""
    OPTION A: Cron (simplest)
    ─────────────────────────
    # Edit crontab
    crontab -e
    
    # Add this line (runs at 3:00 AM daily):
    0 3 * * * cd /path/to/project && /usr/bin/python3 {script_path} >> /tmp/quickcart_logs/cron.log 2>&1
    
    # Verify
    crontab -l
    
    WHY 3:00 AM:
    → Project 1 upload runs at midnight → files land by 00:05 AM
    → Project 19 load runs at 1:00 AM → MariaDB loaded by 1:15 AM
    → Project 20 export at 3:00 AM → exports yesterday's loaded data
    → Gives 2 hour buffer for any upstream delays
    
    
    OPTION B: Systemd Timer (more robust)
    ──────────────────────────────────────
    # /etc/systemd/system/quickcart-export.service
    [Unit]
    Description=QuickCart MariaDB to S3 Export
    
    [Service]
    Type=oneshot
    User=ec2-user
    WorkingDirectory=/path/to/project
    ExecStart=/usr/bin/python3 {script_path}
    Environment=MARIADB_HOST=localhost
    Environment=MARIADB_USER=quickcart_etl
    Environment=MARIADB_PASSWORD=etl_password[REDACTED:PASSWORD]3
    
    # /etc/systemd/system/quickcart-export.timer
    [Unit]
    Description=Run QuickCart export daily at 3 AM
    
    [Timer]
    OnCalendar=*-*-* 03:00:00
    Persistent=true
    
    [Install]
    WantedBy=timers.target
    
    # Enable
    sudo systemctl enable quickcart-export.timer
    sudo systemctl start quickcart-export.timer
    
    # Check status
    systemctl list-timers | grep quickcart
    
    
    OPTION C: EventBridge Scheduler (AWS-native, Project 29/41)
    ────────────────────────────────────────────────────────────
    → EventBridge rule triggers Lambda or Step Functions at 3 AM
    → Lambda invokes this script on EC2 via SSM Run Command
    → Or: Step Functions orchestrates Glue job instead
    → Covered in Projects 29, 41, 55
    
    
    MONITORING:
    ──────────
    → Check log file: cat /tmp/quickcart_logs/export_YYYYMMDD.log
    → Check exit code: echo $? after manual run
    → Set up CloudWatch agent to ship logs (Project 54)
    → Set up SNS alert on failure (Project 54)
    """)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        setup_cron_instructions()
    else:
        exit_code = run_scheduled_export()
        sys.exit(exit_code)