#!/bin/bash
# file: 02_run_mariadb_export.sh
# Wrapper script for crontab execution
# Handles: environment setup, virtualenv, logging, exit codes

# ═══════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════
SCRIPT_DIR="/opt/scripts/mariadb-export"
PYTHON="/opt/venv/bin/python"
SCRIPT="${SCRIPT_DIR}/01_mariadb_nightly_export.py"
LOG_DIR="/var/log/mariadb-export"
LOCK_FILE="/tmp/mariadb_export.lock"

# ═══════════════════════════════════════════
# ENVIRONMENT (cron has minimal env)
# ═══════════════════════════════════════════
export PATH="/usr/local/bin:/usr/bin:/bin:${PATH}"
export AWS_DEFAULT_REGION="us-east-2"
export HOME="/home/ec2-user"

# ═══════════════════════════════════════════
# LOCK FILE (prevent concurrent runs)
# ═══════════════════════════════════════════
if [ -f "${LOCK_FILE}" ]; then
    LOCK_PID=$(cat "${LOCK_FILE}")
    if kill -0 "${LOCK_PID}" 2>/dev/null; then
        echo "$(date): Export already running (PID: ${LOCK_PID}) — skipping" >> "${LOG_DIR}/wrapper.log"
        exit 0
    else
        echo "$(date): Stale lock file found — removing" >> "${LOG_DIR}/wrapper.log"
        rm -f "${LOCK_FILE}"
    fi
fi

# Create lock
echo $$ > "${LOCK_FILE}"
trap "rm -f ${LOCK_FILE}" EXIT

# ═══════════════════════════════════════════
# ENSURE LOG DIRECTORY EXISTS
# ═══════════════════════════════════════════
mkdir -p "${LOG_DIR}"

# ═══════════════════════════════════════════
# RUN EXPORT
# ═══════════════════════════════════════════
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/export_${TIMESTAMP}.log"

echo "$(date): Starting MariaDB export" >> "${LOG_DIR}/wrapper.log"

${PYTHON} ${SCRIPT} "$@" >> "${LOG_FILE}" 2>&1
EXIT_CODE=$?

echo "$(date): Export finished with exit code ${EXIT_CODE}" >> "${LOG_DIR}/wrapper.log"

# ═══════════════════════════════════════════
# CLEANUP OLD LOGS (keep 30 days)
# ═══════════════════════════════════════════
find "${LOG_DIR}" -name "export_*.log" -mtime +30 -delete 2>/dev/null

exit ${EXIT_CODE}