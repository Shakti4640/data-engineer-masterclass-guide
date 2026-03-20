#!/bin/bash
# file: 01_install_mariadb.sh
# Purpose: Install and secure MariaDB on Amazon Linux 2023
# Run as: sudo bash 01_install_mariadb.sh

set -e  # Exit on any error

echo "============================================"
echo "🚀 INSTALLING MariaDB ON AMAZON LINUX 2023"
echo "============================================"

# --- STEP 1: UPDATE SYSTEM ---
echo ""
echo "📦 Step 1: Updating system packages..."
dnf update -y

# --- STEP 2: INSTALL MariaDB ---
echo ""
echo "📦 Step 2: Installing MariaDB server..."
dnf install -y mariadb105-server mariadb105

# Verify installation
mariadb --version
echo "✅ MariaDB installed"

# --- STEP 3: START AND ENABLE SERVICE ---
echo ""
echo "🔧 Step 3: Starting MariaDB service..."
systemctl start mariadb
systemctl enable mariadb    # Auto-start on reboot
systemctl status mariadb --no-pager
echo "✅ MariaDB running and enabled on boot"

# --- STEP 4: CONFIGURE MY.CNF ---
echo ""
echo "🔧 Step 4: Configuring MariaDB for t3.medium (4 GB RAM)..."

cat > /etc/my.cnf.d/quickcart.cnf << 'EOF'
[mysqld]
# === INNODB SETTINGS ===
# Buffer pool: ~70% of 4 GB RAM = 2.8 GB
innodb_buffer_pool_size = 2800M

# Redo log size: larger = better write performance, slower recovery
innodb_log_file_size = 512M

# Flush log at each commit for durability (ACID)
innodb_flush_log_at_trx_commit = 1

# Use one file per table (easier management, backup, monitoring)
innodb_file_per_table = 1

# === CONNECTION SETTINGS ===
max_connections = 100
wait_timeout = 300
interactive_timeout = 300

# === NETWORK ===
# IMPORTANT: bind to localhost only — no remote access by default
# Change to 0.0.0.0 ONLY when needed + Security Group locked
bind-address = 127.0.0.1
port = 3306

# === CHARACTER SET ===
character-set-server = utf8mb4
collation-server = utf8mb4_unicode_ci

# === LOGGING ===
# Slow query log: captures queries taking > 2 seconds
slow_query_log = 1
slow_query_log_file = /var/log/mariadb/slow-query.log
long_query_time = 2

# General log: OFF in production (too verbose)
general_log = 0

# Error log
log_error = /var/log/mariadb/mariadb.log

# Binary log: needed for point-in-time recovery and replication
log_bin = /var/lib/mysql/mysql-bin
binlog_format = ROW
expire_logs_days = 7

# === SAFETY ===
# Prevent loading files from client machine
local_infile = 0

[client]
default-character-set = utf8mb4
EOF

# Create log directory
mkdir -p /var/log/mariadb
chown mysql:mysql /var/log/mariadb

echo "✅ Configuration written to /etc/my.cnf.d/quickcart.cnf"

# Restart to apply configuration
echo ""
echo "🔄 Restarting MariaDB with new configuration..."
systemctl restart mariadb
echo "✅ MariaDB restarted"

# --- STEP 5: VERIFY ---
echo ""
echo "🔍 Step 5: Verification..."
echo "MariaDB version:"
mysql -e "SELECT VERSION();"
echo ""
echo "InnoDB buffer pool size:"
mysql -e "SHOW VARIABLES LIKE 'innodb_buffer_pool_size';"
echo ""
echo "Bind address:"
mysql -e "SHOW VARIABLES LIKE 'bind_address';"
echo ""
echo "Binary log:"
mysql -e "SHOW VARIABLES LIKE 'log_bin';"

echo ""
echo "============================================"
echo "✅ INSTALLATION COMPLETE"
echo "============================================"
echo ""
echo "NEXT: Run 02_secure_mariadb.sh to secure the installation"