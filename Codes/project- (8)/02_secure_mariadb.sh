#!/bin/bash
# file: 02_secure_mariadb.sh
# Purpose: Secure MariaDB — equivalent to mysql_secure_installation
# Run as: sudo bash 02_secure_mariadb.sh

set -e

echo "============================================"
echo "🔒 SECURING MariaDB"
echo "============================================"

# --- SET ROOT PASSWORD ---
# IMPORTANT: Change this to a strong password in production
ROOT_PASSWORD="QuickCart_R00t_2025!"

echo ""
echo "🔑 Setting root password..."
mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED BY '${ROOT_PASSWORD}';"
echo "✅ Root password set"

# From this point, must use password for root
MYSQL_CMD="mysql -u root -p${ROOT_PASSWORD}"

# --- REMOVE ANONYMOUS USERS ---
echo ""
echo "🧹 Removing anonymous users..."
${MYSQL_CMD} -e "DELETE FROM mysql.user WHERE User='';"
echo "✅ Anonymous users removed"

# --- REMOVE TEST DATABASE ---
echo ""
echo "🧹 Removing test database..."
${MYSQL_CMD} -e "DROP DATABASE IF EXISTS test;"
${MYSQL_CMD} -e "DELETE FROM mysql.db WHERE Db='test' OR Db='test\\_%';"
echo "✅ Test database removed"

# --- DISALLOW REMOTE ROOT LOGIN ---
echo ""
echo "🔒 Disabling remote root login..."
${MYSQL_CMD} -e "DELETE FROM mysql.user WHERE User='root' AND Host NOT IN ('localhost', '127.0.0.1', '::1');"
echo "✅ Root can only login locally"

# --- FLUSH PRIVILEGES ---
${MYSQL_CMD} -e "FLUSH PRIVILEGES;"
echo "✅ Privileges flushed"

# --- VERIFY ---
echo ""
echo "🔍 Remaining users:"
${MYSQL_CMD} -e "SELECT User, Host, plugin FROM mysql.user;"

echo ""
echo "============================================"
echo "✅ MariaDB SECURED"
echo "============================================"
echo ""
echo "Root password stored in this script — in production:"
echo "→ Use AWS Secrets Manager (Project 70)"
echo "→ Never store passwords in scripts"
echo ""
echo "NEXT: Run 03_create_database.sh"