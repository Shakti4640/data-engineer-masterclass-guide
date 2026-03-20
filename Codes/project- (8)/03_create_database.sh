#!/bin/bash
# file: 03_create_database.sh
# Purpose: Create QuickCart database, users, and schema
# Run as: sudo bash 03_create_database.sh

set -e

ROOT_PASSWORD="QuickCart_R00t_2025!"
MYSQL_CMD="mysql -u root -p${ROOT_PASSWORD}"

echo "============================================"
echo "🏗️  CREATING QUICKCART DATABASE"
echo "============================================"

# ===================================================================
# CREATE DATABASE
# ===================================================================
echo ""
echo "📦 Creating database..."
${MYSQL_CMD} -e "CREATE DATABASE IF NOT EXISTS quickcart_db 
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;"
echo "✅ Database 'quickcart_db' created"

# ===================================================================
# CREATE APPLICATION USER (for the web app)
# ===================================================================
echo ""
echo "👤 Creating app_user..."
${MYSQL_CMD} << 'EOF'
-- App user: can read/write but NOT drop tables or manage users
CREATE USER IF NOT EXISTS 'app_user'@'localhost' 
    IDENTIFIED BY 'AppUser_2025!';

GRANT SELECT, INSERT, UPDATE, DELETE 
    ON quickcart_db.* 
    TO 'app_user'@'localhost';

-- NO: DROP, CREATE, ALTER, GRANT — app should never do these
EOF
echo "✅ app_user created (CRUD only)"

# ===================================================================
# CREATE ETL USER (for data extraction — Projects 20, 31)
# ===================================================================
echo ""
echo "👤 Creating etl_user..."
${MYSQL_CMD} << 'EOF'
-- ETL user: READ ONLY — used by Glue/Python to extract data
-- Allow from VPC CIDR so Glue can connect via JDBC
CREATE USER IF NOT EXISTS 'etl_user'@'10.0.%' 
    IDENTIFIED BY 'EtlUser_2025!';

GRANT SELECT 
    ON quickcart_db.* 
    TO 'etl_user'@'10.0.%';

-- Also allow from localhost for testing
CREATE USER IF NOT EXISTS 'etl_user'@'localhost' 
    IDENTIFIED BY 'EtlUser_2025!';

GRANT SELECT 
    ON quickcart_db.* 
    TO 'etl_user'@'localhost';
EOF
echo "✅ etl_user created (SELECT only)"

# ===================================================================
# FLUSH AND VERIFY
# ===================================================================
${MYSQL_CMD} -e "FLUSH PRIVILEGES;"

echo ""
echo "🔍 All users:"
${MYSQL_CMD} -e "SELECT User, Host, plugin FROM mysql.user;"

echo ""
echo "🔍 Grants for app_user:"
${MYSQL_CMD} -e "SHOW GRANTS FOR 'app_user'@'localhost';"

echo ""
echo "🔍 Grants for etl_user:"
${MYSQL_CMD} -e "SHOW GRANTS FOR 'etl_user'@'localhost';"

echo ""
echo "============================================"
echo "✅ DATABASE AND USERS CREATED"
echo "============================================"
echo ""
echo "NEXT: Run 04_create_schema.sql to create tables"