#!/bin/bash
# file: 09_install_python_deps.sh
# Purpose: Install Python packages needed for MariaDB interaction
# Run as: sudo bash 09_install_python_deps.sh

echo "📦 Installing Python dependencies..."

# pymysql: Pure Python MySQL/MariaDB client
# → No C compiler needed (unlike mysqlclient)
# → Works everywhere Python works
# → Slightly slower than mysqlclient but negligible for our scale
pip3 install pymysql

# boto3: AWS SDK (likely already installed from Project 1)
pip3 install boto3

echo "✅ Dependencies installed"
echo ""
echo "Verify:"
python3 -c "import pymysql; print(f'pymysql version: {pymysql.__version__}')"
python3 -c "import boto3; print(f'boto3 version: {boto3.__version__}')"