# file: 07_backup_to_s3.py
# Purpose: Dump MariaDB database and upload to S3
# Reuses S3 upload knowledge from Project 1
# Schedule via cron for daily backups

import subprocess
import boto3
import os
import gzip
import shutil
from datetime import datetime
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
DB_HOST = "127.0.0.1"
DB_USER = "root"
DB_PASSWORD = "QuickCart_R00t_2025!"
DB_NAME = "quickcart_db"

BACKUP_DIR = "/tmp/db_backups"
BUCKET_NAME = "quickcart-raw-data-prod"      # Same bucket from Project 1
S3_PREFIX = "db_backups/mariadb"
REGION = "us-east-2"

# Keep local backups for 3 days only (disk is limited on EC2)
LOCAL_RETENTION_DAYS = 3


def create_backup():
    """
    Run mysqldump to create SQL backup file
    
    mysqldump outputs:
    → CREATE TABLE statements
    → INSERT statements for all rows
    → Can restore entire database from this single file
    """
    os.makedirs(BACKUP_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_file = os.path.join(BACKUP_DIR, f"{DB_NAME}_{timestamp}.sql")
    gz_file = f"{sql_file}.gz"
    
    print(f"📦 Creating backup: {sql_file}")
    
    # --- RUN MYSQLDUMP ---
    # --single-transaction: consistent snapshot without locking tables
    #   (CRITICAL for InnoDB — allows reads during backup)
    # --routines: include stored procedures (Project 49)
    # --triggers: include triggers
    # --quick: don't buffer entire table in memory
    # --lock-tables=false: don't lock (single-transaction handles consistency)
    
    cmd = [
        "mysqldump",
        f"--host={DB_HOST}",
        f"--user={DB_USER}",
        f"--password={DB_PASSWORD}",
        "--single-transaction",
        "--routines",
        "--triggers",
        "--quick",
        "--lock-tables=false",
        DB_NAME
    ]
    
    try:
        with open(sql_file, "w") as f:
            result = subprocess.run(
                cmd,
                stdout=f,
                stderr=subprocess.PIPE,
                text=True,
                timeout=300        # 5 minute timeout
            )
        
        if result.returncode != 0:
            print(f"❌ mysqldump failed: {result.stderr}")
            return None
        
        sql_size_mb = round(os.path.getsize(sql_file) / (1024 * 1024), 2)
        print(f"   ✅ SQL dump: {sql_size_mb} MB")
        
        # --- COMPRESS WITH GZIP ---
        # Reduces upload size by 80-90% for SQL files
        print(f"🗜️  Compressing...")
        with open(sql_file, "rb") as f_in:
            with gzip.open(gz_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        gz_size_mb = round(os.path.getsize(gz_file) / (1024 * 1024), 2)
        compression_ratio = round((1 - gz_size_mb / sql_size_mb) * 100, 1) if sql_size_mb > 0 else 0
        print(f"   ✅ Compressed: {gz_size_mb} MB ({compression_ratio}% reduction)")
        
        # Remove uncompressed file to save disk
        os.remove(sql_file)
        
        return gz_file
        
    except subprocess.TimeoutExpired:
        print("❌ mysqldump timed out after 5 minutes")
        return None
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        return None


def upload_backup_to_s3(local_path):
    """
    Upload backup file to S3
    Reuses concepts from Project 1: bucket, key structure, metadata
    """
    s3_client = boto3.client("s3", region_name=REGION)
    
    filename = os.path.basename(local_path)
    today = datetime.now().strftime("%Y/%m/%d")
    s3_key = f"{S3_PREFIX}/{today}/{filename}"
    
    print(f"\n📤 Uploading to s3://{BUCKET_NAME}/{s3_key}")
    
    try:
        s3_client.upload_file(
            Filename=local_path,
            Bucket=BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={
                "ContentType": "application/gzip",
                "ServerSideEncryption": "AES256",
                "Metadata": {
                    "backup-type": "full",
                    "database": DB_NAME,
                    "source-host": DB_HOST,
                    "backup-tool": "mysqldump",
                    "backup-date": datetime.now().isoformat()
                }
            }
        )
        
        # Verify upload — same pattern as Project 1
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        remote_size = response["ContentLength"]
        local_size = os.path.getsize(local_path)
        
        if remote_size == local_size:
            print(f"   ✅ Upload verified — {remote_size} bytes")
            return True
        else:
            print(f"   ⚠️  Size mismatch! Local: {local_size}, Remote: {remote_size}")
            return False
            
    except ClientError as e:
        print(f"   ❌ Upload failed: {e}")
        return False


def cleanup_old_local_backups():
    """Remove local backups older than retention period"""
    import glob
    from datetime import timedelta
    
    cutoff = datetime.now() - timedelta(days=LOCAL_RETENTION_DAYS)
    
    for filepath in glob.glob(os.path.join(BACKUP_DIR, "*.sql.gz")):
        file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        if file_time < cutoff:
            os.remove(filepath)
            print(f"🧹 Removed old backup: {os.path.basename(filepath)}")


def run_backup():
    """Main backup orchestrator"""
    print("=" * 60)
    print(f"🚀 DATABASE BACKUP — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Step 1: Create backup
    backup_file = create_backup()
    if not backup_file:
        print("\n❌ BACKUP FAILED — no file created")
        return 1
    
    # Step 2: Upload to S3
    success = upload_backup_to_s3(backup_file)
    if not success:
        print("\n❌ BACKUP FAILED — upload unsuccessful")
        return 1
    
    # Step 3: Cleanup old local backups
    cleanup_old_local_backups()
    
    print("\n" + "=" * 60)
    print("✅ BACKUP COMPLETE")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit_code = run_backup()
    exit(exit_code)