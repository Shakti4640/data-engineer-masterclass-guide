# --- EXECUTION ORDER ---

# 1. Create the bucket (one-time)
python 01_create_bucket.py

# 2. Generate test data (simulates order system export)
python 03_generate_test_data.py

# 3. Upload to S3
python 02_upload_daily_csvs.py

# 4. Verify uploads
python 04_verify_uploads.py

# 5. Download back (optional verification)
python 05_download_from_s3.pyws