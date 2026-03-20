# file: 12_copy_error_recovery.py
# When COPY fails, this script diagnoses and suggests fixes

import psycopg2

REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "YourSecurePassword123!"


def diagnose_copy_failure():
    """
    Comprehensive COPY failure diagnosis
    
    COMMON FAILURE CATEGORIES:
    1. Permission errors (IAM role misconfigured)
    2. File format errors (wrong delimiter, missing columns)
    3. Data type errors (string in numeric column)
    4. File not found errors (wrong path, manifest mismatch)
    5. Network errors (S3 throttling, timeout)
    """
    conn = psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB, user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    cursor = conn.cursor()

    print("🔍 COPY FAILURE DIAGNOSTIC REPORT")
    print("=" * 70)

    # --- 1. FIND RECENT FAILED COPY COMMANDS ---
    print("\n📋 1. Recent COPY commands:")
    cursor.execute("""
        SELECT 
            query,
            TRIM(querytxt) as sql_text,
            starttime,
            DATEDIFF(seconds, starttime, endtime) as duration,
            aborted
        FROM stl_query
        WHERE TRIM(querytxt) LIKE 'COPY%'
        ORDER BY starttime DESC
        LIMIT 5;
    """)
    for row in cursor.fetchall():
        status = "❌ ABORTED" if row[4] == 1 else "✅ COMPLETED"
        print(f"   Query {row[0]}: {status} ({row[3]}s)")
        print(f"   SQL: {row[1][:80]}...")
        print()

    # --- 2. ROW-LEVEL ERRORS ---
    print("\n📋 2. Row-level load errors (last 20):")
    cursor.execute("""
        SELECT 
            query,
            TRIM(filename) as file,
            line_number,
            TRIM(colname) as col,
            TRIM(type) as dtype,
            TRIM(raw_field_value) as raw_val,
            TRIM(err_reason) as reason
        FROM stl_load_errors
        ORDER BY starttime DESC
        LIMIT 20;
    """)
    errors = cursor.fetchall()
    if not errors:
        print("   ✅ No row-level errors found")
    else:
        for err in errors:
            print(f"   Query: {err[0]}")
            print(f"   File:   {err[1]}")
            print(f"   Line:   {err[2]}, Column: {err[3]} ({err[4]})")
            print(f"   Value:  '{err[5]}'")
            print(f"   Reason: {err[6]}")

            # --- SUGGEST FIX BASED ON ERROR REASON ---
            reason = err[6].lower() if err[6] else ""
            if "delimiter not found" in reason:
                print(f"   💡 FIX: Check DELIMITER in COPY matches actual file delimiter")
            elif "extra column" in reason:
                print(f"   💡 FIX: CSV has more columns than table — add columns or use column list")
            elif "missing column" in reason:
                print(f"   💡 FIX: CSV has fewer columns than table — use column list or DEFAULT")
            elif "invalid digit" in reason or "numeric" in reason:
                print(f"   💡 FIX: Non-numeric value in numeric column — clean data or use VARCHAR")
            elif "date" in reason or "timestamp" in reason:
                print(f"   💡 FIX: Invalid date format — use DATEFORMAT 'auto' or specify exact format")
            elif "length" in reason:
                print(f"   💡 FIX: Value exceeds VARCHAR length — increase column size")
            print()

    # --- 3. S3 CONNECTION ERRORS ---
    print("\n📋 3. S3 connection errors (last 10):")
    cursor.execute("""
        SELECT 
            TRIM(key) as s3_key,
            TRIM(error) as error_msg,
            recordtime
        FROM stl_s3client_error
        ORDER BY recordtime DESC
        LIMIT 10;
    """)
    s3_errors = cursor.fetchall()
    if not s3_errors:
        print("   ✅ No S3 connection errors")
    else:
        for err in s3_errors:
            print(f"   Key:    {err[0]}")
            print(f"   Error:  {err[1]}")
            print(f"   Time:   {err[2]}")

            error_msg = err[1].lower() if err[1] else ""
            if "forbidden" in error_msg or "403" in error_msg:
                print(f"   💡 FIX: IAM role lacks s3:GetObject permission on this key")
            elif "not found" in error_msg or "404" in error_msg:
                print(f"   💡 FIX: File doesn't exist at this S3 path — check key spelling")
            elif "timeout" in error_msg:
                print(f"   💡 FIX: S3 throttling — reduce concurrent COPYs or add retry")
            elif "slow down" in error_msg:
                print(f"   💡 FIX: S3 rate limiting — add exponential backoff")
            print()

    # --- 4. PERMISSION CHECK ---
    print("\n📋 4. IAM Role verification:")
    cursor.execute("""
        SELECT TRIM(role_name) as role 
        FROM svl_user_info 
        WHERE user_name = current_user;
    """)
    # Alternative: check cluster-level roles
    print("   → Verify role is attached to cluster:")
    print("   → aws redshift describe-clusters --cluster-identifier quickcart-warehouse")
    print("   → Check 'IamRoles' array in output")

    # --- 5. SUGGESTED RECOVERY ACTIONS ---
    print("\n" + "=" * 70)
    print("📋 RECOVERY CHECKLIST:")
    print("=" * 70)
    print("""
   1. Fix the root cause (data format, permissions, missing file)
   2. Check if partial data was loaded:
      SELECT COUNT(*) FROM public.orders;
   3. If partial load exists, TRUNCATE and re-run COPY:
      TRUNCATE TABLE public.orders;
   4. Re-run COPY with MAXERROR to skip bad rows (if acceptable):
      COPY ... MAXERROR 100;
   5. After successful load, check skipped rows:
      SELECT * FROM stl_load_errors WHERE query = pg_last_copy_id();
   6. Fix source data to eliminate errors for next load
    """)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    diagnose_copy_failure()