#!/bin/bash
# file: 06_03_cleanup_old_credentials.sh
# Purpose: Run ON the EC2 instance to remove static credentials
# Usage: ssh into EC2, then: bash 06_03_cleanup_old_credentials.sh

echo "=========================================="
echo "🧹 CREDENTIAL CLEANUP SCRIPT"
echo "=========================================="

# --- Step 1: Check for credential files ---
echo ""
echo "📂 Checking for credential files..."

CRED_FILE="$HOME/.aws/credentials"
CONFIG_FILE="$HOME/.aws/config"

if [ -f "$CRED_FILE" ]; then
    echo "   ⚠️  Found: $CRED_FILE"
    echo "   Contents (masked):"
    grep -c "aws_access_key_id" "$CRED_FILE" && echo "   → Contains access key(s)"
    
    # Backup before deleting (paranoia)
    BACKUP="$CRED_FILE.backup.$(date +%Y%m%d_%H%M%S)"
    cp "$CRED_FILE" "$BACKUP"
    echo "   📋 Backed up to: $BACKUP"
    
    # Remove the credentials file
    rm "$CRED_FILE"
    echo "   ✅ Deleted: $CRED_FILE"
else
    echo "   ✅ No credential file found (good!)"
fi

# --- Step 2: Check for environment variables ---
echo ""
echo "🔍 Checking for credential environment variables..."

if [ -n "$AWS_ACCESS_KEY_ID" ]; then
    echo "   ⚠️  AWS_ACCESS_KEY_ID is set in environment"
    echo "   → Unset it: unset AWS_ACCESS_KEY_ID"
    echo "   → Also check ~/.bashrc, ~/.bash_profile, /etc/environment"
    unset AWS_ACCESS_KEY_ID
    unset AWS_SECRET_ACCESS_KEY
    unset AWS_SESSION_TOKEN
    echo "   ✅ Environment variables unset for this session"
else
    echo "   ✅ No credential env vars found (good!)"
fi

# --- Step 3: Check persistent env var files ---
echo ""
echo "🔍 Checking shell config files for hardcoded keys..."

for FILE in ~/.bashrc ~/.bash_profile ~/.profile /etc/environment; do
    if [ -f "$FILE" ]; then
        if grep -q "AWS_ACCESS_KEY_ID\|AWS_SECRET_ACCESS_KEY" "$FILE"; then
            echo "   ⚠️  Found AWS keys in: $FILE"
            echo "   → MANUALLY remove the lines containing AWS_ACCESS_KEY_ID"
            echo "   → MANUALLY remove the lines containing AWS_SECRET_ACCESS_KEY"
        fi
    fi
done

# --- Step 4: Verify IAM Role is working ---
echo ""
echo "=========================================="
echo "🔐 VERIFYING IAM ROLE CREDENTIALS"
echo "=========================================="

echo ""
echo "📡 Calling: aws sts get-caller-identity"
IDENTITY=$(aws sts get-caller-identity 2>&1)

if echo "$IDENTITY" | grep -q "assumed-role"; then
    echo "   ✅ SUCCESS — Using IAM Role!"
    echo "$IDENTITY" | python3 -m json.tool 2>/dev/null || echo "$IDENTITY"
elif echo "$IDENTITY" | grep -q "Unable to locate credentials"; then
    echo "   ❌ FAILED — No credentials found"
    echo "   → Instance Profile may not be attached yet"
    echo "   → Run: aws ec2 describe-iam-instance-profile-associations"
else
    echo "   ⚠️  Unexpected response:"
    echo "$IDENTITY"
fi

# --- Step 5: Test S3 access ---
echo ""
echo "📡 Testing S3 access..."
BUCKET="quickcart-raw-data-prod"
LIST_RESULT=$(aws s3 ls "s3://$BUCKET/" 2>&1)

if echo "$LIST_RESULT" | grep -q "PRE\|20[0-9][0-9]-"; then
    echo "   ✅ S3 ListBucket works!"
    echo "   First few results:"
    echo "$LIST_RESULT" | head -5
elif echo "$LIST_RESULT" | grep -q "AccessDenied"; then
    echo "   ❌ AccessDenied — check role's permission policy"
elif echo "$LIST_RESULT" | grep -q "NoSuchBucket"; then
    echo "   ❌ Bucket doesn't exist — run 01_create_bucket.py first"
else
    echo "   ⚠️  Unexpected: $LIST_RESULT"
fi

echo ""
echo "=========================================="
echo "✅ CLEANUP COMPLETE"
echo "=========================================="