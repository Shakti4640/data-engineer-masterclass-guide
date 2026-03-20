# file: 07_04_incident_role_vs_bucket_policy.py
# Purpose: Demonstrate bucket policy Deny overriding IAM role Allow

import boto3
import json


def demonstrate_role_vs_bucket_conflict():
    """
    SCENARIO:
    → EC2 Instance Profile role has s3:PutObject → Allow
      (from Project 6)
    → Someone updated bucket policy with a Deny for non-HTTPS requests
    → EC2 script suddenly gets AccessDenied
    → DevOps: "But the role says Allow!"
    """
    print("=" * 60)
    print("🔴 INCIDENT 4: Role Allow vs Bucket Policy Deny")
    print("=" * 60)

    # The role's permission policy (from Project 6)
    role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowReadWriteObjects",
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": "arn:aws:s3:::quickcart-raw-data-prod/*"
            }
        ]
    }

    # The UPDATED bucket policy (someone added HTTPS enforcement)
    updated_bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DenyNonHTTPS",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    "arn:aws:s3:::quickcart-raw-data-prod",
                    "arn:aws:s3:::quickcart-raw-data-prod/*"
                ],
                "Condition": {
                    "Bool": {
                        "aws:SecureTransport": "false"
                    }
                }
            },
            {
                "Sid": "DenyOutdatedTLS",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    "arn:aws:s3:::quickcart-raw-data-prod",
                    "arn:aws:s3:::quickcart-raw-data-prod/*"
                ],
                "Condition": {
                    "NumericLessThan": {
                        "s3:TlsVersion": 1.2
                    }
                }
            }
        ]
    }

    print(f"\n📋 IAM Role Policy (from Project 6):")
    print(json.dumps(role_policy, indent=2))

    print(f"\n📋 UPDATED Bucket Policy (someone added this):")
    print(json.dumps(updated_bucket_policy, indent=2))

    print(f"""
    {'=' * 60}
    📊 EVALUATION WALKTHROUGH
    {'=' * 60}
    
    Request: s3:PutObject from EC2 instance
    
    STEP 1: Gather all policies
    → Identity (role) policy: Allow s3:PutObject on bucket/*
    → Resource (bucket) policy: 
       - Deny s3:* WHERE SecureTransport=false
       - Deny s3:* WHERE TLS < 1.2
    
    STEP 2: Check for EXPLICIT DENY
    
    Scenario A — EC2 uses HTTPS (TLS 1.2+):
    → SecureTransport = true → condition "false" does NOT match → Deny SKIPPED
    → TLS version = 1.2 → condition "< 1.2" does NOT match → Deny SKIPPED
    → No Deny applies → proceed to Allow check
    → Role policy says Allow → ✅ GRANTED
    
    Scenario B — EC2 uses HTTP (no TLS):
    → SecureTransport = false → condition matches → EXPLICIT DENY FOUND
    → 🔴 DENIED — regardless of role Allow
    
    Scenario C — EC2 uses HTTPS but TLS 1.0:
    → SecureTransport = true → first Deny skipped
    → TLS version = 1.0 → condition "< 1.2" matches → EXPLICIT DENY FOUND
    → 🔴 DENIED — regardless of role Allow
    
    ROOT CAUSE:
    → boto3 by default uses HTTPS with TLS 1.2+ → Scenario A → works
    → BUT: if someone configured a custom endpoint with HTTP → Scenario B → fails
    → OR: if using an old Python/OpenSSL version → might negotiate TLS 1.0 → fails
    
    FIX:
    → Ensure boto3 uses HTTPS (it does by default)
    → Ensure Python's SSL module supports TLS 1.2+
    → python -c "import ssl; print(ssl.OPENSSL_VERSION)" → must be 1.0.1+
    
    LESSON:
    → The role policy was NEVER the problem
    → The bucket policy Deny was the cause
    → You MUST check ALL policy layers when debugging
    """)


def show_common_bucket_policy_deny_patterns():
    """
    Reference: Common Deny patterns that trip people up
    """
    print(f"\n{'=' * 60}")
    print(f"📋 COMMON BUCKET POLICY DENY PATTERNS")
    print(f"{'=' * 60}")

    patterns = [
        {
            "name": "Deny non-HTTPS",
            "condition": {"Bool": {"aws:SecureTransport": "false"}},
            "trips_when": "Script uses HTTP endpoint or old TLS",
            "fix": "Ensure HTTPS with TLS 1.2+"
        },
        {
            "name": "Deny non-VPC requests",
            "condition": {"StringNotEquals": {"aws:sourceVpc": "vpc-abc123"}},
            "trips_when": "Request comes from outside the VPC (laptop, other VPC)",
            "fix": "Use VPC Endpoint or whitelist source VPC"
        },
        {
            "name": "Deny non-tagged principals",
            "condition": {"StringNotEquals": {"aws:PrincipalTag/Team": "DataEng"}},
            "trips_when": "User/role missing the required tag",
            "fix": "Add the required tag to the principal"
        },
        {
            "name": "Deny without MFA",
            "condition": {"Bool": {"aws:MultiFactorAuthPresent": "false"}},
            "trips_when": "API call made without MFA (roles don't have MFA)",
            "fix": "Use MFA with STS GetSessionToken first"
        },
        {
            "name": "Deny from outside IP range",
            "condition": {"NotIpAddress": {"aws:SourceIp": "10.0.0.0/8"}},
            "trips_when": "Request from public IP, NAT gateway IP changes",
            "fix": "Use VPC-based conditions instead of IP"
        },
        {
            "name": "Deny non-encrypted uploads",
            "condition": {"StringNotEquals": {"s3:x-amz-server-side-encryption": "aws:kms"}},
            "trips_when": "Upload doesn't specify KMS encryption header",
            "fix": "Add ServerSideEncryption='aws:kms' to upload_file ExtraArgs"
        }
    ]

    for p in patterns:
        print(f"\n  🔒 {p['name']}")
        print(f"     Condition: {json.dumps(p['condition'])}")
        print(f"     Trips when: {p['trips_when']}")
        print(f"     Fix: {p['fix']}")


if __name__ == "__main__":
    demonstrate_role_vs_bucket_conflict()
    show_common_bucket_policy_deny_patterns()