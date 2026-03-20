# file: 07_01_incident_admin_denied.py
# Purpose: Demonstrate how Explicit Deny in bucket policy overrides Admin

import boto3
import json

BUCKET_NAME = "quickcart-logs-prod"
REGION = "us-east-2"
ADMIN_USER = "raj"


def setup_incident_scenario():
    """
    Create the scenario:
    1. Bucket policy with Explicit Deny for non-Security team
    2. Admin user "raj" tagged as Team=DataEngineering
    3. Result: raj gets AccessDenied despite AdministratorAccess
    """
    s3_client = boto3.client("s3", region_name=REGION)
    iam_client = boto3.client("iam")

    print("=" * 60)
    print("🔴 INCIDENT 1: Admin Denied by Bucket Policy")
    print("=" * 60)

    # ================================================================
    # STEP 1: Create the restrictive bucket policy
    # ================================================================
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                # This statement DENIES everyone who isn't Security team
                "Sid": "DenyNonSecurityTeam",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/*"
                ],
                "Condition": {
                    "StringNotEquals": {
                        # Only users tagged Team=Security can access
                        "aws:PrincipalTag/Team": "Security"
                    }
                }
            },
            {
                # Explicitly Allow Security team (for clarity)
                "Sid": "AllowSecurityTeam",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}/*",
                "Condition": {
                    "StringEquals": {
                        "aws:PrincipalTag/Team": "Security"
                    }
                }
            }
        ]
    }

    print("\n📋 Bucket Policy being applied:")
    print(json.dumps(bucket_policy, indent=2))

    # Apply the bucket policy
    try:
        s3_client.put_bucket_policy(
            Bucket=BUCKET_NAME,
            Policy=json.dumps(bucket_policy)
        )
        print(f"\n✅ Bucket policy applied to {BUCKET_NAME}")
    except Exception as e:
        print(f"\n⚠️  Could not apply policy (bucket may not exist): {e}")
        print("   → For demonstration, we'll analyze the policy logic")

    # ================================================================
    # STEP 2: Show why Raj (Admin) is denied
    # ================================================================
    print(f"\n{'─' * 60}")
    print(f"📊 EVALUATION WALKTHROUGH FOR USER 'raj':")
    print(f"{'─' * 60}")
    print(f"""
    Request: s3:GetObject on {BUCKET_NAME}/audit-log-2025.csv
    
    Raj's identity:
    → IAM Policy: AdministratorAccess (Allow *)
    → Tag: Team=DataEngineering
    
    EVALUATION:
    
    Step 1: Gather all policies
    → Identity policy: AdministratorAccess → Allow s3:* on *
    → Bucket policy: 
       Statement 1 → Deny s3:* WHERE Team != Security
       Statement 2 → Allow s3:GetObject WHERE Team = Security
    
    Step 2: Check for EXPLICIT DENY
    → Bucket policy Statement 1:
       Effect: Deny
       Condition: aws:PrincipalTag/Team != "Security"
       Raj's tag: Team=DataEngineering → NOT "Security" → condition MATCHES
       → EXPLICIT DENY FOUND ✅
    
    Step 3: STOP — Explicit Deny ALWAYS wins
    → AdministratorAccess Allow is IRRELEVANT
    → 🔴 DENIED
    
    FIX OPTIONS:
    A) Tag Raj as Team=Security (if he should have access)
    B) Add Raj's ARN to a NotPrincipal exception
    C) Remove the Deny condition (weakens security)
    D) Create a separate admin override condition
    """)


def explain_deny_hierarchy():
    """
    Visual explanation of Deny vs Allow precedence
    """
    print(f"\n{'=' * 60}")
    print("📐 DENY vs ALLOW HIERARCHY")
    print(f"{'=' * 60}")
    print(f"""
    ┌─────────────────────────────────────────────────┐
    │           EXPLICIT DENY                          │
    │    ┌─────────────────────────────────────────┐   │
    │    │ Wins over EVERYTHING                     │   │
    │    │ Cannot be overridden                     │   │
    │    │ Found in ANY policy = game over          │   │
    │    └─────────────────────────────────────────┘   │
    │                    │                              │
    │                    ▼                              │
    │           EXPLICIT ALLOW                          │
    │    ┌─────────────────────────────────────────┐   │
    │    │ Only works if no Deny exists             │   │
    │    │ Same-account: identity OR resource       │   │
    │    │ Cross-account: identity AND resource     │   │
    │    └─────────────────────────────────────────┘   │
    │                    │                              │
    │                    ▼                              │
    │           IMPLICIT DENY                           │
    │    ┌─────────────────────────────────────────┐   │
    │    │ DEFAULT state                            │   │
    │    │ Nothing says Allow = Denied              │   │
    │    │ Can be overcome by adding Allow          │   │
    │    │ DIFFERENT from Explicit Deny             │   │
    │    └─────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────┘
    
    ANALOGY:
    → Explicit Deny = "BANNED from the club" (no VIP pass helps)
    → Explicit Allow = "On the guest list" (works if not banned)
    → Implicit Deny = "Not on the list" (can be added later)
    """)


if __name__ == "__main__":
    setup_incident_scenario()
    explain_deny_hierarchy()