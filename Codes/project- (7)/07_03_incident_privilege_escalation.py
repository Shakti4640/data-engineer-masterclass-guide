# file: 07_03_incident_privilege_escalation.py
# Purpose: Demonstrate Permission Boundaries to prevent privilege escalation

import boto3
import json
from botocore.exceptions import ClientError

BOUNDARY_POLICY_NAME = "QuickCart-ContractorBoundary"
CONTRACTOR_USER = "alex-contractor"


def create_permission_boundary():
    """
    Permission Boundary:
    → Sets the MAXIMUM permissions a user can ever have
    → Even if the user creates a new policy with s3:* → boundary caps it
    → Even if someone attaches AdministratorAccess → boundary caps it
    → The effective permissions = INTERSECTION of identity policy and boundary
    
    SCENARIO:
    → Contractor Alex needs s3:GetObject and s3:PutObject
    → Alex also has iam:CreatePolicy (to manage their own Lambda roles)
    → WITHOUT boundary: Alex creates a policy with iam:CreateUser → escalation!
    → WITH boundary: even if Alex creates iam:CreateUser policy → boundary blocks it
    """
    iam_client = boto3.client("iam")

    print("=" * 60)
    print("🔴 INCIDENT 3: Preventing Privilege Escalation")
    print("=" * 60)

    # ================================================================
    # STEP 1: Create the Permission Boundary
    # ================================================================
    boundary_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                # CEILING: Only these S3 actions are allowed (maximum)
                "Sid": "AllowS3Limited",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": "*"
            },
            {
                # CEILING: Only these CloudWatch actions (for monitoring)
                "Sid": "AllowCloudWatchRead",
                "Effect": "Allow",
                "Action": [
                    "cloudwatch:GetMetricData",
                    "cloudwatch:ListMetrics",
                    "logs:GetLogEvents",
                    "logs:DescribeLogGroups"
                ],
                "Resource": "*"
            },
            {
                # CEILING: Allow STS for assuming their own roles
                "Sid": "AllowSTSOwn",
                "Effect": "Allow",
                "Action": "sts:GetCallerIdentity",
                "Resource": "*"
            }
            # NOTICE: NO iam:*, NO ec2:TerminateInstances, NO s3:DeleteObject
            # Even if identity policy grants these → boundary blocks them
        ]
    }

    print("\n📋 Permission Boundary Policy:")
    print(json.dumps(boundary_policy, indent=2))

    try:
        response = iam_client.create_policy(
            PolicyName=BOUNDARY_POLICY_NAME,
            PolicyDocument=json.dumps(boundary_policy),
            Description="Maximum permissions for contractor accounts",
            Tags=[
                {"Key": "Purpose", "Value": "PermissionBoundary"},
                {"Key": "Team", "Value": "Security"}
            ]
        )
        boundary_arn = response["Policy"]["Arn"]
        print(f"\n✅ Boundary policy created: {boundary_arn}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            # Get existing ARN
            account_id = boto3.client("sts").get_caller_identity()["Account"]
            boundary_arn = f"arn:aws:iam::{account_id}:policy/{BOUNDARY_POLICY_NAME}"
            print(f"\nℹ️  Boundary already exists: {boundary_arn}")
        else:
            raise

    # ================================================================
    # STEP 2: Create contractor user WITH boundary
    # ================================================================
    print(f"\n{'─' * 60}")
    print(f"👤 Creating contractor user with boundary attached")
    print(f"{'─' * 60}")

    try:
        iam_client.create_user(
            UserName=CONTRACTOR_USER,
            PermissionsBoundary=boundary_arn,  # ← THE KEY LINE
            Tags=[
                {"Key": "Team", "Value": "Contractor"},
                {"Key": "PermissionBoundary", "Value": "Applied"}
            ]
        )
        print(f"✅ User '{CONTRACTOR_USER}' created with boundary")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            # Attach boundary to existing user
            iam_client.put_user_permissions_boundary(
                UserName=CONTRACTOR_USER,
                PermissionsBoundary=boundary_arn
            )
# file: 07_03_incident_privilege_escalation.py (CONTINUED)

            print(f"ℹ️  User exists — boundary attached to existing user")
        else:
            raise

    # ================================================================
    # STEP 3: Give contractor a seemingly powerful identity policy
    # ================================================================
    contractor_identity_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                # Contractor's identity policy says they can do ALL S3 actions
                "Sid": "S3FullAccess",
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": "*"
            },
            {
                # Contractor's identity policy even says they can manage IAM!
                "Sid": "IAMAccess",
                "Effect": "Allow",
                "Action": [
                    "iam:CreatePolicy",
                    "iam:CreateUser",
                    "iam:AttachUserPolicy",
                    "iam:CreateAccessKey"
                ],
                "Resource": "*"
            }
        ]
    }

    iam_client.put_user_policy(
        UserName=CONTRACTOR_USER,
        PolicyName="ContractorWorkPolicy",
        PolicyDocument=json.dumps(contractor_identity_policy)
    )
    print(f"\n📋 Identity policy attached (seemingly powerful):")
    print(json.dumps(contractor_identity_policy, indent=2))

    # ================================================================
    # STEP 4: Show the EFFECTIVE permissions (intersection)
    # ================================================================
    print(f"\n{'=' * 60}")
    print(f"📐 EFFECTIVE PERMISSION CALCULATION")
    print(f"{'=' * 60}")
    print(f"""
    IDENTITY POLICY grants:         BOUNDARY allows:
    ┌────────────────────────┐     ┌────────────────────────┐
    │ s3:*  (all S3 actions) │     │ s3:GetObject           │
    │ iam:CreatePolicy       │     │ s3:PutObject           │
    │ iam:CreateUser         │     │ s3:ListBucket          │
    │ iam:AttachUserPolicy   │     │ cloudwatch:GetMetric   │
    │ iam:CreateAccessKey    │     │ cloudwatch:ListMetrics │
    │                        │     │ logs:GetLogEvents      │
    │                        │     │ sts:GetCallerIdentity  │
    └────────────────────────┘     └────────────────────────┘
                  │                           │
                  └──────────┬────────────────┘
                             ▼
                    INTERSECTION (effective):
                    ┌────────────────────────┐
                    │ s3:GetObject      ✅   │
                    │ s3:PutObject      ✅   │
                    │ s3:ListBucket     ✅   │
                    │ s3:DeleteObject   ❌   │  ← identity allows, boundary blocks
                    │ s3:DeleteBucket   ❌   │  ← identity allows, boundary blocks
                    │ iam:CreatePolicy  ❌   │  ← identity allows, boundary blocks
                    │ iam:CreateUser    ❌   │  ← identity allows, boundary blocks
                    │ iam:AttachPolicy  ❌   │  ← identity allows, boundary blocks
                    │ iam:CreateAccKey  ❌   │  ← identity allows, boundary blocks
                    └────────────────────────┘
    
    RESULT:
    → Alex CANNOT escalate privileges
    → Alex CANNOT create new IAM users
    → Alex CANNOT delete S3 objects
    → Even if Alex creates a new policy with iam:* → boundary still caps it
    → The boundary is the UNBREAKABLE ceiling
    
    KEY INSIGHT:
    → Permission Boundary is applied at evaluation time, not at creation time
    → Alex CAN create a policy document with iam:CreateUser (the API call succeeds)
    → But USING that policy is blocked by the boundary
    → It's like writing "I can fly" on a piece of paper — doesn't make it true
    """)


def demonstrate_boundary_enforcement():
    """
    Show exactly what happens when contractor tries blocked actions
    """
    print(f"\n{'=' * 60}")
    print(f"🧪 BOUNDARY ENFORCEMENT SIMULATION")
    print(f"{'=' * 60}")

    scenarios = [
        {
            "action": "s3:GetObject",
            "in_identity": True,
            "in_boundary": True,
            "result": "✅ ALLOWED",
            "reason": "Both identity policy and boundary allow it"
        },
        {
            "action": "s3:PutObject",
            "in_identity": True,
            "in_boundary": True,
            "result": "✅ ALLOWED",
            "reason": "Both identity policy and boundary allow it"
        },
        {
            "action": "s3:DeleteObject",
            "in_identity": True,
            "in_boundary": False,
            "result": "🔴 DENIED",
            "reason": "Identity allows (s3:*) but boundary does NOT include DeleteObject"
        },
        {
            "action": "s3:DeleteBucket",
            "in_identity": True,
            "in_boundary": False,
            "result": "🔴 DENIED",
            "reason": "Identity allows (s3:*) but boundary does NOT include DeleteBucket"
        },
        {
            "action": "iam:CreateUser",
            "in_identity": True,
            "in_boundary": False,
            "result": "🔴 DENIED",
            "reason": "Identity explicitly allows but boundary blocks ALL IAM actions"
        },
        {
            "action": "iam:CreateAccessKey",
            "in_identity": True,
            "in_boundary": False,
            "result": "🔴 DENIED",
            "reason": "Privilege escalation BLOCKED by boundary"
        },
        {
            "action": "ec2:TerminateInstances",
            "in_identity": False,
            "in_boundary": False,
            "result": "🔴 DENIED",
            "reason": "Neither identity nor boundary allows it (double denial)"
        },
        {
            "action": "cloudwatch:GetMetricData",
            "in_identity": False,
            "in_boundary": True,
            "result": "🔴 DENIED",
            "reason": "Boundary allows but identity policy does NOT grant it"
        }
    ]

    print(f"\n{'Action':<30} {'Identity':<12} {'Boundary':<12} {'Result':<12}")
    print(f"{'─' * 30} {'─' * 12} {'─' * 12} {'─' * 12}")

    for s in scenarios:
        identity_str = "Allow" if s["in_identity"] else "—"
        boundary_str = "Allow" if s["in_boundary"] else "—"
        print(f"  {s['action']:<28} {identity_str:<12} {boundary_str:<12} {s['result']}")

    print(f"\n📋 DETAILED REASONING:")
    for s in scenarios:
        print(f"\n  {s['action']}: {s['result']}")
        print(f"    → {s['reason']}")

    print(f"""
    ┌──────────────────────────────────────────────────────────────┐
    │ CRITICAL FORMULA:                                            │
    │                                                              │
    │ Effective = Identity Policy  ∩  Permission Boundary          │
    │                                                              │
    │ → Identity Allow + Boundary Allow = ✅ ALLOWED               │
    │ → Identity Allow + Boundary Silent = 🔴 DENIED               │
    │ → Identity Silent + Boundary Allow = 🔴 DENIED               │
    │ → Identity Silent + Boundary Silent = 🔴 DENIED              │
    │                                                              │
    │ Boundary NEVER adds permissions — only RESTRICTS             │
    │ It's a ceiling, not a floor                                  │
    └──────────────────────────────────────────────────────────────┘
    """)


if __name__ == "__main__":
    create_permission_boundary()
    demonstrate_boundary_enforcement()