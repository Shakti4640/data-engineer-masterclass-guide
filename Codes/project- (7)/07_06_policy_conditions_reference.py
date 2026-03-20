# file: 07_06_policy_conditions_reference.py
# Purpose: Reference for common IAM policy conditions used in real projects

import json


def print_condition_reference():
    """
    Conditions are the MOST POWERFUL and MOST MISUNDERSTOOD part of IAM
    This reference covers conditions you'll use in Projects 1-80
    """
    print("=" * 70)
    print("📋 IAM POLICY CONDITIONS — COMPLETE REFERENCE")
    print("=" * 70)

    conditions = [
        {
            "category": "NETWORK CONDITIONS",
            "items": [
                {
                    "key": "aws:SourceIp",
                    "operator": "IpAddress / NotIpAddress",
                    "example": {"IpAddress": {"aws:SourceIp": "203.0.113.0/24"}},
                    "use_case": "Allow only from office IP range",
                    "gotcha": "Does NOT work if request goes through VPC endpoint"
                },
                {
                    "key": "aws:SourceVpc",
                    "operator": "StringEquals",
                    "example": {"StringEquals": {"aws:SourceVpc": "vpc-abc123"}},
                    "use_case": "Allow only from specific VPC",
                    "gotcha": "Only works with VPC endpoints — not direct internet"
                },
                {
                    "key": "aws:SourceVpce",
                    "operator": "StringEquals",
                    "example": {"StringEquals": {"aws:SourceVpce": "vpce-xyz789"}},
                    "use_case": "Allow only through specific VPC endpoint",
                    "gotcha": "Most restrictive network condition"
                },
                {
                    "key": "aws:SecureTransport",
                    "operator": "Bool",
                    "example": {"Bool": {"aws:SecureTransport": "true"}},
                    "use_case": "Enforce HTTPS only",
                    "gotcha": "Use in Deny with false — not Allow with true"
                }
            ]
        },
        {
            "category": "IDENTITY CONDITIONS",
            "items": [
                {
                    "key": "aws:PrincipalTag/TagKey",
                    "operator": "StringEquals",
                    "example": {"StringEquals": {"aws:PrincipalTag/Team": "DataEng"}},
                    "use_case": "ABAC — tag-based access control (scalable!)",
                    "gotcha": "Tags are case-sensitive — 'dataeng' ≠ 'DataEng'"
                },
                {
                    "key": "aws:PrincipalOrgID",
                    "operator": "StringEquals",
                    "example": {"StringEquals": {"aws:PrincipalOrgID": "o-abc123"}},
                    "use_case": "Allow only from same AWS Organization",
                    "gotcha": "Great for cross-account in same org (Project 56)"
                },
                {
                    "key": "aws:PrincipalAccount",
                    "operator": "StringEquals",
                    "example": {"StringEquals": {"aws:PrincipalAccount": "123456789012"}},
                    "use_case": "Allow only from specific AWS account",
                    "gotcha": "Less flexible than OrgID for multi-account"
                }
            ]
        },
        {
            "category": "S3-SPECIFIC CONDITIONS",
            "items": [
                {
                    "key": "s3:prefix",
                    "operator": "StringLike",
                    "example": {"StringLike": {"s3:prefix": "finance/*"}},
                    "use_case": "Allow ListBucket only for specific prefix",
                    "gotcha": "Only applies to ListBucket — not GetObject"
                },
                {
                    "key": "s3:x-amz-server-side-encryption",
                    "operator": "StringEquals",
                    "example": {"StringEquals": {"s3:x-amz-server-side-encryption": "aws:kms"}},
                    "use_case": "Enforce KMS encryption on uploads",
                    "gotcha": "Deny without this → all non-KMS uploads blocked"
                },
                {
                    "key": "s3:TlsVersion",
                    "operator": "NumericLessThan",
                    "example": {"NumericLessThan": {"s3:TlsVersion": 1.2}},
                    "use_case": "Deny requests using old TLS versions",
                    "gotcha": "Combine with SecureTransport for full protection"
                }
            ]
        },
        {
            "category": "TIME CONDITIONS",
            "items": [
                {
                    "key": "aws:CurrentTime",
                    "operator": "DateGreaterThan / DateLessThan",
                    "example": {
                        "DateGreaterThan": {"aws:CurrentTime": "2025-01-01T00:00:00Z"},
                        "DateLessThan": {"aws:CurrentTime": "2025-12-31T23:59:59Z"}
                    },
                    "use_case": "Temporary access that auto-expires",
                    "gotcha": "UTC timezone — adjust for local time"
                }
            ]
        },
        {
            "category": "MFA CONDITIONS",
            "items": [
                {
                    "key": "aws:MultiFactorAuthPresent",
                    "operator": "Bool",
                    "example": {"Bool": {"aws:MultiFactorAuthPresent": "true"}},
                    "use_case": "Require MFA for sensitive operations",
                    "gotcha": "IAM Roles do NOT have MFA — this blocks role access"
                },
                {
                    "key": "aws:MultiFactorAuthAge",
                    "operator": "NumericLessThan",
                    "example": {"NumericLessThan": {"aws:MultiFactorAuthAge": 3600}},
                    "use_case": "Require recent MFA (within 1 hour)",
                    "gotcha": "Value in seconds — 3600 = 1 hour"
                }
            ]
        }
    ]

    for category in conditions:
        print(f"\n{'─' * 70}")
        print(f"📂 {category['category']}")
        print(f"{'─' * 70}")

        for item in category["items"]:
            print(f"\n  🔑 {item['key']}")
            print(f"     Operator:  {item['operator']}")
            print(f"     Example:   {json.dumps(item['example'])}")
            print(f"     Use Case:  {item['use_case']}")
            print(f"     ⚠️  Gotcha: {item['gotcha']}")

    # Print the condition operator reference
    print(f"\n{'=' * 70}")
    print(f"📋 CONDITION OPERATORS — QUICK REFERENCE")
    print(f"{'=' * 70}")
    print(f"""
    STRING operators:
    → StringEquals           exact match (case-sensitive)
    → StringNotEquals        inverse of StringEquals
    → StringLike             wildcard match (* and ?)
    → StringNotLike          inverse of StringLike
    → StringEqualsIgnoreCase case-insensitive match

    NUMERIC operators:
    → NumericEquals          exact number match
    → NumericLessThan        less than
    → NumericGreaterThan     greater than

    DATE operators:
    → DateEquals             exact date match
    → DateLessThan           before date
    → DateGreaterThan        after date

    BOOLEAN operators:
    → Bool                   true or false

    IP operators:
    → IpAddress              CIDR match
    → NotIpAddress           inverse

    EXISTENCE operators:
    → Null                   key exists (false) or doesn't (true)
    → Example: {{"Null": {{"aws:TokenIssueTime": "false"}}}}
      → "Token issue time must exist" = temp credentials only

    MULTI-VALUE operators (for tag lists):
    → ForAllValues:StringEquals    ALL values must match
    → ForAnyValue:StringEquals     ANY value must match
    """)


if __name__ == "__main__":
    print_condition_reference()