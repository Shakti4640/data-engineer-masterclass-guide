# file: 07_02_incident_mystery_access.py
# Purpose: Trace exactly HOW a user has access — through which policy path

import boto3
import json
from botocore.exceptions import ClientError


def trace_user_access(username, bucket_name, action="s3:GetObject"):
    """
    Complete access trace:
    Check EVERY possible path through which a user could have access
    """
    iam_client = boto3.client("iam")
    s3_client = boto3.client("s3")

    print("=" * 60)
    print(f"🔍 ACCESS TRACE — User: {username}")
    print(f"   Action: {action}")
    print(f"   Resource: arn:aws:s3:::{bucket_name}/*")
    print("=" * 60)

    resource_arn = f"arn:aws:s3:::{bucket_name}/*"
    access_paths_found = []

    # ================================================================
    # PATH 1: Direct user inline policies
    # ================================================================
    print(f"\n📋 PATH 1: User Inline Policies")
    print(f"{'─' * 50}")
    try:
        response = iam_client.list_user_policies(UserName=username)
        inline_policies = response["PolicyNames"]

        if not inline_policies:
            print(f"   (none)")
        else:
            for policy_name in inline_policies:
                policy_response = iam_client.get_user_policy(
                    UserName=username,
                    PolicyName=policy_name
                )
                document = policy_response["PolicyDocument"]
                match = check_policy_grants_access(document, action, resource_arn)
                status = "✅ GRANTS ACCESS" if match else "⬜ No match"
                print(f"   {policy_name}: {status}")
                if match:
                    access_paths_found.append(f"User inline policy: {policy_name}")

    except ClientError as e:
        print(f"   Error: {e}")

    # ================================================================
    # PATH 2: Direct user managed policies
    # ================================================================
    print(f"\n📋 PATH 2: User Managed Policies")
    print(f"{'─' * 50}")
    try:
        response = iam_client.list_attached_user_policies(UserName=username)
        managed_policies = response["AttachedPolicies"]

        if not managed_policies:
            print(f"   (none)")
        else:
            for policy in managed_policies:
                policy_arn = policy["PolicyArn"]
                policy_name = policy["PolicyName"]

                # Get the policy document
                version_response = iam_client.get_policy(PolicyArn=policy_arn)
                version_id = version_response["Policy"]["DefaultVersionId"]

                version_doc = iam_client.get_policy_version(
                    PolicyArn=policy_arn,
                    VersionId=version_id
                )
                document = version_doc["PolicyVersion"]["Document"]
                match = check_policy_grants_access(document, action, resource_arn)
                status = "✅ GRANTS ACCESS" if match else "⬜ No match"
                print(f"   {policy_name} ({policy_arn}): {status}")
                if match:
                    access_paths_found.append(f"User managed policy: {policy_name}")

    except ClientError as e:
        print(f"   Error: {e}")

    # ================================================================
    # PATH 3: Group policies (user inherits group permissions)
    # ================================================================
    print(f"\n📋 PATH 3: Group Policies (inherited)")
    print(f"{'─' * 50}")
    try:
        groups_response = iam_client.list_groups_for_user(UserName=username)
        groups = groups_response["Groups"]

        if not groups:
            print(f"   User is not in any groups")
        else:
            for group in groups:
                group_name = group["GroupName"]
                print(f"\n   📁 Group: {group_name}")

                # Check group inline policies
                group_inline = iam_client.list_group_policies(GroupName=group_name)
                for gp_name in group_inline["PolicyNames"]:
                    gp_doc = iam_client.get_group_policy(
                        GroupName=group_name,
                        PolicyName=gp_name
                    )
                    document = gp_doc["PolicyDocument"]
                    match = check_policy_grants_access(document, action, resource_arn)
                    status = "✅ GRANTS ACCESS" if match else "⬜ No match"
                    print(f"      Inline: {gp_name}: {status}")
                    if match:
                        access_paths_found.append(
                            f"Group '{group_name}' inline policy: {gp_name}"
                        )

                # Check group managed policies
                group_managed = iam_client.list_attached_group_policies(
                    GroupName=group_name
                )
                for gm_policy in group_managed["AttachedPolicies"]:
                    gm_arn = gm_policy["PolicyArn"]
                    gm_name = gm_policy["PolicyName"]

                    gm_ver = iam_client.get_policy(PolicyArn=gm_arn)
                    gm_ver_id = gm_ver["Policy"]["DefaultVersionId"]
                    gm_doc = iam_client.get_policy_version(
                        PolicyArn=gm_arn, VersionId=gm_ver_id
                    )
                    document = gm_doc["PolicyVersion"]["Document"]
                    match = check_policy_grants_access(document, action, resource_arn)
                    status = "✅ GRANTS ACCESS" if match else "⬜ No match"
                    print(f"      Managed: {gm_name}: {status}")
                    if match:
                        access_paths_found.append(
                            f"Group '{group_name}' managed policy: {gm_name}"
                        )

    except ClientError as e:
        print(f"   Error: {e}")

    # ================================================================
    # PATH 4: Resource-based policy (bucket policy)
    # ================================================================
    print(f"\n📋 PATH 4: Bucket Policy (Resource-Based)")
    print(f"{'─' * 50}")
    try:
        bp_response = s3_client.get_bucket_policy(Bucket=bucket_name)
        bucket_policy = json.loads(bp_response["Policy"])

        # Get user ARN for matching
        user_info = iam_client.get_user(UserName=username)
        user_arn = user_info["User"]["Arn"]
        account_id = user_arn.split(":")[4]

        for stmt in bucket_policy.get("Statement", []):
            principal = stmt.get("Principal", {})
            effect = stmt.get("Effect", "")
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]

            # Check if principal matches
            principal_match = False
            if principal == "*":
                principal_match = True
            elif isinstance(principal, dict):
                aws_principals = principal.get("AWS", [])
                if isinstance(aws_principals, str):
                    aws_principals = [aws_principals]
                for p in aws_principals:
                    if p == "*" or p == user_arn or p == account_id:
                        principal_match = True

            # Check if action matches
            action_match = any(
                a == action or a == "s3:*" or a == "*"
                for a in actions
            )

            if principal_match and action_match:
                conditions = stmt.get("Condition", {})
                cond_str = f" (Condition: {json.dumps(conditions)})" if conditions else ""

                if effect == "Allow":
                    print(f"   ✅ Bucket policy ALLOWS access{cond_str}")
                    print(f"      Sid: {stmt.get('Sid', 'N/A')}")
                    access_paths_found.append(
                        f"Bucket policy Allow: {stmt.get('Sid', 'unnamed')}"
                    )
                elif effect == "Deny":
                    print(f"   🔴 Bucket policy DENIES access{cond_str}")
                    print(f"      Sid: {stmt.get('Sid', 'N/A')}")
                    print(f"      ⚠️  This Deny OVERRIDES all Allows!")

    except ClientError as e:
        if "NoSuchBucketPolicy" in str(e):
            print(f"   (no bucket policy exists)")
        else:
            print(f"   Error: {e}")

    # ================================================================
    # PATH 5: Permission Boundary check
    # ================================================================
    print(f"\n📋 PATH 5: Permission Boundary")
    print(f"{'─' * 50}")
    try:
        user_info = iam_client.get_user(UserName=username)
        boundary = user_info["User"].get("PermissionsBoundary")

        if boundary:
            boundary_arn = boundary["PermissionsBoundaryArn"]
            print(f"   Boundary: {boundary_arn}")

            # Check if boundary allows the action
            b_ver = iam_client.get_policy(PolicyArn=boundary_arn)
            b_ver_id = b_ver["Policy"]["DefaultVersionId"]
            b_doc = iam_client.get_policy_version(
                PolicyArn=boundary_arn, VersionId=b_ver_id
            )
            document = b_doc["PolicyVersion"]["Document"]
            match = check_policy_grants_access(document, action, resource_arn)

            if match:
                print(f"   ✅ Boundary ALLOWS this action (ceiling not hit)")
            else:
                print(f"   🔴 Boundary DOES NOT allow this action")
                print(f"   → Even if identity policy says Allow → DENIED")
                print(f"   → Boundary is a ceiling — action exceeds it")
        else:
            print(f"   (no permission boundary attached)")

    except ClientError as e:
        print(f"   Error: {e}")

    # ================================================================
    # SUMMARY
    # ================================================================
    print(f"\n{'=' * 60}")
    print(f"📊 ACCESS TRACE SUMMARY")
    print(f"{'=' * 60}")

    if access_paths_found:
        print(f"\n✅ User '{username}' HAS access via {len(access_paths_found)} path(s):")
        for i, path in enumerate(access_paths_found, 1):
            print(f"   {i}. {path}")
    else:
        print(f"\n🔴 User '{username}' has NO access path found")
        print(f"   → Implicit Deny applies")

    return access_paths_found


def check_policy_grants_access(policy_document, target_action, target_resource):
    """
    Simple policy evaluator:
    Check if a policy document grants a specific action on a specific resource
    
    NOTE: This is a SIMPLIFIED version
    → Real AWS evaluation handles wildcards, conditions, NotAction, etc.
    → For interview purposes, this covers 90% of scenarios
    """
    statements = policy_document.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]

    for stmt in statements:
        effect = stmt.get("Effect", "")
        if effect != "Allow":
            continue

        # Check actions
        actions = stmt.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]

        action_match = False
        for a in actions:
            if a == target_action:
                action_match = True
            elif a == "*":
                action_match = True
            elif a.endswith(":*"):
                # e.g., "s3:*" matches "s3:GetObject"
                service = a.split(":")[0]
                target_service = target_action.split(":")[0]
                if service == target_service:
                    action_match = True
            elif "*" in a:
                # Wildcard matching: s3:Get* matches s3:GetObject
                import fnmatch
                if fnmatch.fnmatch(target_action, a):
                    action_match = True

        if not action_match:
            continue

        # Check resources
        resources = stmt.get("Resource", [])
        if isinstance(resources, str):
            resources = [resources]

        resource_match = False
        for r in resources:
            if r == "*":
                resource_match = True
            elif r == target_resource:
                resource_match = True
            elif "*" in r:
                import fnmatch
                if fnmatch.fnmatch(target_resource, r):
                    resource_match = True

        if action_match and resource_match:
            return True

    return False


if __name__ == "__main__":
    # Trace Sara's mysterious access
    trace_user_access(
        username="sara",
        bucket_name="quickcart-raw-data-prod",
        action="s3:GetObject"
    )