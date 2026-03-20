# file: 07_05_access_denied_debugger.py
# Purpose: One-stop diagnostic tool for any AccessDenied situation

import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime


class AccessDeniedDebugger:
    """
    Comprehensive debugger that checks ALL policy layers
    for a given principal + action + resource combination
    """

    def __init__(self):
        self.iam = boto3.client("iam")
        self.s3 = boto3.client("s3")
        self.sts = boto3.client("sts")
        self.org = None
        try:
            self.org = boto3.client("organizations")
        except Exception:
            pass
        self.findings = []

    def debug_access(self, principal_type, principal_name, action, resource_arn):
        """
        Master debug function
        principal_type: "user" or "role"
        principal_name: IAM username or role name
        action: e.g., "s3:PutObject"
        resource_arn: e.g., "arn:aws:s3:::mybucket/*"
        """
        print("=" * 70)
        print(f"🔍 ACCESS DENIED DEBUGGER")
        print(f"=" * 70)
        print(f"  Principal:  {principal_type}/{principal_name}")
        print(f"  Action:     {action}")
        print(f"  Resource:   {resource_arn}")
        print(f"  Timestamp:  {datetime.now().isoformat()}")
        print(f"=" * 70)

        self.findings = []

        # --- Gate 1: Check identity policies ---
        self._check_identity_policies(principal_type, principal_name, action, resource_arn)

        # --- Gate 2: Check group policies (users only) ---
        if principal_type == "user":
            self._check_group_policies(principal_name, action, resource_arn)

        # --- Gate 3: Check permission boundary ---
        self._check_permission_boundary(principal_type, principal_name, action, resource_arn)

        # --- Gate 4: Check resource policy (S3 bucket) ---
        self._check_resource_policy(action, resource_arn)

        # --- Gate 5: Check SCP (if in Organization) ---
        self._check_scp()

        # --- Generate verdict ---
        self._generate_verdict(principal_name, action)

    def _check_identity_policies(self, principal_type, name, action, resource_arn):
        """Gate 3 in evaluation: Identity-based policies"""
        print(f"\n{'─' * 70}")
        print(f"🔎 GATE: Identity-Based Policies")
        print(f"{'─' * 70}")

        allows_found = []
        denies_found = []

        try:
            if principal_type == "user":
                # Inline policies
                inline_resp = self.iam.list_user_policies(UserName=name)
                for policy_name in inline_resp["PolicyNames"]:
                    doc_resp = self.iam.get_user_policy(
                        UserName=name, PolicyName=policy_name
                    )
                    self._scan_policy_document(
                        doc_resp["PolicyDocument"], action, resource_arn,
                        f"User inline: {policy_name}", allows_found, denies_found
                    )

                # Managed policies
                managed_resp = self.iam.list_attached_user_policies(UserName=name)
                for policy in managed_resp["AttachedPolicies"]:
                    doc = self._get_managed_policy_document(policy["PolicyArn"])
                    self._scan_policy_document(
                        doc, action, resource_arn,
                        f"User managed: {policy['PolicyName']}", allows_found, denies_found
                    )

            elif principal_type == "role":
                # Inline policies
                inline_resp = self.iam.list_role_policies(RoleName=name)
                for policy_name in inline_resp["PolicyNames"]:
                    doc_resp = self.iam.get_role_policy(
                        RoleName=name, PolicyName=policy_name
                    )
                    self._scan_policy_document(
                        doc_resp["PolicyDocument"], action, resource_arn,
                        f"Role inline: {policy_name}", allows_found, denies_found
                    )

                # Managed policies
                managed_resp = self.iam.list_attached_role_policies(RoleName=name)
                for policy in managed_resp["AttachedPolicies"]:
                    doc = self._get_managed_policy_document(policy["PolicyArn"])
                    self._scan_policy_document(
                        doc, action, resource_arn,
                        f"Role managed: {policy['PolicyName']}", allows_found, denies_found
                    )

        except ClientError as e:
            print(f"  ❌ Error reading policies: {e}")
            self.findings.append(f"ERROR: Could not read identity policies — {e}")
            return

        # Report findings
        if denies_found:
            for d in denies_found:
                print(f"  🔴 EXPLICIT DENY in {d}")
                self.findings.append(f"EXPLICIT DENY in identity policy: {d}")

        if allows_found:
            for a in allows_found:
                print(f"  ✅ ALLOW found in {a}")
                self.findings.append(f"ALLOW in identity policy: {a}")
        else:
            print(f"  ⬜ No ALLOW found in any identity policy")
            self.findings.append("NO ALLOW in identity policies (implicit deny from this gate)")

    def _check_group_policies(self, username, action, resource_arn):
        """Check policies inherited through IAM groups"""
        print(f"\n{'─' * 70}")
        print(f"🔎 GATE: Group Policies (inherited)")
        print(f"{'─' * 70}")

        allows_found = []
        denies_found = []

        try:
            groups_resp = self.iam.list_groups_for_user(UserName=username)
            groups = groups_resp["Groups"]

            if not groups:
                print(f"  ⬜ User is not in any groups")
                return

            for group in groups:
                gname = group["GroupName"]
                print(f"  📁 Group: {gname}")

                # Group inline policies
                g_inline = self.iam.list_group_policies(GroupName=gname)
                for pname in g_inline["PolicyNames"]:
                    doc_resp = self.iam.get_group_policy(
                        GroupName=gname, PolicyName=pname
                    )
                    self._scan_policy_document(
                        doc_resp["PolicyDocument"], action, resource_arn,
                        f"Group '{gname}' inline: {pname}", allows_found, denies_found
                    )

                # Group managed policies
                g_managed = self.iam.list_attached_group_policies(GroupName=gname)
                for policy in g_managed["AttachedPolicies"]:
                    doc = self._get_managed_policy_document(policy["PolicyArn"])
                    self._scan_policy_document(
                        doc, action, resource_arn,
                        f"Group '{gname}' managed: {policy['PolicyName']}",
                        allows_found, denies_found
                    )

        except ClientError as e:
            print(f"  ❌ Error: {e}")
            return

        if denies_found:
            for d in denies_found:
                print(f"  🔴 EXPLICIT DENY in {d}")
                self.findings.append(f"EXPLICIT DENY in group policy: {d}")

        if allows_found:
            for a in allows_found:
                print(f"  ✅ ALLOW found in {a}")
                self.findings.append(f"ALLOW in group policy: {a}")

    def _check_permission_boundary(self, principal_type, name, action, resource_arn):
        """Gate 2 in evaluation: Permission Boundary"""
        print(f"\n{'─' * 70}")
        print(f"🔎 GATE: Permission Boundary")
        print(f"{'─' * 70}")

        try:
            if principal_type == "user":
                info = self.iam.get_user(UserName=name)
                boundary = info["User"].get("PermissionsBoundary")
            else:
                info = self.iam.get_role(RoleName=name)
                boundary = info["Role"].get("PermissionsBoundary")

            if not boundary:
                print(f"  ⬜ No permission boundary attached (gate skipped)")
                self.findings.append("No permission boundary — gate not applicable")
                return

            boundary_arn = boundary["PermissionsBoundaryArn"]
            print(f"  📋 Boundary: {boundary_arn}")

            doc = self._get_managed_policy_document(boundary_arn)
            allows_found = []
            denies_found = []
            self._scan_policy_document(
                doc, action, resource_arn,
                f"Boundary: {boundary_arn}", allows_found, denies_found
            )

            if denies_found:
                print(f"  🔴 Boundary has EXPLICIT DENY for this action")
                self.findings.append("Permission boundary EXPLICITLY DENIES this action")
            elif allows_found:
                print(f"  ✅ Boundary ALLOWS this action (ceiling not hit)")
                self.findings.append("Permission boundary allows — ceiling not exceeded")
            else:
                print(f"  🔴 Boundary does NOT include this action → DENIED")
                print(f"     → Even if identity policy allows it → boundary caps it")
                self.findings.append("Permission boundary DOES NOT ALLOW — ceiling exceeded → DENIED")

        except ClientError as e:
            print(f"  ❌ Error: {e}")

    def _check_resource_policy(self, action, resource_arn):
        """Gate 4 in evaluation: Resource-based policy"""
        print(f"\n{'─' * 70}")
        print(f"🔎 GATE: Resource-Based Policy (Bucket Policy)")
        print(f"{'─' * 70}")

        # Extract bucket name from ARN
        # arn:aws:s3:::bucket-name/* → bucket-name
        if "s3" not in resource_arn:
            print(f"  ⬜ Not an S3 resource — skipping bucket policy check")
            return

        parts = resource_arn.split(":::")
        if len(parts) < 2:
            print(f"  ⬜ Cannot parse bucket name from ARN")
            return

        bucket_name = parts[1].split("/")[0]

        try:
            bp_response = self.s3.get_bucket_policy(Bucket=bucket_name)
            bucket_policy = json.loads(bp_response["Policy"])

            print(f"  📋 Bucket: {bucket_name}")
            print(f"  📋 Policy has {len(bucket_policy.get('Statement', []))} statement(s)")

            for stmt in bucket_policy.get("Statement", []):
                effect = stmt.get("Effect", "")
                sid = stmt.get("Sid", "unnamed")
                actions = stmt.get("Action", [])
                if isinstance(actions, str):
                    actions = [actions]

                conditions = stmt.get("Condition", {})
                cond_str = f" [Condition: {json.dumps(conditions)}]" if conditions else ""

                action_match = any(
                    a == action or a == "s3:*" or a == "*"
                    for a in actions
                )

                if action_match:
                    if effect == "Deny":
                        print(f"  🔴 DENY statement matches: {sid}{cond_str}")
                        self.findings.append(
                            f"Bucket policy EXPLICIT DENY: {sid}{cond_str}"
                        )
                    elif effect == "Allow":
                        print(f"  ✅ ALLOW statement matches: {sid}{cond_str}")
                        self.findings.append(
                            f"Bucket policy ALLOW: {sid}{cond_str}"
                        )

        except ClientError as e:
            if "NoSuchBucketPolicy" in str(e):
                print(f"  ⬜ No bucket policy exists for '{bucket_name}'")
                self.findings.append("No bucket policy — gate not applicable")
            else:
                print(f"  ❌ Error: {e}")

    def _check_scp(self):
        """Gate 1 in evaluation: Service Control Policies"""
        print(f"\n{'─' * 70}")
        print(f"🔎 GATE: Service Control Policies (SCPs)")
        print(f"{'─' * 70}")

        if not self.org:
            print(f"  ⬜ Cannot check SCPs (no Organizations access)")
            self.findings.append("SCP check skipped — no Organizations access")
            return

        try:
            # Check if account is in an Organization
            org_info = self.org.describe_organization()
            print(f"  📋 Organization: {org_info['Organization']['Id']}")

            # List SCPs attached to this account
            account_id = self.sts.get_caller_identity()["Account"]
            policies = self.org.list_policies_for_target(
                TargetId=account_id,
                Filter="SERVICE_CONTROL_POLICY"
            )

            scp_list = policies.get("Policies", [])
            print(f"  📋 {len(scp_list)} SCP(s) attached to this account")

            for scp in scp_list:
                print(f"     → {scp['Name']} ({scp['Id']})")

            if scp_list:
                print(f"\n  ⚠️  SCPs exist — they may restrict access")
                print(f"     → SCPs are allowlists: action must be in SCP to be permitted")
                self.findings.append(
                    f"{len(scp_list)} SCP(s) found — may restrict access"
                )

        except ClientError as e:
            if "AccessDeniedException" in str(e) or "AWSOrganizationsNotInUse" in str(e):
                print(f"  ⬜ Account is not in an Organization (SCPs don't apply)")
                self.findings.append("No AWS Organization — SCPs not applicable")
            else:
                print(f"  ❌ Error checking SCPs: {e}")

    def _scan_policy_document(self, doc, target_action, target_resource,
                               source_label, allows_list, denies_list):
        """Scan a policy document for matching Allow/Deny statements"""
        import fnmatch

        statements = doc.get("Statement", [])
        if isinstance(statements, dict):
            statements = [statements]

        for stmt in statements:
            effect = stmt.get("Effect", "")
            actions = stmt.get("Action", [])
            resources = stmt.get("Resource", [])

            if isinstance(actions, str):
                actions = [actions]
            if isinstance(resources, str):
                resources = [resources]

            # Check action match
            action_match = False
            for a in actions:
                if a == target_action or a == "*":
                    action_match = True
                elif a.endswith(":*"):
                    if a.split(":")[0] == target_action.split(":")[0]:
                        action_match = True
                elif "*" in a:
                    if fnmatch.fnmatch(target_action, a):
                        action_match = True

            if not action_match:
                continue

            # Check resource match
            resource_match = False
            for r in resources:
                if r == "*" or r == target_resource:
                    resource_match = True
                elif "*" in r:
                    if fnmatch.fnmatch(target_resource, r):
                        resource_match = True

            if not resource_match:
                continue

            # Found a matching statement
            if effect == "Allow":
                allows_list.append(source_label)
            elif effect == "Deny":
                denies_list.append(source_label)

    def _get_managed_policy_document(self, policy_arn):
        """Retrieve the document for a managed policy"""
        policy_resp = self.iam.get_policy(PolicyArn=policy_arn)
        version_id = policy_resp["Policy"]["DefaultVersionId"]
        version_resp = self.iam.get_policy_version(
            PolicyArn=policy_arn, VersionId=version_id
        )
        return version_resp["PolicyVersion"]["Document"]

    def _generate_verdict(self, principal_name, action):
        """Analyze all findings and produce a verdict"""
        print(f"\n{'=' * 70}")
        print(f"📊 VERDICT")
        print(f"{'=' * 70}")

        has_explicit_deny = any("EXPLICIT DENY" in f or "EXPLICITLY DENIES" in f
                                for f in self.findings)
        has_allow_identity = any("ALLOW in identity" in f or "ALLOW in group" in f
                                 for f in self.findings)
        has_allow_resource = any("Bucket policy ALLOW" in f for f in self.findings)
        boundary_blocks = any("ceiling exceeded" in f for f in self.findings)

        print(f"\n  All findings:")
        for i, f in enumerate(self.findings, 1):
            print(f"  {i}. {f}")

        print(f"\n  {'─' * 60}")

        if has_explicit_deny:
            print(f"  🔴 RESULT: DENIED (Explicit Deny found)")
            print(f"     → Explicit Deny ALWAYS wins")
            print(f"     → No amount of Allow can override it")
            print(f"     → Fix: remove the Deny or adjust its Condition")
        elif boundary_blocks:
            print(f"  🔴 RESULT: DENIED (Permission Boundary exceeded)")
            print(f"     → The action exceeds the boundary ceiling")
            print(f"     → Fix: add the action to the boundary policy")
        elif has_allow_identity or has_allow_resource:
            print(f"  ✅ RESULT: LIKELY ALLOWED")
            if has_allow_identity:
                print(f"     → Identity/group policy provides Allow")
            if has_allow_resource:
                print(f"     → Bucket policy provides Allow")
            print(f"     → (Verify: no SCP or VPC endpoint policy blocking)")
        else:
            print(f"  🔴 RESULT: DENIED (Implicit Deny — no Allow found)")
            print(f"     → No policy grants this access")
            print(f"     → Fix: add Allow to identity policy OR bucket policy")


if __name__ == "__main__":
    debugger = AccessDeniedDebugger()

    # --- Debug Incident 1: Admin denied ---
    print("\n\n" + "🔷" * 35)
    print("DEBUGGING INCIDENT 1: Admin 'raj' denied on logs bucket")
    print("🔷" * 35)
    debugger.debug_access(
        principal_type="user",
        principal_name="raj",
        action="s3:GetObject",
        resource_arn="arn:aws:s3:::quickcart-logs-prod/*"
    )

    # --- Debug Incident 2: Mystery access ---
    print("\n\n" + "🔷" * 35)
    print("DEBUGGING INCIDENT 2: Sara's mystery access")
    print("🔷" * 35)
    debugger.debug_access(
        principal_type="user",
        principal_name="sara",
        action="s3:GetObject",
        resource_arn="arn:aws:s3:::quickcart-raw-data-prod/*"
    )

    # --- Debug Incident 4: Role vs bucket policy ---
    print("\n\n" + "🔷" * 35)
    print("DEBUGGING INCIDENT 4: EC2 role denied after bucket policy change")
    print("🔷" * 35)
    debugger.debug_access(
        principal_type="role",
        principal_name="QuickCart-EC2-S3ReadWrite-Prod",
        action="s3:PutObject",
        resource_arn="arn:aws:s3:::quickcart-raw-data-prod/*"
    )