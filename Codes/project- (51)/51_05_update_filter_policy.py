# file: 51_05_update_filter_policy.py
# Purpose: Modify filter policy on live subscription — zero downtime

import boto3
import json

REGION = "us-east-2"


def load_infrastructure():
    with open("/tmp/filtered_infra.json", "r") as f:
        return json.load(f)


def get_current_filter(sns_client, subscription_arn):
    """Retrieve current filter policy from a subscription"""
    response = sns_client.get_subscription_attributes(
        SubscriptionArn=subscription_arn
    )
    attrs = response["Attributes"]
    
    filter_policy = attrs.get("FilterPolicy", None)
    filter_scope = attrs.get("FilterPolicyScope", "MessageAttributes")

    if filter_policy:
        return json.loads(filter_policy), filter_scope
    return None, filter_scope


def update_filter_policy(sns_client, subscription_arn, new_policy):
    """
    Update filter policy on existing subscription
    
    KEY BEHAVIOR:
    → Change takes effect within SECONDS (not minutes)
    → No messages lost during transition
    → Messages in-flight at time of change:
      - May be evaluated against OLD or NEW policy
      - Eventually consistent (brief window)
    → No need to unsubscribe/resubscribe
    """
    sns_client.set_subscription_attributes(
        SubscriptionArn=subscription_arn,
        AttributeName="FilterPolicy",
        AttributeValue=json.dumps(new_policy)
    )
    print(f"  ✅ Filter policy updated")
    print(f"     New policy: {json.dumps(new_policy)}")


def remove_filter_policy(sns_client, subscription_arn):
    """
    Remove filter policy entirely — subscription becomes catch-all
    
    CRITICAL DISTINCTION:
    → Setting FilterPolicy to "{}" = receives NOTHING
    → REMOVING FilterPolicy attribute = receives EVERYTHING
    → This is one of the most confusing SNS behaviors
    """
    # To remove: set to empty string (not empty JSON object)
    sns_client.set_subscription_attributes(
        SubscriptionArn=subscription_arn,
        AttributeName="FilterPolicy",
        AttributeValue=""
    )
    print(f"  ✅ Filter policy REMOVED — subscription now receives ALL messages")


def demo_policy_updates():
    """
    Scenario: Business requirements changed
    
    BEFORE:
    → Alert Queue: event_type IN [alert_event, data_quality_event]
    
    AFTER (new requirement):
    → Alert Queue: event_type IN [alert_event, data_quality_event]
                   AND severity = critical
                   AND environment != dev
    
    → Also: ETL Queue now needs to receive data_quality_event too
    """
    infra = load_infrastructure()
    sns_client = boto3.client("sns", region_name=REGION)

    print("=" * 70)
    print("🔄 UPDATING FILTER POLICIES ON LIVE SUBSCRIPTIONS")
    print("=" * 70)

    # --- Find Alert Queue subscription ---
    alert_queue = None
    etl_queue = None
    for q in infra["queues"]:
        if q["queue_name"] == "quickcart-alert-queue":
            alert_queue = q
        elif q["queue_name"] == "quickcart-etl-queue":
            etl_queue = q

    # --- Show current Alert Queue filter ---
    print(f"\n📌 Alert Queue — BEFORE update:")
    current_policy, scope = get_current_filter(
        sns_client, alert_queue["subscription_arn"]
    )
    print(f"   Current filter: {json.dumps(current_policy)}")
    print(f"   Scope: {scope}")

    # --- Update Alert Queue: add severity AND environment conditions ---
    print(f"\n📌 Alert Queue — UPDATING:")
    new_alert_policy = {
        "event_type": ["alert_event", "data_quality_event"],
        "severity": ["critical"],
        "environment": [{"anything-but": ["dev"]}]
    }
    update_filter_policy(
        sns_client, alert_queue["subscription_arn"], new_alert_policy
    )

    # --- Verify Alert Queue update ---
    print(f"\n📌 Alert Queue — AFTER update:")
    updated_policy, scope = get_current_filter(
        sns_client, alert_queue["subscription_arn"]
    )
    print(f"   Updated filter: {json.dumps(updated_policy)}")
    print(f"   Now receives: critical alerts/DQ events in non-dev environments only")

    # --- Update ETL Queue: add data_quality_event ---
    print(f"\n📌 ETL Queue — BEFORE update:")
    current_etl_policy, scope = get_current_filter(
        sns_client, etl_queue["subscription_arn"]
    )
    print(f"   Current filter: {json.dumps(current_etl_policy)}")

    print(f"\n📌 ETL Queue — UPDATING:")
    new_etl_policy = {
        "event_type": ["etl_event", "data_quality_event"]
    }
    update_filter_policy(
        sns_client, etl_queue["subscription_arn"], new_etl_policy
    )

    # --- Final state ---
    print("\n" + "=" * 70)
    print("📊 FINAL FILTER STATE ACROSS ALL SUBSCRIPTIONS")
    print("=" * 70)

    for q in infra["queues"]:
        policy, scope = get_current_filter(sns_client, q["subscription_arn"])
        policy_str = json.dumps(policy) if policy else "NONE (catch-all)"
        print(f"\n  {q['queue_name']}:")
        print(f"    Filter: {policy_str}")
        print(f"    Scope:  {scope}")

    print("\n" + "=" * 70)
    return True


if __name__ == "__main__":
    demo_policy_updates()