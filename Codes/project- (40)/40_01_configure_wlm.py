# file: 40_01_configure_wlm.py
# Purpose: Generate Redshift SQL for WLM configuration
# Includes: user groups, Automatic WLM, SQA, QMR, Concurrency Scaling

import boto3
import json
from datetime import datetime

AWS_REGION = "us-east-2"
CLUSTER_ID = "quickcart-dwh"

redshift = boto3.client("redshift", region_name=AWS_REGION)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Generate User Group SQL
# ═══════════════════════════════════════════════════════════════════
def generate_user_group_sql():
    """Create Redshift user groups for WLM routing"""
    print(f"""
-- ═══════════════════════════════════════════════════════════════
-- STEP 1: CREATE USER GROUPS
-- WLM routes queries based on user group membership
-- Run on Redshift Query Editor
-- ═══════════════════════════════════════════════════════════════

-- Group 1: Dashboard users (Tableau, Looker, Power BI service accounts)
CREATE GROUP dashboard_users;

-- Group 2: ETL users (Glue, COPY jobs, data pipelines)
CREATE GROUP etl_users;

-- Group 3: Ad-hoc users (analysts running manual queries)
CREATE GROUP adhoc_users;

-- Group 4: Maintenance users (VACUUM, ANALYZE, admin tasks)
CREATE GROUP maintenance_users;

-- ── Assign users to groups ──

-- Tableau service account → dashboard group
ALTER GROUP dashboard_users ADD USER tableau_svc;

-- Glue loader account → ETL group
ALTER GROUP etl_users ADD USER glue_loader;

-- Individual analysts → ad-hoc group
ALTER GROUP adhoc_users ADD USER analyst_jane;
ALTER GROUP adhoc_users ADD USER analyst_bob;
ALTER GROUP adhoc_users ADD USER analyst_sarah;

-- Admin → maintenance group
ALTER GROUP maintenance_users ADD USER admin;

-- NOTE: A user can be in MULTIPLE groups
-- WLM uses the FIRST matching queue rule
-- Order matters in WLM queue configuration

-- Verify group membership
SELECT groname, usename
FROM pg_group g
JOIN pg_user u ON u.usesysid = ANY(g.grolist)
ORDER BY groname, usename;
    """)


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Configure Automatic WLM via Parameter Group
# ═══════════════════════════════════════════════════════════════════
def configure_automatic_wlm():
    """
    Configure Automatic WLM with priority queues
    
    This modifies the cluster's parameter group
    Most changes apply dynamically (no reboot)
    SQA changes require reboot
    """
    print("\n⚙️  Configuring Automatic WLM...")

    # WLM configuration JSON
    # Automatic WLM: set priority + user groups, Redshift manages slots/memory
    wlm_config = [
        {
            # Queue 1: Dashboard (highest priority)
            "name": "dashboard",
            "user_group": ["dashboard_users"],
            "query_group": ["dashboard"],
            "priority": "highest",
            "concurrency_scaling": "auto",  # Enable burst capacity
            "auto_wlm": True,
            "rules": [
                {
                    # QMR: Kill dashboard queries > 60 seconds
                    "rule_name": "dashboard_timeout",
                    "predicate": [
                        {
                            "metric_name": "query_execution_time",
                            "operator": ">",
                            "value": 60
                        }
                    ],
                    "action": "abort"
                }
            ]
        },
        {
            # Queue 2: ETL (medium priority)
            "name": "etl",
            "user_group": ["etl_users"],
            "query_group": ["etl"],
            "priority": "normal",
            "concurrency_scaling": "off",  # ETL runs at off-peak
            "auto_wlm": True,
            "rules": [
                {
                    # QMR: Log ETL queries > 2 hours
                    "rule_name": "etl_long_running",
                    "predicate": [
                        {
                            "metric_name": "query_execution_time",
                            "operator": ">",
                            "value": 7200
                        }
                    ],
                    "action": "log"
                }
            ]
        },
        {
            # Queue 3: Ad-hoc (lower priority)
            "name": "adhoc",
            "user_group": ["adhoc_users"],
            "query_group": ["adhoc"],
            "priority": "low",
            "concurrency_scaling": "auto",
            "auto_wlm": True,
            "rules": [
                {
                    # QMR: Kill ad-hoc queries scanning > 1B rows
                    "rule_name": "adhoc_runaway_scan",
                    "predicate": [
                        {
                            "metric_name": "scan_row_count",
                            "operator": ">",
                            "value": 1000000000
                        }
                    ],
                    "action": "abort"
                },
                {
                    # QMR: Kill ad-hoc queries > 5 minutes
                    "rule_name": "adhoc_timeout",
                    "predicate": [
                        {
                            "metric_name": "query_execution_time",
                            "operator": ">",
                            "value": 300
                        }
                    ],
                    "action": "abort"
                }
            ]
        },
        {
            # Queue 4: Maintenance (lowest priority)
            "name": "maintenance",
            "user_group": ["maintenance_users"],
            "query_group": ["maintenance"],
            "priority": "lowest",
            "concurrency_scaling": "off",
            "auto_wlm": True
        },
        {
            # Default queue (catches unmatched queries)
            "name": "default",
            "user_group": ["*"],
            "query_group": ["*"],
            "priority": "low",
            "concurrency_scaling": "off",
            "auto_wlm": True,
            "rules": [
                {
                    "rule_name": "default_timeout",
                    "predicate": [
                        {
                            "metric_name": "query_execution_time",
                            "operator": ">",
                            "value": 600
                        }
                    ],
                    "action": "abort"
                }
            ]
        }
    ]

    # Get current parameter group
    try:
        cluster_info = redshift.describe_clusters(
            ClusterIdentifier=CLUSTER_ID
        )
        param_group_name = cluster_info["Clusters"][0]["ClusterParameterGroups"][0]["ParameterGroupName"]
        print(f"   Parameter group: {param_group_name}")

        # Modify WLM configuration
        redshift.modify_cluster_parameter_group(
            ParameterGroupName=param_group_name,
            Parameters=[
                {
                    "ParameterName": "wlm_json_configuration",
                    "ParameterValue": json.dumps(wlm_config),
                    "ApplyType": "dynamic"
                },
                {
                    "ParameterName": "max_concurrency_scaling_clusters",
                    "ParameterValue": "3",
                    "ApplyType": "dynamic"
                }
            ]
        )
        print(f"   ✅ WLM configuration applied (dynamic — no reboot needed)")
        print(f"   ✅ Max concurrency scaling clusters: 3")

    except Exception as e:
        print(f"   ❌ Error: {e}")
        print(f"   → May need to create custom parameter group first")
        print(f"   → Or apply via Console: Redshift → Cluster → Properties → WLM")


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Enable Short Query Acceleration
# ═══════════════════════════════════════════════════════════════════
def enable_sqa():
    """
    Enable SQA — requires cluster reboot
    Schedule during maintenance window
    """
    print("\n⚡ Enabling Short Query Acceleration...")

    try:
        cluster_info = redshift.describe_clusters(
            ClusterIdentifier=CLUSTER_ID
        )
        param_group_name = cluster_info["Clusters"][0]["ClusterParameterGroups"][0]["ParameterGroupName"]

        redshift.modify_cluster_parameter_group(
            ParameterGroupName=param_group_name,
            Parameters=[
                {
                    "ParameterName": "short_query_acceleration",
                    "ParameterValue": "true",
                    "ApplyType": "static"   # Requires reboot
                },
                {
                    # SQA threshold: queries predicted < 5 seconds
                    "ParameterName": "short_query_acceleration_max_runtime",
                    "ParameterValue": "5",  # seconds
                    "ApplyType": "dynamic"
                }
            ]
        )
        print(f"   ✅ SQA enabled (requires cluster reboot to take effect)")
        print(f"   ✅ SQA threshold: 5 seconds")
        print(f"   ⚠️  Schedule reboot during maintenance window:")
        print(f"      aws redshift reboot-cluster --cluster-identifier {CLUSTER_ID}")

    except Exception as e:
        print(f"   ❌ Error: {e}")


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Enable Concurrency Scaling
# ═══════════════════════════════════════════════════════════════════
def enable_concurrency_scaling():
    """Enable Concurrency Scaling on the cluster"""
    print("\n📈 Enabling Concurrency Scaling...")

    print(f"""
-- Run on Redshift Query Editor:

-- Enable concurrency scaling usage for dashboard queue
-- This is set in WLM config (already done above)
-- But also need to enable at cluster level:

-- Check current concurrency scaling status
SELECT * FROM stv_wlm_service_class_config
WHERE service_class > 5;

-- Verify concurrency scaling is active
SELECT service_class, num_query_tasks, query_working_mem
FROM stv_wlm_service_class_state;

-- Monitor concurrency scaling usage
SELECT w.service_class AS queue,
       w.num_queued_queries,
       w.num_executing_queries,
       cs.num_concurrency_scaling_queries
FROM stv_wlm_service_class_state w
LEFT JOIN stv_concurrency_scaling cs 
    ON w.service_class = cs.service_class;
    """)


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Generate Verification and Monitoring SQL
# ═══════════════════════════════════════════════════════════════════
def generate_monitoring_sql():
    """Generate SQL for monitoring WLM effectiveness"""
    print(f"""
-- ═══════════════════════════════════════════════════════════════
-- WLM MONITORING QUERIES
-- Run these to verify WLM is working correctly
-- ═══════════════════════════════════════════════════════════════

-- ── Monitor 1: Current queue status ──
SELECT
    w.service_class AS queue_id,
    TRIM(c.name) AS queue_name,
    c.num_query_tasks AS max_concurrency,
    w.num_executing_queries AS running,
    w.num_queued_queries AS queued,
    c.query_working_mem AS memory_mb
FROM stv_wlm_service_class_state w
JOIN stv_wlm_service_class_config c 
    ON w.service_class = c.service_class
WHERE c.service_class > 5  -- Skip internal queues
ORDER BY c.service_class;


-- ── Monitor 2: Query queue wait times (last hour) ──
SELECT
    w.service_class AS queue_id,
    COUNT(*) AS query_count,
    ROUND(AVG(w.total_queue_time / 1000000.0), 2) AS avg_wait_seconds,
    ROUND(MAX(w.total_queue_time / 1000000.0), 2) AS max_wait_seconds,
    ROUND(AVG(w.total_exec_time / 1000000.0), 2) AS avg_exec_seconds,
    SUM(CASE WHEN w.total_queue_time > 5000000 THEN 1 ELSE 0 END) AS queries_waited_5s_plus
FROM stl_wlm_query w
WHERE w.service_class_start_time > DATEADD(hour, -1, CURRENT_TIMESTAMP)
GROUP BY w.service_class
ORDER BY avg_wait_seconds DESC;


-- ── Monitor 3: SQA effectiveness ──
SELECT
    DATE_TRUNC('hour', starttime) AS hour,
    COUNT(*) AS total_queries,
    SUM(CASE WHEN final_state = 'Completed' THEN 1 ELSE 0 END) AS sqa_completed,
    SUM(CASE WHEN final_state = 'Evicted' THEN 1 ELSE 0 END) AS sqa_evicted,
    ROUND(AVG(elapsed / 1000.0), 1) AS avg_runtime_ms
FROM stl_sqa_log
WHERE starttime > DATEADD(day, -1, CURRENT_TIMESTAMP)
GROUP BY DATE_TRUNC('hour', starttime)
ORDER BY hour DESC
LIMIT 24;


-- ── Monitor 4: Concurrency Scaling usage ──
SELECT
    DATE_TRUNC('hour', starttime) AS hour,
    COUNT(*) AS scaling_queries,
    ROUND(SUM(elapsed / 1000000.0), 1) AS total_scaling_seconds,
    ROUND(SUM(elapsed / 1000000.0) / 3600 * 0.25, 4) AS estimated_cost
FROM stl_query
WHERE concurrency_scaling_status = 3  -- Ran on scaling cluster
  AND starttime > DATEADD(day, -7, CURRENT_TIMESTAMP)
GROUP BY DATE_TRUNC('hour', starttime)
ORDER BY hour DESC;


-- ── Monitor 5: QMR violations ──
SELECT
    rule_name,
    action,
    COUNT(*) AS violation_count,
    MIN(starttime) AS first_violation,
    MAX(starttime) AS last_violation
FROM stl_wlm_rule_action
WHERE starttime > DATEADD(day, -7, CURRENT_TIMESTAMP)
GROUP BY rule_name, action
ORDER BY violation_count DESC;


-- ── Monitor 6: Per-queue throughput ──
SELECT
    w.service_class AS queue_id,
    DATE_TRUNC('hour', w.service_class_start_time) AS hour,
    COUNT(*) AS queries_processed,
    ROUND(AVG(w.total_exec_time / 1000000.0), 2) AS avg_exec_seconds,
    ROUND(AVG(w.total_queue_time / 1000000.0), 2) AS avg_queue_seconds
FROM stl_wlm_query w
WHERE w.service_class_start_time > DATEADD(day, -1, CURRENT_TIMESTAMP)
GROUP BY w.service_class, DATE_TRUNC('hour', w.service_class_start_time)
ORDER BY hour DESC, queue_id;


-- ── Monitor 7: Queries killed by QMR ──
SELECT
    q.query,
    q.userid,
    u.usename,
    r.rule_name,
    r.action,
    q.elapsed / 1000000.0 AS runtime_seconds,
    SUBSTRING(q.querytxt, 1, 100) AS query_preview,
    q.starttime
FROM stl_wlm_rule_action r
JOIN stl_query q ON r.query = q.query
JOIN pg_user u ON q.userid = u.usesysid
WHERE r.action = 'abort'
  AND r.starttime > DATEADD(day, -7, CURRENT_TIMESTAMP)
ORDER BY r.starttime DESC
LIMIT 20;
    """)


# ═══════════════════════════════════════════════════════════════════
# STEP 6: Generate Query Group Usage SQL
# ═══════════════════════════════════════════════════════════════════
def generate_query_group_sql():
    """
    Show how to use query groups for dynamic routing
    Useful when same user needs different queue based on context
    """
    print(f"""
-- ═══════════════════════════════════════════════════════════════
-- QUERY GROUP USAGE (for dynamic routing)
-- ═══════════════════════════════════════════════════════════════

-- By default, queries route based on USER GROUP
-- But you can override by setting query_group:

-- Route this session's queries to dashboard queue
SET query_group TO 'dashboard';
SELECT * FROM analytics.fact_orders WHERE order_date > CURRENT_DATE - 7;
RESET query_group;

-- Route this session's queries to ETL queue
SET query_group TO 'etl';
COPY analytics.fact_orders FROM 's3://...' IAM_ROLE '...';
RESET query_group;

-- Route to maintenance queue
SET query_group TO 'maintenance';
VACUUM FULL analytics.fact_orders;
ANALYZE analytics.fact_orders;
RESET query_group;

-- This is how Glue/scripts can control their queue placement
-- without changing user group membership
    """)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 70)
    print("🚀 REDSHIFT WORKLOAD MANAGEMENT CONFIGURATION")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    generate_user_group_sql()
    configure_automatic_wlm()
    enable_sqa()
    enable_concurrency_scaling()
    generate_monitoring_sql()
    generate_query_group_sql()

    print("\n" + "=" * 70)
    print("✅ WLM CONFIGURATION COMPLETE")
    print("=" * 70)
    print(f"""
   SUMMARY OF CHANGES:
   ━━━━━━━━━━━━━━━━━━━
   1. Created 4 user groups (dashboard, etl, adhoc, maintenance)
   2. Configured Automatic WLM with 5 queues (4 + default)
   3. Set priorities: HIGHEST → NORMAL → LOW → LOWEST
   4. Enabled SQA (requires cluster reboot)
   5. Enabled Concurrency Scaling on dashboard queue
   6. Configured QMR rules (timeout + scan limits)

   NEXT STEPS:
   ━━━━━━━━━━━
   1. Run user group SQL on Redshift Query Editor
   2. Reboot cluster for SQA to take effect
   3. Monitor for 1 week using monitoring queries
   4. Tune thresholds based on observed patterns
    """)