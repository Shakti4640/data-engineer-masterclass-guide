# file: 46_05_federated_monitoring.py
# Purpose: Monitor federated query usage, cost, and impact on MariaDB
# Run daily to track trends and identify queries that should become pipelines

import boto3
import pymysql
from datetime import datetime, timedelta
from collections import defaultdict

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
WORKGROUP = "federated-adhoc"
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-pipeline-alerts"

MARIADB_CONFIG = {
    "host": "10.0.1.50",
    "port": 3306,
    "user": "quickcart_app",
    "password": "[REDACTED:PASSWORD]",
    "database": "quickcart"
}


def analyze_federated_query_history(days_back=7):
    """
    Analyze recent federated query patterns.
    
    Identifies:
    → Most frequent queries (candidates for ETL pipeline)
    → Slowest queries (need optimization)
    → Most expensive queries (need WHERE clause improvement)
    → Total cost of federated queries
    """
    athena_client = boto3.client("athena", region_name=REGION)

    print(f"{'='*70}")
    print(f"📊 FEDERATED QUERY ANALYSIS — Last {days_back} days")
    print(f"{'='*70}")

    # List recent query executions from federated workgroup
    paginator = athena_client.get_paginator("list_query_executions")

    execution_ids = []
    for page in paginator.paginate(WorkGroup=WORKGROUP, MaxResults=50):
        execution_ids.extend(page.get("QueryExecutionIds", []))

    if not execution_ids:
        print(f"   ℹ️  No federated queries found in workgroup '{WORKGROUP}'")
        return

    # Get details for each execution (batch of 50 max)
    queries = []
    for i in range(0, len(execution_ids), 50):
        batch = execution_ids[i:i+50]
        response = athena_client.batch_get_query_execution(QueryExecutionIds=batch)

        for exec_info in response.get("QueryExecutions", []):
            status = exec_info["Status"]
            state = status["State"]
            submit_time = status.get("SubmissionDateTime")

            # Filter by date range
            if submit_time:
                cutoff = datetime.now(submit_time.tzinfo) - timedelta(days=days_back)
                if submit_time < cutoff:
                    continue

            stats = exec_info.get("Statistics", {})
            query_text = exec_info.get("Query", "")

            queries.append({
                "id": exec_info["QueryExecutionId"],
                "query": query_text[:200],
                "state": state,
                "submit_time": submit_time,
                "duration_ms": stats.get("EngineExecutionTimeInMillis", 0),
                "data_scanned_bytes": stats.get("DataScannedInBytes", 0),
                "error": status.get("StateChangeReason", "")
            })

    # ━━━ ANALYSIS ━━━
    total_queries = len(queries)
    succeeded = [q for q in queries if q["state"] == "SUCCEEDED"]
    failed = [q for q in queries if q["state"] == "FAILED"]

    print(f"\n📋 QUERY SUMMARY:")
    print(f"   Total queries: {total_queries}")
    print(f"   Succeeded: {len(succeeded)}")
    print(f"   Failed: {len(failed)}")

    if succeeded:
        # Cost analysis
        total_scanned = sum(q["data_scanned_bytes"] for q in succeeded)
        total_cost = total_scanned / (1024 ** 4) * 5  # $5/TB
        avg_duration = sum(q["duration_ms"] for q in succeeded) / len(succeeded)

        print(f"\n💰 COST ANALYSIS:")
        print(f"   Total data scanned: {total_scanned / (1024**2):.2f} MB")
        print(f"   Total est. cost: ${total_cost:.4f}")
        print(f"   Avg query duration: {avg_duration:.0f}ms ({avg_duration/1000:.1f}s)")

        # Slowest queries
        by_duration = sorted(succeeded, key=lambda q: q["duration_ms"], reverse=True)
        print(f"\n🐌 SLOWEST QUERIES (top 5):")
        for q in by_duration[:5]:
            print(f"   {q['duration_ms']/1000:.1f}s | {q['data_scanned_bytes']/(1024**2):.1f}MB | {q['query'][:80]}")

        # Most expensive
        by_cost = sorted(succeeded, key=lambda q: q["data_scanned_bytes"], reverse=True)
        print(f"\n💸 MOST EXPENSIVE QUERIES (top 5):")
        for q in by_cost[:5]:
            cost = q["data_scanned_bytes"] / (1024 ** 4) * 5
            print(f"   ${cost:.6f} | {q['data_scanned_bytes']/(1024**2):.1f}MB | {q['query'][:80]}")

        # Query frequency (detect repeating patterns)
        query_patterns = defaultdict(int)
        for q in succeeded:
            # Normalize query: remove literals, keep structure
            normalized = q["query"][:100].lower().strip()
            query_patterns[normalized] += 1

        repeated = {k: v for k, v in query_patterns.items() if v > 3}
        if repeated:
            print(f"\n🔄 FREQUENTLY REPEATED QUERIES (>3 times — consider ETL pipeline):")
            for pattern, count in sorted(repeated.items(), key=lambda x: -x[1]):
                print(f"   {count}× | {pattern[:80]}")
        else:
            print(f"\n✅ No frequently repeated queries detected")

    # Failed query analysis
    if failed:
        print(f"\n❌ FAILED QUERIES:")
        for q in failed[:5]:
            print(f"   {q['submit_time']} | {q['error'][:100]}")
            print(f"   Query: {q['query'][:80]}")

    return {
        "total_queries": total_queries,
        "succeeded": len(succeeded),
        "failed": len(failed),
        "total_cost": total_cost if succeeded else 0
    }


def check_mariadb_impact():
    """
    Check MariaDB health metrics that might indicate
    federated query impact on production.
    """
    print(f"\n{'='*70}")
    print(f"🏥 MARIADB HEALTH CHECK (federated query impact)")
    print(f"{'='*70}")

    conn = pymysql.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()

    # Check active connections
    cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
    threads = cursor.fetchone()
    print(f"   Active connections: {threads[1]}")

    # Check max connections
    cursor.execute("SHOW VARIABLES LIKE 'max_connections'")
    max_conn = cursor.fetchone()
    print(f"   Max connections: {max_conn[1]}")

    connection_pct = (int(threads[1]) / int(max_conn[1])) * 100
    status = "✅" if connection_pct < 70 else "⚠️" if connection_pct < 90 else "❌"
    print(f"   Usage: {connection_pct:.1f}% {status}")

    # Check slow queries
    cursor.execute("SHOW STATUS LIKE 'Slow_queries'")
    slow = cursor.fetchone()
    print(f"   Slow queries (total): {slow[1]}")

    # Check questions (total queries)
    cursor.execute("SHOW STATUS LIKE 'Questions'")
    questions = cursor.fetchone()
    print(f"   Total queries served: {questions[1]}")

    # Check current running queries
    cursor.execute("SHOW PROCESSLIST")
    processes = cursor.fetchall()
    athena_processes = [p for p in processes if p[1] == "athena_reader"]
    print(f"   Active Athena reader connections: {len(athena_processes)}")

    for proc in athena_processes:
        proc_id, user, host, db, command, proc_time, state, info = proc[:8]
        query_preview = (info[:60] + "...") if info and len(info) > 60 else (info or "")
        print(f"      ID:{proc_id} Time:{proc_time}s State:{state} Query:{query_preview}")

    # Check InnoDB status
    cursor.execute("SHOW STATUS LIKE 'Innodb_row_lock_waits'")
    lock_waits = cursor.fetchone()
    print(f"   InnoDB row lock waits: {lock_waits[1]}")

    cursor.close()
    conn.close()

    # Alerts
    alerts = []
    if connection_pct > 80:
        alerts.append(f"Connection usage at {connection_pct:.0f}% — consider reducing Lambda concurrency")
    if len(athena_processes) > 5:
        alerts.append(f"{len(athena_processes)} concurrent federated queries — heavy load")

    if alerts:
        print(f"\n⚠️  ALERTS:")
        for alert in alerts:
            print(f"   → {alert}")
        send_impact_alert(alerts)
    else:
        print(f"\n✅ MariaDB health: GOOD (minimal federated query impact)")

    return {"connection_pct": connection_pct, "athena_connections": len(athena_processes)}


def send_impact_alert(alerts):
    """Send alert if federated queries are impacting MariaDB"""
    sns_client = boto3.client("sns", region_name=REGION)

    message = "\n".join([
        "⚠️ Federated Query Impact Alert",
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Issues detected:",
        *[f"  → {a}" for a in alerts],
        "",
        "Recommended actions:",
        "  1. Reduce Lambda reserved concurrency",
        "  2. Identify and convert frequent queries to ETL pipelines",
        "  3. Consider routing federated queries to read replica"
    ])

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="⚠️ Federated Query Impact — MariaDB",
            Message=message
        )
        print(f"   📧 Impact alert sent")
    except Exception as e:
        print(f"   ⚠️  Alert failed: {e}")


def generate_recommendations(query_analysis, mariadb_health):
    """Generate actionable recommendations based on analysis"""
    print(f"\n{'='*70}")
    print(f"💡 RECOMMENDATIONS")
    print(f"{'='*70}")

    recommendations = []

    # Cost-based recommendations
    if query_analysis and query_analysis.get("total_cost", 0) > 10:
        recommendations.append(
            "HIGH COST: Federated queries cost >${:.2f}/week. "
            "Convert top 5 most expensive queries to ETL pipelines.".format(
                query_analysis["total_cost"]
            )
        )

    # Frequency-based recommendations
    if query_analysis and query_analysis.get("total_queries", 0) > 50:
        recommendations.append(
            "HIGH FREQUENCY: {} queries/week via federated. "
            "Identify repeated patterns and build scheduled ETL.".format(
                query_analysis["total_queries"]
            )
        )

    # MariaDB impact recommendations
    if mariadb_health and mariadb_health.get("connection_pct", 0) > 70:
        recommendations.append(
            "DB LOAD: MariaDB at {:.0f}% connection capacity. "
            "Route federated queries to read replica.".format(
                mariadb_health["connection_pct"]
            )
        )

    # Failure-based recommendations
    if query_analysis and query_analysis.get("failed", 0) > 5:
        recommendations.append(
            "FAILURES: {} failed queries. "
            "Check Lambda logs for connection timeouts or OOM errors.".format(
                query_analysis["failed"]
            )
        )

    if not recommendations:
        recommendations.append(
            "ALL GOOD: Federated query usage is healthy. "
            "Continue monitoring weekly."
        )

    for i, rec in enumerate(recommendations, 1):
        print(f"   {i}. {rec}")

    print(f"{'='*70}")


if __name__ == "__main__":
    query_analysis = analyze_federated_query_history(days_back=7)
    mariadb_health = check_mariadb_impact()
    generate_recommendations(query_analysis, mariadb_health)