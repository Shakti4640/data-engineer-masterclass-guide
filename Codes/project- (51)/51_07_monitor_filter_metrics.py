# file: 51_07_monitor_filter_metrics.py
# Purpose: Monitor SNS filtering effectiveness via CloudWatch

import boto3
import json
from datetime import datetime, timedelta

REGION = "us-east-2"


def load_infrastructure():
    with open("/tmp/filtered_infra.json", "r") as f:
        return json.load(f)


def get_sns_filter_metrics(topic_arn, hours_back=1):
    """
    Retrieve CloudWatch metrics that show filter effectiveness
    
    KEY METRICS:
    → NumberOfMessagesPublished: total messages published to topic
    → NumberOfNotificationsDelivered: messages actually delivered (per subscription)
    → NumberOfNotificationsFilteredOut: messages blocked by filter
    → NumberOfNotificationsFilteredOut-NoMessageAttributes:
      messages dropped because publisher didn't set attributes
    → NumberOfNotificationsFailed: delivery failures (SQS down, policy wrong)
    """
    cw_client = boto3.client("cloudwatch", region_name=REGION)
    
    # Extract topic name from ARN for dimension
    topic_name = topic_arn.split(":")[-1]

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours_back)

    print("=" * 70)
    print(f"📊 SNS FILTER METRICS — Last {hours_back} hour(s)")
    print(f"   Topic: {topic_name}")
    print("=" * 70)

    metrics_to_check = [
        {
            "name": "NumberOfMessagesPublished",
            "stat": "Sum",
            "description": "Total messages published to topic"
        },
        {
            "name": "NumberOfNotificationsDelivered",
            "stat": "Sum",
            "description": "Messages successfully delivered to subscriptions"
        },
        {
            "name": "NumberOfNotificationsFilteredOut",
            "stat": "Sum",
            "description": "Messages blocked by filter policies"
        },
        {
            "name": "NumberOfNotificationsFilteredOut-NoMessageAttributes",
            "stat": "Sum",
            "description": "Messages dropped — publisher forgot attributes"
        },
        {
            "name": "NumberOfNotificationsFilteredOut-InvalidAttributes",
            "stat": "Sum",
            "description": "Messages dropped — attribute type mismatch"
        },
        {
            "name": "NumberOfNotificationsFailed",
            "stat": "Sum",
            "description": "Delivery failures (SQS permission, queue deleted)"
        }
    ]

    for metric_info in metrics_to_check:
        response = cw_client.get_metric_statistics(
            Namespace="AWS/SNS",
            MetricName=metric_info["name"],
            Dimensions=[
                {"Name": "TopicName", "Value": topic_name}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,    # 1-hour granularity
            Statistics=[metric_info["stat"]]
        )

        datapoints = response.get("Datapoints", [])
        if datapoints:
            # Sum all datapoints in the window
            total = sum(dp[metric_info["stat"]] for dp in datapoints)
        else:
            total = 0

        status = "⚠️" if "FilteredOut" in metric_info["name"] and total > 0 else "✅"
        if "Failed" in metric_info["name"] and total > 0:
            status = "🔴"

        print(f"\n  {status} {metric_info['name']}")
        print(f"     Value: {int(total)}")
        print(f"     Description: {metric_info['description']}")

    return True


def create_filter_alarm(topic_arn):
    """
    Create CloudWatch alarm for when too many messages
    are filtered out due to missing attributes
    
    WHY THIS MATTERS:
    → If NumberOfNotificationsFilteredOut-NoMessageAttributes spikes
    → It means a publisher is sending without MessageAttributes
    → Those messages reach ONLY the catch-all queue (Audit)
    → All filtered queues miss them → silent data loss for consumers
    """
    cw_client = boto3.client("cloudwatch", region_name=REGION)
    sns_client = boto3.client("sns", region_name=REGION)
    topic_name = topic_arn.split(":")[-1]

    # Create alert topic (separate from data topic)
    alert_topic = sns_client.create_topic(Name="quickcart-filter-alerts")
    alert_topic_arn = alert_topic["TopicArn"]

    # Create alarm
    cw_client.put_metric_alarm(
        AlarmName="SNS-FilteredOut-NoAttributes-High",
        AlarmDescription=(
            "ALERT: Messages being published WITHOUT MessageAttributes. "
            "Filtered queues are missing messages. "
            "Check all publishers for missing MessageAttributes."
        ),
        Namespace="AWS/SNS",
        MetricName="NumberOfNotificationsFilteredOut-NoMessageAttributes",
        Dimensions=[
            {"Name": "TopicName", "Value": topic_name}
        ],
        Statistic="Sum",
        Period=300,             # 5-minute evaluation window
        EvaluationPeriods=1,    # Alert after 1 period
        Threshold=10,           # More than 10 messages without attributes
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[alert_topic_arn],
        OKActions=[alert_topic_arn],
        Tags=[
            {"Key": "Team", "Value": "Data-Engineering"},
            {"Key": "Severity", "Value": "High"}
        ]
    )

    print(f"\n🔔 CloudWatch Alarm Created:")
    print(f"   Name: SNS-FilteredOut-NoAttributes-High")
    print(f"   Triggers when: >10 messages in 5 min lack MessageAttributes")
    print(f"   Alert topic: {alert_topic_arn}")
    print(f"   → Subscribe your email/PagerDuty to alert topic")

    # Also create alarm for delivery failures
    cw_client.put_metric_alarm(
        AlarmName="SNS-DeliveryFailures-High",
        AlarmDescription=(
            "ALERT: SNS failing to deliver messages to SQS subscriptions. "
            "Check SQS queue policies, queue existence, and IAM permissions."
        ),
        Namespace="AWS/SNS",
        MetricName="NumberOfNotificationsFailed",
        Dimensions=[
            {"Name": "TopicName", "Value": topic_name}
        ],
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=1,
        Threshold=1,            # Even 1 failure is concerning
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[alert_topic_arn],
        Tags=[
            {"Key": "Team", "Value": "Data-Engineering"},
            {"Key": "Severity", "Value": "Critical"}
        ]
    )

    print(f"\n🔔 CloudWatch Alarm Created:")
    print(f"   Name: SNS-DeliveryFailures-High")
    print(f"   Triggers when: >1 delivery failure in 5 min")

    return alert_topic_arn


def calculate_cost_savings(total_published, num_subscriptions, avg_match_rate):
    """
    Calculate SQS cost savings from SNS filtering
    
    Without filtering: every message goes to every queue
    With filtering: only matching messages go to matching queues
    """
    print("\n" + "=" * 70)
    print("💰 COST SAVINGS CALCULATION")
    print("=" * 70)

    # Without filtering
    sqs_messages_without = total_published * num_subscriptions
    sqs_cost_without = (sqs_messages_without / 1_000_000) * 0.40  # $0.40 per million

    # With filtering
    sqs_messages_with = total_published * num_subscriptions * avg_match_rate
    sqs_cost_with = (sqs_messages_with / 1_000_000) * 0.40

    savings = sqs_cost_without - sqs_cost_with
    savings_pct = ((sqs_cost_without - sqs_cost_with) / sqs_cost_without) * 100

    print(f"\n  Assumptions:")
    print(f"    Messages published/day:     {total_published:>12,}")
    print(f"    Number of subscriptions:    {num_subscriptions:>12}")
    print(f"    Average filter match rate:  {avg_match_rate:>11.0%}")

    print(f"\n  WITHOUT filtering:")
    print(f"    SQS messages/day:           {sqs_messages_without:>12,}")
    print(f"    SQS cost/day:               ${sqs_cost_without:>11.2f}")
    print(f"    SQS cost/month (30 days):   ${sqs_cost_without * 30:>11.2f}")

    print(f"\n  WITH filtering:")
    print(f"    SQS messages/day:           {int(sqs_messages_with):>12,}")
    print(f"    SQS cost/day:               ${sqs_cost_with:>11.2f}")
    print(f"    SQS cost/month (30 days):   ${sqs_cost_with * 30:>11.2f}")

    print(f"\n  💰 SAVINGS:")
    print(f"    Per day:                    ${savings:>11.2f}")
    print(f"    Per month:                  ${savings * 30:>11.2f}")
    print(f"    Percentage saved:           {savings_pct:>11.1f}%")
    print(f"    + Compute savings from NOT processing unwanted messages")

    print("=" * 70)


if __name__ == "__main__":
    infra = load_infrastructure()
    
    # Check metrics
    get_sns_filter_metrics(infra["topic_arn"], hours_back=1)
    
    # Create alarms
    create_filter_alarm(infra["topic_arn"])
    
    # Calculate savings
    calculate_cost_savings(
        total_published=200_000,     # 200K messages/day
        num_subscriptions=4,         # 4 queues
        avg_match_rate=0.30          # each queue matches ~30% of messages
    )