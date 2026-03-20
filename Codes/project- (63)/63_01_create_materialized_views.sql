# file: 63_01_create_materialized_views.sql
# Run on: Redshift cluster in Account B
# Purpose: Create all 12 dashboard MVs
# Execute via: psycopg2 from Python or Redshift Query Editor

MATERIALIZED_VIEWS_SQL = """
-- ============================================================
-- MV 1: Daily Revenue (Last 30 Days)
-- Dashboard Q1: Line chart of daily revenue
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_daily_revenue
DISTSTYLE ALL
SORTKEY (order_date)
AUTO REFRESH YES
AS
SELECT
    order_date,
    COUNT(order_id) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM quickcart_ops.orders
WHERE order_date >= CURRENT_DATE - 30
  AND status != 'cancelled'
GROUP BY order_date;


-- ============================================================
-- MV 2: Revenue by Product Category (Top 10)
-- Dashboard Q2: Bar chart of category performance
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_category_revenue
DISTSTYLE ALL
AUTO REFRESH YES
AS
SELECT
    p.category,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM quickcart_ops.orders o
JOIN quickcart_ops.dim_products p
    ON o.product_id = p.product_id
WHERE o.order_date >= CURRENT_DATE - 30
  AND o.status != 'cancelled'
GROUP BY p.category;


-- ============================================================
-- MV 3: Customer Tier Distribution with Average Spend
-- Dashboard Q3: Donut chart of tier breakdown
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_customer_tier_spend
DISTSTYLE ALL
AUTO REFRESH YES
AS
SELECT
    c.tier,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    SUM(o.total_amount) / NULLIF(COUNT(DISTINCT c.customer_id), 0) AS avg_ltv
FROM quickcart_ops.dim_customers c
LEFT JOIN quickcart_ops.orders o
    ON c.customer_id = o.customer_id
    AND o.order_date >= CURRENT_DATE - 90
    AND o.status != 'cancelled'
GROUP BY c.tier;


-- ============================================================
-- MV 4: Hourly Order Volume Heatmap (7 Days)
-- Dashboard Q4: Heatmap of order volume by day × hour
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_hourly_volume
DISTSTYLE ALL
SORTKEY (order_date, order_hour)
AUTO REFRESH YES
AS
SELECT
    order_date,
    EXTRACT(DOW FROM order_date) AS day_of_week,
    EXTRACT(HOUR FROM order_date) AS order_hour,
    COUNT(order_id) AS order_count,
    SUM(total_amount) AS total_revenue
FROM quickcart_ops.orders
WHERE order_date >= CURRENT_DATE - 7
  AND status != 'cancelled'
GROUP BY order_date,
         EXTRACT(DOW FROM order_date),
         EXTRACT(HOUR FROM order_date);


-- ============================================================
-- MV 5: Cumulative Revenue (90 Days)
-- Dashboard Q5: Running total line chart
-- NOTE: Window function prevents incremental refresh
--        → Will use full refresh only
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_cumulative_revenue
DISTSTYLE ALL
SORTKEY (order_date)
AUTO REFRESH YES
AS
SELECT
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING) AS cumulative_revenue
FROM (
    SELECT
        order_date,
        SUM(total_amount) AS daily_revenue
    FROM quickcart_ops.orders
    WHERE order_date >= CURRENT_DATE - 90
      AND status != 'cancelled'
    GROUP BY order_date
);


-- ============================================================
-- MV 6: Top 100 Customers by Lifetime Value
-- Dashboard Q6: Leaderboard table
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_top_customers
DISTSTYLE ALL
AUTO REFRESH YES
AS
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.city,
    c.tier,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS lifetime_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM quickcart_ops.dim_customers c
JOIN quickcart_ops.orders o
    ON c.customer_id = o.customer_id
WHERE o.status != 'cancelled'
GROUP BY c.customer_id, c.name, c.email, c.city, c.tier;


-- ============================================================
-- MV 7: Product Return Rate by Category
-- Dashboard Q7: Horizontal bar chart
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_return_rate
DISTSTYLE ALL
AUTO REFRESH YES
AS
SELECT
    p.category,
    COUNT(o.order_id) AS total_orders,
    SUM(CASE WHEN o.status = 'refunded' THEN 1 ELSE 0 END) AS refunded_orders,
    ROUND(
        100.0 * SUM(CASE WHEN o.status = 'refunded' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(o.order_id), 0), 2
    ) AS return_rate_pct
FROM quickcart_ops.orders o
JOIN quickcart_ops.dim_products p
    ON o.product_id = p.product_id
WHERE o.order_date >= CURRENT_DATE - 90
GROUP BY p.category;


-- ============================================================
-- MV 8: New vs Returning Customer Orders (30 Days)
-- Dashboard Q8: Stacked area chart
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_new_vs_returning
DISTSTYLE ALL
SORTKEY (order_date)
AUTO REFRESH YES
AS
SELECT
    o.order_date,
    SUM(CASE WHEN c.signup_date >= CURRENT_DATE - 30 THEN 1 ELSE 0 END) AS new_customer_orders,
    SUM(CASE WHEN c.signup_date < CURRENT_DATE - 30 THEN 1 ELSE 0 END) AS returning_customer_orders,
    SUM(CASE WHEN c.signup_date >= CURRENT_DATE - 30 THEN o.total_amount ELSE 0 END) AS new_customer_revenue,
    SUM(CASE WHEN c.signup_date < CURRENT_DATE - 30 THEN o.total_amount ELSE 0 END) AS returning_customer_revenue
FROM quickcart_ops.orders o
JOIN quickcart_ops.dim_customers c
    ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - 30
  AND o.status != 'cancelled'
GROUP BY o.order_date;


-- ============================================================
-- MV 9: Weekly Average Order Value (52 Weeks)
-- Dashboard Q9: Trend line with moving average
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_weekly_aov
DISTSTYLE ALL
SORTKEY (week_start)
AUTO REFRESH YES
AS
SELECT
    DATE_TRUNC('week', order_date) AS week_start,
    COUNT(order_id) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM quickcart_ops.orders
WHERE order_date >= CURRENT_DATE - 364
  AND status != 'cancelled'
GROUP BY DATE_TRUNC('week', order_date);


-- ============================================================
-- MV 10: Geographic Revenue by City
-- Dashboard Q10: Map visualization
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_geo_revenue
DISTSTYLE ALL
AUTO REFRESH YES
AS
SELECT
    c.city,
    c.state,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value
FROM quickcart_ops.orders o
JOIN quickcart_ops.dim_customers c
    ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - 90
  AND o.status != 'cancelled'
GROUP BY c.city, c.state;


-- ============================================================
-- MV 11: Order Status Funnel (7 Days)
-- Dashboard Q11: Funnel chart
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_order_funnel
DISTSTYLE ALL
AUTO REFRESH YES
AS
SELECT
    status,
    COUNT(order_id) AS order_count,
    SUM(total_amount) AS total_value,
    ROUND(
        100.0 * COUNT(order_id)
        / NULLIF(SUM(COUNT(order_id)) OVER (), 0), 2
    ) AS pct_of_total
FROM quickcart_ops.orders
WHERE order_date >= CURRENT_DATE - 7
GROUP BY status;


-- ============================================================
-- MV 12: Monthly Forecast vs Actual Revenue
-- Dashboard Q12: Comparison bar chart (12 months)
-- ============================================================
CREATE MATERIALIZED VIEW quickcart_ops.mv_forecast_vs_actual
DISTSTYLE ALL
SORTKEY (revenue_month)
AUTO REFRESH YES
AS
SELECT
    DATE_TRUNC('month', order_date) AS revenue_month,
    SUM(total_amount) AS actual_revenue,
    COUNT(order_id) AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    AVG(total_amount) AS avg_order_value
FROM quickcart_ops.orders
WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
  AND status != 'cancelled'
GROUP BY DATE_TRUNC('month', order_date);
"""