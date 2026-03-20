# file: 39_04_benchmark.py
# Purpose: Generate before/after benchmark queries
# Run BEFORE optimization to capture baseline
# Run AFTER optimization to measure improvement

def generate_benchmark_sql():
    """Generate benchmark queries with timing"""

    print("=" * 70)
    print("📊 PERFORMANCE BENCHMARK QUERIES")
    print("=" * 70)
    print(f"""
-- ═══════════════════════════════════════════════════════════════
-- RUN THESE BEFORE AND AFTER OPTIMIZATION
-- Record execution times for comparison
-- Use SET enable_result_cache_for_session TO OFF; to disable cache
-- ═══════════════════════════════════════════════════════════════

-- Disable result caching (forces fresh execution)
SET enable_result_cache_for_session TO OFF;

-- ── BENCHMARK 1: Simple JOIN (fact × dimension) ──
-- Tests: DISTKEY co-location effectiveness
-- Before (EVEN): ~6 seconds (network shuffle)
-- After (DISTKEY match): ~0.3 seconds (co-located)

SELECT o.order_id, o.total_amount, o.status,
       c.name AS customer_name, c.tier
FROM analytics.fact_orders o
JOIN analytics.dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - 90
LIMIT 1000;


-- ── BENCHMARK 2: Date-Filtered Aggregation ──
-- Tests: SORTKEY zone map effectiveness
-- Before (no SORTKEY): ~3.8 seconds (full scan)
-- After (SORTKEY order_date): ~0.2 seconds (zone map skip)

SELECT
    order_date,
    COUNT(*) AS order_count,
    SUM(total_amount) AS daily_revenue,
    AVG(total_amount) AS avg_order_value
FROM analytics.fact_orders
WHERE order_date >= '2025-01-01'
GROUP BY order_date
ORDER BY order_date;


-- ── BENCHMARK 3: Three-Way JOIN ──
-- Tests: DISTKEY + ALL distribution combined
-- Before: ~12 seconds (double shuffle)
-- After: ~0.4 seconds (co-located + broadcast)

SELECT
    p.category AS product_category,
    c.tier AS customer_tier,
    COUNT(DISTINCT o.order_id) AS orders,
    ROUND(SUM(o.total_amount), 2) AS revenue,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM analytics.fact_orders o
JOIN analytics.dim_customers c ON o.customer_id = c.customer_id
JOIN analytics.dim_products p ON o.product_id = p.product_id
WHERE o.order_date >= CURRENT_DATE - 30
  AND o.status = 'completed'
GROUP BY p.category, c.tier
ORDER BY revenue DESC;


-- ── BENCHMARK 4: Mixed Query (Local + Spectrum) ──
-- Tests: Spectrum join with optimized local table
-- Before: ~15 seconds (local shuffle + Spectrum)
-- After: ~2 seconds (co-located local + Spectrum broadcast)

SELECT
    o.order_id,
    o.total_amount,
    o.order_date,
    r.rfm_segment,
    r.monetary AS lifetime_value
FROM analytics.fact_orders o
LEFT JOIN ext_silver.customer_rfm r ON o.customer_id = r.customer_id
WHERE o.order_date >= CURRENT_DATE - 7
ORDER BY o.total_amount DESC
LIMIT 100;


-- ── BENCHMARK 5: Heavy Aggregation ──
-- Tests: overall query engine efficiency after optimization
-- Before: ~8 seconds
-- After: ~0.5 seconds

SELECT
    DATE_TRUNC('month', order_date) AS month,
    status,
    COUNT(*) AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    ROUND(STDDEV(total_amount), 2) AS stddev_order_value,
    MIN(total_amount) AS min_order,
    MAX(total_amount) AS max_order
FROM analytics.fact_orders
WHERE order_date >= '2024-01-01'
GROUP BY DATE_TRUNC('month', order_date), status
ORDER BY month DESC, total_revenue DESC;


-- ── BENCHMARK 6: Subquery with Window Function ──
-- Tests: complex query optimization
-- Before: ~18 seconds
-- After: ~1.5 seconds

SELECT *
FROM (
    SELECT
        customer_id,
        order_date,
        total_amount,
        SUM(total_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY order_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY order_date DESC
        ) AS recency_rank
    FROM analytics.fact_orders
    WHERE status = 'completed'
      AND order_date >= '2025-01-01'
) ranked
WHERE recency_rank = 1
ORDER BY running_total DESC
LIMIT 50;


-- ═══════════════════════════════════════════════════════════════
-- CAPTURE BENCHMARK RESULTS
-- Run this AFTER each benchmark to see execution stats
-- ═══════════════════════════════════════════════════════════════

SELECT
    q.query,
    q.elapsed / 1000000.0 AS elapsed_seconds,
    q.rows AS result_rows,
    SUBSTRING(q.querytxt, 1, 80) AS query_preview,
    q.starttime
FROM stl_query q
WHERE q.userid > 1
  AND q.starttime > CURRENT_TIMESTAMP - INTERVAL '10 minutes'
  AND q.querytxt NOT LIKE '%stl_%'
  AND q.querytxt NOT LIKE 'SET%'
  AND q.querytxt NOT LIKE 'EXPLAIN%'
ORDER BY q.starttime DESC
LIMIT 20;


-- Re-enable result caching for normal operations
SET enable_result_cache_for_session TO ON;
    """)
    print("=" * 70)
    print(f"""
📊 BENCHMARK RECORDING TEMPLATE:

┌────────────────────────────────┬──────────────┬──────────────┬──────────┐
│ Benchmark                      │ Before (sec) │ After (sec)  │ Speedup  │
├────────────────────────────────┼──────────────┼──────────────┼──────────┤
│ 1. Simple JOIN (fact × dim)    │ ___________  │ ___________  │ _____x   │
│ 2. Date-Filtered Aggregation   │ ___________  │ ___________  │ _____x   │
│ 3. Three-Way JOIN              │ ___________  │ ___________  │ _____x   │
│ 4. Mixed (Local + Spectrum)    │ ___________  │ ___________  │ _____x   │
│ 5. Heavy Aggregation           │ ___________  │ ___________  │ _____x   │
│ 6. Window Function             │ ___________  │ ___________  │ _____x   │
└────────────────────────────────┴──────────────┴──────────────┴──────────┘

Fill in before/after times and share with stakeholders.
    """)
    print("=" * 70)


if __name__ == "__main__":
    generate_benchmark_sql()