-- ── Orders Analysis Queries ───────────────────────────────────────────────────
-- Run these in Athena against the processed data lake
-- Replace 'datalake_etl_dev_db' with your actual Glue database name

-- 1. Daily revenue summary
SELECT
    year,
    month,
    day,
    COUNT(*)                            AS total_orders,
    SUM(total_amount)                   AS daily_revenue,
    AVG(total_amount)                   AS avg_order_value,
    COUNT(DISTINCT customer_id)         AS unique_customers
FROM datalake_etl_dev_db.orders
WHERE status != 'cancelled'
GROUP BY year, month, day
ORDER BY year, month, day;

-- 2. Top products by revenue
SELECT
    product_name,
    COUNT(*)            AS order_count,
    SUM(quantity)       AS units_sold,
    SUM(total_amount)   AS total_revenue
FROM datalake_etl_dev_db.orders
WHERE status = 'completed'
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Customer order history
SELECT
    customer_id,
    COUNT(*)            AS total_orders,
    SUM(total_amount)   AS lifetime_value,
    MIN(created_at)     AS first_order,
    MAX(created_at)     AS last_order
FROM datalake_etl_dev_db.orders
GROUP BY customer_id
ORDER BY lifetime_value DESC;

-- 4. Order status breakdown
SELECT
    status,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM datalake_etl_dev_db.orders
GROUP BY status;
