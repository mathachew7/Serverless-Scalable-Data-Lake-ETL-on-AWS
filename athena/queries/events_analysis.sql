-- ── Events Analysis Queries ───────────────────────────────────────────────────

-- 1. Event type distribution
SELECT
    event_type,
    COUNT(*)  AS event_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM datalake_etl_dev_db.events
GROUP BY event_type
ORDER BY event_count DESC;

-- 2. Traffic by country and device
SELECT
    country,
    device,
    COUNT(*) AS sessions
FROM datalake_etl_dev_db.events
WHERE event_type = 'page_view'
GROUP BY country, device
ORDER BY sessions DESC;

-- 3. Conversion funnel (page_view → click → purchase)
WITH funnel AS (
    SELECT
        session_id,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS has_view,
        MAX(CASE WHEN event_type = 'click'     THEN 1 ELSE 0 END) AS has_click,
        MAX(CASE WHEN event_type = 'purchase'  THEN 1 ELSE 0 END) AS has_purchase
    FROM datalake_etl_dev_db.events
    GROUP BY session_id
)
SELECT
    SUM(has_view)     AS views,
    SUM(has_click)    AS clicks,
    SUM(has_purchase) AS purchases,
    ROUND(SUM(has_purchase) * 100.0 / NULLIF(SUM(has_view), 0), 2) AS conversion_rate_pct
FROM funnel;
