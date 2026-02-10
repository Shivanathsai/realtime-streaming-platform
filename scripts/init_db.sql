-- Initialize streaming platform database
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Materialized view for real-time dashboard
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_summary AS
SELECT
    date_trunc('hour', timestamp) AS hour,
    count(*) AS event_count,
    sum(amount) AS total_amount,
    avg(amount) AS avg_amount,
    count(DISTINCT user_id) AS unique_users,
    count(DISTINCT merchant_id) AS unique_merchants
FROM events
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY date_trunc('hour', timestamp)
ORDER BY hour DESC;

-- Alert summary view
CREATE MATERIALIZED VIEW IF NOT EXISTS alert_summary AS
SELECT
    date_trunc('hour', timestamp) AS hour,
    severity,
    pattern_name,
    count(*) AS alert_count,
    count(DISTINCT user_id) AS affected_users
FROM alerts
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY date_trunc('hour', timestamp), severity, pattern_name
ORDER BY hour DESC;
