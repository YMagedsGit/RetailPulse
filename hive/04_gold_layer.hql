-- =====================================================================
-- Gold layer — Hive-built tables only
-- Source: retailpulse_silver  |  Target: retailpulse_gold
-- Run after 02_silver_dimensions.hql and 03_silver_facts.hql
--
-- daily_store_revenue, payment_health, inventory_health, and
-- data_quality_summary are NOT here — those four kept hitting
-- OutOfMemoryError under Hive-on-MR's ~512MB local-mode heap and were
-- rebuilt in PySpark instead (GoldLayerBatch.py), writing to the same
-- /retailpulse/gold/<table> paths. See documentation for the memory
-- root cause. Everything below ran fine under Hive as-is.
-- =====================================================================

USE retailpulse_gold;

-- ---------------------------------------------------------------------
-- customer_summary
--   One row per customer: lifetime value, order history, recency.
--   Small-ish table (~40K rows) — full reload each run, no partitioning.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS customer_summary;

CREATE TABLE customer_summary
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
SELECT
    c.customer_id,
    c.full_name,
    c.country_code,
    c.signup_at,
    COUNT(DISTINCT CASE WHEN o.order_status != 'cancelled' THEN o.order_id END) AS completed_order_count,
    SUM(CASE WHEN o.order_status != 'cancelled' THEN o.order_total - o.discount_amount ELSE 0 END) AS lifetime_value,
    MIN(o.order_timestamp) AS first_order_at,
    MAX(o.order_timestamp) AS last_order_at,
    DATEDIFF(CURRENT_DATE, MAX(o.order_timestamp)) AS days_since_last_order
FROM retailpulse_silver.customers c
LEFT JOIN retailpulse_silver.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.full_name, c.country_code, c.signup_at;

-- ---------------------------------------------------------------------
-- product_performance
--   Units sold, revenue, and margin realized per product.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS product_performance;

CREATE TABLE product_performance
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
SELECT
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.net_line_amount) AS revenue,
    SUM(oi.quantity * p.unit_cost) AS total_cost,
    SUM(oi.net_line_amount) - SUM(oi.quantity * p.unit_cost) AS realized_margin
FROM retailpulse_silver.order_items oi
JOIN retailpulse_silver.products p ON oi.product_id = p.product_id
JOIN retailpulse_silver.orders o ON oi.order_id = o.order_id AND o.order_status != 'cancelled'
GROUP BY p.product_id, p.sku, p.product_name, p.category;

-- ---------------------------------------------------------------------
-- fulfillment_sla
--   Avg hours from packed -> shipped -> delivered, per warehouse per day.
--   Uses conditional aggregation to pivot event_type into columns first.
--
--   KNOWN GAP: this table only holds averages, so it cannot answer
--   "how many orders missed SLA" — that needs an order-grain rebuild
--   with a defined threshold and an is_late flag before aggregating.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS fulfillment_sla;

CREATE TABLE fulfillment_sla
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
WITH pivoted AS (
    SELECT
        order_id,
        warehouse_code,
        MIN(CASE WHEN event_type = 'packed'    THEN event_timestamp END) AS packed_at,
        MIN(CASE WHEN event_type = 'shipped'   THEN event_timestamp END) AS shipped_at,
        MIN(CASE WHEN event_type = 'delivered' THEN event_timestamp END) AS delivered_at
    FROM retailpulse_silver.fulfillment_events
    GROUP BY order_id, warehouse_code
)
SELECT
    warehouse_code,
    DATE_FORMAT(packed_at, 'yyyy-MM-dd') AS packed_date,
    COUNT(*) AS order_count,
    AVG((UNIX_TIMESTAMP(shipped_at) - UNIX_TIMESTAMP(packed_at)) / 3600.0) AS avg_hours_packed_to_shipped,
    AVG((UNIX_TIMESTAMP(delivered_at) - UNIX_TIMESTAMP(shipped_at)) / 3600.0) AS avg_hours_shipped_to_delivered,
    AVG((UNIX_TIMESTAMP(delivered_at) - UNIX_TIMESTAMP(packed_at)) / 3600.0) AS avg_hours_packed_to_delivered
FROM pivoted
WHERE packed_at IS NOT NULL
GROUP BY warehouse_code, DATE_FORMAT(packed_at, 'yyyy-MM-dd');

-- ---------------------------------------------------------------------
-- digital_funnel_daily
--   Grain: one row per (event_date, channel).
--   Source: retailpulse_silver.app_events (streaming track)
--   Refresh: full recompute each run.
--   Stage counts use COUNT(DISTINCT session_id), not raw event counts.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS digital_funnel_daily;

CREATE TABLE digital_funnel_daily (
    channel                  STRING,
    sessions_viewed          BIGINT,
    sessions_cart            BIGINT,
    sessions_checkout        BIGINT,
    sessions_confirmed       BIGINT,
    view_to_cart_rate        DECIMAL(5,4),
    cart_to_checkout_rate    DECIMAL(5,4),
    checkout_to_order_rate   DECIMAL(5,4),
    view_to_order_rate       DECIMAL(5,4)
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE digital_funnel_daily PARTITION (event_date)
SELECT
    channel,
    COUNT(DISTINCT CASE WHEN event_type = 'product_viewed'   THEN session_id END) AS sessions_viewed,
    COUNT(DISTINCT CASE WHEN event_type = 'cart_updated'     THEN session_id END) AS sessions_cart,
    COUNT(DISTINCT CASE WHEN event_type = 'checkout_started' THEN session_id END) AS sessions_checkout,
    COUNT(DISTINCT CASE WHEN event_type = 'order_confirmed'  THEN session_id END) AS sessions_confirmed,
    CAST(COUNT(DISTINCT CASE WHEN event_type = 'cart_updated' THEN session_id END)
       / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'product_viewed' THEN session_id END), 0)
       AS DECIMAL(5,4)) AS view_to_cart_rate,
    CAST(COUNT(DISTINCT CASE WHEN event_type = 'checkout_started' THEN session_id END)
       / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'cart_updated' THEN session_id END), 0)
       AS DECIMAL(5,4)) AS cart_to_checkout_rate,
    CAST(COUNT(DISTINCT CASE WHEN event_type = 'order_confirmed' THEN session_id END)
       / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'checkout_started' THEN session_id END), 0)
       AS DECIMAL(5,4)) AS checkout_to_order_rate,
    CAST(COUNT(DISTINCT CASE WHEN event_type = 'order_confirmed' THEN session_id END)
       / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'product_viewed' THEN session_id END), 0)
       AS DECIMAL(5,4)) AS view_to_order_rate,
    event_date
FROM retailpulse_silver.app_events
GROUP BY channel, event_date;

-- ---------------------------------------------------------------------
-- Validation — only the four tables this script actually builds
-- ---------------------------------------------------------------------
SELECT 'customer_summary' AS tbl, COUNT(*) AS row_count FROM customer_summary
UNION ALL SELECT 'product_performance', COUNT(*) FROM product_performance
UNION ALL SELECT 'fulfillment_sla', COUNT(*) FROM fulfillment_sla
UNION ALL SELECT 'digital_funnel_daily', COUNT(*) FROM digital_funnel_daily;
