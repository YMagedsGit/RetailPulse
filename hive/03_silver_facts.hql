-- =====================================================================
-- Silver layer — FACT tables
-- Source: retailpulse_bronze  |  Target: retailpulse_silver
-- Run after 02_silver_dimensions.hql (order_items joins to silver.orders)
-- =====================================================================

USE retailpulse_silver;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- ---------------------------------------------------------------------
-- orders
--   - order_status: lowercased/trimmed ('COMPLETED' -> 'completed')
--   - discount_amount: NULL -> 0
--   - partitioned by order_date for partition-pruned daily queries
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id         BIGINT,
    customer_id      INT,
    store_id         INT,
    order_status     STRING,
    order_timestamp  TIMESTAMP,
    order_total      DECIMAL(12,2),
    discount_amount  DECIMAL(12,2),
    updated_at       TIMESTAMP
)
PARTITIONED BY (order_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');


INSERT OVERWRITE TABLE orders PARTITION (order_date)
SELECT
    order_id,
    customer_id,
    store_id,
    LOWER(TRIM(order_status)) AS order_status,
    order_timestamp,
    CAST(order_total AS DECIMAL(12,2)) AS order_total,
    CAST(COALESCE(discount_amount, 0) AS DECIMAL(12,2)) AS discount_amount,
    updated_at,
    DATE_FORMAT(order_timestamp, 'yyyy-MM-dd') AS order_date
FROM retailpulse_bronze.orders
WHERE order_id IS NOT NULL
DISTRIBUTE BY DATE_FORMAT(order_timestamp, 'yyyy-MM-dd');




SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=2000;

-- Memory & Sorting Tuning for Parquet Writers
SET mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts=-Xmx3072m;
SET mapred.max.split.size=256000000;
SET hive.optimize.sort.dynamic.partition=true;

DROP TABLE IF EXISTS order_items;

CREATE TABLE order_items (
    order_item_id    BIGINT,
    order_id         BIGINT,
    product_id       INT,
    quantity         INT,
    unit_price       DECIMAL(12,2),
    line_discount    DECIMAL(12,2),
    net_line_amount  DECIMAL(12,2),
    updated_at       TIMESTAMP
)
PARTITIONED BY (order_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');


INSERT OVERWRITE TABLE order_items PARTITION (order_date)
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    CAST(oi.unit_price AS DECIMAL(12,2)) AS unit_price,
    CAST(COALESCE(oi.line_discount, 0) AS DECIMAL(12,2)) AS line_discount,
    CAST((oi.quantity * oi.unit_price) - COALESCE(oi.line_discount, 0) AS DECIMAL(12,2)) AS net_line_amount,
    oi.updated_at,
    o.order_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY updated_at DESC) AS rn
    FROM retailpulse_bronze.order_items
    WHERE order_item_id IS NOT NULL
) oi
JOIN orders o ON oi.order_id = o.order_id
WHERE oi.rn = 1
DISTRIBUTE BY o.order_date SORT BY oi.order_item_id;


DROP TABLE IF EXISTS payments;

CREATE TABLE payments (
    payment_id      BIGINT,
    order_id        BIGINT,
    payment_method  STRING,
    payment_status  STRING,
    amount          DECIMAL(12,2),
    paid_at         TIMESTAMP,
    updated_at      TIMESTAMP
)
PARTITIONED BY (payment_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE payments PARTITION (payment_date)
SELECT
    payment_id,
    order_id,
    LOWER(TRIM(payment_method)) AS payment_method,
    CASE LOWER(TRIM(payment_status))
        WHEN 'captured' THEN 'paid'
        WHEN 'paid'     THEN 'paid'
        WHEN 'failed'   THEN 'failed'
        WHEN 'pending'  THEN 'pending'
        WHEN 'refunded' THEN 'refunded'
        ELSE LOWER(TRIM(payment_status))
    END AS payment_status,
    CAST(amount AS DECIMAL(12,2)) AS amount,
    paid_at,
    updated_at,
    DATE_FORMAT(COALESCE(paid_at, updated_at), 'yyyy-MM-dd') AS payment_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY updated_at DESC) AS rn
    FROM retailpulse_bronze.payments
    WHERE payment_id IS NOT NULL
) dedup
WHERE rn = 1
DISTRIBUTE BY DATE_FORMAT(COALESCE(paid_at, updated_at), 'yyyy-MM-dd') SORT BY payment_id;

-- =====================================================================
-- 3. FULFILLMENT_EVENTS
-- =====================================================================
DROP TABLE IF EXISTS fulfillment_events;

CREATE TABLE fulfillment_events (
    fulfillment_event_id  BIGINT,
    order_id              BIGINT,
    event_type            STRING,
    event_timestamp       TIMESTAMP,
    warehouse_code        STRING,
    updated_at            TIMESTAMP
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE fulfillment_events PARTITION (event_date)
SELECT
    fulfillment_event_id,
    order_id,
    LOWER(TRIM(event_type)) AS event_type,
    event_timestamp,
    TRIM(warehouse_code) AS warehouse_code,
    updated_at,
    DATE_FORMAT(event_timestamp, 'yyyy-MM-dd') AS event_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY fulfillment_event_id ORDER BY updated_at DESC) AS rn
    FROM retailpulse_bronze.fulfillment_events
    WHERE fulfillment_event_id IS NOT NULL
) dedup
WHERE rn = 1
DISTRIBUTE BY DATE_FORMAT(event_timestamp, 'yyyy-MM-dd') SORT BY fulfillment_event_id;

-- =====================================================================
-- 4. INVENTORY_SNAPSHOTS
-- =====================================================================
DROP TABLE IF EXISTS inventory_snapshots;

CREATE TABLE inventory_snapshots (
    inventory_snapshot_id  BIGINT,
    product_id              INT,
    store_id                INT,
    stock_on_hand           INT,
    snapshot_at             TIMESTAMP,
    updated_at              TIMESTAMP
)
PARTITIONED BY (snapshot_date STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE inventory_snapshots PARTITION (snapshot_date)
SELECT
    inventory_snapshot_id,
    product_id,
    store_id,
    stock_on_hand,
    snapshot_at,
    updated_at,
    DATE_FORMAT(snapshot_at, 'yyyy-MM-dd') AS snapshot_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY inventory_snapshot_id ORDER BY updated_at DESC) AS rn
    FROM retailpulse_bronze.inventory_snapshots
    WHERE inventory_snapshot_id IS NOT NULL
) dedup
WHERE rn = 1
DISTRIBUTE BY DATE_FORMAT(snapshot_at, 'yyyy-MM-dd') SORT BY inventory_snapshot_id;

-- =====================================================================
-- 5. VALIDATION AGGREGATION
-- =====================================================================
SELECT 'orders' AS tbl, COUNT(*) AS row_count FROM orders
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL SELECT 'payments', COUNT(*) FROM payments
UNION ALL SELECT 'fulfillment_events', COUNT(*) FROM fulfillment_events
UNION ALL SELECT 'inventory_snapshots', COUNT(*) FROM inventory_snapshots;

