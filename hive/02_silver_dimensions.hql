-- =====================================================================
-- Silver layer — DIMENSION tables
-- Source: retailpulse_bronze  |  Target: retailpulse_silver
-- Run after 01_create_databases.hql
-- =====================================================================

USE retailpulse_silver;

-- ---------------------------------------------------------------------
-- customers
--   - country_code: collapse free text / abbreviations to ISO-3166 alpha-2
--   - email: lowercased, trimmed
--   - dedup: Sqoop incremental --merge-key can still leave stragglers in
--     edge cases (e.g. reprocessing) — keep the latest row per customer_id
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS customers;

CREATE TABLE customers
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
SELECT
    customer_id,
    full_name,
    email,
    phone,
    country_code,
    signup_at,
    updated_at
FROM (
    SELECT
        customer_id,
        TRIM(full_name) AS full_name,
        LOWER(TRIM(email)) AS email,
        TRIM(phone) AS phone,
        CASE
            WHEN LOWER(TRIM(country_code)) IN ('eg', 'egypt')                THEN 'EG'
            WHEN LOWER(TRIM(country_code)) IN ('sa', 'ksa', 'saudi arabia')  THEN 'SA'
            WHEN LOWER(TRIM(country_code)) IN ('ae', 'uae', 'united arab emirates') THEN 'AE'
            ELSE UPPER(TRIM(country_code))
        END AS country_code,
        signup_at,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
    FROM retailpulse_bronze.customers
    WHERE customer_id IS NOT NULL
) dedup
WHERE rn = 1;

-- ---------------------------------------------------------------------
-- stores  (51 rows — small reference table, full reload every run)
--   No updated_at column in source, so no incremental dedup needed;
--   DISTINCT guards against any accidental full-reload duplication.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stores;

CREATE TABLE stores
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
SELECT DISTINCT
    store_id,
    TRIM(store_name) AS store_name,
    TRIM(city) AS city,
    CASE
        WHEN LOWER(TRIM(country_code)) IN ('eg', 'egypt')                THEN 'EG'
        WHEN LOWER(TRIM(country_code)) IN ('sa', 'ksa', 'saudi arabia')  THEN 'SA'
        WHEN LOWER(TRIM(country_code)) IN ('ae', 'uae', 'united arab emirates') THEN 'AE'
        ELSE UPPER(TRIM(country_code))
    END AS country_code,
    opened_at
FROM retailpulse_bronze.stores
WHERE store_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- products
--   - category: trimmed (title-casing via INITCAP requires Hive 2.1+;
--     drop the INITCAP wrapper below and keep TRIM only on older Hive)
--   - margin: derived column, saves recomputation in every Gold query
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS products;

CREATE TABLE products
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
SELECT
    product_id,
    sku,
    product_name,
    category,
    unit_cost,
    list_price,
    list_price - unit_cost AS margin,
    updated_at
FROM (
    SELECT
        product_id,
        TRIM(sku) AS sku,
        TRIM(product_name) AS product_name,
        INITCAP(TRIM(category)) AS category,          -- Hive >= 2.1; else use TRIM(category)
        CAST(unit_cost AS DECIMAL(12,2)) AS unit_cost,
        CAST(list_price AS DECIMAL(12,2)) AS list_price,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) AS rn
    FROM retailpulse_bronze.products
    WHERE product_id IS NOT NULL
) dedup
WHERE rn = 1;

SELECT 'customers' AS tbl, COUNT(*) AS row_count FROM customers
UNION ALL SELECT 'stores', COUNT(*) FROM stores
UNION ALL SELECT 'products', COUNT(*) FROM products;
