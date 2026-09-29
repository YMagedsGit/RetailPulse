-- Already created bronze and silver in streaming part

CREATE DATABASE IF NOT EXISTS retailpulse_bronze
  COMMENT 'Raw data ingested from cloud'
  LOCATION '/retailpulse/retailpulse_bronze.db';

CREATE DATABASE IF NOT EXISTS retailpulse_silver
  COMMENT 'Cleaned, conformed, deduplicated retail data'
  LOCATION '/retailpulse/retailpulse_silver.db';

CREATE DATABASE IF NOT EXISTS retailpulse_gold
  COMMENT 'Business-level aggregates for Metabase dashboards'
  LOCATION '/retailpulse/retailpulse_gold.db';

SHOW DATABASES LIKE 'retailpulse_*';
