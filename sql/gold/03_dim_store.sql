-- dim_store: SCD Type 1.
-- Grain: 1 row = 1 store.   Key: store_key (= legacy store_id).
CREATE OR REPLACE TABLE gold.dim_store
USING DELTA LOCATION '${lake}/gold/dim_store' AS
SELECT
  store_id   AS store_key,
  store_name,
  city,
  region,
  opened_date
FROM silver.stores;
