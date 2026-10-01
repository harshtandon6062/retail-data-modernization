-- dim_product: SCD Type 1 (overwrite - we only need the current price/category).
-- Grain: 1 row = 1 product.   Key: product_key (= legacy product_id, a stable durable key).
CREATE OR REPLACE TABLE gold.dim_product
USING DELTA LOCATION '${lake}/gold/dim_product' AS
SELECT
  product_id   AS product_key,
  product_name,
  category,
  brand,
  unit_price   AS current_unit_price,
  CASE WHEN unit_price < 500 THEN 'Budget'
       WHEN unit_price < 5000 THEN 'Mid'
       ELSE 'Premium' END AS price_band
FROM silver.products;
