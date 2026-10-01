-- fact_sales: the central fact table of the star schema.
-- GRAIN: one row per ORDER LINE (one product in one order).  -> order_item_id is unique.
-- Foreign keys: date_key -> dim_date, customer_sk -> dim_customer (the version valid
--               at order time = "point-in-time" SCD2 join), product_key -> dim_product,
--               store_key -> dim_store.
-- Measures (additive): quantity, gross_amount, discount_amount, net_amount.
CREATE OR REPLACE TABLE gold.fact_sales
USING DELTA LOCATION '${lake}/gold/fact_sales' AS
SELECT
  oi.order_item_id,
  oi.order_id,                                                   -- degenerate dimension
  CAST(date_format(o.order_ts, 'yyyyMMdd') AS INT)               AS date_key,
  dc.customer_sk,
  oi.product_id                                                  AS product_key,
  o.store_id                                                     AS store_key,
  o.order_status,
  o.payment_method,
  oi.quantity,
  oi.unit_price,
  oi.discount_pct,
  CAST(oi.quantity * oi.unit_price AS DECIMAL(14,2))                               AS gross_amount,
  CAST(round(oi.quantity * oi.unit_price * oi.discount_pct / 100, 2) AS DECIMAL(14,2)) AS discount_amount,
  CAST(oi.quantity * oi.unit_price
       - round(oi.quantity * oi.unit_price * oi.discount_pct / 100, 2) AS DECIMAL(14,2)) AS net_amount,
  o.order_ts
FROM silver.order_items oi
JOIN silver.orders o
  ON o.order_id = oi.order_id
LEFT JOIN gold.dim_customer dc
  ON dc.customer_id = o.customer_id
 AND o.order_ts >= dc.effective_from
 AND o.order_ts <  dc.effective_to;
