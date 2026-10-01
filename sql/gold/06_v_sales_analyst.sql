-- Role-based access, illustrated: a PII-FREE view for the analyst role.
-- Analysts query this view; they never get the raw dim_customer table
-- (names, email hash, phone). Only non-identifying attributes are exposed.
CREATE OR REPLACE VIEW gold.v_sales_analyst AS
SELECT
  f.order_item_id, f.order_id, d.full_date AS order_date, d.year, d.month, d.year_month,
  f.customer_sk,                         -- pseudonymous id: can count customers, cannot identify them
  c.city  AS customer_city, c.state AS customer_state,
  p.product_name, p.category, p.brand, p.price_band,
  s.store_name, s.city AS store_city, s.region,
  f.order_status, f.payment_method,
  f.quantity, f.gross_amount, f.discount_amount, f.net_amount
FROM gold.fact_sales f
JOIN gold.dim_date     d ON f.date_key    = d.date_key
JOIN gold.dim_customer c ON f.customer_sk = c.customer_sk
JOIN gold.dim_product  p ON f.product_key = p.product_key
JOIN gold.dim_store    s ON f.store_key   = s.store_key;

-- On a governed platform (Unity Catalog / Synapse / BigQuery / Hive with Ranger)
-- you would then run, for example:
--   GRANT SELECT ON VIEW gold.v_sales_analyst TO `analysts`;
--   REVOKE ALL ON TABLE gold.dim_customer FROM `analysts`;
-- Spark in local mode has no users/roles, so those statements are documentation here.
