-- dim_customer: SCD TYPE 2 - keeps history when a customer moves city/state.
-- Grain: 1 row = 1 VERSION of a customer.
-- Keys:  customer_sk = surrogate key (unique per version; facts point to this)
--        customer_id = natural/business key from the legacy system (repeats across versions)
-- Tracked (Type 2) attributes: city, state. Validity window: [effective_from, effective_to).
-- PII: email/phone arrive already masked from silver.

CREATE TABLE IF NOT EXISTS gold.dim_customer (
  customer_sk     BIGINT,
  customer_id     INT,
  first_name      STRING,
  last_name       STRING,
  email_hash      STRING,
  phone_masked    STRING,
  city            STRING,
  state           STRING,
  signup_date     DATE,
  row_hash        STRING,
  effective_from  TIMESTAMP,
  effective_to    TIMESTAMP,
  is_current      BOOLEAN
) USING DELTA LOCATION '${lake}/gold/dim_customer';

-- Step 1: which customers are new, or changed a tracked attribute?
CREATE OR REPLACE TEMP VIEW customer_changes AS
SELECT s.*,
       sha2(concat_ws('|', s.city, s.state), 256) AS new_hash,
       d.customer_id IS NOT NULL                   AS is_existing
FROM silver.customers s
LEFT JOIN gold.dim_customer d
       ON d.customer_id = s.customer_id AND d.is_current
WHERE d.customer_id IS NULL                                        -- brand-new customer
   OR d.row_hash <> sha2(concat_ws('|', s.city, s.state), 256);   -- tracked attribute changed

-- Step 2: the classic MERGE trick. A changed customer appears TWICE in the source:
--   merge_key = customer_id -> matches the current row  -> UPDATE (close it)
--   merge_key = NULL        -> never matches            -> INSERT (new version)
-- New customers appear once (merge_key = customer_id, no match -> INSERT).
CREATE OR REPLACE TEMP VIEW customer_merge_source AS
SELECT customer_id AS merge_key, c.* FROM customer_changes c
UNION ALL
SELECT NULL AS merge_key, c.* FROM customer_changes c WHERE c.is_existing;

CREATE OR REPLACE TEMP VIEW customer_merge_source_keyed AS
SELECT m.*,
       (SELECT coalesce(max(customer_sk), 0) FROM gold.dim_customer)
         + row_number() OVER (ORDER BY customer_id) AS new_sk      -- next surrogate keys
FROM customer_merge_source m;

MERGE INTO gold.dim_customer AS t
USING customer_merge_source_keyed AS s
   ON t.customer_id = s.merge_key AND t.is_current
WHEN MATCHED THEN UPDATE SET
     t.is_current   = false,
     t.effective_to = s.updated_at                -- old version ends when the change happened
WHEN NOT MATCHED THEN INSERT (
     customer_sk, customer_id, first_name, last_name, email_hash, phone_masked,
     city, state, signup_date, row_hash, effective_from, effective_to, is_current)
VALUES (
     s.new_sk, s.customer_id, s.first_name, s.last_name, s.email_hash, s.phone_masked,
     s.city, s.state, s.signup_date, s.new_hash,
     -- first version is valid "since forever" so old orders find it; later versions from the change time
     CASE WHEN s.is_existing THEN s.updated_at ELSE TIMESTAMP'1900-01-01 00:00:00' END,
     TIMESTAMP'9999-12-31 00:00:00',
     true);
