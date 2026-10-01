-- Q3. Customer retention cohorts.
-- Cohort = the quarter of a customer's FIRST purchase. For each later quarter,
-- what % of that cohort bought again? A customer who moved city has several SCD2
-- versions (customer_sk), so we map back to the durable customer_id before counting.
WITH cust_orders AS (
  SELECT DISTINCT c.customer_id,
         date_trunc('quarter', v.order_date) AS order_q
  FROM gold.v_sales_analyst v
  JOIN gold.dim_customer c ON v.customer_sk = c.customer_sk   -- SCD2 versions -> one customer
  WHERE v.order_status <> 'CANCELLED'
),
first_q AS (
  SELECT customer_id, min(order_q) AS cohort_q FROM cust_orders GROUP BY customer_id
),
activity AS (
  SELECT f.cohort_q,
         CAST(months_between(o.order_q, f.cohort_q) / 3 AS INT) AS quarters_since,
         count(DISTINCT o.customer_id)                           AS active_customers
  FROM cust_orders o JOIN first_q f ON o.customer_id = f.customer_id
  GROUP BY 1, 2
)
SELECT date_format(cohort_q, 'yyyy-MM') AS cohort_quarter_start,
       quarters_since,
       active_customers,
       round(100 * active_customers
             / first_value(active_customers) OVER (PARTITION BY cohort_q ORDER BY quarters_since), 1)
         AS retention_pct
FROM activity
ORDER BY cohort_quarter_start, quarters_since
