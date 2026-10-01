-- Q1. Monthly revenue trend with month-over-month growth (window function LAG).
-- Revenue = net_amount of orders that were not CANCELLED or RETURNED.
-- Reads the PII-free analyst view, exactly what an analyst role would be granted.
WITH monthly AS (
  SELECT year_month,
         round(sum(net_amount), 2)          AS revenue,
         count(DISTINCT order_id)           AS orders
  FROM gold.v_sales_analyst
  WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
  GROUP BY year_month
)
SELECT year_month, revenue, orders,
       round(100 * (revenue - LAG(revenue) OVER (ORDER BY year_month))
                 / LAG(revenue) OVER (ORDER BY year_month), 1) AS mom_growth_pct
FROM monthly
ORDER BY year_month
