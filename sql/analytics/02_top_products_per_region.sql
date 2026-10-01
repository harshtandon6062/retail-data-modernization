-- Q2. Top 3 products by revenue in each region (window function RANK).
-- RANK gives ties the same rank (1,1,3); DENSE_RANK would give (1,1,2); ROW_NUMBER never ties.
WITH product_region AS (
  SELECT region, product_name, category,
         round(sum(net_amount), 2) AS revenue,
         sum(quantity)             AS units
  FROM gold.v_sales_analyst
  WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
  GROUP BY region, product_name, category
),
ranked AS (
  SELECT *, RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS rnk
  FROM product_region
)
SELECT region, rnk, product_name, category, revenue, units
FROM ranked
WHERE rnk <= 3
ORDER BY region, rnk
