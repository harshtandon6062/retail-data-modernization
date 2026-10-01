-- Q4. Store revenue year-over-year growth (window function LAG over years).
-- Only complete years (12 months of data) are compared - a partial year would look like a crash.
WITH full_years AS (
  SELECT year FROM gold.v_sales_analyst GROUP BY year HAVING count(DISTINCT month) = 12
),
store_year AS (
  SELECT store_name, region, year, round(sum(net_amount), 2) AS revenue
  FROM gold.v_sales_analyst
  WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
    AND year IN (SELECT year FROM full_years)
  GROUP BY store_name, region, year
)
SELECT store_name, region, year, revenue,
       LAG(revenue) OVER (PARTITION BY store_name ORDER BY year) AS prev_year_revenue,
       round(100 * (revenue - LAG(revenue) OVER (PARTITION BY store_name ORDER BY year))
                 / LAG(revenue) OVER (PARTITION BY store_name ORDER BY year), 1) AS yoy_growth_pct
FROM store_year
ORDER BY year DESC, yoy_growth_pct DESC
