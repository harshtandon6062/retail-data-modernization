-- Q5. Average basket: items per order and average order value (AOV) by region and year.
WITH baskets AS (
  SELECT order_id, region, year,
         sum(quantity)   AS items,
         sum(net_amount) AS order_value
  FROM gold.v_sales_analyst
  WHERE order_status NOT IN ('CANCELLED', 'RETURNED')
  GROUP BY order_id, region, year
)
SELECT region, year,
       count(*)                   AS orders,
       round(avg(items), 2)       AS avg_items_per_order,
       round(avg(order_value), 2) AS avg_order_value
FROM baskets
GROUP BY region, year
ORDER BY region, year
