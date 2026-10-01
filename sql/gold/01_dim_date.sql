-- dim_date: one row per calendar day covering every order date.
-- Grain: 1 row = 1 day.   Key: date_key (INT yyyymmdd, e.g. 20250131) - a "smart" key
-- that BI tools and humans can read, and that is cheap to join on.
CREATE OR REPLACE TABLE gold.dim_date
USING DELTA LOCATION '${lake}/gold/dim_date' AS
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT)               AS date_key,
  d                                                      AS full_date,
  year(d)                                                AS year,
  quarter(d)                                             AS quarter,
  month(d)                                               AS month,
  date_format(d, 'MMMM')                                 AS month_name,
  date_format(d, 'yyyy-MM')                              AS year_month,
  dayofweek(d)                                           AS day_of_week,
  date_format(d, 'EEEE')                                 AS day_name,
  dayofweek(d) IN (1, 7)                                 AS is_weekend,
  -- Indian financial year runs 1 April - 31 March
  CASE WHEN month(d) >= 4 THEN concat('FY', year(d) + 1) ELSE concat('FY', year(d)) END AS fiscal_year
FROM (
  SELECT explode(sequence(
           (SELECT trunc(min(order_date), 'year') FROM silver.orders),
           (SELECT last_day(max(order_date))      FROM silver.orders),
           INTERVAL 1 DAY)) AS d
);
