-- AdventureWorks Sales Executive Overview dashboard queries.
--
-- Set catalog and schema before running, for example:
-- USE CATALOG adventure_works_dw;
-- USE SCHEMA <dev_schema>;

-- KPI summary
WITH sales_totals AS (
  SELECT
    sum(net_sales_amount) AS net_sales_amount,
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity,
    sum(order_count) AS order_count
  FROM bi_sales_monthly
),
customer_totals AS (
  SELECT count(DISTINCT customer_id) AS customer_count
  FROM bi_sales_order_line
)
SELECT
  sales.net_sales_amount,
  sales.gross_sales_amount,
  sales.discount_amount,
  sales.order_quantity,
  sales.order_count,
  customers.customer_count,
  CASE
    WHEN sales.order_count = 0 THEN NULL
    ELSE sales.net_sales_amount / sales.order_count
  END AS average_order_value,
  CASE
    WHEN sales.gross_sales_amount = 0 THEN NULL
    ELSE sales.discount_amount / sales.gross_sales_amount
  END AS discount_rate
FROM sales_totals AS sales
CROSS JOIN customer_totals AS customers;

-- Monthly sales trend
SELECT
  order_year,
  order_month,
  order_month_name,
  make_date(order_year, order_month, 1) AS order_month_start_date,
  net_sales_amount,
  gross_sales_amount,
  discount_amount,
  order_quantity,
  order_count,
  customer_count,
  average_order_value,
  discount_rate
FROM bi_sales_monthly
ORDER BY order_year, order_month;

-- Yearly sales summary
SELECT
  order_year,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  CASE
    WHEN sum(order_count) = 0 THEN NULL
    ELSE sum(net_sales_amount) / sum(order_count)
  END AS average_order_value,
  CASE
    WHEN sum(gross_sales_amount) = 0 THEN NULL
    ELSE sum(discount_amount) / sum(gross_sales_amount)
  END AS discount_rate
FROM bi_sales_monthly
GROUP BY order_year
ORDER BY order_year;

-- Monthly discount impact
SELECT
  order_year,
  order_month,
  order_month_name,
  make_date(order_year, order_month, 1) AS order_month_start_date,
  gross_sales_amount,
  discount_amount,
  net_sales_amount,
  discount_rate
FROM bi_sales_monthly
ORDER BY order_year, order_month;
