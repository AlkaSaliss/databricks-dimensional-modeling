-- Customer Segments dashboard queries.
--
-- Set catalog and schema before running, for example:
-- USE CATALOG adventure_works_dw;
-- USE SCHEMA <dev_schema>;

-- Income segment performance
SELECT
  yearly_income,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  sum(customer_count) AS segment_customer_count_sum,
  CASE
    WHEN sum(order_count) = 0 THEN NULL
    ELSE sum(net_sales_amount) / sum(order_count)
  END AS average_order_value,
  CASE
    WHEN sum(gross_sales_amount) = 0 THEN NULL
    ELSE sum(discount_amount) / sum(gross_sales_amount)
  END AS discount_rate
FROM bi_customer_segments
GROUP BY yearly_income
ORDER BY net_sales_amount DESC;

-- Education and occupation performance
SELECT
  education,
  occupation,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  sum(customer_count) AS segment_customer_count_sum,
  CASE
    WHEN sum(order_count) = 0 THEN NULL
    ELSE sum(net_sales_amount) / sum(order_count)
  END AS average_order_value,
  CASE
    WHEN sum(gross_sales_amount) = 0 THEN NULL
    ELSE sum(discount_amount) / sum(gross_sales_amount)
  END AS discount_rate
FROM bi_customer_segments
GROUP BY education, occupation
ORDER BY net_sales_amount DESC;

-- Demographic segment detail
SELECT
  yearly_income,
  education,
  occupation,
  gender,
  marital_status,
  home_owner_flag,
  commute_distance,
  net_sales_amount,
  gross_sales_amount,
  discount_amount,
  order_quantity,
  order_count,
  customer_count,
  average_order_value,
  discount_rate
FROM bi_customer_segments
ORDER BY net_sales_amount DESC
LIMIT 50;
