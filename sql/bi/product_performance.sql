-- Product Performance dashboard queries.
--
-- Set catalog and schema before running, for example:
-- USE CATALOG adventure_works_dw;
-- USE SCHEMA <dev_schema>;

-- Product category performance
SELECT
  product_category_name,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  count(DISTINCT product_id) AS product_count,
  CASE
    WHEN sum(order_count) = 0 THEN NULL
    ELSE sum(net_sales_amount) / sum(order_count)
  END AS average_order_value,
  CASE
    WHEN sum(gross_sales_amount) = 0 THEN NULL
    ELSE sum(discount_amount) / sum(gross_sales_amount)
  END AS discount_rate
FROM bi_product_performance
GROUP BY product_category_name
ORDER BY net_sales_amount DESC;

-- Product subcategory performance
SELECT
  product_category_name,
  product_subcategory_name,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  count(DISTINCT product_id) AS product_count,
  CASE
    WHEN sum(order_count) = 0 THEN NULL
    ELSE sum(net_sales_amount) / sum(order_count)
  END AS average_order_value,
  CASE
    WHEN sum(gross_sales_amount) = 0 THEN NULL
    ELSE sum(discount_amount) / sum(gross_sales_amount)
  END AS discount_rate
FROM bi_product_performance
GROUP BY product_category_name, product_subcategory_name
ORDER BY net_sales_amount DESC;

-- Top products by net sales
SELECT
  product_category_name,
  product_subcategory_name,
  product_id,
  product_name,
  product_line,
  product_class,
  product_style,
  product_color,
  net_sales_amount,
  gross_sales_amount,
  discount_amount,
  order_quantity,
  order_count,
  customer_count,
  average_order_value,
  discount_rate
FROM bi_product_performance
ORDER BY net_sales_amount DESC
LIMIT 25;

-- Products with highest discount rate among meaningful sales volume
SELECT
  product_category_name,
  product_subcategory_name,
  product_id,
  product_name,
  net_sales_amount,
  gross_sales_amount,
  discount_amount,
  order_count,
  discount_rate
FROM bi_product_performance
WHERE order_count >= 10
ORDER BY discount_rate DESC, net_sales_amount DESC
LIMIT 25;
