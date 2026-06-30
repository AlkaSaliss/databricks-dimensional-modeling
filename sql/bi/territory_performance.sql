-- Territory Performance dashboard queries.
--
-- Set catalog and schema before running, for example:
-- USE CATALOG adventure_works_dw;
-- USE SCHEMA <dev_schema>;

-- Territory group performance
SELECT
  territory_group,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  sum(customer_count) AS territory_customer_count_sum,
  CASE
    WHEN sum(order_count) = 0 THEN NULL
    ELSE sum(net_sales_amount) / sum(order_count)
  END AS average_order_value,
  CASE
    WHEN sum(gross_sales_amount) = 0 THEN NULL
    ELSE sum(discount_amount) / sum(gross_sales_amount)
  END AS discount_rate
FROM bi_territory_performance
GROUP BY territory_group
ORDER BY net_sales_amount DESC;

-- Territory performance
SELECT
  territory_country_region_code,
  territory_group,
  territory_id,
  territory_name,
  net_sales_amount,
  gross_sales_amount,
  discount_amount,
  order_quantity,
  order_count,
  customer_count,
  average_order_value,
  discount_rate
FROM bi_territory_performance
ORDER BY net_sales_amount DESC;

-- Territory discount comparison
SELECT
  territory_country_region_code,
  territory_group,
  territory_name,
  gross_sales_amount,
  discount_amount,
  net_sales_amount,
  discount_rate
FROM bi_territory_performance
ORDER BY discount_rate DESC, net_sales_amount DESC;
