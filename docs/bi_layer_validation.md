# BI Layer Validation

Run these checks after each dev Lakeflow pipeline run that updates the BI
tables. All queries assume the workspace context is set to the project catalog
and dev schema, for example:

```sql
USE CATALOG adventure_works_dw;
USE SCHEMA dev_alkasalissou_alkasalissou;
```

## BI Detail Row Count

`bi_sales_order_line` must stay at the same grain as `fact_internet_sales`.

```sql
WITH counts AS (
  SELECT 'fact_internet_sales' AS table_name, count(*) AS row_count
  FROM fact_internet_sales
  UNION ALL
  SELECT 'bi_sales_order_line', count(*)
  FROM bi_sales_order_line
)
SELECT
  max(CASE WHEN table_name = 'fact_internet_sales' THEN row_count END) AS fact_rows,
  max(CASE WHEN table_name = 'bi_sales_order_line' THEN row_count END) AS bi_rows,
  max(CASE WHEN table_name = 'bi_sales_order_line' THEN row_count END)
    - max(CASE WHEN table_name = 'fact_internet_sales' THEN row_count END) AS row_count_delta
FROM counts;
```

Expected result: `row_count_delta = 0`.

## BI Detail Grain Uniqueness

```sql
SELECT count(*) AS duplicate_bi_grains
FROM (
  SELECT sales_order_id, sales_order_detail_id
  FROM bi_sales_order_line
  GROUP BY sales_order_id, sales_order_detail_id
  HAVING count(*) > 1
);
```

Expected result: `duplicate_bi_grains = 0`.

## BI Detail Measure Reconciliation

```sql
WITH fact_totals AS (
  SELECT
    sum(sales_amount_before_discount) AS gross_sales_amount,
    sum(sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity
  FROM fact_internet_sales
),
bi_totals AS (
  SELECT
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(net_sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity
  FROM bi_sales_order_line
)
SELECT
  bi.gross_sales_amount - fact.gross_sales_amount AS gross_sales_amount_delta,
  bi.net_sales_amount - fact.net_sales_amount AS net_sales_amount_delta,
  bi.discount_amount - fact.discount_amount AS discount_amount_delta,
  bi.order_quantity - fact.order_quantity AS order_quantity_delta
FROM fact_totals AS fact
CROSS JOIN bi_totals AS bi;
```

Expected result: every delta is `0`.

## Monthly Aggregate Reconciliation

```sql
WITH detail_totals AS (
  SELECT
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(net_sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity,
    count(*) AS order_line_count
  FROM bi_sales_order_line
),
aggregate_totals AS (
  SELECT
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(net_sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity,
    sum(order_line_count) AS order_line_count
  FROM bi_sales_monthly
)
SELECT
  agg.gross_sales_amount - detail.gross_sales_amount AS gross_sales_amount_delta,
  agg.net_sales_amount - detail.net_sales_amount AS net_sales_amount_delta,
  agg.discount_amount - detail.discount_amount AS discount_amount_delta,
  agg.order_quantity - detail.order_quantity AS order_quantity_delta,
  agg.order_line_count - detail.order_line_count AS order_line_count_delta
FROM detail_totals AS detail
CROSS JOIN aggregate_totals AS agg;
```

Expected result: every delta is `0`.

## Product Aggregate Reconciliation

```sql
WITH detail_totals AS (
  SELECT
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(net_sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity,
    count(*) AS order_line_count
  FROM bi_sales_order_line
),
aggregate_totals AS (
  SELECT
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(net_sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity,
    sum(order_line_count) AS order_line_count
  FROM bi_product_performance
)
SELECT
  agg.gross_sales_amount - detail.gross_sales_amount AS gross_sales_amount_delta,
  agg.net_sales_amount - detail.net_sales_amount AS net_sales_amount_delta,
  agg.discount_amount - detail.discount_amount AS discount_amount_delta,
  agg.order_quantity - detail.order_quantity AS order_quantity_delta,
  agg.order_line_count - detail.order_line_count AS order_line_count_delta
FROM detail_totals AS detail
CROSS JOIN aggregate_totals AS agg;
```

Expected result: every delta is `0`.

## Territory Aggregate Reconciliation

```sql
WITH detail_totals AS (
  SELECT
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(net_sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity,
    count(*) AS order_line_count
  FROM bi_sales_order_line
),
aggregate_totals AS (
  SELECT
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(net_sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity,
    sum(order_line_count) AS order_line_count
  FROM bi_territory_performance
)
SELECT
  agg.gross_sales_amount - detail.gross_sales_amount AS gross_sales_amount_delta,
  agg.net_sales_amount - detail.net_sales_amount AS net_sales_amount_delta,
  agg.discount_amount - detail.discount_amount AS discount_amount_delta,
  agg.order_quantity - detail.order_quantity AS order_quantity_delta,
  agg.order_line_count - detail.order_line_count AS order_line_count_delta
FROM detail_totals AS detail
CROSS JOIN aggregate_totals AS agg;
```

Expected result: every delta is `0`.

## Customer Segment Aggregate Reconciliation

```sql
WITH detail_totals AS (
  SELECT
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(net_sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity,
    count(*) AS order_line_count
  FROM bi_sales_order_line
),
aggregate_totals AS (
  SELECT
    sum(gross_sales_amount) AS gross_sales_amount,
    sum(net_sales_amount) AS net_sales_amount,
    sum(discount_amount) AS discount_amount,
    sum(order_quantity) AS order_quantity,
    sum(order_line_count) AS order_line_count
  FROM bi_customer_segments
)
SELECT
  agg.gross_sales_amount - detail.gross_sales_amount AS gross_sales_amount_delta,
  agg.net_sales_amount - detail.net_sales_amount AS net_sales_amount_delta,
  agg.discount_amount - detail.discount_amount AS discount_amount_delta,
  agg.order_quantity - detail.order_quantity AS order_quantity_delta,
  agg.order_line_count - detail.order_line_count AS order_line_count_delta
FROM detail_totals AS detail
CROSS JOIN aggregate_totals AS agg;
```

Expected result: every delta is `0`.

## Aggregate Rate Calculation Checks

Rates should match the documented metric formulas:

- `average_order_value = net_sales_amount / order_count`
- `discount_rate = discount_amount / gross_sales_amount`

```sql
SELECT 'bi_sales_monthly' AS table_name, count(*) AS invalid_rate_rows
FROM bi_sales_monthly
WHERE
  (order_count = 0 AND average_order_value IS NOT NULL)
  OR (order_count <> 0 AND average_order_value <> net_sales_amount / order_count)
  OR (gross_sales_amount = 0 AND discount_rate IS NOT NULL)
  OR (gross_sales_amount <> 0 AND discount_rate <> discount_amount / gross_sales_amount)
UNION ALL
SELECT 'bi_product_performance', count(*)
FROM bi_product_performance
WHERE
  (order_count = 0 AND average_order_value IS NOT NULL)
  OR (order_count <> 0 AND average_order_value <> net_sales_amount / order_count)
  OR (gross_sales_amount = 0 AND discount_rate IS NOT NULL)
  OR (gross_sales_amount <> 0 AND discount_rate <> discount_amount / gross_sales_amount)
UNION ALL
SELECT 'bi_territory_performance', count(*)
FROM bi_territory_performance
WHERE
  (order_count = 0 AND average_order_value IS NOT NULL)
  OR (order_count <> 0 AND average_order_value <> net_sales_amount / order_count)
  OR (gross_sales_amount = 0 AND discount_rate IS NOT NULL)
  OR (gross_sales_amount <> 0 AND discount_rate <> discount_amount / gross_sales_amount)
UNION ALL
SELECT 'bi_customer_segments', count(*)
FROM bi_customer_segments
WHERE
  (order_count = 0 AND average_order_value IS NOT NULL)
  OR (order_count <> 0 AND average_order_value <> net_sales_amount / order_count)
  OR (gross_sales_amount = 0 AND discount_rate IS NOT NULL)
  OR (gross_sales_amount <> 0 AND discount_rate <> discount_amount / gross_sales_amount);
```

Expected result: every `invalid_rate_rows` value is `0`.

## Required Dashboard Display Fields

BI display fields used by the first dashboard should be populated. Unknown
members should appear as `Unknown`, not null.

```sql
SELECT
  count_if(product_name IS NULL) AS null_product_name,
  count_if(product_category_name IS NULL) AS null_product_category_name,
  count_if(product_subcategory_name IS NULL) AS null_product_subcategory_name,
  count_if(customer_name IS NULL) AS null_customer_name,
  count_if(yearly_income IS NULL) AS null_yearly_income,
  count_if(education IS NULL) AS null_education,
  count_if(occupation IS NULL) AS null_occupation,
  count_if(gender IS NULL) AS null_gender,
  count_if(marital_status IS NULL) AS null_marital_status,
  count_if(home_owner_flag IS NULL) AS null_home_owner_flag,
  count_if(commute_distance IS NULL) AS null_commute_distance,
  count_if(promotion_description IS NULL) AS null_promotion_description,
  count_if(territory_name IS NULL) AS null_territory_name,
  count_if(territory_group IS NULL) AS null_territory_group,
  count_if(territory_country_region_code IS NULL) AS null_territory_country_region_code,
  count_if(order_date IS NULL) AS null_order_date,
  count_if(order_year IS NULL) AS null_order_year,
  count_if(order_month IS NULL) AS null_order_month,
  count_if(order_month_name IS NULL) AS null_order_month_name
FROM bi_sales_order_line;
```

Expected result: every null-count value is `0`.

## Aggregate Required Fields

```sql
SELECT 'bi_sales_monthly' AS table_name, count(*) AS null_required_rows
FROM bi_sales_monthly
WHERE order_year IS NULL OR order_month IS NULL OR order_month_name IS NULL
UNION ALL
SELECT 'bi_product_performance', count(*)
FROM bi_product_performance
WHERE
  product_category_name IS NULL
  OR product_subcategory_name IS NULL
  OR product_name IS NULL
UNION ALL
SELECT 'bi_territory_performance', count(*)
FROM bi_territory_performance
WHERE
  territory_country_region_code IS NULL
  OR territory_group IS NULL
  OR territory_name IS NULL
UNION ALL
SELECT 'bi_customer_segments', count(*)
FROM bi_customer_segments
WHERE
  yearly_income IS NULL
  OR education IS NULL
  OR occupation IS NULL
  OR gender IS NULL
  OR marital_status IS NULL
  OR home_owner_flag IS NULL
  OR commute_distance IS NULL;
```

Expected result: every `null_required_rows` value is `0`.
