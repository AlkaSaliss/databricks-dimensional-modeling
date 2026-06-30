# Dimensional Model Validation

Run these checks after each dev Lakeflow pipeline run. All queries assume the
workspace context is set to the project catalog and dev schema, for example:

```sql
USE CATALOG adventure_works_dw;
USE SCHEMA dev_alkasalissou_alkasalissou;
```

## Gold Pipeline Expectations

The gold pipeline defines one-row-per-key uniqueness checks as temporary views
with Lakeflow expectations. These views are scoped to the pipeline run and are
not persisted as queryable schema objects. Review the pipeline run expectations
for failures. Use the persisted-table checks below for manual SQL validation
after the run completes.

## Dimension Current-Row Uniqueness

```sql
SELECT 'dim_promotion' AS table_name, count(*) AS duplicate_keys
FROM (
  SELECT special_offer_id
  FROM dim_promotion
  GROUP BY special_offer_id
  HAVING count(*) > 1
)
UNION ALL
SELECT 'dim_product', count(*)
FROM (
  SELECT product_id
  FROM dim_product
  WHERE __END_AT IS NULL
  GROUP BY product_id
  HAVING count(*) > 1
)
UNION ALL
SELECT 'dim_currency', count(*)
FROM (
  SELECT currency_rate_id
  FROM dim_currency
  WHERE __END_AT IS NULL
  GROUP BY currency_rate_id
  HAVING count(*) > 1
)
UNION ALL
SELECT 'dim_sales_territory', count(*)
FROM (
  SELECT territory_id
  FROM dim_sales_territory
  WHERE __END_AT IS NULL
  GROUP BY territory_id
  HAVING count(*) > 1
)
UNION ALL
SELECT 'dim_customer', count(*)
FROM (
  SELECT customer_id
  FROM dim_customer
  WHERE __END_AT IS NULL
  GROUP BY customer_id
  HAVING count(*) > 1
);
```

Expected result: every `duplicate_keys` value is `0`.

## SCD2 Interval Overlap

```sql
WITH product_versions AS (
  SELECT
    product_id AS business_key,
    __START_AT,
    __END_AT,
    lead(__START_AT) OVER (PARTITION BY product_id ORDER BY __START_AT) AS next_start_at
  FROM dim_product
),
currency_versions AS (
  SELECT
    currency_rate_id AS business_key,
    __START_AT,
    __END_AT,
    lead(__START_AT) OVER (PARTITION BY currency_rate_id ORDER BY __START_AT) AS next_start_at
  FROM dim_currency
),
territory_versions AS (
  SELECT
    territory_id AS business_key,
    __START_AT,
    __END_AT,
    lead(__START_AT) OVER (PARTITION BY territory_id ORDER BY __START_AT) AS next_start_at
  FROM dim_sales_territory
),
customer_versions AS (
  SELECT
    customer_id AS business_key,
    __START_AT,
    __END_AT,
    lead(__START_AT) OVER (PARTITION BY customer_id ORDER BY __START_AT) AS next_start_at
  FROM dim_customer
),
overlap_counts AS (
  SELECT 'dim_product' AS table_name, count(*) AS overlap_count
  FROM product_versions
  WHERE __END_AT IS NOT NULL AND next_start_at IS NOT NULL AND __END_AT > next_start_at
  UNION ALL
  SELECT 'dim_currency', count(*)
  FROM currency_versions
  WHERE __END_AT IS NOT NULL AND next_start_at IS NOT NULL AND __END_AT > next_start_at
  UNION ALL
  SELECT 'dim_sales_territory', count(*)
  FROM territory_versions
  WHERE __END_AT IS NOT NULL AND next_start_at IS NOT NULL AND __END_AT > next_start_at
  UNION ALL
  SELECT 'dim_customer', count(*)
  FROM customer_versions
  WHERE __END_AT IS NOT NULL AND next_start_at IS NOT NULL AND __END_AT > next_start_at
)
SELECT *
FROM overlap_counts;
```

Expected result: every `overlap_count` value is `0`.

## Fact Grain Uniqueness

```sql
SELECT count(*) AS duplicate_fact_grains
FROM (
  SELECT sales_order_id, sales_order_detail_id
  FROM fact_internet_sales
  GROUP BY sales_order_id, sales_order_detail_id
  HAVING count(*) > 1
);
```

Expected result: `duplicate_fact_grains = 0`.

## Fact-To-Dimension Resolution

```sql
SELECT
  count_if(product.product_key IS NULL) AS missing_product,
  count_if(customer.customer_key IS NULL) AS missing_customer,
  count_if(promotion.promotion_key IS NULL) AS missing_promotion,
  count_if(currency.currency_key IS NULL) AS missing_currency,
  count_if(territory.sales_territory_key IS NULL) AS missing_sales_territory,
  count_if(order_date.date_key IS NULL) AS missing_order_date,
  count_if(due_date.date_key IS NULL) AS missing_due_date,
  count_if(fact.ship_date_key IS NOT NULL AND ship_date.date_key IS NULL) AS missing_ship_date
FROM fact_internet_sales AS fact
LEFT JOIN dim_product AS product
  ON fact.product_key = product.product_key
LEFT JOIN dim_customer AS customer
  ON fact.customer_key = customer.customer_key
LEFT JOIN dim_promotion AS promotion
  ON fact.promotion_key = promotion.promotion_key
LEFT JOIN dim_currency AS currency
  ON fact.currency_key = currency.currency_key
LEFT JOIN dim_sales_territory AS territory
  ON fact.sales_territory_key = territory.sales_territory_key
LEFT JOIN dim_date AS order_date
  ON fact.order_date_key = order_date.date_key
LEFT JOIN dim_date AS due_date
  ON fact.due_date_key = due_date.date_key
LEFT JOIN dim_date AS ship_date
  ON fact.ship_date_key = ship_date.date_key;
```

Expected result: every missing-count value is `0`.

## Fact Measure Reconciliation

```sql
WITH source_detail_versions AS (
  SELECT
    detail.sales_order_id,
    detail.sales_order_detail_id,
    detail.line_total AS expected_sales_amount,
    detail.unit_price * detail.order_quantity AS expected_sales_amount_before_discount,
    detail.unit_price * detail.order_quantity * detail.unit_price_discount AS expected_discount_amount,
    row_number() OVER (
      PARTITION BY detail.sales_order_id, detail.sales_order_detail_id
      ORDER BY detail.modified_at DESC, detail.__ingestion_time DESC
    ) AS row_num
  FROM silver_sales_order_detail AS detail
  INNER JOIN silver_sales_order_header AS header
    ON detail.sales_order_id = header.sales_order_id
  WHERE header.online_order_flag
),
source_detail AS (
  SELECT
    sales_order_id,
    sales_order_detail_id,
    expected_sales_amount,
    expected_sales_amount_before_discount,
    expected_discount_amount
  FROM source_detail_versions
  WHERE row_num = 1
)
SELECT
  max(abs(fact.sales_amount - source.expected_sales_amount)) AS max_sales_amount_delta,
  max(abs(fact.sales_amount_before_discount - source.expected_sales_amount_before_discount)) AS max_before_discount_delta,
  max(abs(fact.discount_amount - source.expected_discount_amount)) AS max_discount_delta
FROM fact_internet_sales AS fact
INNER JOIN source_detail AS source
  ON fact.sales_order_id = source.sales_order_id
  AND fact.sales_order_detail_id = source.sales_order_detail_id;
```

Expected result: every delta is `0`.

## Fact Row Count

Silver is intentionally not deduplicated, so this check compares the fact table
to the distinct online order-line grain present in silver instead of the raw
silver row count.

```sql
WITH source_count AS (
  SELECT count(*) AS grain_count
  FROM (
    SELECT DISTINCT detail.sales_order_id, detail.sales_order_detail_id
    FROM silver_sales_order_detail AS detail
    INNER JOIN silver_sales_order_header AS header
      ON detail.sales_order_id = header.sales_order_id
    WHERE header.online_order_flag
  )
),
fact_count AS (
  SELECT count(*) AS grain_count
  FROM fact_internet_sales
)
SELECT
  source_count.grain_count AS source_grains,
  fact_count.grain_count AS fact_rows,
  fact_count.grain_count - source_count.grain_count AS grain_count_delta
FROM source_count
CROSS JOIN fact_count;
```

Expected result: `grain_count_delta = 0`.
