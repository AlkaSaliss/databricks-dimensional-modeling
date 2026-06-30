# AI/BI Dashboard Genie Design

This document is a detailed build specification for creating the first
AdventureWorks AI/BI dashboard in Databricks. It is written so the section
`Paste-Ready Genie Instructions` can be copied directly into Databricks Genie or
the Databricks coding assistant.

## Dashboard Summary

Dashboard name:

```text
AdventureWorks Internet Sales Executive Overview
```

Purpose:

Create an executive BI dashboard that explains Internet Sales performance across
time, product hierarchy, sales territory, discounting, and customer segments.
The dashboard should use only the BI-facing tables that already exist in the
current Databricks schema.

Primary audience:

- Executives reviewing overall revenue and order performance.
- Sales managers comparing territories.
- Product managers identifying top products and discount patterns.
- Analysts validating dashboard totals against curated BI tables.

Business questions:

- What are total net sales, gross sales, discounts, orders, customers, and
  average order value?
- How are sales trending by month and year?
- Which product categories, subcategories, and products drive sales?
- Which territories and territory groups perform best?
- Where is discounting most concentrated?
- Which customer income, education, and occupation segments contribute most to
  sales?

## Data Context

Set the target context before creating datasets:

```sql
USE CATALOG adventure_works_dw;
USE SCHEMA <target_schema>;
```

Replace `<target_schema>` with the active schema where the Lakeflow pipeline
materialized the BI tables. For dev, this is usually the current user's short
name.

Use only these BI tables:

- `bi_sales_order_line`
- `bi_sales_monthly`
- `bi_product_performance`
- `bi_territory_performance`
- `bi_customer_segments`

Do not query bronze, silver, gold dimension, or gold fact tables directly from
the dashboard. The BI tables already apply the required joins, display labels,
and metric definitions.

## Metric Definitions

Use these definitions consistently in all visuals:

| Metric | Definition |
| --- | --- |
| Net Sales | `sum(net_sales_amount)` |
| Gross Sales | `sum(gross_sales_amount)` |
| Discount Amount | `sum(discount_amount)` |
| Discount Rate | `sum(discount_amount) / sum(gross_sales_amount)` when gross sales is nonzero |
| Order Quantity | `sum(order_quantity)` |
| Order Count | `sum(order_count)` for aggregate BI tables; `count(DISTINCT sales_order_id)` for `bi_sales_order_line` |
| Customer Count | `count(DISTINCT customer_id)` from `bi_sales_order_line` when dashboard-wide distinct customers are needed |
| Average Order Value | `sum(net_sales_amount) / sum(order_count)` when order count is nonzero |

Amount values use the currency context already represented in the BI layer.

## Global Filters

Add dashboard-level filters that apply to all visuals when the selected dataset
contains the relevant columns:

- Order year: `order_year`
- Order month: `order_month`
- Product category: `product_category_name`
- Product subcategory: `product_subcategory_name`
- Territory group: `territory_group`
- Territory name: `territory_name`
- Customer income: `yearly_income`
- Customer education: `education`
- Customer occupation: `occupation`

If a filter cannot apply to a dataset because the column is absent, leave that
visual unfiltered by that field rather than changing the source query.

## Dashboard Layout

Use a clean executive dashboard layout with four sections.

### Section 1: Executive KPI Summary

Top row with KPI tiles:

1. Net Sales
2. Gross Sales
3. Discount Amount
4. Discount Rate
5. Order Count
6. Customer Count
7. Average Order Value
8. Order Quantity

Use compact numeric formatting:

- Currency amounts: currency format, no more than two decimals.
- Rates: percentage format with one or two decimals.
- Counts: whole numbers with thousands separators.

### Section 2: Sales Trend

Visuals:

- Monthly Net Sales trend line.
- Monthly Gross Sales vs Net Sales comparison.
- Yearly Sales Summary table.
- Monthly Discount Impact line or combo chart.

### Section 3: Product And Territory Performance

Visuals:

- Net Sales by Product Category bar chart.
- Net Sales by Product Subcategory table or bar chart.
- Top 25 Products by Net Sales table.
- Net Sales by Territory Group bar chart.
- Territory Performance table.

### Section 4: Discount And Customer Segments

Visuals:

- Products with Highest Discount Rate table.
- Territory Discount Comparison table.
- Income Segment Performance bar chart.
- Education and Occupation Performance table.
- Top Customer Demographic Segments table.

## Dataset And Visual Specifications

Create the following datasets and visuals.

### Dataset: KPI Summary

Source:

```sql
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
```

Visuals:

- KPI tile: `net_sales_amount`
- KPI tile: `gross_sales_amount`
- KPI tile: `discount_amount`
- KPI tile: `discount_rate`
- KPI tile: `order_count`
- KPI tile: `customer_count`
- KPI tile: `average_order_value`
- KPI tile: `order_quantity`

### Dataset: Monthly Sales Trend

Source:

```sql
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
```

Visuals:

- Line chart: x-axis `order_month_start_date`, y-axis `net_sales_amount`.
- Line or combo chart: x-axis `order_month_start_date`, y-axis fields
  `gross_sales_amount` and `net_sales_amount`.
- Line chart: x-axis `order_month_start_date`, y-axis `discount_rate`.

### Dataset: Yearly Sales Summary

Source:

```sql
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
```

Visual:

- Table sorted by `order_year`.

### Dataset: Product Category Performance

Source:

```sql
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
```

Visual:

- Bar chart: x-axis `product_category_name`, y-axis `net_sales_amount`, sorted
  descending by `net_sales_amount`.

### Dataset: Product Subcategory Performance

Source:

```sql
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
```

Visual:

- Table with category, subcategory, net sales, order quantity, order count,
  product count, average order value, and discount rate.

### Dataset: Top Products

Source:

```sql
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
```

Visual:

- Table sorted by `net_sales_amount` descending.

### Dataset: High Discount Products

Source:

```sql
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
```

Visual:

- Table sorted by `discount_rate` descending, then `net_sales_amount`
  descending.

### Dataset: Territory Group Performance

Source:

```sql
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
```

Visual:

- Bar chart: x-axis `territory_group`, y-axis `net_sales_amount`, sorted
  descending.

### Dataset: Territory Performance

Source:

```sql
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
```

Visual:

- Table sorted by `net_sales_amount` descending.

### Dataset: Territory Discount Comparison

Source:

```sql
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
```

Visual:

- Table sorted by `discount_rate` descending.

### Dataset: Income Segment Performance

Source:

```sql
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
```

Visual:

- Bar chart: x-axis `yearly_income`, y-axis `net_sales_amount`, sorted
  descending.

### Dataset: Education And Occupation Performance

Source:

```sql
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
```

Visual:

- Table sorted by `net_sales_amount` descending.

### Dataset: Demographic Segment Detail

Source:

```sql
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
```

Visual:

- Table sorted by `net_sales_amount` descending.

## Styling And Formatting

- Use a restrained business dashboard style.
- Keep sections visually separated with clear section titles.
- Prefer dense, readable layout over decorative elements.
- Use currency formatting for sales and discount amount fields.
- Use percentage formatting for `discount_rate`.
- Use integer formatting for count and quantity fields.
- Use sorted tables with the strongest business metric first, usually
  `net_sales_amount`.
- Use readable visual titles, for example:
  - `Net Sales`
  - `Monthly Net Sales`
  - `Gross vs Net Sales`
  - `Net Sales by Product Category`
  - `Top Products by Net Sales`
  - `Net Sales by Territory Group`
  - `Customer Income Segment Performance`

## Validation After Build

After the dashboard is created, run the checks in:

```text
docs/bi_layer_validation.md
```

Minimum dashboard-specific validation:

- KPI net sales equals total `sum(net_sales_amount)` from `bi_sales_monthly`.
- KPI gross sales equals total `sum(gross_sales_amount)` from
  `bi_sales_monthly`.
- KPI discount amount equals total `sum(discount_amount)` from
  `bi_sales_monthly`.
- KPI customer count equals `count(DISTINCT customer_id)` from
  `bi_sales_order_line`.
- Monthly trend row count is greater than zero.
- Product, territory, and customer segment visuals return rows.

## Paste-Ready Genie Instructions

Copy the following prompt into Databricks Genie or the Databricks coding
assistant:

```text
Create an AI/BI dashboard named "AdventureWorks Internet Sales Executive Overview".

Use this SQL context:

USE CATALOG adventure_works_dw;
USE SCHEMA <target_schema>;

Replace <target_schema> with the active schema containing the BI tables.

Use only these source tables:
- bi_sales_order_line
- bi_sales_monthly
- bi_product_performance
- bi_territory_performance
- bi_customer_segments

Do not query bronze, silver, fact_internet_sales, dim_date, dim_product,
dim_customer, dim_promotion, dim_currency, or dim_sales_territory directly.

Build an executive dashboard with four sections:

1. Executive KPI Summary
2. Sales Trend
3. Product And Territory Performance
4. Discount And Customer Segments

Add dashboard filters where possible:
- order_year
- order_month
- product_category_name
- product_subcategory_name
- territory_group
- territory_name
- yearly_income
- education
- occupation

Create these datasets and visuals:

Dataset: KPI Summary
SQL:
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
  CASE WHEN sales.order_count = 0 THEN NULL ELSE sales.net_sales_amount / sales.order_count END AS average_order_value,
  CASE WHEN sales.gross_sales_amount = 0 THEN NULL ELSE sales.discount_amount / sales.gross_sales_amount END AS discount_rate
FROM sales_totals AS sales
CROSS JOIN customer_totals AS customers;
Visuals: KPI tiles for net_sales_amount, gross_sales_amount, discount_amount,
discount_rate, order_count, customer_count, average_order_value, and
order_quantity.

Dataset: Monthly Sales Trend
SQL:
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
Visuals:
- Line chart: order_month_start_date by net_sales_amount.
- Line or combo chart: order_month_start_date by gross_sales_amount and net_sales_amount.
- Line chart: order_month_start_date by discount_rate.

Dataset: Yearly Sales Summary
SQL:
SELECT
  order_year,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  CASE WHEN sum(order_count) = 0 THEN NULL ELSE sum(net_sales_amount) / sum(order_count) END AS average_order_value,
  CASE WHEN sum(gross_sales_amount) = 0 THEN NULL ELSE sum(discount_amount) / sum(gross_sales_amount) END AS discount_rate
FROM bi_sales_monthly
GROUP BY order_year
ORDER BY order_year;
Visual: Table sorted by order_year.

Dataset: Product Category Performance
SQL:
SELECT
  product_category_name,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  count(DISTINCT product_id) AS product_count,
  CASE WHEN sum(order_count) = 0 THEN NULL ELSE sum(net_sales_amount) / sum(order_count) END AS average_order_value,
  CASE WHEN sum(gross_sales_amount) = 0 THEN NULL ELSE sum(discount_amount) / sum(gross_sales_amount) END AS discount_rate
FROM bi_product_performance
GROUP BY product_category_name
ORDER BY net_sales_amount DESC;
Visual: Bar chart of product_category_name by net_sales_amount sorted descending.

Dataset: Product Subcategory Performance
SQL:
SELECT
  product_category_name,
  product_subcategory_name,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  count(DISTINCT product_id) AS product_count,
  CASE WHEN sum(order_count) = 0 THEN NULL ELSE sum(net_sales_amount) / sum(order_count) END AS average_order_value,
  CASE WHEN sum(gross_sales_amount) = 0 THEN NULL ELSE sum(discount_amount) / sum(gross_sales_amount) END AS discount_rate
FROM bi_product_performance
GROUP BY product_category_name, product_subcategory_name
ORDER BY net_sales_amount DESC;
Visual: Table sorted by net_sales_amount descending.

Dataset: Top Products
SQL:
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
Visual: Table sorted by net_sales_amount descending.

Dataset: High Discount Products
SQL:
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
Visual: Table sorted by discount_rate descending.

Dataset: Territory Group Performance
SQL:
SELECT
  territory_group,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  sum(customer_count) AS territory_customer_count_sum,
  CASE WHEN sum(order_count) = 0 THEN NULL ELSE sum(net_sales_amount) / sum(order_count) END AS average_order_value,
  CASE WHEN sum(gross_sales_amount) = 0 THEN NULL ELSE sum(discount_amount) / sum(gross_sales_amount) END AS discount_rate
FROM bi_territory_performance
GROUP BY territory_group
ORDER BY net_sales_amount DESC;
Visual: Bar chart of territory_group by net_sales_amount sorted descending.

Dataset: Territory Performance
SQL:
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
Visual: Table sorted by net_sales_amount descending.

Dataset: Territory Discount Comparison
SQL:
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
Visual: Table sorted by discount_rate descending.

Dataset: Income Segment Performance
SQL:
SELECT
  yearly_income,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  sum(customer_count) AS segment_customer_count_sum,
  CASE WHEN sum(order_count) = 0 THEN NULL ELSE sum(net_sales_amount) / sum(order_count) END AS average_order_value,
  CASE WHEN sum(gross_sales_amount) = 0 THEN NULL ELSE sum(discount_amount) / sum(gross_sales_amount) END AS discount_rate
FROM bi_customer_segments
GROUP BY yearly_income
ORDER BY net_sales_amount DESC;
Visual: Bar chart of yearly_income by net_sales_amount sorted descending.

Dataset: Education And Occupation Performance
SQL:
SELECT
  education,
  occupation,
  sum(net_sales_amount) AS net_sales_amount,
  sum(gross_sales_amount) AS gross_sales_amount,
  sum(discount_amount) AS discount_amount,
  sum(order_quantity) AS order_quantity,
  sum(order_count) AS order_count,
  sum(customer_count) AS segment_customer_count_sum,
  CASE WHEN sum(order_count) = 0 THEN NULL ELSE sum(net_sales_amount) / sum(order_count) END AS average_order_value,
  CASE WHEN sum(gross_sales_amount) = 0 THEN NULL ELSE sum(discount_amount) / sum(gross_sales_amount) END AS discount_rate
FROM bi_customer_segments
GROUP BY education, occupation
ORDER BY net_sales_amount DESC;
Visual: Table sorted by net_sales_amount descending.

Dataset: Demographic Segment Detail
SQL:
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
Visual: Table sorted by net_sales_amount descending.

Use currency formatting for amount columns, percentage formatting for discount_rate,
and integer formatting for counts and quantities. Keep the dashboard clean,
business-focused, and readable.
```
