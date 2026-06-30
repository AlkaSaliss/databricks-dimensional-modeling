# BI Layer Design

This document defines the first BI layer for the AdventureWorks dimensional
warehouse. The BI layer is intentionally thin: it makes the existing gold star
schema easier to consume for dashboard and analyst workflows without replacing
the dimensional model.

## Scope

The first BI domain is Internet Sales. It uses only these gold tables:

- `fact_internet_sales`
- `dim_date`
- `dim_product`
- `dim_customer`
- `dim_promotion`
- `dim_currency`
- `dim_sales_territory`

The BI layer does not read bronze or silver tables.

## Target Users

- Executives reviewing sales performance.
- Sales managers comparing territories and customer segments.
- Product managers reviewing category and product performance.
- Analysts validating dashboard totals against the dimensional model.

## Business Questions

- How are sales trending by month?
- Which product categories, subcategories, and products drive sales?
- Which sales territories perform best?
- How much discounting is applied, and where?
- Which customer segments contribute most to sales?

## BI Assets

### `bi_sales_order_line`

Base BI table at Internet Sales order-line grain:

```text
sales_order_id + sales_order_detail_id
```

This table joins the fact to descriptive dimension attributes and keeps
surrogate keys plus source business keys for traceability.

### `bi_sales_monthly`

Monthly sales aggregate for trend charts and executive KPIs.

### `bi_product_performance`

Product hierarchy aggregate by category, subcategory, and product.

### `bi_territory_performance`

Territory aggregate by country, group, and territory.

### `bi_customer_segments`

Customer demographic aggregate by income, education, occupation, gender,
marital status, home ownership, and commute distance.

## Metric Definitions

| Metric | Definition | Grain |
| --- | --- | --- |
| `gross_sales_amount` | Sum of `sales_amount_before_discount` | Any BI aggregate grain |
| `net_sales_amount` | Sum of `sales_amount` | Any BI aggregate grain |
| `discount_amount` | Sum of `discount_amount` | Any BI aggregate grain |
| `order_quantity` | Sum of `order_quantity` | Any BI aggregate grain |
| `order_line_count` | Count of fact rows | Any BI aggregate grain |
| `order_count` | Count of distinct `sales_order_id` | Any BI aggregate grain |
| `customer_count` | Count of distinct `customer_id` | Any BI aggregate grain |
| `average_order_value` | `net_sales_amount / order_count` | Any BI aggregate grain |
| `discount_rate` | `discount_amount / gross_sales_amount` | Any BI aggregate grain |

All amount metrics use the fact table currency context. Base-currency sales use
the existing inferred `currency_rate_id = -1` behavior from the gold model.

## Null Display Handling

BI tables coalesce required display dimensions to `Unknown` so dashboards do
not show empty labels. Keys remain nullable or unchanged where that preserves
traceability back to the dimensional model.

## Validation Strategy

BI validation should prove:

- `bi_sales_order_line` row count equals `fact_internet_sales`.
- `bi_sales_order_line` has no duplicate order-line grains.
- BI detail measures reconcile to `fact_internet_sales`.
- Aggregate totals reconcile to `bi_sales_order_line`.
- Required dashboard display fields are not null.

The runnable validation SQL is documented in
`docs/bi_layer_validation.md`.
