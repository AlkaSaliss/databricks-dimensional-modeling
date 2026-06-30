# BI Layer Implementation Plan

This document defines a scoped implementation plan for adding a BI layer on top
of the existing AdventureWorks dimensional model. It is documentation only: it
describes the work needed to make the project feel like a complete BI project,
but does not implement pipeline code.

## Assumptions

- The existing bronze, silver, and gold layers remain the system of record for
  ingestion, cleaning, dimensional modeling, and dimensional validation.
- The BI layer should sit above `fact_internet_sales` and the gold dimensions;
  it should not duplicate CDC logic or replace the star schema.
- Initial BI assets should target Databricks SQL users and dashboards.
- The first BI milestone should be a complete vertical slice before adding
  broader dashboards or semantic features.

## Success Criteria

- Analysts can answer common sales questions without joining raw dimensions and
  facts manually.
- BI metrics are named, documented, and reconciled to the gold fact table.
- Dashboard queries run from deployed bundle resources or documented SQL files.
- BI validation checks prove that aggregate BI outputs reconcile to the
  underlying dimensional model.
- README and technical docs explain how to deploy, run, and validate the BI
  layer.

## Target BI Architecture

```text
gold dimensions and fact
  -> BI-facing views or tables
  -> documented metric definitions
  -> dashboard SQL queries
  -> Databricks SQL dashboards
  -> BI validation checks
```

## Task 1: Define BI Layer Design

### Goal

Document the business questions, target users, metric definitions, and dashboard
inventory before writing BI transformations.

### Files Likely Affected

- `docs/bi_layer_design.md`
- `README.md`

### Implementation Notes

- Define the first business domain as Internet Sales.
- List the core questions the BI layer should answer:
  - How are sales trending over time?
  - Which products and categories drive revenue?
  - Which territories perform best?
  - How much discounting is applied?
  - What customer segments contribute to sales?
- Define each metric once, including numerator, denominator, grain, and known
  exclusions.
- Keep scope limited to existing gold tables.

### Acceptance Criteria

- BI scope is clear enough to prevent speculative marts.
- Each planned BI view maps to at least one business question.
- Each metric has a single documented definition.

### Verification

- Review the design against the existing gold tables.
- Confirm no required metric depends on a source table that is not already in
  the pipeline.

### Dependencies

- Existing gold model.

## Task 2: Add BI Transformation Module

### Goal

Create a dedicated BI transformation module loaded by the existing Lakeflow
pipeline glob.

### Files Likely Affected

- `src/adventure_works_etl/transformations/bi.py`

### Implementation Notes

- Add BI views or tables using `pyspark.pipelines`.
- Read only from `fact_internet_sales` and gold dimensions.
- Start with a small set of BI assets:
  - `bi_sales_order_line`
  - `bi_sales_monthly`
  - `bi_product_performance`
- Avoid helper abstractions until duplication is proven.

### Acceptance Criteria

- The pipeline discovers the new module.
- BI outputs are business-friendly and do not expose internal CDC columns unless
  needed for validation.
- BI outputs retain enough keys to trace rows back to gold records.

### Verification

- Run `uv run ruff check src/adventure_works_etl/transformations`.
- Run `databricks bundle validate -t dev --profile personal`.

### Dependencies

- Task 1.

## Task 3: Build `bi_sales_order_line`

### Goal

Create the base BI-facing detail view at the same grain as
`fact_internet_sales`.

### Files Likely Affected

- `src/adventure_works_etl/transformations/bi.py`

### Implementation Notes

- Join `fact_internet_sales` to the current descriptive attributes needed for
  reporting.
- Include order, due, and ship date attributes through `dim_date`.
- Include product category, subcategory, and product attributes.
- Include customer demographic and geography attributes.
- Include promotion, currency, and sales territory attributes.
- Keep the fact grain unchanged:

```text
sales_order_id + sales_order_detail_id
```

### Acceptance Criteria

- One row exists per Internet Sales order line.
- Required reporting dimensions are available without manual joins.
- Measures reconcile to `fact_internet_sales`.

### Verification

- Compare row count to `fact_internet_sales`.
- Check duplicate `sales_order_id, sales_order_detail_id` combinations.
- Compare total `sales_amount`, `sales_amount_before_discount`, and
  `discount_amount` to `fact_internet_sales`.

### Dependencies

- Task 2.

## Task 4: Build Aggregate BI Views

### Goal

Add aggregate views that power common dashboard tiles without forcing every
dashboard query to repeat aggregation logic.

### Files Likely Affected

- `src/adventure_works_etl/transformations/bi.py`

### Implementation Notes

- Create:
  - `bi_sales_monthly`
  - `bi_product_performance`
  - `bi_territory_performance`
  - `bi_customer_segments`
- Use documented metric definitions from Task 1.
- Include counts and denominator columns needed to audit rates.
- Prefer simple grouped views over broad parameterized logic.

### Acceptance Criteria

- Aggregate views cover the first dashboard.
- Aggregate totals reconcile to `bi_sales_order_line`.
- Rates expose enough input columns to verify calculations.

### Verification

- Reconcile monthly sales totals to `bi_sales_order_line`.
- Reconcile product sales totals to `bi_sales_order_line`.
- Reconcile territory sales totals to `bi_sales_order_line`.
- Confirm no aggregate view has unexpected null grouping labels for required
  dashboard fields.

### Dependencies

- Task 3.

## Task 5: Add BI Validation SQL

### Goal

Document repeatable checks that prove the BI layer is consistent with the gold
model.

### Files Likely Affected

- `docs/bi_layer_validation.md`

### Implementation Notes

- Add SQL checks for:
  - BI detail row count versus `fact_internet_sales`
  - BI detail grain uniqueness
  - BI detail measure reconciliation
  - Monthly aggregate reconciliation
  - Product aggregate reconciliation
  - Territory aggregate reconciliation
  - Required dashboard fields not null after display coalescing
- Keep checks runnable manually in Databricks SQL.

### Acceptance Criteria

- Every BI view has at least one reconciliation check.
- Expected results are stated next to each query.
- Checks can be run after the Lakeflow pipeline completes.

### Verification

- Run checks in the dev schema after a pipeline update.
- Document the latest verified result in README or a validation note.

### Dependencies

- Task 4.

## Task 6: Add Dashboard Query Assets

### Goal

Create versioned SQL query files for the first dashboard.

### Files Likely Affected

- `sql/bi/executive_sales_overview.sql`
- `sql/bi/product_performance.sql`
- `sql/bi/territory_performance.sql`
- `sql/bi/customer_segments.sql`

### Implementation Notes

- Start with query files even if dashboard resources are added later.
- Use BI views as query sources.
- Keep queries readable and dashboard-specific.
- Do not embed metric definitions that belong in BI views unless the query is
  only shaping data for visualization.

### Acceptance Criteria

- Query files can be copied into Databricks SQL with only catalog/schema context
  changes.
- Query names map directly to dashboard sections.
- Queries do not read silver or bronze tables.

### Verification

- Run each query against the dev schema.
- Confirm result shapes are appropriate for dashboard visualization.

### Dependencies

- Task 4.

## Task 7: Create First Dashboard

### Goal

Build one complete Databricks SQL dashboard for an executive sales overview.

### Files Likely Affected

- `resources/dashboards.yml`, if bundle-managed dashboards are used.
- Dashboard JSON or Databricks SQL workspace asset files, depending on the
  chosen Databricks dashboard deployment path.
- `docs/bi_layer_design.md`

### Implementation Notes

- Dashboard sections:
  - KPI summary: sales, orders, customers, average order value
  - Monthly sales trend
  - Product category performance
  - Territory performance
  - Discount impact
  - Customer segment contribution
- Use BI query assets from Task 6.
- Keep the first dashboard focused on Internet Sales only.

### Acceptance Criteria

- Dashboard renders from dev data.
- Each visual has a documented source query.
- The dashboard can be recreated from repository assets or documented steps.

### Verification

- Deploy or manually create the dashboard in Databricks SQL.
- Compare dashboard KPI totals with BI validation SQL.

### Dependencies

- Task 6.

## Task 8: Add Bundle and Makefile Workflow

### Goal

Make BI validation and deployment discoverable through existing project
commands.

### Files Likely Affected

- `resources/*.yml`
- `Makefile`
- `README.md`

### Implementation Notes

- Add bundle resources only after confirming the exact supported dashboard and
  query resource format for the Databricks CLI version in use.
- Add Make targets only for commands that are actually used:
  - `validate-bi`
  - `deploy-bi`
  - `run-bi-validation`
- Continue using Databricks profile `personal`.

### Acceptance Criteria

- Developers can find BI commands from `make help`.
- Bundle validation includes BI resources when resources are bundle-managed.
- Commands do not require hard-coded personal schema names.

### Verification

- Run `make validate-bundle`.
- Run any new Make targets against the dev target.

### Dependencies

- Tasks 5 and 7.

## Task 9: Update Project Documentation

### Goal

Expose the BI layer as a first-class part of the project.

### Files Likely Affected

- `README.md`
- `docs/technical_design.md`
- `docs/bi_layer_design.md`
- `docs/bi_layer_validation.md`

### Implementation Notes

- Update the architecture diagram to include the BI layer.
- Link BI design and validation docs from README.
- Document how to run the pipeline, validate the BI layer, and open dashboards.
- Record the latest verified BI validation result after a dev run.

### Acceptance Criteria

- A new contributor can understand the full warehouse-to-dashboard flow.
- BI validation instructions are separate from dimensional model validation.
- Documentation reflects the implemented assets, not planned assets.

### Verification

- Follow README instructions from a clean shell.
- Confirm every linked file exists.

### Dependencies

- Tasks 1 through 8.

## Recommended First Milestone

Implement a narrow vertical slice before adding more dashboards:

1. `docs/bi_layer_design.md`
2. `src/adventure_works_etl/transformations/bi.py`
3. `bi_sales_order_line`
4. `bi_sales_monthly`
5. `bi_product_performance`
6. `docs/bi_layer_validation.md`
7. One executive dashboard query file

This milestone is realistic because it proves the full path from dimensional
model to BI-ready output without committing the project to a large dashboard
surface area too early.
