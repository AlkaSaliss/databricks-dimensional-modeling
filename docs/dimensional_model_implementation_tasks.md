# Dimensional Model Implementation Tasks

This document breaks the AdventureWorks dimensional model implementation into small, independently verifiable tasks. It is documentation only: the tasks below describe the implementation path, but do not implement pipeline code.

The goal is to reproduce the Databricks dimensional modeling blog series as closely as practical using the existing Lakeflow Spark Declarative Pipeline, current bronze/silver layers, and Auto CDC from snapshot-style inputs.

## Task 1: Add Gold Layer Skeleton

### Goal

Introduce a gold-layer transformation module without changing bronze or silver behavior.

### Files Likely Affected

- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Create an empty or minimal `gold.py` module loaded by the existing pipeline glob.
- Add section headers for staging views, dimensions, inferred members, and facts.
- Do not modify existing bronze or silver transformations.

### Acceptance Criteria

- Pipeline source discovery still includes bronze, silver, and gold modules.
- No gold tables are required yet.
- Existing bronze and silver definitions remain unchanged.

### Verification

- Run `databricks bundle validate -t dev --profile personal`.
- Optionally run a local import or static check if available.

### Dependencies

- None.

## Task 2: Define Shared Gold Helpers

### Goal

Add only the helper functions needed for deterministic dimensional keys and common metadata.

### Files Likely Affected

- `src/adventure_works_etl/transformations/helpers.py`
- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Add a deterministic surrogate key helper based on `sha2`.
- Add helper logic for standard inferred-member flags only if needed by later tasks.
- Keep helpers narrow and avoid generic framework abstractions.
- Do not alter the existing `record_hash` behavior used by silver tables.

### Acceptance Criteria

- Surrogate keys are stable across reruns for the same inputs.
- Existing silver `record_hash` behavior is unchanged.
- No broad helper framework is introduced.

### Verification

- Run a unit-style local Spark check if practical.
- Run `databricks bundle validate -t dev --profile personal`.

### Dependencies

- Task 1.

## Task 3: Build `dim_date`

### Goal

Implement one static role-playing date dimension.

### Files Likely Affected

- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Create a single `dim_date` table.
- Generate date rows covering the minimum and maximum order, due, and ship dates from `silver_sales_order_header`, plus a small future/past buffer.
- Include calendar attributes needed for analytics: date key, date, year, quarter, month, month name, day, day of week, and week of year.
- Facts will later reference this table through `order_date_key`, `due_date_key`, and `ship_date_key`.
- Handle null ship dates in fact logic later; do not create invalid date dimension rows for nulls.

### Acceptance Criteria

- `dim_date` has one row per calendar date.
- Date key is deterministic.
- Null ship dates can be handled by facts later without breaking the dimension.

### Verification

- Pipeline run succeeds.
- Count check confirms no duplicate date keys.
- Manual query verifies order, due, and ship source date ranges are covered.

### Dependencies

- Task 1.

## Task 4: Build Dimension Staging Views

### Goal

Create clean, dimension-ready staging views from the existing silver layer.

### Files Likely Affected

- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Add staging views for:
  - `stg_dim_customer`
  - `stg_dim_product`
  - `stg_dim_promotion`
  - `stg_dim_currency`
  - `stg_dim_sales_territory`
- Join only silver tables needed to reproduce the blog's target star schema.
- Include business keys, descriptive attributes, `modified_at`, `record_hash`, and source metadata needed for CDC sequencing.
- Do not create final dimensions in this task.

### Acceptance Criteria

- Each staging view has a clear business key.
- Staging views expose one logical source row per business key and sequence point.
- Joins do not multiply rows unexpectedly.

### Verification

- Pipeline run succeeds.
- Row-count and duplicate-key checks pass per staging view.
- Several known AdventureWorks source records can be spot-checked against silver inputs.

### Dependencies

- Task 1.

## Task 5: Implement Type 1 `dim_promotion`

### Goal

Create the promotion dimension using Type 1 semantics.

### Files Likely Affected

- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Use `stg_dim_promotion` as the source.
- Create `dim_promotion` using Lakeflow `create_auto_cdc_from_snapshot_flow` with SCD Type 1 behavior.
- Use `special_offer_id` as the business key.
- Generate deterministic `promotion_key`.

### Acceptance Criteria

- One current row exists per promotion business key.
- Updates overwrite descriptive attributes in place.
- No SCD2 start/end metadata is required for promotion.

### Verification

- Pipeline run succeeds.
- Duplicate current-key check returns zero duplicates.
- Fact join readiness check confirms all known detail `special_offer_id` values can resolve or are explicitly identified.

### Dependencies

- Task 4.

## Task 6: Implement SCD2 Product, Currency, and Territory Dimensions

### Goal

Create Type 2 dimensions that do not yet require inferred-member feedback from facts.

### Files Likely Affected

- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Implement:
  - `dim_product`
  - `dim_currency`
  - `dim_sales_territory`
- Use `create_auto_cdc_from_snapshot_flow` with SCD Type 2 behavior.
- Use deterministic surrogate keys based on dimension name, business key, and effective start timestamp.
- Preserve Lakeflow-generated SCD metadata columns for current/effective interval filtering.

### Acceptance Criteria

- Each dimension supports historical versions.
- Current rows are unique per business key.
- Surrogate keys are stable across reruns for the same version.

### Verification

- Pipeline run succeeds.
- Duplicate-current-row checks return zero.
- Historical interval overlap checks return zero.

### Dependencies

- Tasks 2 and 4.

## Task 7: Implement SCD2 Customer Dimension

### Goal

Build the customer dimension with richer customer, person, and address attributes.

### Files Likely Affected

- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Use `stg_dim_customer`.
- Implement `dim_customer` with SCD Type 2 behavior.
- Include customer business key, account number, person attributes, email, address, city, state/province, country/region, and sales territory reference where available.
- Use deterministic customer surrogate keys.
- Include `is_late_arriving` support for inferred-member reconciliation.

### Acceptance Criteria

- Current customer rows are unique by `customer_id`.
- Customer history is preserved when descriptive attributes change.
- Late-arriving placeholder rows can later be replaced or updated when real source data appears.

### Verification

- Pipeline run succeeds.
- Duplicate-current-row and interval-overlap checks return zero.
- Customer attributes can be spot-checked against silver tables.

### Dependencies

- Tasks 2 and 4.

## Task 8: Add Inferred-Member Inputs for SCD2 Dimensions

### Goal

Allow facts to resolve missing SCD2 dimension members without blocking fact loads.

### Files Likely Affected

- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Detect missing business keys referenced by sales facts for:
  - customer
  - product
  - currency
  - sales territory
- Create inferred rows with minimal attributes and `is_late_arriving = true`.
- Feed inferred rows into the same SCD2 dimension staging flow before final dimension CDC processing.
- Do not infer promotion rows, because promotion is Type 1.

### Acceptance Criteria

- Missing SCD2 keys produce deterministic inferred dimension rows.
- Inferred rows can be replaced or reconciled when real source rows arrive.
- Fact processing can resolve surrogate keys for inferred members.

### Verification

- Pipeline run succeeds.
- Missing-dimension-key diagnostic query returns zero unresolved SCD2 keys after inference.
- Inferred rows are visibly flagged.

### Dependencies

- Tasks 6 and 7.

## Task 9: Implement Incremental `fact_internet_sales`

### Goal

Create the Internet Sales fact table at order-line grain.

### Files Likely Affected

- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Grain: one row per `sales_order_id` + `sales_order_detail_id`.
- Source: `silver_sales_order_detail` joined to `silver_sales_order_header`.
- Resolve foreign keys:
  - `product_key`
  - `customer_key`
  - `promotion_key`
  - `currency_key`
  - `sales_territory_key`
  - `order_date_key`
  - `due_date_key`
  - `ship_date_key`
- Resolve SCD2 dimensions using the fact order date within the dimension effective interval.
- Include degenerate dimensions such as `sales_order_number`, `sales_order_id`, and `sales_order_detail_id`.
- Include measures:
  - `order_quantity`
  - `unit_price`
  - `sales_amount`
  - `sales_amount_before_discount`
  - `discount_amount`

### Acceptance Criteria

- Fact grain is unique.
- All required dimension keys resolve.
- Measure formulas match source order detail values.
- Incremental reruns do not duplicate fact rows.

### Verification

- Pipeline run succeeds.
- Unique-grain check returns zero duplicates.
- Foreign-key unresolved checks return zero for required dimensions.
- Fact row count matches valid silver order detail grain.
- Measure reconciliation query passes.

### Dependencies

- Tasks 3, 5, 6, 7, and 8.

## Task 10: Add Gold Data Quality Expectations

### Goal

Add focused Lakeflow expectations for the final dimensional model.

### Files Likely Affected

- `src/adventure_works_etl/transformations/gold.py`

### Implementation Notes

- Add expectations for:
  - Non-null business keys in dimension staging.
  - Unique current dimension business keys.
  - Non-null fact grain keys.
  - Positive quantities and non-negative monetary measures.
  - Required fact foreign keys.
- Keep expectations limited to realistic failures.

### Acceptance Criteria

- Bad dimensional records are dropped, quarantined, or fail according to the severity chosen in code.
- Expectations are named clearly and visible in pipeline event logs.

### Verification

- Pipeline run succeeds.
- Inspect Lakeflow event log for expectation metrics.

### Dependencies

- Tasks 5 through 9.

## Task 11: Create Validation Queries Document

### Goal

Add a lightweight validation checklist for manual and CI-oriented checks.

### Files Likely Affected

- `docs/dimensional_model_validation.md`

### Implementation Notes

- Document SQL checks for:
  - Dimension current-row uniqueness.
  - SCD2 interval overlap.
  - Fact grain uniqueness.
  - Fact-to-dimension resolution.
  - Fact measure reconciliation.
  - Row-count comparison against silver order details.

### Acceptance Criteria

- Each validation query has expected pass criteria.
- Queries use final gold table names.
- The document can be used after every pipeline run.

### Verification

- Run queries in Databricks SQL or a notebook using profile/workspace context.
- Confirm all checks pass after Task 9 or Task 10.

### Dependencies

- Tasks 5 through 10.

## Task 12: End-to-End Bundle and Pipeline Verification

### Goal

Verify the implemented dimensional model in Databricks.

### Files Likely Affected

- No code changes expected.

### Implementation Notes

- Use Databricks profile `personal`.
- Run `databricks bundle validate -t dev --profile personal`.
- Deploy and run the Lakeflow pipeline in dev.
- Execute validation queries from `docs/dimensional_model_validation.md`.
- Record results in task or PR notes, not in committed generated output unless explicitly requested.

### Acceptance Criteria

- Bundle validation succeeds.
- Pipeline run succeeds.
- Gold tables exist.
- Validation queries pass.
- No unrelated bronze/silver regressions are observed.

### Verification

- `databricks bundle validate -t dev --profile personal`
- Dev Lakeflow pipeline run succeeds.
- Validation queries from `docs/dimensional_model_validation.md` pass.

### Dependencies

- Tasks 1 through 11.

## Assumptions

- This is a documentation-only deliverable until implementation begins.
- Implementation should be done in the task order above, one small verified chunk at a time.
- Existing bronze and silver layers are stable inputs unless a later task proves a specific missing column or data-quality gap.
- The current JSON inputs represent ordered snapshots, so dimension CDC should use Auto CDC from snapshot semantics.
- The Databricks profile for verification is `personal`.
