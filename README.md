# Databricks Dimensional Modeling

This project implements an AdventureWorks dimensional data warehouse on
Databricks using Lakeflow Spark Declarative Pipelines and Auto CDC.

It reproduces the Databricks dimensional modeling use case from the Databricks
SQL blog series, adapted to this project structure:

- Bronze ingestion from raw AdventureWorks JSON files.
- Silver typed and quality-checked source-aligned tables.
- Gold dimensional model with dimensions and the Internet Sales fact table.
- BI-facing tables and a bundle-managed Databricks AI/BI dashboard.
- Lakeflow expectations and validation queries for dimensional correctness.

## Architecture

```text
Raw JSON files
  -> bronze streaming tables
  -> silver cleaned source tables
  -> gold staging views
  -> gold dimensions and fact table
  -> gold validation check tables
  -> BI-facing tables
  -> Databricks AI/BI dashboard
```

The pipeline is deployed as the Databricks Asset Bundle
`adventure_works_dw`. The main Lakeflow pipeline is `adventure_works_etl`.

## Gold Model

The gold layer contains:

- `dim_date`
- `dim_customer`
- `dim_product`
- `dim_promotion`
- `dim_currency`
- `dim_sales_territory`
- `fact_internet_sales`

`fact_internet_sales` is modeled at this grain:

```text
sales_order_id + sales_order_detail_id
```

SCD behavior:

- `dim_promotion`: Type 1
- `dim_customer`: Type 2
- `dim_product`: Type 2
- `dim_currency`: Type 2
- `dim_sales_territory`: Type 2
- `fact_internet_sales`: Type 1 incremental fact

## BI Layer

The pipeline also materializes Internet Sales BI tables that sit above the gold
star schema:

- `bi_sales_order_line`
- `bi_sales_monthly`
- `bi_product_performance`
- `bi_territory_performance`
- `bi_customer_segments`

Dashboard query assets live in `sql/bi/`, and the bundle-managed dashboard is
defined in `resources/dashboards.yml` from
`assets/AdventureWorks Executive Sales Dashboard.lvdash.json`.

## Documentation

- [Technical design](docs/technical_design.md)
- [Operations guide](docs/operations.md)
- [Implementation task roadmap](docs/dimensional_model_implementation_tasks.md)
- [Validation SQL checklist](docs/dimensional_model_validation.md)
- [BI layer design](docs/bi_layer_design.md)
- [BI layer validation checklist](docs/bi_layer_validation.md)
- [AI/BI dashboard and Genie design](docs/ai_bi_dashboard_genie_design.md)
- [AI/BI Genie Space design plan](docs/ai_bi_genie_space_design_plan.md)
- [Target dimensional model image](docs/target_dimensional_model.png)

## Prerequisites

- Databricks CLI configured with profile `personal`
- Access to workspace `https://dbc-911569fe-22cc.cloud.databricks.com`
- Python 3.12 and `uv` for local development commands
- Raw AdventureWorks JSON extracts available at:

```text
/Volumes/workspace/default/adventure_works_dump
```

## Run Locally

Show repository commands:

```bash
make help
```

Install the dev environment:

```bash
uv sync --only-group dev
```

Run the same local checks used by CI:

```bash
make pre-commit
```

Run lint directly:

```bash
uv run ruff check src/adventure_works_etl/transformations
```

Validate the Databricks bundle:

```bash
make validate-bundle
```

Deploy the bundle:

```bash
make deploy-bundle
```

Run the Lakeflow pipeline:

```bash
make run-bundle
```

## Validate The Dimensional Model

After a successful pipeline run, execute the SQL checks in:

```text
docs/dimensional_model_validation.md
```

The validation checks cover:

- Gold uniqueness expectation tables
- Current-row uniqueness for dimensions
- SCD2 interval overlap
- Fact grain uniqueness
- Fact-to-dimension key resolution
- Measure reconciliation
- Source-to-fact grain count comparison

Silver is intentionally not deduplicated, so fact row-count validation compares
the fact table to distinct online source order-line grains.

## Validate The BI Layer

After dimensional validation passes, execute the SQL checks in:

```text
docs/bi_layer_validation.md
```

These checks prove that BI detail rows remain at fact grain, BI measures
reconcile to `fact_internet_sales`, aggregate tables reconcile to
`bi_sales_order_line`, and required dashboard display fields are populated.

For deployment, CI, prod variables, and dashboard notes, see
`docs/operations.md`.

## Current Verified Dev Result

Latest verified dev pipeline update:

```text
22ba0e3c-aa4d-4231-baa9-df7c8b724665
```

Validation summary:

- Source distinct Internet Sales grains: `60398`
- Fact rows: `60398`
- Grain count delta: `0`
- Fact measure reconciliation deltas: `0`
- Fact-to-dimension unresolved keys: `0`
- SCD2 interval overlaps: `0`

These are the latest dev verification results, not fixed assumptions baked into
the pipeline.
