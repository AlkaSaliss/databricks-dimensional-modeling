# Databricks Dimensional Modeling

This project implements an AdventureWorks dimensional data warehouse on
Databricks using Lakeflow Spark Declarative Pipelines and Auto CDC.

It reproduces the Databricks dimensional modeling use case from the Databricks
SQL blog series, adapted to this project structure:

- Bronze ingestion from raw AdventureWorks JSON files.
- Silver typed and quality-checked source-aligned tables.
- Gold dimensional model with dimensions and the Internet Sales fact table.
- Lakeflow expectations and validation queries for dimensional correctness.

## Architecture

```text
Raw JSON files
  -> bronze streaming tables
  -> silver cleaned source tables
  -> gold staging views
  -> gold dimensions and fact table
  -> gold validation check tables
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

## Documentation

- [Technical design](docs/technical_design.md)
- [Implementation task roadmap](docs/dimensional_model_implementation_tasks.md)
- [Validation SQL checklist](docs/dimensional_model_validation.md)
- [Target dimensional model image](docs/target_dimensional_model.png)

## Prerequisites

- Databricks CLI configured with profile `personal`
- Access to workspace `https://dbc-911569fe-22cc.cloud.databricks.com`
- Raw AdventureWorks JSON extracts available at:

```text
/Volumes/workspace/default/adventure_works_dump
```

## Run Locally

Run lint:

```bash
uv run ruff check src/adventure_works_etl/transformations
```

Validate the Databricks bundle:

```bash
databricks bundle validate -t dev --profile personal
```

Deploy the bundle:

```bash
databricks bundle deploy -t dev --profile personal
```

Run the Lakeflow pipeline:

```bash
databricks bundle run adventure_works_etl -t dev --profile personal
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
