# Operations Guide

This guide documents the routine commands and deployment behavior for the
AdventureWorks Databricks dimensional modeling project.

## Local Development

Use Python 3.12 and `uv`:

```bash
uv sync --only-group dev
```

Show available project commands:

```bash
make help
```

Run the same local checks used by CI:

```bash
make pre-commit
```

The pre-commit configuration runs common file checks, Ruff lint/format,
`nbstripout`, and `gitleaks`.

Run only the transformation lint check:

```bash
uv run ruff check src/adventure_works_etl/transformations
```

## Databricks Bundle Targets

The bundle is named `adventure_works_dw` and is configured in `databricks.yml`.
Use the Databricks CLI profile `personal` for local Databricks commands.

The default `dev` target:

- Uses workspace `https://dbc-911569fe-22cc.cloud.databricks.com`.
- Runs as the current Databricks user.
- Uses catalog `adventure_works_dw`.
- Uses the current user's short name as the pipeline schema.
- Uses `dev_${workspace.current_user.short_name}_logs` for pipeline event logs.
- Sets the dashboard dataset schema to
  `dev_${workspace.current_user.short_name}_${var.schema}`.

The `prod` target:

- Uses schema `prod`.
- Runs as `${var.deployment_service_principal_name}`.
- Requires `${var.deployment_user_name}` for bundle ownership and grants.
- Sets `pipeline_development_mode` to `false`.

## Bundle Commands

Validate the dev bundle:

```bash
make validate-bundle
```

Preview planned resources:

```bash
make plan-bundle
```

Deploy the dev bundle:

```bash
make deploy-bundle
```

Run the Lakeflow pipeline:

```bash
make run-bundle
```

Run the Lakeflow pipeline with a full refresh:

```bash
make run-bundle-full
```

Equivalent raw Databricks CLI commands use `--target dev --profile personal`.

## Pipeline Inputs And Outputs

Raw AdventureWorks JSON extracts must be available at:

```text
/Volumes/workspace/default/adventure_works_dump
```

If the source AdventureWorks tables are available in the Databricks catalog
`adventure-work`, the notebook
`src/adventure_works_etl/explorations/extract_all_tables.ipynb` can recreate
the JSON extract volume used by the pipeline. It creates the
`workspace.default.adventure_works_dump` volume and writes the source tables as
JSON under the path above.

The pipeline resource is `adventure_works_etl`. It loads transformation modules
from `src/adventure_works_etl/transformations/**`.

The main persisted outputs are:

- Bronze streaming tables from the raw AdventureWorks source tables.
- Silver source-aligned cleaned tables.
- Gold dimensions and `fact_internet_sales`.
- BI tables for Internet Sales reporting.

## Exploration Notebooks

Notebooks under `src/adventure_works_etl/explorations/` are utilities for data
inspection and raw extract preparation. They are not loaded by the Lakeflow
pipeline because `resources/pipelines.yml` only includes files under
`src/adventure_works_etl/transformations/**`.

Current notebooks:

- `extract_all_tables.ipynb`: exports the required AdventureWorks source tables
  to the raw JSON volume.
- `EDA.ipynb`: a small scratch notebook for inspecting source tables.

## Dashboard Resource

The bundle-managed dashboard is defined in `resources/dashboards.yml` and uses:

- Dashboard file:
  `assets/AdventureWorks Executive Sales Dashboard.lvdash.json`
- Display name variable: `${var.dashboard_display_name}`
- Dataset catalog variable: `${var.catalog}`
- Dataset schema variable: `${var.dataset_schema}`
- Warehouse ID: `a10e62df4c510ef3`

Dashboard source SQL examples live in `sql/bi/`. The BI dashboard should read
only the `bi_*` tables, not bronze, silver, or gold tables directly.

## Validation Order

After a successful pipeline run:

1. Review Lakeflow expectations for the pipeline update.
2. Run the dimensional model SQL checks in
   `docs/dimensional_model_validation.md`.
3. Run the BI layer SQL checks in `docs/bi_layer_validation.md`.
4. Spot-check dashboard totals against the BI validation queries.

Use the active catalog and schema before running validation SQL, for example:

```sql
USE CATALOG adventure_works_dw;
USE SCHEMA <target_schema>;
```

For dev, `<target_schema>` is usually the current Databricks user's short name.

## CI Behavior

The GitHub workflow in `.github/workflows/deploy-databricks-bundle.yml` runs on
every branch push.

For all branches, CI runs:

- `uv sync --only-group dev`
- `uv run pre-commit run --all-files`

For non-`main` branches, CI then:

- Grants catalog permissions for the deployment service principal and deployment
  user, including catalog traversal for the deployment user.
- Grants `READ_VOLUME` on `workspace.default.adventure_works_dump` for the raw
  input files.
- Validates the dev bundle.
- Plans the dev bundle.
- Deploys the dev bundle.

For `main`, CI:

- Grants catalog and raw volume permissions for the deployment service principal
  and deployment user.
- Applies prod schema grants that give the deployment user `USE_SCHEMA` and
  `SELECT` on the created data objects.
- Validates and plans the prod bundle.
- Deploys prod through the `production` GitHub environment.

Prod job task keys are stable machine identifiers. Do not derive task keys from
dashboard display names, because Databricks job task keys only allow
alphanumeric characters, hyphens, and underscores.

CI expects these secrets:

- `DATABRICKS_HOST`
- `DATABRICKS_CLIENT_ID`
- `DATABRICKS_CLIENT_SECRET`
- `DATABRICKS_DEPLOYMENT_USER_NAME`
