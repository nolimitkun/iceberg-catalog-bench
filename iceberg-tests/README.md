# Iceberg Cross-Engine Interoperability Test Framework

This framework turns the interoperability specification in `ICEBERG-Interoperability-Test-Spec.md` into a configurable, SQL-driven harness. Each test step executes engine-specific SQL against Apache Iceberg tables hosted on ADLS Gen2, supporting Spark, Azure Databricks (Unity Catalog), and Snowflake (Polaris/Open Catalog).

## Layout
```
iceberg-tests/
  config/
    framework.yaml          # storage, engines, catalogs, datasets, plans
  framework/
    cli.py                  # entry point for running plans
    config.py               # Pydantic models + loader
    runner.py               # plan orchestration and validation
    engines/                # engine adapters (Spark, Databricks, Snowflake)
    sql.py                  # templating utilities
    validators.py           # reusable validation rules
  sql/
    spark/
      open_catalog/         # Spark SQL scripts for Polaris/Open Catalog
      unity_catalog/        # Spark SQL scripts for Unity Catalog
    databricks/
      unity_catalog/        # Databricks SQL warehouse scripts
    snowflake/
      open_catalog/         # Snowflake Polaris/Open Catalog SQL scripts
  orchestrator.py           # thin wrapper around framework.cli
  requirements.txt
  env.example               # copy to .env and fill with credentials
```

## Getting Started
1. Create and activate a Python 3.10+ virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `env.example` to `.env` and provide connection details for ADLS, Spark, Databricks, and Snowflake (including Polaris/Open Catalog endpoints and Unity Catalog warehouse settings).
4. Review and customise `config/framework.yaml`:
   - `storage`: ADLS locations used by the tests.
   - `catalogs`: logical catalogs (`open_catalog`, `unity_catalog`).
   - `engines`: connector metadata plus per-catalog overrides (session conf, REST endpoints, SQL variables).
   - `datasets`: logical dataset descriptions shared across SQL templates.
   - `test_cases`: mapping of logical steps to engine/catalog-specific SQL scripts.
   - `plans`: ordered orchestration of steps, including optional validation directives.

Environment variables referenced in the YAML are expanded at runtime, so secrets can stay in `.env`.

## Running a Plan
Execute the sample interoperability flow that exercises Spark (Open Catalog), Databricks (Unity Catalog), and Snowflake (Polaris):
```bash
python orchestrator.py \
  --plan interop_small \
  --namespace demo_ns \
  --var run_owner=$(whoami)
```

Key options:
- `--config`: path to a framework YAML (defaults to `config/framework.yaml`).
- `--plan`: plan identifier from the YAML.
- `--namespace`: logical namespace token; engine-specific namespace templates (see `namespace_template` in overrides) derive the fully-qualified schema/catalog names.
- `--var KEY=VALUE`: inject additional template variables available to SQL scripts and validations.
- `--json`: emit run results (step status, validation outcomes) as JSON.

## SQL Templating
Each test case is backed by SQL stored in `sql/<engine>/<catalog>/...`. Scripts are rendered with Jinja2, exposing:
- `namespace`: raw namespace argument provided on the CLI.
- `target_namespace`: fully-qualified namespace derived from the catalog override.
- `dataset`: dataset metadata (columns, partition spec, properties).
- `engine`, `catalog`, `engine_catalog`: engine and catalog configuration dictionaries.
- `storage`: ADLS configuration block.
- `state`: shared execution state (validations such as `store_rows_as` can stash values for later steps).
- Any key injected via `--var` or defined under `test_cases[*].variables`.

Example (excerpt from `sql/spark/open_catalog/bulk_insert_sales_events.sql`):
```sql
INSERT INTO {{ target_namespace }}.{{ test_case.variables.table_name }} VALUES (...);
SELECT COUNT(*) AS row_count FROM {{ target_namespace }}.{{ test_case.variables.table_name }};
```

## Validation Rules
Plans can attach validations to steps. Built-ins include:
- `rowcount_equals` / `rowcount_at_least`
- `store_rows_as` / `store_rowcount_as`
- `compare_rows_with_state`

Validation payloads are rendered with the same variable context, enabling dynamic expectations such as `expected: "{{ state.baseline_rowcount }}"`.

## Extending the Framework
- Add new SQL scripts under `sql/<engine>/<catalog>/` and reference them from a `test_cases` entry.
- Introduce new plans by composing existing test cases or defining new ones.
- Implement additional validations inside `framework/validators.py` as scenarios evolve (e.g., checksum comparisons, schema assertions).
- Contribute new engine adapters in `framework/engines/` if more execution backends are required.

The framework is intentionally config-first: no Python test needs to embed SQL; each scenario remains transparent and auditable in SQL form, aligning with the interoperability specification.
