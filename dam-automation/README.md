# DAM Automation

Data Automation Management (DAM) provisions complete lakehouse datasources across Azure, Databricks Unity Catalog, Microsoft Entra ID, and Snowflake with a single command. The service creates and wires storage, identities, catalog objects, and OAuth plumbing so teams can onboard new datasources consistently.

## Capabilities

- Idempotent end-to-end orchestration backed by a JSON state store (`./state`)
- Azure resources: ADLS Gen2 container, RBAC assignments, and user-assigned managed identity
- Microsoft Entra ID assets: application registration, service principal, app role assignments, and security groups
- Databricks artifacts: account/workspace service principals, Unity Catalog storage credential, external location, catalog, and access groups
- Snowflake setup: external volume, OAuth catalog integration, catalog-linked database primed with starter objects
- Typed configuration via Pydantic with built-in validation for URLs, scopes, and naming

## Requirements

- Python 3.10 or newer
- Access to Azure, Databricks account/workspace APIs, Microsoft Graph, and Snowflake as configured
- Optional Azure SDK dependencies (`.[azure]` extra) when running real Azure provisioning

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[azure]"  # omit [azure] if you only need mock/testing
```

The package installs a `dam-automation` Typer-powered CLI entrypoint.

## Configuration

Provisioning is driven by a YAML configuration file. Start from `config/example-config.yaml` and replace every credential with tenant-specific secrets before running in any environment.

Top-level sections:

- `azure`: Subscription, tenant, service principal credentials, target resource groups, storage account, and region.
- `databricks`: Workspace and account URLs, OAuth client credentials for both scopes, Unity Catalog metastore, storage root, and required access connector resource ID.
- `identity`: Microsoft Graph credentials used to create applications, service principals, and groups.
- `snowflake`: Connection details, default role/warehouse, catalog integration scopes, and namespace behavior.
- `state`: Filesystem state backend location (defaults to `./state`).
- `naming`: Optional global prefix and separator applied to generated resource names.

All URLs must be fully qualified. Databricks account operations require the accounts domain (`https://accounts.<region>.databricks.com`), not the workspace URL. The service normalizes datasource names and stores state files under the configured path to make provisioning idempotent.

## CLI Usage

Every command requires the path to a configuration file:

```bash
dam-automation --help
dam-automation create-datasource my-datasource --config ./config/example-config.yaml --description "Data mart" --owner user@example.com
```

- `create-datasource <name>`: Provisions the full stack and prints a JSON summary (catalog, group, external location, storage credential). Re-running uses cached state and secrets when available.
- `drop-snowflake <name>`: Removes only the Snowflake external volume, catalog integration, and linked database for a datasource. Azure/Databricks/identity assets remain intact.
- `delete-datasource <name>`: Attempts a complete teardown across Snowflake, Databricks, identity, and Azure, then removes the state record. The command exits with a non-zero code if any subsystem fails so you can address partial deletions.

Set `DAM_AUTOMATION_LOG_LEVEL` (DEBUG/INFO/WARNING/ERROR) to adjust CLI logging.

## State and Idempotency

State files are stored as pretty-printed JSON under the configured `state.path` (default `./state`). Provisioning reads existing state to avoid duplicating resources and reuses previously issued secrets when possible. If a run fails, the last error is persisted in the state record for inspection.

## Development

```bash
pip install -e ".[azure]"
pytest
```

The test suite relies on responses- and requests-mocking; no live cloud calls are made. When iterating on CLI behavior, prefer running commands against sandbox configurations and cleaning up with `delete-datasource`.
