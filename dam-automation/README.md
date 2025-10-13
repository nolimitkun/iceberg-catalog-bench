# DAM Automation

Data Automation Management (DAM) service provisions lakehouse data sources across Azure and Databricks. It automates creation of Azure Data Lake Storage (ADLS) containers, managed identities, Databricks Unity Catalog objects, and service principals.

## Features

- Idempotent datasource provisioning workflow
- Abstractions for Azure, Databricks, and Microsoft Entra ID (Azure AD)
- Automated Snowflake external volume, catalog integration, and catalog-linked database setup
- Configuration-driven execution via YAML or environment variables
- CLI entrypoint for running automation jobs locally or in CI

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
dam-automation create-datasource my_datasource --config ./config/example-config.yaml
# optional cleanup of Snowflake artifacts only
dam-automation drop-snowflake my_datasource --config ./config/example-config.yaml
```

## Configuration

The service expects an automation config file with Azure, Databricks, and identity parameters. See `config/example-config.yaml` for a starting template. Databricks operations require service-principal OAuth credentials for both workspace and account APIs: provide `databricks.workspace_client_id` / `workspace_client_secret` (optionally `workspace_oauth_scopes`) and `databricks.account_client_id` / `account_client_secret` (optionally `account_oauth_scopes`). Tokens are requested automatically from the Databricks OIDC token endpoints (`<workspace>/oidc/v1/token` and `<accounts>/oidc/accounts/<account_id>/v1/token`) using the client credentials — no Personal Access Tokens are needed. During provisioning we also create a user-assigned managed identity and wire it directly into the Unity Catalog storage credential so Databricks can access ADLS without shared keys. Configure `databricks.access_connector_id` with the Azure Databricks access connector resource ID so the credential can be bound to your storage account.

For Snowflake automation, supply a `snowflake` block with connection credentials, default role/warehouse, and catalog integration preferences. The service uses these settings to create external volumes pointing at the provisioned ADLS container, establish an OAuth-backed catalog integration against the Databricks Unity Catalog, and materialize a catalog-linked database for querying through Snowflake. Use `dam-automation drop-snowflake <name>` to delete the linked database, catalog integration, and external volume if you need to reset just the Snowflake side without touching Azure or Databricks resources.

## Current Status

This repository contains the orchestration scaffolding and HTTP integrations. Additional hardening is required for production (secret rotation, retries on Azure async operations, unit tests against live sandboxes).
