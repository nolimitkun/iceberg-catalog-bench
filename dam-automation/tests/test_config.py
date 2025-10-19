from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from dam_automation.config import (
    AutomationConfig,
    DatabricksConfig,
    NamingConfig,
    load_config,
)


def _databricks_payload(**overrides) -> dict:
    payload = {
        "account_id": "1234567890",
        "workspace_url": "https://adb-123.azuredatabricks.net",
        "account_url": "https://accounts.azuredatabricks.net",
        "metastore_id": "metastore",
        "storage_root": "abfss://root@datastore.dfs.core.windows.net/",
        "access_connector_id": "/subscriptions/abc/resourceGroups/rg/providers/Microsoft.Databricks/accessConnectors/example",
        "workspace_client_id": "workspace-client",
        "workspace_client_secret": "workspace-secret",
        "workspace_oauth_scopes": ["all-apis"],
        "account_client_id": "account-client",
        "account_client_secret": "account-secret",
        "account_oauth_scopes": ["all-apis"],
    }
    payload.update(overrides)
    return payload


def _automation_payload() -> dict:
    return {
        "azure": {
            "subscription_id": "sub",
            "tenant_id": "tenant",
            "client_id": "client",
            "client_secret": "secret",
            "resource_group": "rg",
            "storage_account": "datastore",
            "location": "eastus",
            "identity_resource_group": "identity-rg",
        },
        "databricks": _databricks_payload(),
        "identity": {
            "graph_url": "https://graph.microsoft.com",
            "client_id": "id-client",
            "client_secret": "id-secret",
            "tenant_id": "tenant",
            "app_roles": ["Reader"],
        },
        "snowflake": {
            "account": "xy12345",
            "user": "snowflake_user",
            "password": "snowflake_pass",
            "role": "ACCOUNTADMIN",
            "warehouse": "COMPUTE_WH",
            "database": "DAM",
            "schema": "PUBLIC",
        },
        "state": {
            "type": "filesystem",
            "path": "./state",
        },
        "naming": {
            "prefix": "acme",
            "separator": "_",
        },
    }


def test_databricks_config_rejects_placeholder_values() -> None:
    payload = _databricks_payload(access_connector_id="<ACCESS>")

    with pytest.raises(ValueError) as excinfo:
        DatabricksConfig(**payload)

    assert "access_connector_id" in str(excinfo.value)


def test_databricks_config_strips_whitespace() -> None:
    payload = _databricks_payload(
        workspace_client_id="  workspace-id  ",
        workspace_client_secret=" workspace-secret ",
        access_connector_id=" /subscriptions/id ",
    )

    config = DatabricksConfig(**payload)

    assert config.workspace_client_id == "workspace-id"
    assert config.workspace_client_secret == "workspace-secret"
    assert config.access_connector_id == "/subscriptions/id"


def test_automation_config_qualify_name_applies_prefix() -> None:
    raw = _automation_payload()
    config = AutomationConfig.from_dict(raw)

    assert config.qualify_name("dataset") == "acme_dataset"

    config.naming.prefix = None
    assert config.qualify_name("dataset") == "dataset"


def test_naming_config_requires_single_character_separator() -> None:
    with pytest.raises(ValueError):
        NamingConfig(prefix="acme", separator="--")


def test_databricks_config_requires_accounts_domain() -> None:
    payload = _databricks_payload(account_url="https://common.azuredatabricks.net")

    with pytest.raises(ValueError) as excinfo:
        DatabricksConfig(**payload)

    assert "accounts" in str(excinfo.value)


def test_databricks_config_rejects_account_matching_workspace() -> None:
    workspace = "https://adb-123.azuredatabricks.net"
    payload = _databricks_payload(account_url=workspace, workspace_url=workspace)

    with pytest.raises(ValueError):
        DatabricksConfig(**payload)


def test_load_config_missing_file(tmp_path) -> None:
    missing_path = Path(tmp_path) / "missing.yaml"

    with pytest.raises(FileNotFoundError):
        load_config(missing_path)


def test_automation_config_from_yaml(tmp_path) -> None:
    config_path = Path(tmp_path) / "config.yaml"
    config_path.write_text(yaml.safe_dump(_automation_payload()))

    config = AutomationConfig.from_yaml(config_path)

    assert config.azure.subscription_id == "sub"
    assert config.databricks.account_id == "1234567890"
    assert config.naming.prefix == "acme"
