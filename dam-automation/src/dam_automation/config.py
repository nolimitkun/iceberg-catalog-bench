"""Configuration loading utilities for the DAM automation service."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, Field, validator
from pydantic import model_validator
from pydantic import ConfigDict


class AzureConfig(BaseModel):
    subscription_id: str
    tenant_id: str
    client_id: str
    client_secret: str
    resource_group: str
    storage_account: str
    location: str
    identity_resource_group: str
    data_plane_dns_suffix: str = Field(
        "dfs.core.windows.net",
        description="DNS suffix for ADLS Gen2 endpoints",
    )


class DatabricksConfig(BaseModel):
    account_id: str
    workspace_url: str
    account_url: str
    metastore_id: str
    storage_root: str
    access_connector_id: str = Field(
        description="Resource ID of the Azure access connector to associate with Unity Catalog storage credentials",
    )
    workspace_client_id: Optional[str] = Field(
        default=None,
        description="Databricks service principal client ID for workspace OAuth",
    )
    workspace_client_secret: Optional[str] = Field(
        default=None,
        description="Databricks service principal secret for workspace OAuth",
    )
    workspace_oauth_scopes: list[str] = Field(
        default_factory=lambda: ["all-apis"],
        description="Scopes for workspace OAuth tokens",
    )
    account_client_id: Optional[str] = Field(
        default=None,
        description="Databricks service principal client ID for account-level OAuth",
    )
    account_client_secret: Optional[str] = Field(
        default=None,
        description="Databricks service principal secret for account-level OAuth",
    )
    account_oauth_scopes: list[str] = Field(
        default_factory=lambda: ["all-apis"],
        description="Scopes to request when fetching Databricks OAuth tokens",
    )

    @validator("account_url")
    def validate_account_url(cls, value: str, values: Dict[str, Any]) -> str:
        workspace_url = values.get("workspace_url")
        if workspace_url and workspace_url.rstrip("/") == value.rstrip("/"):
            raise ValueError("databricks.account_url must be the Databricks accounts domain, not the workspace URL")
        parsed = urlparse(value)
        if "accounts" not in parsed.netloc:
            raise ValueError(
                "databricks.account_url should point to the Databricks accounts endpoint (hostname contains 'accounts')"
            )
        return value

    @model_validator(mode="before")
    def _strip_placeholders(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        placeholders = {
            "workspace_client_id",
            "workspace_client_secret",
            "account_client_id",
            "account_client_secret",
            "access_connector_id",
        }
        for key in placeholders:
            value = values.get(key)
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped or (stripped.startswith("<") and stripped.endswith(">")):
                    values[key] = None
                else:
                    values[key] = stripped
        if not values.get("access_connector_id"):
            raise ValueError("databricks.access_connector_id is required for Unity Catalog storage credentials")
        return values

    @model_validator(mode="after")
    def _ensure_account_auth(cls, values: "DatabricksConfig") -> "DatabricksConfig":
        if not (values.account_client_id and values.account_client_secret):
            raise ValueError(
                "Databricks account API requires account_client_id and account_client_secret."
            )
        if not (values.workspace_client_id and values.workspace_client_secret):
            raise ValueError(
                "Databricks workspace API requires workspace_client_id and workspace_client_secret."
            )
        return values

    @property
    def api_headers(self) -> Dict[str, str]:
        raise NotImplementedError("Workspace OAuth tokens are generated dynamically; api_headers unused")


class IdentityConfig(BaseModel):
    graph_url: str = "https://graph.microsoft.com"
    client_id: str
    client_secret: str
    tenant_id: str
    app_roles: list[str] = Field(default_factory=list)


class SnowflakeConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    account: str = Field(
        description="Snowflake account identifier (e.g. xy12345 or xy12345.us-east-1)"
    )
    user: str = Field(description="Snowflake user with privileges to manage external volumes and catalog integrations")
    password: str = Field(description="Password for the Snowflake user")
    role: str = Field(description="Default role to use when connecting")
    warehouse: Optional[str] = Field(
        default=None,
        description="Warehouse to resume for executing DDL (optional)",
    )
    database: Optional[str] = Field(
        default=None,
        description="Default database context (optional)",
    )
    default_schema: Optional[str] = Field(
        default=None,
        description="Default schema context (optional)",
        alias="schema",
    )
    oauth_allowed_scopes: list[str] = Field(
        default_factory=lambda: ["PRINCIPAL_ROLE:snowflake"],
        description="Scopes requested when Snowflake fetches OAuth tokens for the catalog integration",
    )
    namespace_mode: str = Field(
        default="FLATTEN_NESTED_NAMESPACE",
        description="Namespace handling strategy for catalog linked databases",
    )
    namespace_flatten_delimiter: str = Field(
        default="-",
        description="Delimiter used when flattening nested namespaces",
    )
    access_delegation_mode: str = Field(
        default="EXTERNAL_VOLUME_CREDENTIALS",
        description="Delegation mode for catalog integrations",
    )
    catalog_source: str = Field(
        default="ICEBERG_REST",
        description="Snowflake catalog source value for integrations",
    )
    table_format: str = Field(
        default="ICEBERG",
        description="Table format advertised to Snowflake",
    )


class StateConfig(BaseModel):
    type: str = Field("filesystem", description="Type of state backend")
    path: str = Field("./state", description="Filesystem path for state persistence")


class NamingConfig(BaseModel):
    prefix: Optional[str] = Field(default=None, description="Optional global prefix")
    separator: str = Field("-", description="Delimiter between naming segments")

    @validator("separator")
    def validate_separator(cls, value: str) -> str:  # noqa: D417 - pydantic validator signature
        if len(value) > 1:
            raise ValueError("Separator must be a single character or empty string")
        return value


class AutomationConfig(BaseModel):
    azure: AzureConfig
    databricks: DatabricksConfig
    identity: IdentityConfig
    state: StateConfig
    snowflake: SnowflakeConfig
    naming: NamingConfig = Field(default_factory=NamingConfig)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "AutomationConfig":
        return cls.model_validate(raw)

    @classmethod
    def from_yaml(cls, path: Path) -> "AutomationConfig":
        data = yaml.safe_load(path.read_text())
        return cls.from_dict(data)

    def qualify_name(self, base: str) -> str:
        """Derive a resource name using the global prefix (if any)."""
        segments = [base]
        if self.naming.prefix:
            segments.insert(0, self.naming.prefix)
        return self.naming.separator.join(segments)


def load_config(path: str | Path) -> AutomationConfig:
    """Load an AutomationConfig from a YAML file."""
    config_path = Path(path).expanduser().resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    return AutomationConfig.from_yaml(config_path)
