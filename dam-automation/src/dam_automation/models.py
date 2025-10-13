"""Domain models for datasource automation."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional


@dataclass(slots=True)
class DatasourceRequest:
    """User-facing request payload for provisioning a datasource."""

    name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class DatasourceResources:
    """Materialized resources per datasource."""

    container_url: str
    managed_identity_id: str
    storage_credential_name: str
    external_location_name: str
    catalog_name: str
    group_name: str
    service_principal_app_id: str
    service_principal_client_secret: str
    databricks_oauth_client_secret: str
    snowflake_external_volume_name: str
    snowflake_catalog_integration_name: str
    snowflake_database_name: str
    created_at: datetime = field(default_factory=lambda: datetime.utcnow())


@dataclass(slots=True)
class DatasourceRecord:
    """State persisted for idempotent operations."""

    request: DatasourceRequest
    resources: DatasourceResources
    status: str = "succeeded"
    last_error: Optional[str] = None
    updated_at: datetime = field(default_factory=lambda: datetime.utcnow())

    def mark_failed(self, error: Exception) -> None:
        self.status = "failed"
        self.last_error = str(error)
        self.updated_at = datetime.utcnow()

    def mark_succeeded(self) -> None:
        self.status = "succeeded"
        self.last_error = None
        self.updated_at = datetime.utcnow()


@dataclass(slots=True)
class DeletionOutcome:
    """Result details for a subsystem deletion attempt."""

    succeeded: bool
    message: Optional[str] = None


@dataclass(slots=True)
class DatasourceDeletionResult:
    """Outcome of deleting a datasource across all managed systems."""

    input_name: str
    normalized_name: str
    state_record_name: str
    state_deleted: bool
    state_found: bool
    azure: DeletionOutcome
    identity: DeletionOutcome
    databricks: DeletionOutcome
    snowflake: DeletionOutcome
