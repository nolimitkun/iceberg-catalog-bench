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
