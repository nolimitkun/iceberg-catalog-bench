"""Service orchestration for datasource automation."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .auth import ClientCredentialProvider
from .azure import AzureProvisioner
from .config import AutomationConfig
from .databricks import DatabricksProvisioner
from .identity import IdentityProvisioner
from .models import DatasourceRecord, DatasourceRequest, DatasourceResources
from .state import StateStore

logger = logging.getLogger(__name__)


class DatasourceAutomationService:
    """Coordinates resource creation across Azure, Databricks, and identity."""

    def __init__(self, config: AutomationConfig) -> None:
        self._config = config
        self._state = StateStore(config.state.path)
        self._azure = AzureProvisioner(
            config.azure,
            ClientCredentialProvider(
                tenant_id=config.azure.tenant_id,
                client_id=config.azure.client_id,
                client_secret=config.azure.client_secret,
            ),
        )
        self._identity = IdentityProvisioner(
            config.identity,
            ClientCredentialProvider(
                tenant_id=config.identity.tenant_id,
                client_id=config.identity.client_id,
                client_secret=config.identity.client_secret,
            ),
        )
        self._databricks = DatabricksProvisioner(config.databricks)

    def create_datasource(self, request: DatasourceRequest) -> DatasourceRecord:
        normalized_name = self._normalize_name(request.name)
        existing = self._state.get(normalized_name)
        if existing and existing.status == "succeeded":
            logger.info("Datasource '%s' already provisioned", normalized_name)
            return existing

        tags = self._build_tags(request)
        logger.info("Starting provisioning for datasource '%s'", normalized_name)

        try:
            record = self._provision_resources(normalized_name, request, tags)
            record.mark_succeeded()
            self._state.save(record)
            return record
        except Exception as exc:  # noqa: BLE001 - we want to capture all failures
            logger.exception("Provisioning failed for datasource '%s'", normalized_name)
            resources = existing.resources if existing else self._empty_resources()
            record = DatasourceRecord(request=request, resources=resources)
            record.mark_failed(exc)
            self._state.save(record)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        reraise=True,
    )
    def _provision_resources(self, normalized_name: str, request: DatasourceRequest, tags: dict[str, str]) -> DatasourceRecord:
        container = self._azure.ensure_container(normalized_name, tags)
        identity = self._azure.ensure_user_assigned_identity(normalized_name, tags)
        self._azure.ensure_storage_account_role_assignment(identity.principal_id)
        self._azure.attach_identity_to_access_connector(
            self._config.databricks.access_connector_id,
            identity.resource_id,
        )

        app = self._identity.ensure_application(normalized_name)
        service_principal = self._identity.ensure_service_principal(app_object_id=app.object_id)
        group = self._identity.ensure_group(normalized_name, description=request.description)
        self._identity.add_group_member(group.object_id, service_principal.object_id)

        account_sp = self._databricks.ensure_account_service_principal(
            application_id=service_principal.app_id,
            display_name=service_principal.display_name,
        )
        managed_identity_sp = self._databricks.ensure_account_service_principal(
            application_id=identity.client_id,
            display_name=identity.name,
        )
        databricks_group = self._databricks.ensure_group(normalized_name)
        self._databricks.add_service_principal_to_group(databricks_group["id"], account_sp.id)

        storage_credential = self._databricks.ensure_storage_credential(normalized_name, identity.resource_id)
        external_location = self._databricks.ensure_external_location(
            normalized_name,
            container.abfss_url,
            storage_credential.name,
        )
        catalog = self._databricks.ensure_catalog(normalized_name, external_location.url)

        self._databricks.grant_catalog_privileges_all(
            catalog_name=catalog.name,
            principal=normalized_name,
        )

        resources = DatasourceResources(
            container_url=external_location.url,
            managed_identity_id=identity.resource_id,
            storage_credential_name=storage_credential.name,
            external_location_name=external_location.name,
            catalog_name=catalog.name,
            group_name=databricks_group.get("displayName", normalized_name),
            service_principal_app_id=service_principal.app_id,
        )
        return DatasourceRecord(request=request, resources=resources)

    def _normalize_name(self, raw_name: str) -> str:
        candidate = raw_name.lower()
        candidate = re.sub(r"[^a-z0-9-]", "-", candidate)
        candidate = candidate.strip("-")
        candidate = re.sub(r"-+", "-", candidate)
        qualified = self._config.qualify_name(candidate)
        return qualified[:63]

    def _build_tags(self, request: DatasourceRequest) -> dict[str, str]:
        tags = {"datasource": request.name}
        if request.owner:
            tags["owner"] = request.owner
        tags.update(request.labels)
        return tags

    def _empty_resources(self) -> DatasourceResources:
        return DatasourceResources(
            container_url="",
            managed_identity_id="",
            storage_credential_name="",
            external_location_name="",
            catalog_name="",
            group_name="",
            service_principal_app_id="",
        )


@dataclass(slots=True)
class AutomationContext:
    """Convenience wrapper bundling config and service."""

    config: AutomationConfig
    service: DatasourceAutomationService


def build_service(config: AutomationConfig) -> DatasourceAutomationService:
    return DatasourceAutomationService(config)
