"""Service orchestration for datasource automation."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Callable

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .auth import ClientCredentialProvider
from .azure import AzureProvisioner, AzureIdentity
from .config import AutomationConfig
from .databricks import DatabricksProvisioner
from .identity import IdentityProvisioner, ServicePrincipal
from .snowflake import (
    SnowflakeAuthorizationError,
    SnowflakeIntegrationInUseError,
    SnowflakeProvisioner,
    SnowflakeDropSummary,
)
from .models import DeletionOutcome, DatasourceDeletionResult, DatasourceRecord, DatasourceRequest, DatasourceResources
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
        self._snowflake = SnowflakeProvisioner(config.snowflake)

    def create_datasource(self, request: DatasourceRequest) -> DatasourceRecord:
        normalized_name = self._normalize_name(request.name)
        existing = self._state.get(normalized_name)
        if existing and existing.status == "succeeded":
            logger.info("Datasource '%s' already provisioned", normalized_name)
            return existing

        tags = self._build_tags(request)
        logger.info("Starting provisioning for datasource '%s'", normalized_name)

        try:
            record = self._provision_resources(normalized_name, request, tags, existing)
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
    def _provision_resources(
        self,
        normalized_name: str,
        request: DatasourceRequest,
        tags: dict[str, str],
        existing: DatasourceRecord | None,
    ) -> DatasourceRecord:
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
        self._azure.ensure_storage_account_role_assignment(service_principal.object_id)

        cached_azure_secret = ""
        if existing and existing.resources.service_principal_client_secret:
            cached_azure_secret = existing.resources.service_principal_client_secret

        def _create_azure_secret() -> str:
            logger.info("Issuing new Azure AD client secret for application '%s'", app.object_id)
            secret = self._identity.create_application_secret(
                app_object_id=app.object_id,
                display_name=f"{normalized_name}-snowflake",
            )
            return secret.secret_text

        if cached_azure_secret:
            azure_client_secret_value = cached_azure_secret
        else:
            azure_client_secret_value = _create_azure_secret()

        account_sp = self._databricks.ensure_account_service_principal(
            application_id=service_principal.app_id,
            display_name=service_principal.display_name,
        )
        self._databricks.ensure_workspace_service_principal(
            application_id=service_principal.app_id,
            display_name=service_principal.display_name,
        )
        managed_identity_sp = self._databricks.ensure_account_service_principal(
            application_id=identity.client_id,
            display_name=identity.name,
        )
        databricks_groups = self._databricks.ensure_group(normalized_name)
        rw_group = databricks_groups["rw"]
        self._databricks.add_service_principal_to_group(rw_group["id"], account_sp.id)

        cached_databricks_secret = ""
        if existing and existing.resources.databricks_oauth_client_secret:
            cached_databricks_secret = existing.resources.databricks_oauth_client_secret

        secret_name = f"{normalized_name}-snowflake"

        def _create_databricks_secret() -> str:
            secret = self._databricks.create_service_principal_secret(
                service_principal_id=account_sp.id,
                secret_name=secret_name,
            )
            return secret.secret_value

        if cached_databricks_secret:
            databricks_client_secret_value = cached_databricks_secret
        else:
            databricks_client_secret_value = _create_databricks_secret()

        storage_credential = self._databricks.ensure_storage_credential(normalized_name, identity.resource_id)
        external_location = self._databricks.ensure_external_location(
            normalized_name,
            container.abfss_url,
            storage_credential.name,
        )
        catalog = self._databricks.ensure_catalog(normalized_name, external_location.url)

        self._databricks.grant_catalog_privileges_all(
            catalog_name=catalog.name,
            principal=rw_group.get("displayName", f"{normalized_name}-rw"),
        )

        storage_base_url = self._to_azure_storage_base_url(container.blob_url)
        tenant_id = self._config.azure.tenant_id
        external_volume = self._snowflake.ensure_external_volume(
            name=normalized_name,
            storage_base_url=storage_base_url,
            tenant_id=tenant_id,
        )

        token_endpoint = f"{self._config.databricks.workspace_url.rstrip('/')}/oidc/v1/token"
        catalog_uri = f"{self._config.databricks.workspace_url.rstrip('/')}/api/2.1/unity-catalog/iceberg-rest"
        linked_db_name = f"{normalized_name}_linked_db"
        schema_name = self._config.snowflake.default_schema or f"{normalized_name}_schema"
        table_name = "test_table"

        try:
            catalog_integration = self._snowflake.ensure_catalog_integration(
                name=normalized_name,
                catalog_name=catalog.name,
                catalog_uri=catalog_uri,
                client_id=service_principal.app_id,
                client_secret=databricks_client_secret_value,
                token_endpoint=token_endpoint,
                scopes=self._config.snowflake.oauth_allowed_scopes,
                catalog_source=self._config.snowflake.catalog_source,
                table_format=self._config.snowflake.table_format,
            )
        except SnowflakeIntegrationInUseError:
            logger.warning(
                "Catalog integration '%s' is in use; dropping existing catalog-linked artifacts before retrying",
                normalized_name.upper(),
            )
            self._snowflake.cleanup_catalog_linked_artifacts(
                database_name=linked_db_name,
                schema_name=schema_name,
                table_name=table_name,
            )
            catalog_integration = self._snowflake.ensure_catalog_integration(
                name=normalized_name,
                catalog_name=catalog.name,
                catalog_uri=catalog_uri,
                client_id=service_principal.app_id,
                client_secret=databricks_client_secret_value,
                token_endpoint=token_endpoint,
                scopes=self._config.snowflake.oauth_allowed_scopes,
                catalog_source=self._config.snowflake.catalog_source,
                table_format=self._config.snowflake.table_format,
            )
        try:
            catalog_linked_db = self._snowflake.ensure_catalog_linked_database(
                database_name=linked_db_name,
                integration_name=catalog_integration.name,
                external_volume_name=external_volume.name,
                namespace_mode=self._config.snowflake.namespace_mode.upper(),
                namespace_delimiter=self._config.snowflake.namespace_flatten_delimiter,
            )
        except SnowflakeAuthorizationError as exc:
            logger.error(
                "Snowflake catalog integration authentication failed for '%s': %s",
                normalized_name,
                exc,
            )
            raise

        try:
            self._snowflake.prime_catalog_linked_database(
                database_name=catalog_linked_db.name,
                schema_name=schema_name,
                table_name=table_name,
            )
        except Exception as exc:  # noqa: BLE001 - best effort initialization
            logger.warning(
                "Failed to initialize Snowflake catalog-linked database '%s' with starter artifacts: %s",
                catalog_linked_db.name,
                exc,
            )

        resources = DatasourceResources(
            container_url=external_location.url,
            managed_identity_id=identity.resource_id,
            storage_credential_name=storage_credential.name,
            external_location_name=external_location.name,
            catalog_name=catalog.name,
            group_name=rw_group.get("displayName", f"{normalized_name}-rw"),
            service_principal_app_id=service_principal.app_id,
            service_principal_client_secret=azure_client_secret_value,
            databricks_oauth_client_secret=databricks_client_secret_value,
            snowflake_external_volume_name=external_volume.name,
            snowflake_catalog_integration_name=catalog_integration.name,
            snowflake_database_name=catalog_linked_db.name,
        )
        return DatasourceRecord(request=request, resources=resources)

    def drop_datasource(self, name: str) -> SnowflakeDropSummary:
        normalized_name = self._normalize_name(name)
        record = self._state.get(normalized_name)

        external_volume_name = normalized_name
        catalog_integration_name = normalized_name
        database_name = f"{normalized_name}_linked_db"

        if record:
            if record.resources.snowflake_external_volume_name:
                external_volume_name = record.resources.snowflake_external_volume_name
            if record.resources.snowflake_catalog_integration_name:
                catalog_integration_name = record.resources.snowflake_catalog_integration_name
            if record.resources.snowflake_database_name:
                database_name = record.resources.snowflake_database_name

        logger.info("Dropping Snowflake objects for datasource '%s'", normalized_name)
        summary = self._snowflake.drop_objects(
            database_name=database_name,
            catalog_integration_name=catalog_integration_name,
            external_volume_name=external_volume_name,
        )
        return summary

    def delete_datasource(self, name: str) -> DatasourceDeletionResult:
        normalized_name = self._normalize_name(name)
        logger.info("Deleting datasource '%s' and all managed resources", normalized_name)

        record, state_key, state_exists = self._find_state_record(name, normalized_name)
        resources = record.resources

        container_name = self._extract_container_name(resources.container_url) or normalized_name
        identity_name = self._extract_identity_name(resources.managed_identity_id, normalized_name)
        storage_credential_name = resources.storage_credential_name or normalized_name
        external_location_name = resources.external_location_name or normalized_name
        catalog_name = resources.catalog_name or normalized_name
        databricks_rw_group_name = resources.group_name or f"{normalized_name}-rw"
        databricks_ro_group_name = self._derive_ro_group_name(databricks_rw_group_name, normalized_name)

        identity_info: AzureIdentity | None = None
        identity_lookup_error: str | None = None
        if identity_name:
            try:
                identity_info = self._azure.get_user_assigned_identity(identity_name)
            except Exception as exc:  # noqa: BLE001 - defensive logging
                identity_lookup_error = str(exc)
                logger.warning(
                    "Failed to fetch Azure managed identity '%s' prior to deletion: %s",
                    identity_name,
                    exc,
                )

        service_principal_info: ServicePrincipal | None = None
        service_principal_lookup_error: str | None = None
        if resources.service_principal_app_id:
            try:
                service_principal_info = self._identity.get_service_principal(resources.service_principal_app_id)
            except Exception as exc:  # noqa: BLE001 - defensive logging
                service_principal_lookup_error = str(exc)
                logger.warning(
                    "Failed to locate Azure AD service principal with appId '%s': %s",
                    resources.service_principal_app_id,
                    exc,
                )

        snowflake_outcome = self._delete_snowflake_resources(normalized_name, resources)
        databricks_outcome = self._delete_databricks_resources(
            normalized_name,
            storage_credential_name,
            external_location_name,
            catalog_name,
            databricks_rw_group_name,
            databricks_ro_group_name,
            resources.service_principal_app_id,
            identity_info,
        )
        identity_outcome = self._delete_identity_resources(
            normalized_name,
            resources.service_principal_app_id,
        )
        azure_outcome = self._delete_azure_resources(
            normalized_name,
            container_name,
            identity_name,
            resources.managed_identity_id,
            identity_info,
            service_principal_info,
            identity_lookup_error,
            service_principal_lookup_error,
        )

        outcomes = [snowflake_outcome, databricks_outcome, identity_outcome, azure_outcome]
        state_deleted = False
        if all(outcome.succeeded for outcome in outcomes):
            if state_exists:
                state_deleted = self._state.delete(state_key)
                if not state_deleted:
                    logger.warning(
                        "All resource deletions succeeded but failed to remove state file '%s'",
                        state_key,
                    )
            else:
                state_deleted = True
        else:
            logger.info(
                "Skipping state deletion for datasource '%s' because some resource deletions failed",
                normalized_name,
            )

        return DatasourceDeletionResult(
            input_name=name,
            normalized_name=normalized_name,
            state_record_name=record.request.name,
            state_deleted=state_deleted,
            state_found=state_exists,
            azure=azure_outcome,
            identity=identity_outcome,
            databricks=databricks_outcome,
            snowflake=snowflake_outcome,
        )

    def _find_state_record(self, original_name: str, normalized_name: str) -> tuple[DatasourceRecord, str, bool]:
        state_key = normalized_name
        record = self._state.get(state_key)

        if record is None:
            logger.debug("Datasource '%s' not found under normalized key; checking original name", normalized_name)
            state_key = original_name
            record = self._state.get(state_key)

        if record is None:
            logger.debug(
                "Datasource '%s' not found using normalized or original name; searching recorded entries",
                normalized_name,
            )
            for candidate in self._state.list_records():
                candidate_normalized = self._normalize_name(candidate.request.name)
                if candidate_normalized == normalized_name:
                    return candidate, candidate.request.name, True

        if record is None:
            logger.info(
                "Datasource '%s' not found in state storage; proceeding with inferred resource names",
                normalized_name,
            )
            inferred_record = self._build_inferred_record(normalized_name)
            return inferred_record, normalized_name, False

        return record, state_key, True

    def _delete_snowflake_resources(self, normalized_name: str, resources: DatasourceResources) -> DeletionOutcome:
        external_volume_name = resources.snowflake_external_volume_name or normalized_name
        catalog_integration_name = resources.snowflake_catalog_integration_name or normalized_name
        database_name = resources.snowflake_database_name or f"{normalized_name}_linked_db"

        try:
            summary = self._snowflake.drop_objects(
                database_name=database_name,
                catalog_integration_name=catalog_integration_name,
                external_volume_name=external_volume_name,
            )
            message = (
                f"database_dropped={summary.database_dropped}, "
                f"catalog_integration_dropped={summary.catalog_integration_dropped}, "
                f"external_volume_dropped={summary.external_volume_dropped}"
            )
            return DeletionOutcome(True, message)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to delete Snowflake resources for '%s': %s", normalized_name, exc)
            return DeletionOutcome(False, str(exc))

    def _delete_databricks_resources(
        self,
        normalized_name: str,
        storage_credential_name: str,
        external_location_name: str,
        catalog_name: str,
        rw_group_name: str,
        ro_group_name: str,
        service_principal_app_id: str,
        identity_info: AzureIdentity | None,
    ) -> DeletionOutcome:
        errors: list[str] = []
        notes: list[str] = []

        def attempt(label: str, func: Callable[[], Any]) -> None:
            try:
                result = func()
                if isinstance(result, bool):
                    notes.append(f"{label}={result}")
                elif result is not None:
                    notes.append(f"{label}={result}")
            except Exception as exc:  # noqa: BLE001
                logger.exception("Databricks cleanup step '%s' failed for '%s': %s", label, normalized_name, exc)
                errors.append(f"{label}: {exc}")

        rw_group = self._databricks.get_account_group(rw_group_name)
        ro_group = self._databricks.get_account_group(ro_group_name)

        account_sp = None
        if service_principal_app_id:
            account_sp = self._databricks.get_account_service_principal(service_principal_app_id)

        workspace_sp = None
        if service_principal_app_id:
            workspace_sp = self._databricks.get_workspace_service_principal(service_principal_app_id)

        identity_account_sp = None
        identity_workspace_sp = None
        if identity_info:
            identity_account_sp = self._databricks.get_account_service_principal(identity_info.client_id)
            identity_workspace_sp = self._databricks.get_workspace_service_principal(identity_info.client_id)

        steps: list[tuple[str, Callable[[], Any]]] = []

        schema_entries: list[dict[str, Any]] = []
        try:
            schema_entries = self._databricks.list_schemas(catalog_name)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to list schemas for catalog '%s': %s", catalog_name, exc)
            errors.append(f"list_schemas: {exc}")
            schema_entries = []

        for schema in schema_entries:
            schema_name = schema.get("name")
            if not schema_name:
                continue
            if schema_name.upper() == "INFORMATION_SCHEMA":
                notes.append("skip_schema=INFORMATION_SCHEMA")
                continue
            full_schema_name = schema.get("full_name") or f"{catalog_name}.{schema_name}"
            try:
                tables = self._databricks.list_tables(catalog_name, schema_name)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to list tables for schema '%s': %s", full_schema_name, exc)
                errors.append(f"list_tables({full_schema_name}): {exc}")
                tables = []
            for table in tables:
                table_full_name = table.get("full_name")
                if not table_full_name:
                    table_name = table.get("name")
                    if not table_name:
                        continue
                    table_full_name = f"{full_schema_name}.{table_name}"
                steps.append(
                    (
                        f"delete_table[{table_full_name}]",
                        lambda tbl=table_full_name: self._databricks.delete_table(tbl),
                    )
                )
            steps.append(
                (
                    f"delete_schema[{full_schema_name}]",
                    lambda schema_full=full_schema_name: self._databricks.delete_schema(schema_full),
                )
            )

        steps.append(("delete_catalog", lambda: self._databricks.delete_catalog(catalog_name)))
        steps.append(
            ("delete_external_location", lambda: self._databricks.delete_external_location(external_location_name))
        )
        steps.append(
            ("delete_storage_credential", lambda: self._databricks.delete_storage_credential(storage_credential_name))
        )

        if rw_group and account_sp:
            steps.append(
                (
                    "remove_rw_membership_sp",
                    lambda group_id=rw_group["id"], sp_id=account_sp.id: self._databricks.remove_service_principal_from_group(
                        group_id, sp_id
                    ),
                )
            )
        if ro_group and account_sp:
            steps.append(
                (
                    "remove_ro_membership_sp",
                    lambda group_id=ro_group["id"], sp_id=account_sp.id: self._databricks.remove_service_principal_from_group(
                        group_id, sp_id
                    ),
                )
            )
        if identity_account_sp and rw_group:
            steps.append(
                (
                    "remove_rw_membership_identity",
                    lambda group_id=rw_group["id"], sp_id=identity_account_sp.id: self._databricks.remove_service_principal_from_group(
                        group_id, sp_id
                    ),
                )
            )
        if identity_account_sp and ro_group:
            steps.append(
                (
                    "remove_ro_membership_identity",
                    lambda group_id=ro_group["id"], sp_id=identity_account_sp.id: self._databricks.remove_service_principal_from_group(
                        group_id, sp_id
                    ),
                )
            )

        if rw_group:
            steps.append(("delete_group_rw", lambda: self._databricks.delete_account_group(rw_group_name)))
        if ro_group:
            steps.append(("delete_group_ro", lambda: self._databricks.delete_account_group(ro_group_name)))

        if workspace_sp:
            steps.append(
                (
                    "delete_workspace_sp",
                    lambda app_id=service_principal_app_id: self._databricks.delete_workspace_service_principal(app_id),
                )
            )
        if account_sp:
            steps.append(
                (
                    "delete_account_sp",
                    lambda app_id=service_principal_app_id: self._databricks.delete_account_service_principal(app_id),
                )
            )

        if identity_workspace_sp and identity_info:
            steps.append(
                (
                    "delete_identity_workspace_sp",
                    lambda app_id=identity_info.client_id: self._databricks.delete_workspace_service_principal(app_id),
                )
            )
        if identity_account_sp and identity_info:
            steps.append(
                (
                    "delete_identity_account_sp",
                    lambda app_id=identity_info.client_id: self._databricks.delete_account_service_principal(app_id),
                )
            )

        for label, func in steps:
            attempt(label, func)

        if errors:
            message_parts = ["; ".join(errors)]
            if notes:
                message_parts.append("notes=" + ", ".join(notes))
            return DeletionOutcome(False, "; ".join(message_parts))

        message = ", ".join(notes) if notes else None
        return DeletionOutcome(True, message)

    def _delete_azure_resources(
        self,
        normalized_name: str,
        container_name: str,
        identity_name: str,
        identity_resource_id: str,
        identity_info: AzureIdentity | None,
        service_principal_info: ServicePrincipal | None,
        identity_lookup_error: str | None,
        service_principal_lookup_error: str | None,
    ) -> DeletionOutcome:
        errors: list[str] = []
        notes: list[str] = []

        if identity_lookup_error:
            errors.append(f"identity_lookup: {identity_lookup_error}")
        if service_principal_lookup_error:
            errors.append(f"service_principal_lookup: {service_principal_lookup_error}")

        if identity_info and identity_info.principal_id:
            try:
                removed = self._azure.remove_storage_account_role_assignments(identity_info.principal_id)
                notes.append(f"identity_role_assignments_removed={removed}")
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Failed to remove storage role assignments for identity '%s': %s",
                    identity_name,
                    exc,
                )
                errors.append(f"identity_role_assignment: {exc}")

        if service_principal_info:
            try:
                removed = self._azure.remove_storage_account_role_assignments(service_principal_info.object_id)
                notes.append(f"sp_role_assignments_removed={removed}")
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Failed to remove storage role assignments for service principal '%s': %s",
                    service_principal_info.display_name,
                    exc,
                )
                errors.append(f"service_principal_role_assignment: {exc}")

        if identity_resource_id:
            try:
                detached = self._azure.detach_identity_from_access_connector(
                    self._config.databricks.access_connector_id,
                    identity_resource_id,
                )
                notes.append(f"identity_detached_from_connector={detached}")
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Failed to detach identity '%s' from access connector: %s",
                    identity_resource_id,
                    exc,
                )
                errors.append(f"detach_identity: {exc}")

        if identity_name:
            try:
                deleted_identity = self._azure.delete_user_assigned_identity(identity_name)
                notes.append(f"identity_deleted={deleted_identity}")
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to delete user-assigned identity '%s': %s", identity_name, exc)
                errors.append(f"delete_identity: {exc}")

        try:
            deleted_container = self._azure.delete_container(container_name)
            notes.append(f"container_deleted={deleted_container}")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to delete ADLS container '%s': %s", container_name, exc)
            errors.append(f"delete_container: {exc}")

        if errors:
            message_parts = ["; ".join(errors)]
            if notes:
                message_parts.append("notes=" + ", ".join(notes))
            return DeletionOutcome(False, "; ".join(message_parts))

        message = ", ".join(notes) if notes else None
        return DeletionOutcome(True, message)

    def _build_inferred_record(self, normalized_name: str) -> DatasourceRecord:
        resources = self._inferred_resources(normalized_name)
        request = DatasourceRequest(name=normalized_name)
        return DatasourceRecord(request=request, resources=resources)

    def _inferred_resources(self, normalized_name: str) -> DatasourceResources:
        azure_config = self._config.azure
        container_url = (
            f"abfss://{normalized_name}@{azure_config.storage_account}."
            f"{azure_config.data_plane_dns_suffix}/"
        )
        identity_resource_id = (
            f"/subscriptions/{azure_config.subscription_id}"
            f"/resourceGroups/{azure_config.identity_resource_group}"
            f"/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{normalized_name}"
        )

        service_principal_app_id = ""
        try:
            app_id = self._identity.resolve_application_app_id(normalized_name)
            if app_id:
                service_principal_app_id = app_id
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to resolve application appId for '%s'; continuing with defaults: %s",
                normalized_name,
                exc,
            )

        return DatasourceResources(
            container_url=container_url,
            managed_identity_id=identity_resource_id,
            storage_credential_name=normalized_name,
            external_location_name=normalized_name,
            catalog_name=normalized_name,
            group_name=f"{normalized_name}-rw",
            service_principal_app_id=service_principal_app_id,
            service_principal_client_secret="",
            databricks_oauth_client_secret="",
            snowflake_external_volume_name=normalized_name,
            snowflake_catalog_integration_name=normalized_name,
            snowflake_database_name=f"{normalized_name}_linked_db",
        )

    def _delete_identity_resources(
        self,
        normalized_name: str,
        service_principal_app_id: str,
    ) -> DeletionOutcome:
        errors: list[str] = []
        notes: list[str] = []

        try:
            deleted_group = self._identity.delete_group(normalized_name)
            notes.append(f"group_deleted={deleted_group}")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to delete Azure AD group '%s': %s", normalized_name, exc)
            errors.append(f"delete_group: {exc}")

        resolved_app_id = service_principal_app_id
        if not resolved_app_id:
            try:
                resolved_app_id = self._identity.resolve_application_app_id(normalized_name) or ""
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to resolve application appId for '%s' during identity cleanup: %s",
                    normalized_name,
                    exc,
                )

        if resolved_app_id:
            try:
                deleted_sp = self._identity.delete_service_principal(resolved_app_id)
                notes.append(f"service_principal_deleted={deleted_sp}")
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Failed to delete service principal with appId '%s': %s",
                    resolved_app_id,
                    exc,
                )
                errors.append(f"delete_service_principal: {exc}")

        try:
            deleted_app = self._identity.delete_application(normalized_name)
            notes.append(f"application_deleted={deleted_app}")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to delete application '%s': %s", normalized_name, exc)
            errors.append(f"delete_application: {exc}")

        if errors:
            message_parts = ["; ".join(errors)]
            if notes:
                message_parts.append("notes=" + ", ".join(notes))
            return DeletionOutcome(False, "; ".join(message_parts))

        message = ", ".join(notes) if notes else None
        return DeletionOutcome(True, message)

    def _extract_container_name(self, container_url: str) -> str:
        if not container_url:
            return ""
        if "://" in container_url:
            _, remainder = container_url.split("://", 1)
        else:
            remainder = container_url
        if "@" in remainder:
            remainder = remainder.split("@", 1)[0]
        return remainder.split("/", 1)[0]

    def _extract_identity_name(self, resource_id: str, fallback: str) -> str:
        if resource_id:
            return resource_id.rstrip("/").split("/")[-1]
        return fallback

    def _derive_ro_group_name(self, rw_group_name: str, fallback_base: str) -> str:
        if rw_group_name.endswith("-rw"):
            base = rw_group_name[:-3]
        elif rw_group_name.endswith("_rw"):
            base = rw_group_name[:-3]
        else:
            base = fallback_base
        return f"{base}-ro"

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

    def _to_azure_storage_base_url(self, blob_url: str) -> str:
        base = blob_url.replace("https://", "azure://", 1).rstrip("/")
        return f"{base}/"

    def _empty_resources(self) -> DatasourceResources:
        return DatasourceResources(
            container_url="",
            managed_identity_id="",
            storage_credential_name="",
            external_location_name="",
            catalog_name="",
            group_name="",
            service_principal_app_id="",
            service_principal_client_secret="",
            databricks_oauth_client_secret="",
            snowflake_external_volume_name="",
            snowflake_catalog_integration_name="",
            snowflake_database_name="",
        )


@dataclass(slots=True)
class AutomationContext:
    """Convenience wrapper bundling config and service."""

    config: AutomationConfig
    service: DatasourceAutomationService


def build_service(config: AutomationConfig) -> DatasourceAutomationService:
    return DatasourceAutomationService(config)
