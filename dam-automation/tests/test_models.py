from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dam_automation.models import (
    DatasourceRecord,
    DatasourceRequest,
    DatasourceResources,
)


def _request(name: str = "example") -> DatasourceRequest:
    return DatasourceRequest(
        name=name,
        description="Example",
        owner="owner@example.com",
        labels={"env": "test"},
    )


def _resources() -> DatasourceResources:
    return DatasourceResources(
        container_url="abfss://example@datastore.dfs.core.windows.net/",
        managed_identity_id="/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/example",
        storage_credential_name="cred",
        external_location_name="location",
        catalog_name="catalog",
        group_name="group",
        service_principal_app_id="app-id",
        service_principal_client_secret="secret",
        databricks_oauth_client_secret="db-secret",
        snowflake_external_volume_name="volume",
        snowflake_catalog_integration_name="integration",
        snowflake_database_name="database",
    )


def test_datasource_resources_default_created_at_is_recent() -> None:
    resources = _resources()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    delta = now - resources.created_at
    assert delta < timedelta(seconds=5)


def test_mark_failed_updates_status_and_error() -> None:
    record = DatasourceRecord(request=_request(), resources=_resources())
    original_updated_at = record.updated_at

    record.mark_failed(RuntimeError("failure"))

    assert record.status == "failed"
    assert record.last_error == "failure"
    assert record.updated_at > original_updated_at


def test_mark_succeeded_clears_error() -> None:
    record = DatasourceRecord(request=_request(), resources=_resources())
    record.mark_failed(RuntimeError("boom"))
    failed_updated_at = record.updated_at

    record.mark_succeeded()

    assert record.status == "succeeded"
    assert record.last_error is None
    assert record.updated_at > failed_updated_at
