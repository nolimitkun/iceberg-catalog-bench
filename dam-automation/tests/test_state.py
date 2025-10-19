from __future__ import annotations

from datetime import datetime

from dam_automation.models import DatasourceRecord, DatasourceRequest, DatasourceResources
from dam_automation.state import StateStore


def _resources(created_at: datetime | None = None) -> DatasourceResources:
    created = created_at or datetime(2024, 1, 1)
    return DatasourceResources(
        container_url="abfss://example@datastore.dfs.core.windows.net/",
        managed_identity_id="/subscriptions/0000/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/example",
        storage_credential_name="example-cred",
        external_location_name="example-location",
        catalog_name="example-catalog",
        group_name="example-group",
        service_principal_app_id="0000-1111",
        service_principal_client_secret="azure-secret",
        databricks_oauth_client_secret="db-secret",
        snowflake_external_volume_name="example-volume",
        snowflake_catalog_integration_name="example-integration",
        snowflake_database_name="example_db",
        created_at=created.replace(microsecond=0),
    )


def _record(name: str = "example") -> DatasourceRecord:
    request = DatasourceRequest(
        name=name,
        description="Example datasource",
        owner="owner@example.com",
        labels={"env": "test"},
    )
    resources = _resources()
    record = DatasourceRecord(request=request, resources=resources)
    record.updated_at = datetime(2024, 1, 2)
    return record


def test_state_round_trip(tmp_path) -> None:
    store = StateStore(tmp_path)
    record = _record()

    store.save(record)
    loaded = store.get("example")

    assert loaded is not None
    assert loaded.request == record.request
    assert loaded.resources == record.resources
    assert loaded.status == record.status
    assert loaded.last_error == record.last_error
    assert loaded.updated_at == record.updated_at


def test_state_uses_sanitized_filenames(tmp_path) -> None:
    store = StateStore(tmp_path)
    record = _record("foo/bar")

    store.save(record)

    sanitized_path = tmp_path / "foo_bar.json"
    assert sanitized_path.exists()
    assert store.exists("foo/bar")
    loaded = store.get("foo/bar")
    assert loaded is not None
    assert loaded.request.name == "foo/bar"

    assert store.delete("foo/bar") is True
    assert store.delete("foo/bar") is False
    assert not sanitized_path.exists()


def test_list_records_returns_all_entries(tmp_path) -> None:
    store = StateStore(tmp_path)
    record_one = _record("alpha")
    record_two = _record("beta")

    store.save(record_one)
    store.save(record_two)

    records = store.list_records()
    names = {item.request.name for item in records}
    assert names == {"alpha", "beta"}
