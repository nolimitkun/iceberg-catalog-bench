from __future__ import annotations

from datetime import datetime

from dam_automation.models import DatasourceRecord, DatasourceRequest, DatasourceResources
from dam_automation.state import (
    _deserialize_datetime,
    _record_to_json,
    _serialize_datetime,
    _json_to_record,
)


def _sample_record() -> DatasourceRecord:
    request = DatasourceRequest(name="sample")
    resources = DatasourceResources(
        container_url="abfss://sample@acct.dfs.core.windows.net/",
        managed_identity_id="/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/sample",
        storage_credential_name="sample",
        external_location_name="sample",
        catalog_name="sample",
        group_name="sample-rw",
        service_principal_app_id="app",
        service_principal_client_secret="secret",
        databricks_oauth_client_secret="db-secret",
        snowflake_external_volume_name="sample",
        snowflake_catalog_integration_name="sample",
        snowflake_database_name="sample_db",
    )
    record = DatasourceRecord(request=request, resources=resources)
    return record


def test_serialize_datetime_trims_microseconds() -> None:
    stamp = datetime(2024, 1, 2, 3, 4, 5, 987000)
    serialized = _serialize_datetime(stamp)
    assert serialized == "2024-01-02T03:04:05Z"
    deserialized = _deserialize_datetime(serialized)
    assert deserialized == datetime(2024, 1, 2, 3, 4, 5)


def test_record_to_json_and_back_round_trip() -> None:
    record = _sample_record()
    record.resources.created_at = datetime(2024, 1, 1, 0, 0, 0)
    record.updated_at = datetime(2024, 1, 2, 0, 0, 0)

    payload = _record_to_json(record)
    restored = _json_to_record(payload)

    assert restored.request == record.request
    assert restored.resources == record.resources
    assert restored.updated_at == record.updated_at
