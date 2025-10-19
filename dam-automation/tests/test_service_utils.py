from __future__ import annotations

from types import SimpleNamespace
from typing import Optional

from dam_automation.models import DatasourceRequest
from dam_automation.service import DatasourceAutomationService


def _service_with_prefix(
    prefix: str = "acme",
    separator: str = "-",
    *,
    identity_app_id: Optional[str] = None,
    identity_raises: bool = False,
) -> DatasourceAutomationService:
    service = DatasourceAutomationService.__new__(DatasourceAutomationService)

    class DummyConfig:
        def __init__(self, prefix_value: str, sep: str) -> None:
            self._prefix = prefix_value
            self._separator = sep

        def qualify_name(self, base: str) -> str:
            if self._prefix:
                return f"{self._prefix}{self._separator}{base}"
            return base

        @property
        def azure(self) -> SimpleNamespace:
            return SimpleNamespace(
                storage_account="storageacct",
                data_plane_dns_suffix="dfs.core.windows.net",
                subscription_id="0000-1111",
                identity_resource_group="identity-rg",
            )

    service._config = DummyConfig(prefix, separator)  # type: ignore[attr-defined]
    service._identity = SimpleNamespace(
        resolve_application_app_id=(
            (lambda _: (_ for _ in ()).throw(RuntimeError("lookup failed")))
            if identity_raises
            else (lambda _: identity_app_id)
        )
    )
    return service


def test_normalize_name_applies_qualifier_and_truncates() -> None:
    service = _service_with_prefix()
    raw = "My Fancy/Data_Source Name!!" + "X" * 100

    result = service._normalize_name(raw)

    assert result.startswith("acme-")
    assert "-" not in result[-1]
    assert len(result) <= 63
    assert result == result.lower()


def test_build_tags_includes_owner_and_labels() -> None:
    service = _service_with_prefix()
    request = DatasourceRequest(
        name="example",
        owner="owner@example.com",
        labels={"env": "dev", "team": "data"},
    )

    tags = service._build_tags(request)

    assert tags["datasource"] == "example"
    assert tags["owner"] == "owner@example.com"
    assert tags["env"] == "dev"
    assert tags["team"] == "data"


def test_derive_ro_group_name_handles_suffixes() -> None:
    service = _service_with_prefix()

    assert service._derive_ro_group_name("example-rw", "example") == "example-ro"
    assert service._derive_ro_group_name("example_rw", "fallback") == "example-ro"
    assert service._derive_ro_group_name("example", "fallback") == "fallback-ro"


def test_extract_container_name_parses_urls() -> None:
    service = _service_with_prefix()

    url = "abfss://sample@storageacct.dfs.core.windows.net/path"
    assert service._extract_container_name(url) == "sample"
    assert service._extract_container_name("container") == "container"
    assert service._extract_container_name("") == ""


def test_extract_identity_name_returns_last_segment() -> None:
    service = _service_with_prefix()
    resource = (
        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ManagedIdentity"
        "/userAssignedIdentities/example-id"
    )

    assert service._extract_identity_name(resource, "fallback") == "example-id"
    assert service._extract_identity_name("", "fallback") == "fallback"


def test_to_azure_storage_base_url_transforms_scheme() -> None:
    service = _service_with_prefix()

    blob_url = "https://storageacct.blob.core.windows.net/example/path"
    assert service._to_azure_storage_base_url(blob_url) == "azure://storageacct.blob.core.windows.net/example/path/"


def test_empty_resources_returns_blank_fields() -> None:
    service = _service_with_prefix()
    resources = service._empty_resources()

    assert resources.container_url == ""
    assert resources.managed_identity_id == ""
    assert resources.storage_credential_name == ""
    assert resources.external_location_name == ""
    assert resources.catalog_name == ""
    assert resources.group_name == ""
    assert resources.service_principal_app_id == ""
    assert resources.service_principal_client_secret == ""
    assert resources.databricks_oauth_client_secret == ""
    assert resources.snowflake_external_volume_name == ""
    assert resources.snowflake_catalog_integration_name == ""
    assert resources.snowflake_database_name == ""


def test_inferred_resources_populates_expected_fields() -> None:
    service = _service_with_prefix(identity_app_id="app-123")

    resources = service._inferred_resources("sample")

    assert resources.container_url == "abfss://sample@storageacct.dfs.core.windows.net/"
    assert "identity-rg" in resources.managed_identity_id
    assert resources.service_principal_app_id == "app-123"
    assert resources.snowflake_database_name == "sample_linked_db"


def test_inferred_resources_handles_lookup_failure() -> None:
    service = _service_with_prefix(identity_raises=True)

    resources = service._inferred_resources("sample")

    assert resources.service_principal_app_id == ""
    assert resources.group_name == "sample-rw"


def test_build_inferred_record_uses_normalized_name() -> None:
    service = _service_with_prefix()

    record = service._build_inferred_record("sample")

    assert record.request.name == "sample"
    assert record.resources.catalog_name == "sample"
