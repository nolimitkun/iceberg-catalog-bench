from __future__ import annotations

from typing import Any, Dict

import pytest

from dam_automation.azure import AzureProvisioner
from dam_automation.config import AzureConfig


class DummyCredentials:
    def acquire_token(self, scope: str) -> str:
        return "dummy-token"


class DummyResponse:
    def __init__(self, *, status_code: int = 200, json_data: Dict[str, Any] | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._json = json_data or {}
        self.text = text

    def json(self) -> Dict[str, Any]:
        return self._json

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(self.text or f"http error {self.status_code}")


def _config() -> AzureConfig:
    return AzureConfig(
        subscription_id="sub",
        tenant_id="tenant",
        client_id="client",
        client_secret="secret",
        resource_group="rg",
        storage_account="datastore",
        location="eastus",
        identity_resource_group="identity-rg",
    )


def test_attach_identity_no_patch_when_already_linked(monkeypatch: pytest.MonkeyPatch) -> None:
    provisioner = AzureProvisioner(_config(), DummyCredentials())
    identity_id = "/subscriptions/sub/resourceGroups/identity-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/example"
    existing = {
        "identity": {
            "type": "SystemAssigned,UserAssigned",
            "userAssignedIdentities": {
                identity_id: {},
            },
        }
    }
    calls: list[tuple[str, str]] = []

    def fake_request(self, method: str, url: str, **kwargs: Any) -> DummyResponse:
        calls.append((method, url))
        assert method == "GET"
        return DummyResponse(json_data=existing)

    monkeypatch.setattr(AzureProvisioner, "_authorized_request", fake_request)  # type: ignore[assignment]

    provisioner.attach_identity_to_access_connector("/access/connector", identity_id)

    assert calls == [("GET", provisioner._mgmt_url + "/access/connector?api-version=2023-05-01")]  # type: ignore[attr-defined]


def test_attach_identity_adds_assignment(monkeypatch: pytest.MonkeyPatch) -> None:
    provisioner = AzureProvisioner(_config(), DummyCredentials())
    identity_id = "/subscriptions/sub/resourceGroups/identity-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/example"
    connector_url = provisioner._mgmt_url + "/access/connector?api-version=2023-05-01"  # type: ignore[attr-defined]
    get_body = {
        "identity": {
            "type": "SystemAssigned",
            "userAssignedIdentities": {},
        }
    }
    calls: list[tuple[str, str, dict[str, Any]]] = []

    def fake_request(self, method: str, url: str, **kwargs: Any) -> DummyResponse:
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse(json_data=get_body)
        if method == "PATCH":
            patch_body = kwargs["json"]
            identities = patch_body["identity"]["userAssignedIdentities"]
            assert identity_id in identities
            assert patch_body["identity"]["type"] in {"SystemAssigned,UserAssigned", "UserAssigned,SystemAssigned"}
            return DummyResponse()
        raise AssertionError("Unexpected method")

    monkeypatch.setattr(AzureProvisioner, "_authorized_request", fake_request)  # type: ignore[assignment]

    provisioner.attach_identity_to_access_connector("/access/connector", identity_id)

    methods = [call[0] for call in calls]
    assert methods == ["GET", "PATCH"]
    assert calls[1][1] == connector_url


def test_detach_identity_removes_assignment(monkeypatch: pytest.MonkeyPatch) -> None:
    provisioner = AzureProvisioner(_config(), DummyCredentials())
    identity_id = "/subscriptions/sub/resourceGroups/identity-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/example"
    connector_url = provisioner._mgmt_url + "/access/connector?api-version=2023-05-01"  # type: ignore[attr-defined]
    get_body = {
        "identity": {
            "type": "SystemAssigned,UserAssigned",
            "userAssignedIdentities": {identity_id: {}},
        }
    }
    calls: list[tuple[str, str, dict[str, Any]]] = []

    def fake_request(self, method: str, url: str, **kwargs: Any) -> DummyResponse:
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse(json_data=get_body)
        if method == "PATCH":
            identity_block = kwargs["json"]["identity"]
            assert identity_block["userAssignedIdentities"] == {}
            assert identity_block["type"] in {"SystemAssigned", "None"}
            return DummyResponse()
        raise AssertionError("Unexpected method")

    monkeypatch.setattr(AzureProvisioner, "_authorized_request", fake_request)  # type: ignore[assignment]

    result = provisioner.detach_identity_from_access_connector("/access/connector", identity_id)

    assert result is True
    methods = [call[0] for call in calls]
    assert methods == ["GET", "PATCH"]
    assert calls[1][1] == connector_url


def test_detach_identity_returns_false_when_not_linked(monkeypatch: pytest.MonkeyPatch) -> None:
    provisioner = AzureProvisioner(_config(), DummyCredentials())
    identity_id = "/subscriptions/sub/resourceGroups/identity-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/example"
    get_body = {
        "identity": {
            "type": "SystemAssigned",
            "userAssignedIdentities": {},
        }
    }
    methods: list[str] = []

    def fake_request(self, method: str, url: str, **kwargs: Any) -> DummyResponse:
        methods.append(method)
        assert method == "GET"
        return DummyResponse(json_data=get_body)

    monkeypatch.setattr(AzureProvisioner, "_authorized_request", fake_request)  # type: ignore[assignment]

    result = provisioner.detach_identity_from_access_connector("/access/connector", identity_id)

    assert result is False
    assert methods == ["GET"]


def test_resource_id_helpers_use_config_values() -> None:
    config = _config()
    provisioner = AzureProvisioner(config, DummyCredentials())

    assert provisioner._container_resource_id("container").startswith("/subscriptions/sub/resourceGroups/rg")  # type: ignore[attr-defined]
    assert "storageAccounts/datastore/blobServices/default/containers/container" in provisioner._container_resource_id("container")  # type: ignore[attr-defined]
    assert provisioner._identity_resource_id("identity").endswith("/userAssignedIdentities/identity")  # type: ignore[attr-defined]
