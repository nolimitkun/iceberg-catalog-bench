from __future__ import annotations

import json
from typing import Any

import pytest
from requests import Request, Response

from dam_automation.config import IdentityConfig
from dam_automation.identity import (
    ApplicationSecret,
    DirectoryGroup,
    IdentityProvisioner,
)


def _json_response(payload: dict[str, Any], status: int = 200, url: str = "https://graph.microsoft.com/v1.0") -> Response:
    response = Response()
    response.status_code = status
    response._content = json.dumps(payload).encode("utf-8")
    response.headers["Content-Type"] = "application/json"
    response.encoding = "utf-8"
    response.request = Request("GET", url).prepare()
    return response


class DummyCredentials:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def acquire_token(self, scope: str) -> str:
        self.calls.append(scope)
        return "fake-token"


def _provisioner(monkeypatch: pytest.MonkeyPatch, handler) -> IdentityProvisioner:
    config = IdentityConfig(
        graph_url="https://graph.microsoft.com",
        client_id="client",
        client_secret="secret",
        tenant_id="tenant",
    )
    credentials = DummyCredentials()
    provisioner = IdentityProvisioner(config, credentials)
    monkeypatch.setattr(IdentityProvisioner, "_authorized_request", handler)
    return provisioner


def test_ensure_group_returns_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = DirectoryGroup(object_id="1", display_name="Example", mail_nickname="Example")
    response_payload = {
        "value": [
            {
                "id": expected.object_id,
                "displayName": expected.display_name,
                "mailNickname": expected.mail_nickname,
            }
        ]
    }

    def handler(self, method: str, path: str, **kwargs: Any) -> Response:
        assert method == "GET"
        assert path.startswith("/v1.0/groups?")
        return _json_response(response_payload)

    provisioner = _provisioner(monkeypatch, handler)
    result = provisioner.ensure_group("Example")
    assert result == expected


def test_ensure_application_creates_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    def handler(self, method: str, path: str, **kwargs: Any) -> Response:
        calls.append((method, path))
        if method == "GET" and path.startswith("/v1.0/applications?"):
            return _json_response({"value": []})
        if method == "POST" and path == "/v1.0/applications":
            body = kwargs["json"]
            assert body["displayName"] == "Example"
            return _json_response({"id": "1", "displayName": "Example"})
        raise AssertionError(f"Unexpected call {method} {path}")

    provisioner = _provisioner(monkeypatch, handler)
    result = provisioner.ensure_application("Example")

    assert result.object_id == "1"
    assert result.display_name == "Example"
    assert calls == [
        ("GET", "/v1.0/applications?$filter=displayName eq 'Example'"),
        ("POST", "/v1.0/applications"),
    ]


def test_authorized_request_sets_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    config = IdentityConfig(
        graph_url="https://graph.microsoft.com",
        client_id="client",
        client_secret="secret",
        tenant_id="tenant",
    )
    credentials = DummyCredentials()
    provisioner = IdentityProvisioner(config, credentials)

    captured_headers: dict[str, str] = {}

    def fake_request(method: str, url: str, headers: dict[str, str], timeout: int, **kwargs: Any) -> Response:  # type: ignore[override]
        captured_headers.update(headers)
        response = _json_response({}, status=200, url=url)
        response.request = Request(method, url).prepare()
        return response

    monkeypatch.setattr("dam_automation.identity.requests.request", fake_request)

    response = provisioner._authorized_request("GET", "/v1.0/me")
    assert response.status_code == 200

    assert credentials.calls == ["https://graph.microsoft.com/.default"]
    assert captured_headers["Authorization"] == "Bearer fake-token"
    assert captured_headers["Accept"] == "application/json"
    assert captured_headers["Content-Type"] == "application/json"


def test_resolve_application_app_id_returns_none_on_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(self, method: str, path: str, **kwargs: Any) -> Response:
        if method == "GET" and path.startswith("/v1.0/applications?"):
            return _json_response({"value": []})
        raise AssertionError(f"Unexpected call: {method} {path}")

    provisioner = _provisioner(monkeypatch, handler)
    assert provisioner.resolve_application_app_id("Example") is None


def test_delete_group_handles_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    def handler(self, method: str, path: str, **kwargs: Any) -> Response:
        calls.append((method, path))
        if method == "GET" and path.startswith("/v1.0/groups?"):
            return _json_response({"value": []})
        raise AssertionError(f"Unexpected call: {method} {path}")

    provisioner = _provisioner(monkeypatch, handler)
    assert provisioner.delete_group("Example") is False
    assert calls == [("GET", "/v1.0/groups?$filter=displayName eq 'Example'")]


def test_create_application_secret_parses_response(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(self, method: str, path: str, **kwargs: Any) -> Response:
        if method == "POST" and path.endswith("/addPassword"):
            return _json_response(
                {
                    "secretText": "secret",
                    "endDateTime": "2025-01-01T00:00:00Z",
                    "keyId": "key",
                    "displayName": "custom",
                }
            )
        raise AssertionError(f"Unexpected call: {method} {path}")

    provisioner = _provisioner(monkeypatch, handler)
    secret = provisioner.create_application_secret("app", display_name="custom")
    assert isinstance(secret, ApplicationSecret)
    assert secret.secret_text == "secret"
    assert secret.display_name == "custom"


def test_create_application_secret_raises_when_missing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(self, method: str, path: str, **kwargs: Any) -> Response:
        if method == "POST" and path.endswith("/addPassword"):
            return _json_response({}, status=200)
        raise AssertionError(f"Unexpected call: {method} {path}")

    provisioner = _provisioner(monkeypatch, handler)
    with pytest.raises(RuntimeError):
        provisioner.create_application_secret("app")


def test_ensure_service_principal_by_app_id_creates_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    def handler(self, method: str, path: str, **kwargs: Any) -> Response:
        calls.append((method, path))
        if method == "GET" and path.startswith("/v1.0/servicePrincipals?"):
            return _json_response({"value": []})
        if method == "POST" and path == "/v1.0/servicePrincipals":
            assert kwargs["json"] == {"appId": "2646"}
            return _json_response(
                {
                    "id": "sp-1",
                    "displayName": "Databricks Access Connector",
                    "appId": "2646",
                }
            )
        raise AssertionError(f"Unexpected call: {method} {path}")

    provisioner = _provisioner(monkeypatch, handler)
    result = provisioner.ensure_service_principal_by_app_id("2646")

    assert result.object_id == "sp-1"
    assert result.display_name == "Databricks Access Connector"
    assert result.app_id == "2646"
    assert result.client_id == "2646"
    assert calls == [
        ("GET", "/v1.0/servicePrincipals?$filter=appId eq '2646'"),
        ("POST", "/v1.0/servicePrincipals"),
    ]
