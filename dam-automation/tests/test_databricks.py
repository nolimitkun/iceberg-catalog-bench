from __future__ import annotations

import json
from collections import deque

import pytest
from requests import Request, Response

from dam_automation.databricks import _DatabricksOAuthClient, _DatabricksOAuthToken
from dam_automation.http import UnexpectedResponseError


def _oauth_response(token: str, expires_in: int = 120) -> Response:
    response = Response()
    response.status_code = 200
    response._content = json.dumps({"access_token": token, "expires_in": expires_in}).encode("utf-8")
    response.headers["Content-Type"] = "application/json"
    response.encoding = "utf-8"
    response.request = Request("POST", "https://example.com/token").prepare()
    return response


def test_oauth_token_is_valid_until_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("dam_automation.databricks.time.time", lambda: 1000.0)
    token = _DatabricksOAuthToken("token", expires_in=120)

    assert token.is_valid() is True

    monkeypatch.setattr("dam_automation.databricks.time.time", lambda: 1050.0)
    assert token.is_valid() is True

    # expires_in minus 60 means cached token becomes invalid after 60 seconds
    monkeypatch.setattr("dam_automation.databricks.time.time", lambda: 1061.0)
    assert token.is_valid() is False


def test_oauth_client_caches_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    issued: deque[str] = deque()
    current_time = {"value": 1_000.0}

    def fake_time() -> float:
        return current_time["value"]

    def fake_post(url: str, data: dict[str, str], auth: tuple[str, str], timeout: int) -> Response:  # type: ignore[override]
        issued.append(data["scope"])
        return _oauth_response(token=f"token{len(issued)}", expires_in=120)

    monkeypatch.setattr("dam_automation.databricks.requests.post", fake_post)
    monkeypatch.setattr("dam_automation.databricks.time.time", fake_time)

    client = _DatabricksOAuthClient(
        token_url="https://example.com/token",
        client_id="client",
        client_secret="secret",
        scopes=["scope1", "scope2"],
    )

    first = client.get_token()
    assert first == "token1"
    assert list(issued) == ["scope1 scope2"]

    current_time["value"] = 1_030.0
    second = client.get_token()
    assert second == "token1"
    assert list(issued) == ["scope1 scope2"]

    current_time["value"] = 1_080.0
    third = client.get_token()
    assert third == "token2"
    assert list(issued) == ["scope1 scope2", "scope1 scope2"]


def test_oauth_client_raises_on_error_status(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, data: dict[str, str], auth: tuple[str, str], timeout: int) -> Response:  # type: ignore[override]
        response = Response()
        response.status_code = 500
        response._content = b"error"
        response.request = Request("POST", url).prepare()
        return response

    monkeypatch.setattr("dam_automation.databricks.requests.post", fake_post)
    client = _DatabricksOAuthClient(
        token_url="https://example.com/token",
        client_id="client",
        client_secret="secret",
        scopes=["scope"],
    )

    with pytest.raises(RuntimeError) as excinfo:
        client.get_token()

    assert "Failed to acquire Databricks OAuth token" in str(excinfo.value)


def test_oauth_client_propagates_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, data: dict[str, str], auth: tuple[str, str], timeout: int) -> Response:  # type: ignore[override]
        response = Response()
        response.status_code = 200
        response._content = b"{invalid"
        response.request = Request("POST", url).prepare()
        response.headers["Content-Type"] = "application/json"
        return response

    monkeypatch.setattr("dam_automation.databricks.requests.post", fake_post)
    client = _DatabricksOAuthClient(
        token_url="https://example.com/token",
        client_id="client",
        client_secret="secret",
        scopes=["scope"],
    )

    with pytest.raises(UnexpectedResponseError):
        client.get_token()
