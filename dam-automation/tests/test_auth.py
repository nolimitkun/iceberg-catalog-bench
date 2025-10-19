from __future__ import annotations

import json
from collections import deque

import pytest
from requests import Request, Response

from dam_automation.auth import ClientCredentialProvider


def _token_response(token: str, expires_in: int = 120) -> Response:
    response = Response()
    response.status_code = 200
    response._content = json.dumps(
        {"access_token": token, "expires_in": expires_in}
    ).encode("utf-8")
    response.headers["Content-Type"] = "application/json"
    response.encoding = "utf-8"
    response.request = Request("POST", "https://login.microsoftonline.com/token").prepare()
    return response


def test_acquire_token_caches_and_refreshes(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = ClientCredentialProvider(
        tenant_id="tenant",
        client_id="client",
        client_secret="secret",
    )

    call_history: deque[str] = deque()

    def fake_post(url: str, data: dict[str, str], timeout: int, **kwargs) -> Response:  # type: ignore[override]
        index = len(call_history) + 1
        token = f"token{index}"
        call_history.append(data["scope"])
        return _token_response(token, expires_in=120)

    current_time = {"value": 1_000.0}

    def fake_time() -> float:
        return current_time["value"]

    monkeypatch.setattr("dam_automation.auth.requests.post", fake_post)
    monkeypatch.setattr("dam_automation.auth.time.time", fake_time)

    scope = "https://graph.microsoft.com/.default"

    first = provider.acquire_token(scope)
    assert first == "token1"
    assert list(call_history) == [scope]

    current_time["value"] = 1_010.0
    second = provider.acquire_token(scope)
    assert second == "token1"
    assert list(call_history) == [scope]

    current_time["value"] = 1_120.0
    third = provider.acquire_token(scope)
    assert third == "token2"
    assert list(call_history) == [scope, scope]

    other_scope = "https://management.azure.com/.default"
    fourth = provider.acquire_token(other_scope)
    assert fourth == "token3"
    assert list(call_history) == [scope, scope, other_scope]
