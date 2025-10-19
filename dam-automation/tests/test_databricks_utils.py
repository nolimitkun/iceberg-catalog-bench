from __future__ import annotations

import json

import pytest

from requests import Request, Response

from dam_automation.databricks import DatabricksProvisioner
from dam_automation.http import UnexpectedResponseError


def _response(payload: dict, status: int = 200) -> Response:
    resp = Response()
    resp.status_code = status
    resp._content = json.dumps(payload).encode("utf-8")
    resp.headers["Content-Type"] = "application/json"
    resp.encoding = "utf-8"
    resp.request = Request("GET", "https://example.com").prepare()
    return resp


def _dummy_provisioner() -> DatabricksProvisioner:
    provisioner = DatabricksProvisioner.__new__(DatabricksProvisioner)
    provisioner._workspace_request = lambda *args, **kwargs: None  # type: ignore[assignment]
    return provisioner


def test_paginate_workspace_collects_pages(monkeypatch) -> None:
    provisioner = _dummy_provisioner()

    responses = [
        _response({"schemas": [{"name": "one"}], "next_page_token": "abc"}),
        _response({"schemas": [{"name": "two"}]}),
    ]

    def fake_workspace_request(method: str, path: str, params=None):  # type: ignore[override]
        assert method == "GET"
        return responses.pop(0)

    provisioner._workspace_request = fake_workspace_request  # type: ignore[assignment]

    items = provisioner._paginate_workspace("/path")

    assert items == [{"name": "one"}, {"name": "two"}]


def test_paginate_workspace_raises_on_non_json(monkeypatch) -> None:
    provisioner = _dummy_provisioner()

    bad_response = Response()
    bad_response.status_code = 200
    bad_response._content = b"{invalid"
    bad_response.request = Request("GET", "https://example.com").prepare()
    bad_response.headers["Content-Type"] = "application/json"
    bad_response.encoding = "utf-8"

    provisioner._workspace_request = lambda *args, **kwargs: bad_response  # type: ignore[assignment]

    with pytest.raises(UnexpectedResponseError):
        provisioner._paginate_workspace("/path")


def test_should_retry_external_location_detects_messages() -> None:
    provisioner = _dummy_provisioner()

    assert provisioner._should_retry_external_location({"message": "Not authorized to perform action"})
    assert provisioner._should_retry_external_location({"message": "Managed identity does not have access"})
    assert provisioner._should_retry_external_location({"message": "validate_credential failure"})
    assert provisioner._should_retry_external_location({"message": "something else"}) is False
