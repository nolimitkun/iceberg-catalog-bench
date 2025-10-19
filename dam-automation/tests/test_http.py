from __future__ import annotations

import pytest
from requests import Request, Response

from dam_automation.http import UnexpectedResponseError, parse_json


def _response_with_content(content: bytes, status: int = 200, url: str = "https://example.com") -> Response:
    response = Response()
    response.status_code = status
    response._content = content
    response.request = Request("GET", url).prepare()
    response.headers["Content-Type"] = "application/json"
    response.encoding = "utf-8"
    return response


def test_parse_json_returns_payload() -> None:
    response = _response_with_content(b'{"value": 42}')

    data = parse_json(response)

    assert data == {"value": 42}


def test_parse_json_raises_on_empty_body() -> None:
    response = _response_with_content(b"", status=204)

    with pytest.raises(UnexpectedResponseError) as excinfo:
        parse_json(response)

    assert excinfo.value.status_code == 204
    assert "<empty body>" in str(excinfo.value)


def test_parse_json_raises_on_invalid_json() -> None:
    response = _response_with_content(b"{invalid}")

    with pytest.raises(UnexpectedResponseError) as excinfo:
        parse_json(response)

    assert excinfo.value.status_code == 200
    assert "Unexpected response" in str(excinfo.value)


def test_unexpected_response_error_str_contains_context() -> None:
    error = UnexpectedResponseError(
        status_code=500,
        url="https://example.com/api",
        body_preview="Internal Server Error",
    )

    message = str(error)

    assert "500" in message
    assert "https://example.com/api" in message
    assert "Internal Server Error" in message
