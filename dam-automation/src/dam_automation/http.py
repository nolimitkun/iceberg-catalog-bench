"""HTTP utilities for working with JSON responses."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from requests import Response


@dataclass(slots=True)
class UnexpectedResponseError(RuntimeError):
    """Raised when an HTTP response payload is not the expected JSON."""

    status_code: int
    url: str
    body_preview: str

    def __str__(self) -> str:  # noqa: D401 - simple representation
        return (
            f"Unexpected response while calling {self.url} (status {self.status_code}): "
            f"{self.body_preview}"
        )


def parse_json(response: Response) -> Any:
    """Return JSON content or raise UnexpectedResponseError with helpful context."""

    if not response.content:
        raise UnexpectedResponseError(
            status_code=response.status_code,
            url=response.request.url if response.request else "<unknown>",
            body_preview="<empty body>",
        )
    try:
        return response.json()
    except json.JSONDecodeError as exc:  # pragma: no cover - depends on http responses
        text = response.text
        preview = text[:500].replace("\n", " ").strip()
        raise UnexpectedResponseError(
            status_code=response.status_code,
            url=response.request.url if response.request else "<unknown>",
            body_preview=preview or "<no text>",
        ) from exc
