"""Authentication helpers for Azure and Microsoft Graph."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict

import requests

from .http import parse_json

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class OAuthToken:
    access_token: str
    expires_at: float

    def is_expired(self) -> bool:
        return time.time() >= self.expires_at - 60  # refresh 1 min early


class ClientCredentialProvider:
    """Minimal client credential flow helper."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._cache: Dict[str, OAuthToken] = {}

    def acquire_token(self, scope: str) -> str:
        cached = self._cache.get(scope)
        if cached and not cached.is_expired():
            return cached.access_token

        token = self._request_token(scope)
        self._cache[scope] = token
        return token.access_token

    def _request_token(self, scope: str) -> OAuthToken:
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": scope,
            "grant_type": "client_credentials",
        }
        response = requests.post(token_url, data=payload, timeout=30)
        if response.status_code >= 400:
            logger.error("Failed to acquire token: %s", response.text)
            response.raise_for_status()
        body = parse_json(response)
        expires_in = int(body.get("expires_in", 3600))
        return OAuthToken(access_token=body["access_token"], expires_at=time.time() + expires_in)
