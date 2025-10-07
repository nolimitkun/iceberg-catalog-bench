"""Microsoft Entra ID (Azure AD) identity helpers."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

from .auth import ClientCredentialProvider
from .config import IdentityConfig
from .http import parse_json

logger = logging.getLogger(__name__)

GRAPH_SCOPE = "https://graph.microsoft.com/.default"


@dataclass(slots=True)
class DirectoryObject:
    object_id: str
    display_name: str


@dataclass(slots=True)
class ServicePrincipal(DirectoryObject):
    app_id: str
    client_id: str


@dataclass(slots=True)
class DirectoryGroup(DirectoryObject):
    mail_nickname: str


class IdentityProvisioner:
    """Creates applications, service principals, and groups."""

    def __init__(self, config: IdentityConfig, credential_provider: ClientCredentialProvider) -> None:
        self._config = config
        self._credentials = credential_provider
        self._base_url = config.graph_url.rstrip("/")

    def ensure_group(self, name: str, description: str | None = None) -> DirectoryGroup:
        existing = self._find_group(name)
        if existing:
            return existing
        payload = {
            "displayName": name,
            "mailEnabled": False,
            "securityEnabled": True,
            "mailNickname": name.replace(" ", ""),
        }
        if description:
            payload["description"] = description
        response = self._authorized_request("POST", "/v1.0/groups", json=payload)
        response.raise_for_status()
        body = parse_json(response)
        return DirectoryGroup(
            object_id=body["id"],
            display_name=body["displayName"],
            mail_nickname=body["mailNickname"],
        )

    def ensure_application(self, name: str) -> DirectoryObject:
        existing = self._find_application(name)
        if existing:
            return existing
        payload = {
            "displayName": name,
            "signInAudience": "AzureADMyOrg",
        }
        response = self._authorized_request("POST", "/v1.0/applications", json=payload)
        response.raise_for_status()
        body = parse_json(response)
        return DirectoryObject(object_id=body["id"], display_name=body["displayName"])

    def ensure_service_principal(self, app_object_id: str, app_id: Optional[str] = None) -> ServicePrincipal:
        if app_id is None:
            app = self._authorized_request("GET", f"/v1.0/applications/{app_object_id}")
            app.raise_for_status()
            app_body = parse_json(app)
            app_id = app_body["appId"]
        existing = self._find_service_principal(app_id)
        if existing:
            return existing
        payload = {"appId": app_id}
        response = self._authorized_request("POST", "/v1.0/servicePrincipals", json=payload)
        response.raise_for_status()
        body = parse_json(response)
        return ServicePrincipal(
            object_id=body["id"],
            display_name=body["displayName"],
            app_id=body["appId"],
            client_id=body["appId"],
        )

    def add_group_member(self, group_id: str, principal_id: str) -> None:
        payload = {
            "@odata.id": f"{self._base_url}/v1.0/directoryObjects/{principal_id}"
        }
        response = self._authorized_request(
            "POST",
            f"/v1.0/groups/{group_id}/members/$ref",
            json=payload,
        )
        if response.status_code == 204:
            return
        if response.status_code == 400 and "One or more added object references" in response.text:
            logger.info("Service principal already a member of group '%s'", group_id)
            return
        response.raise_for_status()

    def _find_group(self, name: str) -> Optional[DirectoryGroup]:
        query = f"$filter=displayName eq '{name}'"
        response = self._authorized_request("GET", f"/v1.0/groups?{query}")
        response.raise_for_status()
        payload = parse_json(response)
        data = payload.get("value", [])
        if not data:
            return None
        item = data[0]
        return DirectoryGroup(
            object_id=item["id"],
            display_name=item["displayName"],
            mail_nickname=item["mailNickname"],
        )

    def _find_application(self, name: str) -> Optional[DirectoryObject]:
        query = f"$filter=displayName eq '{name}'"
        response = self._authorized_request("GET", f"/v1.0/applications?{query}")
        response.raise_for_status()
        payload = parse_json(response)
        data = payload.get("value", [])
        if not data:
            return None
        item = data[0]
        return DirectoryObject(object_id=item["id"], display_name=item["displayName"])

    def _find_service_principal(self, app_id: str) -> Optional[ServicePrincipal]:
        query = f"$filter=appId eq '{app_id}'"
        response = self._authorized_request("GET", f"/v1.0/servicePrincipals?{query}")
        response.raise_for_status()
        payload = parse_json(response)
        data = payload.get("value", [])
        if not data:
            return None
        item = data[0]
        return ServicePrincipal(
            object_id=item["id"],
            display_name=item["displayName"],
            app_id=item["appId"],
            client_id=item["appId"],
        )

    def _authorized_request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        token = self._credentials.acquire_token(GRAPH_SCOPE)
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        headers.setdefault("Content-Type", "application/json")
        headers.setdefault("Accept", "application/json")
        url = f"{self._base_url}{path}"
        response = requests.request(method, url, headers=headers, timeout=40, **kwargs)
        return response
