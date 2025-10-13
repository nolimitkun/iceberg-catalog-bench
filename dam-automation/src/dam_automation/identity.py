"""Microsoft Entra ID (Azure AD) identity helpers."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
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


@dataclass(slots=True)
class ApplicationSecret:
    key_id: str
    display_name: str
    secret_text: str
    expires_on: datetime


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

    def resolve_application_app_id(self, name: str) -> Optional[str]:
        application = self._find_application(name)
        if not application:
            return None
        response = self._authorized_request("GET", f"/v1.0/applications/{application.object_id}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = parse_json(response)
        return payload.get("appId")

    def get_group(self, name: str) -> Optional[DirectoryGroup]:
        return self._find_group(name)

    def get_service_principal(self, app_id: str) -> Optional[ServicePrincipal]:
        return self._find_service_principal(app_id)

    def get_application(self, name: str) -> Optional[DirectoryObject]:
        return self._find_application(name)

    def delete_group(self, name: str) -> bool:
        group = self._find_group(name)
        if not group:
            logger.info("Azure AD group '%s' not found; skipping delete", name)
            return False
        response = self._authorized_request("DELETE", f"/v1.0/groups/{group.object_id}")
        if response.status_code in {200, 202, 204}:
            logger.info("Deleted Azure AD group '%s'", name)
            return True
        if response.status_code == 404:
            logger.info("Azure AD group '%s' not found during delete", name)
            return False
        logger.error("Failed to delete Azure AD group '%s': %s", name, response.text)
        response.raise_for_status()
        return False

    def delete_service_principal(self, app_id: str) -> bool:
        service_principal = self._find_service_principal(app_id)
        if not service_principal:
            logger.info("Service principal with appId '%s' not found; skipping delete", app_id)
            return False
        response = self._authorized_request("DELETE", f"/v1.0/servicePrincipals/{service_principal.object_id}")
        if response.status_code in {200, 202, 204}:
            logger.info("Deleted service principal '%s'", service_principal.display_name)
            return True
        if response.status_code == 404:
            logger.info("Service principal with appId '%s' not found during delete", app_id)
            return False
        logger.error(
            "Failed to delete service principal '%s' (appId '%s'): %s",
            service_principal.display_name,
            app_id,
            response.text,
        )
        response.raise_for_status()
        return False

    def delete_application(self, name: str) -> bool:
        application = self._find_application(name)
        if not application:
            logger.info("Application '%s' not found; skipping delete", name)
            return False
        response = self._authorized_request("DELETE", f"/v1.0/applications/{application.object_id}")
        if response.status_code in {200, 202, 204}:
            logger.info("Deleted application '%s'", name)
            return True
        if response.status_code == 404:
            logger.info("Application '%s' not found during delete", name)
            return False
        logger.error("Failed to delete application '%s': %s", name, response.text)
        response.raise_for_status()
        return False

    def create_application_secret(
        self,
        app_object_id: str,
        *,
        display_name: Optional[str] = None,
        validity_days: int = 730,
    ) -> ApplicationSecret:
        end_time = datetime.utcnow() + timedelta(days=validity_days)
        payload = {
            "passwordCredential": {
                "displayName": display_name or "dam-automation",
                "endDateTime": end_time.replace(microsecond=0).isoformat() + "Z",
            }
        }
        response = self._authorized_request(
            "POST",
            f"/v1.0/applications/{app_object_id}/addPassword",
            json=payload,
        )
        response.raise_for_status()
        body = parse_json(response)
        secret_text = body.get("secretText")
        if not secret_text:
            raise RuntimeError("Azure AD did not return a client secret for the application password")
        expires_raw = body.get("endDateTime")
        expires_on = datetime.utcnow()
        if isinstance(expires_raw, str):
            expires_on = datetime.fromisoformat(expires_raw.rstrip("Z"))
        return ApplicationSecret(
            key_id=body.get("keyId", body.get("id", "")),
            display_name=body.get("displayName", display_name or ""),
            secret_text=secret_text,
            expires_on=expires_on,
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
