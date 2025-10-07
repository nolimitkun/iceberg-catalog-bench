"""Databricks provisioning helpers for Unity Catalog."""
from __future__ import annotations

import logging
from dataclasses import dataclass
import time
from typing import Any, Dict, List, Optional

import requests

from .config import DatabricksConfig
from .http import UnexpectedResponseError, parse_json


class _DatabricksOAuthToken:
    def __init__(self, access_token: str, expires_in: int) -> None:
        self.access_token = access_token
        self.expires_at = time.time() + max(expires_in - 60, 0)  # refresh one minute early

    def is_valid(self) -> bool:
        return time.time() < self.expires_at


class _DatabricksOAuthClient:
    def __init__(self, token_url: str, client_id: str, client_secret: str, scopes: list[str]) -> None:
        self._token_endpoint = token_url.rstrip("/")
        self._client_id = client_id
        self._client_secret = client_secret
        self._scopes = scopes
        self._cached_token: Optional[_DatabricksOAuthToken] = None

    def get_token(self) -> str:
        if self._cached_token and self._cached_token.is_valid():
            return self._cached_token.access_token
        self._cached_token = self._request_token()
        return self._cached_token.access_token

    def _request_token(self) -> _DatabricksOAuthToken:
        payload = {
            "grant_type": "client_credentials",
            "scope": " ".join(self._scopes),
        }
        response = requests.post(
            self._token_endpoint,
            data=payload,
            auth=(self._client_id, self._client_secret),
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Failed to acquire Databricks OAuth token (status {response.status_code}): {response.text}"
            )
        body = parse_json(response)
        return _DatabricksOAuthToken(
            access_token=body["access_token"],
            expires_in=int(body.get("expires_in", 3600)),
        )

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StorageCredential:
    name: str
    id: str


@dataclass(slots=True)
class ExternalLocation:
    name: str
    url: str


@dataclass(slots=True)
class Catalog:
    name: str
    metastore_id: str
    storage_root: str


@dataclass(slots=True)
class AccountServicePrincipal:
    id: str
    application_id: str
    display_name: str


class DatabricksProvisioner:
    """Performs workspace- and account-level operations in Databricks."""

    def __init__(self, config: DatabricksConfig) -> None:
        self._config = config
        self._workspace = config.workspace_url.rstrip("/")
        self._account = config.account_url.rstrip("/")
        self._workspace_oauth = _DatabricksOAuthClient(
            token_url=f"{self._workspace}/oidc/v1/token",
            client_id=config.workspace_client_id,
            client_secret=config.workspace_client_secret,
            scopes=config.workspace_oauth_scopes,
        )
        self._account_oauth = _DatabricksOAuthClient(
            token_url=f"{self._account}/oidc/accounts/{config.account_id}/v1/token",
            client_id=config.account_client_id,
            client_secret=config.account_client_secret,
            scopes=config.account_oauth_scopes,
        )

    # Account-level -----------------------------------------------------

    def ensure_account_service_principal(self, application_id: str, display_name: str) -> AccountServicePrincipal:
        try:
            existing = self._find_account_service_principal(application_id)
        except UnexpectedResponseError as exc:  # pragma: no cover - network dependent
            raise RuntimeError(
                "Databricks account API returned a non-JSON response. Verify that the service principal "
                "configured via 'databricks.account_client_id' and 'databricks.account_client_secret' is valid, "
                "has account-level access, and that the OAuth scopes supplied are correct."
            ) from exc
        if existing:
            return existing
        payload = {
            "applicationId": application_id,
            "displayName": display_name,
            "active": True,
        }
        response = self._account_request(
            "POST",
            f"/api/2.0/accounts/{self._config.account_id}/scim/v2/ServicePrincipals",
            json=payload,
        )
        response.raise_for_status()
        body = parse_json(response)
        return AccountServicePrincipal(
            id=body["id"],
            application_id=body["applicationId"],
            display_name=body.get("displayName", display_name),
        )

    def _find_account_service_principal(self, application_id: str) -> Optional[AccountServicePrincipal]:
        response = self._account_request(
            "GET",
            f"/api/2.0/accounts/{self._config.account_id}/scim/v2/ServicePrincipals",
        )
        response.raise_for_status()
        payload = parse_json(response)
        for sp in payload.get("Resources", []):
            if sp.get("applicationId") == application_id:
                return AccountServicePrincipal(
                    id=sp["id"],
                    application_id=sp.get("applicationId", sp.get("appId", "")),
                    display_name=sp.get("displayName", sp["id"]),
                )
        return None

    def _find_account_group(self, name: str) -> Optional[Dict[str, Any]]:
        response = self._account_request(
            "GET",
            f"/api/2.0/accounts/{self._config.account_id}/scim/v2/Groups",
            params={"filter": f'displayName eq "{name}"'},
        )
        response.raise_for_status()
        payload = parse_json(response)
        resources = payload.get("Resources", [])
        if not resources:
            return None
        return resources[0]

    # Workspace-level ---------------------------------------------------

    def ensure_storage_credential(self, name: str, managed_identity_id: str) -> StorageCredential:
        payload = {
            "name": name,
            "comment": "Provisioned by DAM automation",
            "read_only": False,
            "purpose": "STORAGE",
            "azure_managed_identity": {
                "managed_identity_id": managed_identity_id,
                "access_connector_id": self._config.access_connector_id,
            },
        }
        response = self._workspace_request("POST", "/api/2.1/unity-catalog/credentials", json=payload)
        if response.status_code == 409:
            logger.info("Storage credential '%s' already exists", name)
            existing = self._workspace_request("GET", f"/api/2.1/unity-catalog/credentials/{name}")
            existing.raise_for_status()
            data = parse_json(existing)
            return StorageCredential(name=data["name"], id=data["id"])
        if response.status_code >= 400:
            try:
                error_body = response.json()
            except ValueError:
                error_body = response.text
            raise RuntimeError(
                "Failed to create Unity Catalog storage credential: "
                f"status={response.status_code}, body={error_body}"
            )
        body = parse_json(response)
        return StorageCredential(name=body["name"], id=body["id"])

    def ensure_external_location(self, name: str, url: str, credential_name: str) -> ExternalLocation:
        payload = {
            "name": name,
            "url": url,
            "credential_name": credential_name,
            "read_only": False,
            "comment": "Provisioned by DAM automation",
        }
        response = self._workspace_request("POST", "/api/2.1/unity-catalog/external-locations", json=payload)
        if response.status_code == 409:
            logger.info("External location '%s' already exists", name)
            return ExternalLocation(name=name, url=url)
        if response.status_code >= 400:
            body: Any
            try:
                body = response.json()
            except ValueError:
                body = response.text
            if isinstance(body, dict) and body.get("error_code") == "EXTERNAL_LOCATION_ALREADY_EXISTS":
                logger.info("External location '%s' already exists (reported via error payload)", name)
                return ExternalLocation(name=name, url=url)
            raise RuntimeError(
                "Failed to create external location: "
                f"status={response.status_code}, body={body}"
            )
        return ExternalLocation(name=name, url=url)

    def ensure_catalog(self, name: str, storage_root: str) -> Catalog:
        payload = {
            "name": name,
            "metastore_id": self._config.metastore_id,
            "storage_root": storage_root,
            "comment": "Provisioned by DAM automation",
        }
        response = self._workspace_request("POST", "/api/2.1/unity-catalog/catalogs", json=payload)
        if response.status_code == 409:
            logger.info("Catalog '%s' already exists", name)
            return Catalog(name=name, metastore_id=self._config.metastore_id, storage_root=storage_root)
        if response.status_code >= 400:
            try:
                body = response.json()
            except ValueError:
                body = response.text
            if isinstance(body, dict) and body.get("error_code") == "CATALOG_ALREADY_EXISTS":
                logger.info("Catalog '%s' already exists (reported via error payload)", name)
                return Catalog(name=name, metastore_id=self._config.metastore_id, storage_root=storage_root)
            raise RuntimeError(
                "Failed to create catalog: "
                f"status={response.status_code}, body={body}"
            )
        body = parse_json(response)
        return Catalog(
            name=body["name"],
            metastore_id=body["metastore_id"],
            storage_root=body.get("storage_root", storage_root),
        )

    def ensure_group(self, name: str) -> Dict[str, Any]:
        existing = self._find_account_group(name)
        if existing:
            logger.info("Databricks account group '%s' already exists", name)
            return existing

        payload = {"displayName": name}
        response = self._account_request(
            "POST",
            f"/api/2.0/accounts/{self._config.account_id}/scim/v2/Groups",
            json=payload,
        )
        if response.status_code == 409:
            logger.info("Databricks account group '%s' already exists", name)
            existing = self._find_account_group(name)
            if existing:
                return existing
        response.raise_for_status()
        return parse_json(response)

    def add_service_principal_to_group(self, group_id: str, service_principal_id: str) -> None:
        payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [
                {
                    "op": "Add",
                    "path": "members",
                    "value": [{"value": service_principal_id}],
                }
            ],
        }
        response = self._account_request(
            "PATCH",
            f"/api/2.0/accounts/{self._config.account_id}/scim/v2/Groups/{group_id}",
            json=payload,
        )
        if response.status_code in {200, 204}:
            return
        if response.status_code == 409:
            logger.info("Service principal already in Databricks account group '%s'", group_id)
            return
        if response.status_code == 400 and "already" in response.text:
            logger.info("Service principal already in Databricks account group '%s'", group_id)
            return
        response.raise_for_status()

    def grant_catalog_privileges(self, catalog_name: str, principal: str, privileges: List[str]) -> None:
        payload = {
            "changes": [
                {
                    "principal": principal,
                    "add": privileges,
                }
            ]
        }
        response = self._workspace_request(
            "PATCH",
            f"/api/2.1/unity-catalog/permissions/catalog/{catalog_name}",
            json=payload,
        )
        if response.status_code >= 400:
            try:
                body = response.json()
            except ValueError:
                body = response.text
            if isinstance(body, dict) and body.get("error_code") == "INVALID_PARAMETER_VALUE":
                logger.warning(
                    "Failed to grant catalog privileges on '%s' for '%s': %s",
                    catalog_name,
                    principal,
                    body.get("message"),
                )
                return
            raise RuntimeError(
                "Failed to grant catalog privileges: "
                f"status={response.status_code}, body={body}"
            )

    def grant_catalog_privileges_all(self, catalog_name: str, principal: str) -> None:
        self.grant_catalog_privileges(
            catalog_name,
            principal,
            [
                "ALL_PRIVILEGES",
                "EXTERNAL_USE_SCHEMA"
            ],
        )

    def grant_external_location_privileges(self, location_name: str, principal: str, privileges: List[str]) -> None:
        payload = {
            "changes": [
                {
                    "principal": principal,
                    "add": privileges,
                }
            ]
        }
        response = self._workspace_request(
            "PATCH",
            f"/api/2.1/unity-catalog/permissions/external-location/{location_name}",
            json=payload,
        )
        if response.status_code >= 400:
            try:
                body = response.json()
            except ValueError:
                body = response.text
            if isinstance(body, dict) and body.get("error_code") == "INVALID_PARAMETER_VALUE":
                logger.warning(
                    "Failed to grant external location privileges on '%s' for '%s': %s. Retrying with minimal privilege set.",
                    location_name,
                    principal,
                    body.get("message"),
                )
                fallback_privileges = [priv for priv in privileges if priv != "ALL_PRIVILEGES"]
                if fallback_privileges:
                    self.grant_external_location_privileges(location_name, principal, fallback_privileges)
                return
            if isinstance(body, dict) and body.get("error_code") == "EXTERNAL_LOCATION_ALREADY_EXISTS":
                logger.info(
                    "External location '%s' already has privileges set for '%s'",
                    location_name,
                    principal,
                )
                return
            raise RuntimeError(
                "Failed to grant external location privileges: "
                f"status={response.status_code}, body={body}"
            )

    # HTTP helpers ------------------------------------------------------

    def _workspace_request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._workspace_oauth.get_token()}"
        headers.setdefault("Content-Type", "application/json")
        headers.setdefault("Accept", "application/json")
        url = f"{self._workspace}{path}"
        response = requests.request(method, url, headers=headers, timeout=40, **kwargs)
        return response

    def _account_request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._account_oauth.get_token()}"
        headers.setdefault("Content-Type", "application/json")
        headers.setdefault("Accept", "application/json")
        if "accounts" not in self._account:
            raise RuntimeError(
                "Configured databricks.account_url does not appear to be the Databricks accounts endpoint. "
                f"Resolved value: {self._account}"
            )
        url = f"{self._account}{path}"
        response = requests.request(method, url, headers=headers, timeout=40, **kwargs)
        return response
