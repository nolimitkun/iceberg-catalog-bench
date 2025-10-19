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


@dataclass(slots=True)
class ServicePrincipalSecret:
    client_id: str
    secret_id: str
    secret_value: str
    secret_name: str


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

    def get_account_service_principal(self, application_id: str) -> Optional[AccountServicePrincipal]:
        return self._find_account_service_principal(application_id)

    def get_workspace_service_principal(self, application_id: str) -> Optional[Dict[str, Any]]:
        query = f'applicationId eq "{application_id}"'
        response = self._workspace_request(
            "GET",
            "/api/2.0/preview/scim/v2/ServicePrincipals",
            params={"filter": query},
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = parse_json(response)
        resources = payload.get("Resources", [])
        for sp in resources:
            if sp.get("applicationId") == application_id or sp.get("appId") == application_id:
                return sp
        return None

    def get_account_group(self, name: str) -> Optional[Dict[str, Any]]:
        return self._find_account_group(name)

    # Workspace-level ---------------------------------------------------

    def _paginate_workspace(self, path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        next_page_token: Optional[str] = None
        while True:
            query = dict(params or {})
            if next_page_token:
                query["page_token"] = next_page_token
            response = self._workspace_request("GET", path, params=query)
            response.raise_for_status()
            payload = parse_json(response)
            items = payload.get("schemas") or payload.get("tables") or payload.get("items") or []
            if isinstance(items, list):
                results.extend(items)
            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break
        return results

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
        max_attempts = 5
        backoff_seconds = 5.0
        for attempt in range(1, max_attempts + 1):
            response = self._workspace_request("POST", "/api/2.1/unity-catalog/credentials", json=payload)
            if response.status_code == 409:
                logger.info("Storage credential '%s' already exists", name)
                existing = self._workspace_request("GET", f"/api/2.1/unity-catalog/credentials/{name}")
                existing.raise_for_status()
                data = parse_json(existing)
                return StorageCredential(name=data["name"], id=data["id"])
            if response.status_code < 400:
                body = parse_json(response)
                return StorageCredential(name=body["name"], id=body["id"])

            try:
                error_body = response.json()
            except ValueError:
                error_body = response.text

            if (
                self._should_retry_storage_credential(response.status_code, error_body)
                and attempt < max_attempts
            ):
                logger.warning(
                    "Databricks reported the managed identity was unavailable when creating storage credential "
                    "'%s' (attempt %s/%s); waiting %.1fs before retrying",
                    name,
                    attempt,
                    max_attempts,
                    backoff_seconds,
                )
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60.0)
                continue

            raise RuntimeError(
                "Failed to create Unity Catalog storage credential: "
                f"status={response.status_code}, body={error_body}"
            )
        raise RuntimeError(
            "Failed to create Unity Catalog storage credential after retrying "
            f"{max_attempts} times; last payload name='{name}'"
        )

    def _should_retry_storage_credential(self, status_code: int, error_body: Any) -> bool:
        if status_code != 404:
            return False
        message = ""
        if isinstance(error_body, dict):
            message = str(error_body.get("message", ""))
        else:
            message = str(error_body)
        retry_indicators = ("AADSTS700016", "was not found in the directory")
        return any(indicator in message for indicator in retry_indicators)

    def ensure_external_location(self, name: str, url: str, credential_name: str) -> ExternalLocation:
        payload = {
            "name": name,
            "url": url,
            "credential_name": credential_name,
            "read_only": False,
            "comment": "Provisioned by DAM automation",
        }
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
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
                if response.status_code == 403 and self._should_retry_external_location(body) and attempt < max_attempts:
                    backoff = min(60, 5 * attempt)
                    logger.info(
                        "External location creation for '%s' denied (permissions not yet propagated). Retrying in %s seconds.",
                        name,
                        backoff,
                    )
                    time.sleep(backoff)
                    continue
                raise RuntimeError(
                    "Failed to create external location: "
                    f"status={response.status_code}, body={body}"
                )
            return ExternalLocation(name=name, url=url)
        raise RuntimeError(
            f"Failed to create external location '{name}' after {max_attempts} attempts due to permission propagation delays."
        )

    @staticmethod
    def _should_retry_external_location(body: Any) -> bool:
        if not isinstance(body, dict):
            return False
        message = str(body.get("message", "")).lower()
        if "not authorized" in message:
            return True
        if "managed identity does not have" in message:
            return True
        if "validate_credential" in message:
            return True
        return False

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

    def ensure_group(self, base_name: str) -> Dict[str, Dict[str, Any]]:
        """Ensure paired read-only and read-write account groups exist."""
        ro_name = f"{base_name}-ro"
        rw_name = f"{base_name}-rw"
        ro_group = self._ensure_or_create_account_group(ro_name)
        rw_group = self._ensure_or_create_account_group(rw_name)
        return {"ro": ro_group, "rw": rw_group}

    def _ensure_or_create_account_group(self, display_name: str) -> Dict[str, Any]:
        existing = self._find_account_group(display_name)
        if existing:
            logger.info("Databricks account group '%s' already exists", display_name)
            return existing

        payload = {"displayName": display_name}
        response = self._account_request(
            "POST",
            f"/api/2.0/accounts/{self._config.account_id}/scim/v2/Groups",
            json=payload,
        )
        if response.status_code == 409:
            logger.info("Databricks account group '%s' already exists", display_name)
            existing = self._find_account_group(display_name)
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

    def remove_service_principal_from_group(self, group_id: str, service_principal_id: str) -> bool:
        payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [
                {
                    "op": "Remove",
                    "path": f'members[value eq "{service_principal_id}"]',
                }
            ],
        }
        response = self._account_request(
            "PATCH",
            f"/api/2.0/accounts/{self._config.account_id}/scim/v2/Groups/{group_id}",
            json=payload,
        )
        if response.status_code in {200, 204}:
            return True
        if response.status_code == 404:
            logger.info("Databricks account group '%s' not found when removing membership", group_id)
            return False
        if response.status_code == 400 and "not found" in response.text.lower():
            logger.info(
                "Service principal '%s' not a member of group '%s'; nothing to remove",
                service_principal_id,
                group_id,
            )
            return False
        response.raise_for_status()
        return True

    def ensure_workspace_service_principal(self, application_id: str, display_name: str) -> Dict[str, Any]:
        """Ensure the service principal is available within the Databricks workspace."""
        query = f'applicationId eq "{application_id}"'
        response = self._workspace_request(
            "GET",
            "/api/2.0/preview/scim/v2/ServicePrincipals",
            params={"filter": query},
        )
        response.raise_for_status()
        payload = parse_json(response)
        resources = payload.get("Resources", [])
        for sp in resources:
            if sp.get("applicationId") == application_id or sp.get("appId") == application_id:
                logger.info("Databricks workspace service principal '%s' already exists", display_name)
                return sp

        logger.info("Creating Databricks workspace service principal '%s'", display_name)
        create_payload = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
            "displayName": display_name,
            "applicationId": application_id,
        }
        create_resp = self._workspace_request(
            "POST",
            "/api/2.0/preview/scim/v2/ServicePrincipals",
            json=create_payload,
        )
        if create_resp.status_code == 409:
            logger.info("Workspace service principal '%s' already exists (reported via conflict)", display_name)
            return self.ensure_workspace_service_principal(application_id, display_name)
        create_resp.raise_for_status()
        return parse_json(create_resp)

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

    def delete_account_group(self, display_name: str) -> bool:
        group = self._find_account_group(display_name)
        if not group:
            logger.info("Databricks account group '%s' not found; skipping delete", display_name)
            return False
        group_id = group.get("id")
        if not group_id:
            return False
        response = self._account_request(
            "DELETE",
            f"/api/2.0/accounts/{self._config.account_id}/scim/v2/Groups/{group_id}",
        )
        if response.status_code in {200, 204}:
            logger.info("Deleted Databricks account group '%s'", display_name)
            return True
        if response.status_code == 404:
            logger.info("Databricks account group '%s' not found during delete", display_name)
            return False
        if response.status_code == 400:
            try:
                body = response.json()
            except ValueError:
                body = response.text
            logger.warning(
                "Databricks reported 400 while deleting catalog '%s': %s",
                name,
                body,
            )
            return False
        response.raise_for_status()
        return False

    def delete_storage_credential(self, name: str) -> bool:
        response = self._workspace_request("DELETE", f"/api/2.1/unity-catalog/credentials/{name}")
        if response.status_code in {200, 202, 204}:
            logger.info("Deleted storage credential '%s'", name)
            return True
        if response.status_code == 404:
            logger.info("Storage credential '%s' not found; skipping delete", name)
            return False
        response.raise_for_status()
        return False

    def delete_external_location(self, name: str) -> bool:
        response = self._workspace_request(
            "DELETE",
            f"/api/2.1/unity-catalog/external-locations/{name}",
            params={"force": "true"},
        )
        if response.status_code in {200, 202, 204}:
            logger.info("Deleted external location '%s'", name)
            return True
        if response.status_code == 404:
            logger.info("External location '%s' not found; skipping delete", name)
            return False
        response.raise_for_status()
        return False

    def delete_catalog(self, name: str) -> bool:
        response = self._workspace_request("DELETE", f"/api/2.1/unity-catalog/catalogs/{name}")
        if response.status_code in {200, 202, 204}:
            logger.info("Deleted catalog '%s'", name)
            return True
        if response.status_code == 404:
            logger.info("Catalog '%s' not found; skipping delete", name)
            return False
        body: Any
        try:
            body = response.json()
        except ValueError:
            body = response.text
        if isinstance(body, dict):
            error_code = body.get("error_code")
            if error_code in {"CATALOG_DOES_NOT_EXIST", "NOT_FOUND"}:
                logger.info("Catalog '%s' already absent (error_code=%s)", name, error_code)
                return False
            message = body.get("message", "")
            if error_code == "INVALID_STATE" and "already deleted" in message.lower():
                logger.info("Catalog '%s' already deleted (Databricks reported INVALID_STATE)", name)
                return False
        response.raise_for_status()
        return False

    def list_schemas(self, catalog_name: str) -> List[Dict[str, Any]]:
        params = {"catalog_name": catalog_name}
        return self._paginate_workspace("/api/2.1/unity-catalog/schemas", params=params)

    def list_tables(self, catalog_name: str, schema_name: str) -> List[Dict[str, Any]]:
        params = {"catalog_name": catalog_name, "schema_name": schema_name}
        return self._paginate_workspace("/api/2.1/unity-catalog/tables", params=params)

    def delete_table(self, full_name: str) -> bool:
        response = self._workspace_request("DELETE", f"/api/2.1/unity-catalog/tables/{full_name}")
        if response.status_code in {200, 202, 204}:
            logger.info("Deleted table '%s'", full_name)
            return True
        if response.status_code == 404:
            logger.info("Table '%s' not found; skipping delete", full_name)
            return False
        body: Any
        try:
            body = response.json()
        except ValueError:
            body = response.text
        logger.warning("Failed to delete table '%s': %s", full_name, body)
        response.raise_for_status()
        return False

    def delete_schema(self, full_name: str) -> bool:
        response = self._workspace_request("DELETE", f"/api/2.1/unity-catalog/schemas/{full_name}")
        if response.status_code in {200, 202, 204}:
            logger.info("Deleted schema '%s'", full_name)
            return True
        if response.status_code == 404:
            logger.info("Schema '%s' not found; skipping delete", full_name)
            return False
        body: Any
        try:
            body = response.json()
        except ValueError:
            body = response.text
        logger.warning("Failed to delete schema '%s': %s", full_name, body)
        response.raise_for_status()
        return False

    def delete_account_service_principal(self, application_id: str) -> bool:
        service_principal = self._find_account_service_principal(application_id)
        if not service_principal:
            logger.info("Databricks account service principal '%s' not found; skipping delete", application_id)
            return False
        response = self._account_request(
            "DELETE",
            f"/api/2.0/accounts/{self._config.account_id}/scim/v2/ServicePrincipals/{service_principal.id}",
        )
        if response.status_code in {200, 204}:
            logger.info("Deleted Databricks account service principal '%s'", application_id)
            return True
        if response.status_code == 404:
            logger.info("Databricks account service principal '%s' not found during delete", application_id)
            return False
        response.raise_for_status()
        return False

    def delete_workspace_service_principal(self, application_id: str) -> bool:
        service_principal = self.get_workspace_service_principal(application_id)
        if not service_principal:
            logger.info("Databricks workspace service principal '%s' not found; skipping delete", application_id)
            return False
        resource_id = service_principal.get("id")
        if not resource_id:
            return False
        response = self._workspace_request(
            "DELETE",
            f"/api/2.0/preview/scim/v2/ServicePrincipals/{resource_id}",
        )
        if response.status_code in {200, 204}:
            logger.info("Deleted Databricks workspace service principal '%s'", application_id)
            return True
        if response.status_code == 404:
            logger.info("Databricks workspace service principal '%s' not found during delete", application_id)
            return False
        response.raise_for_status()
        return False

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

    def create_service_principal_secret(
        self,
        service_principal_id: str,
        *,
        secret_name: str,
    ) -> ServicePrincipalSecret:
        if self._service_principal_secret_exists(service_principal_id, secret_name):
            raise RuntimeError(
                f"Databricks service principal '{service_principal_id}' already has a secret named '{secret_name}'."
            )

        payload = {"secret_name": secret_name}
        response = self._account_request(
            "POST",
            f"/api/2.0/accounts/{self._config.account_id}/servicePrincipals/{service_principal_id}/credentials/secrets",
            json=payload,
        )
        if response.status_code >= 400:
            try:
                body = response.json()
            except ValueError:
                body = response.text
            raise RuntimeError(
                "Failed to create Databricks OAuth secret: "
                f"status={response.status_code}, body={body}"
            )
        payload = parse_json(response)
        secret_value = payload.get("secret_value") or payload.get("secret")
        if not secret_value:
            raise RuntimeError(
                "Databricks API did not return an OAuth client secret when creating service principal secret."
            )
        secret = ServicePrincipalSecret(
            client_id=service_principal_id,
            secret_id=str(payload.get("secret_id", "")),
            secret_value=secret_value,
            secret_name=secret_name,
        )
        logger.info(
            "Created Databricks OAuth secret '%s' (name='%s') for service principal '%s'",
            secret.secret_id or "<unknown>",
            secret_name,
            service_principal_id,
        )
        return secret

    def _service_principal_secret_exists(self, service_principal_id: str, secret_name: str) -> bool:
        response = self._account_request(
            "GET",
            f"/api/2.0/accounts/{self._config.account_id}/servicePrincipals/{service_principal_id}/credentials/secrets",
        )
        if response.status_code == 404:
            return False
        response.raise_for_status()
        payload = parse_json(response)
        secrets = payload.get("secrets") or payload.get("items") or []
        for item in secrets:
            if str(item.get("secret_name")) == secret_name:
                return True
        return False

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
