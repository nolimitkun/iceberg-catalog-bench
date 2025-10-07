"""Azure resource provisioning helpers."""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict

import requests

from .auth import ClientCredentialProvider
from .config import AzureConfig
from .http import parse_json

logger = logging.getLogger(__name__)

AZURE_MANAGEMENT_SCOPE = "https://management.azure.com/.default"
ROLE_STORAGE_BLOB_DATA_CONTRIBUTOR = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"


@dataclass(slots=True)
class AzureContainer:
    name: str
    blob_url: str
    abfss_url: str
    resource_id: str


@dataclass(slots=True)
class AzureIdentity:
    name: str
    client_id: str
    principal_id: str
    resource_id: str


class AzureProvisioner:
    """Performs Azure-side provisioning for datasources."""

    def __init__(self, config: AzureConfig, credential_provider: ClientCredentialProvider) -> None:
        self._config = config
        self._credentials = credential_provider
        self._mgmt_url = "https://management.azure.com"

    def ensure_container(self, container_name: str, tags: Dict[str, str]) -> AzureContainer:
        resource_id = self._container_resource_id(container_name)
        url = f"{self._mgmt_url}{resource_id}?api-version=2023-01-01"
        body = {
            "properties": {"publicAccess": "None"},
            "tags": tags,
        }
        logger.info("Ensuring ADLS container '%s'", container_name)
        response = self._authorized_request("PUT", url, json=body)
        if response.status_code not in {200, 201}:
            logger.error("Container creation failed: %s", response.text)
            response.raise_for_status()
        account = self._config.storage_account
        dns = self._config.data_plane_dns_suffix
        return AzureContainer(
            name=container_name,
            blob_url=f"https://{account}.blob.core.windows.net/{container_name}",
            abfss_url=f"abfss://{container_name}@{account}.{dns}/",
            resource_id=resource_id,
        )

    def ensure_user_assigned_identity(self, identity_name: str, tags: Dict[str, str]) -> AzureIdentity:
        resource_id = self._identity_resource_id(identity_name)
        url = f"{self._mgmt_url}{resource_id}?api-version=2023-01-31"
        body = {
            "location": self._config.location,
            "tags": tags,
        }
        logger.info("Ensuring user-assigned managed identity '%s'", identity_name)
        response = self._authorized_request("PUT", url, json=body)
        if response.status_code not in {200, 201}:
            logger.error("Managed identity creation failed: %s", response.text)
            response.raise_for_status()
        payload = parse_json(response)
        return AzureIdentity(
            name=identity_name,
            client_id=payload["properties"]["clientId"],
            principal_id=payload["properties"]["principalId"],
            resource_id=payload["id"],
        )

    def ensure_storage_account_role_assignment(self, principal_id: str) -> str:
        """Guarantee the principal can access the entire storage account."""
        return self.ensure_role_assignment(principal_id, self._storage_account_resource_id())

    def ensure_role_assignment(self, principal_id: str, scope: str, role_definition_id: str = ROLE_STORAGE_BLOB_DATA_CONTRIBUTOR) -> str:
        assignment_id = str(uuid.uuid4())
        url = (
            f"{self._mgmt_url}{scope}/providers/Microsoft.Authorization/roleAssignments/{assignment_id}"
            "?api-version=2022-04-01"
        )
        body = {
            "properties": {
                "roleDefinitionId": f"/subscriptions/{self._config.subscription_id}/providers/Microsoft.Authorization/roleDefinitions/{role_definition_id}",
                "principalId": principal_id,
                "principalType": "ServicePrincipal",
            }
        }
        logger.info("Assigning role '%s' on scope '%s'", role_definition_id, scope)
        response = self._authorized_request("PUT", url, json=body)
        if response.status_code not in {200, 201}:
            if response.status_code == 409:
                logger.info("Role assignment already exists for principal '%s'", principal_id)
                return assignment_id
            logger.error("Role assignment failed: %s", response.text)
            response.raise_for_status()
        return assignment_id

    def _authorized_request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        token = self._credentials.acquire_token(AZURE_MANAGEMENT_SCOPE)
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        headers.setdefault("Content-Type", "application/json")
        headers.setdefault("Accept", "application/json")
        response = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        return response

    def attach_identity_to_access_connector(self, access_connector_id: str, identity_resource_id: str) -> None:
        """Ensure the user-assigned identity is linked to the Databricks access connector."""

        api_version = "2023-05-01"
        url = f"{self._mgmt_url}{access_connector_id}?api-version={api_version}"

        get_response = self._authorized_request("GET", url)
        if get_response.status_code >= 400:
            logger.error(
                "Failed to fetch access connector '%s': %s",
                access_connector_id,
                get_response.text,
            )
            get_response.raise_for_status()

        connector = get_response.json()
        identity_block = connector.get("identity", {}) or {}
        current_assignments = identity_block.get("userAssignedIdentities", {}) or {}

        if identity_resource_id in current_assignments:
            logger.info(
                "Managed identity '%s' already linked to access connector '%s'",
                identity_resource_id,
                access_connector_id,
            )
            return

        logger.info(
            "Linking managed identity '%s' to access connector '%s'",
            identity_resource_id,
            access_connector_id,
        )

        current_assignments[identity_resource_id] = {}
        existing_type = identity_block.get("type", "")
        parts = {part.strip() for part in existing_type.split(",") if part.strip()}
        parts.add("UserAssigned")
        identity_block["type"] = ",".join(sorted(parts)) if parts else "UserAssigned"
        identity_block["userAssignedIdentities"] = current_assignments

        patch_body: Dict[str, Any] = {
            "identity": identity_block,
        }

        patch_response = self._authorized_request("PATCH", url, json=patch_body)
        if patch_response.status_code >= 400:
            logger.error(
                "Failed to link managed identity to access connector: %s",
                patch_response.text,
            )
            patch_response.raise_for_status()

    def _container_resource_id(self, container_name: str) -> str:
        return (
            f"/subscriptions/{self._config.subscription_id}"
            f"/resourceGroups/{self._config.resource_group}"
            f"/providers/Microsoft.Storage/storageAccounts/{self._config.storage_account}"
            f"/blobServices/default/containers/{container_name}"
        )

    def _storage_account_resource_id(self) -> str:
        return (
            f"/subscriptions/{self._config.subscription_id}"
            f"/resourceGroups/{self._config.resource_group}"
            f"/providers/Microsoft.Storage/storageAccounts/{self._config.storage_account}"
        )

    def _identity_resource_id(self, identity_name: str) -> str:
        return (
            f"/subscriptions/{self._config.subscription_id}"
            f"/resourceGroups/{self._config.identity_resource_group}"
            f"/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{identity_name}"
        )
