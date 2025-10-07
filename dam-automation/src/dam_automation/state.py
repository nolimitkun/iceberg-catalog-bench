"""State persistence for datasource automation."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional

from .models import DatasourceRecord, DatasourceRequest, DatasourceResources


def _serialize_datetime(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat() + "Z"


def _deserialize_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.rstrip("Z"))


def _record_to_json(record: DatasourceRecord) -> dict:
    payload = asdict(record)
    payload["resources"]["created_at"] = _serialize_datetime(record.resources.created_at)
    payload["updated_at"] = _serialize_datetime(record.updated_at)
    return payload


def _json_to_record(data: dict) -> DatasourceRecord:
    resources_payload = data["resources"]
    resources = DatasourceResources(
        container_url=resources_payload["container_url"],
        managed_identity_id=resources_payload["managed_identity_id"],
        storage_credential_name=resources_payload["storage_credential_name"],
        external_location_name=resources_payload["external_location_name"],
        catalog_name=resources_payload["catalog_name"],
        group_name=resources_payload["group_name"],
        service_principal_app_id=resources_payload["service_principal_app_id"],
        created_at=_deserialize_datetime(resources_payload["created_at"]),
    )
    request_payload = data["request"]
    request = DatasourceRequest(
        name=request_payload["name"],
        description=request_payload.get("description"),
        owner=request_payload.get("owner"),
        labels=request_payload.get("labels", {}),
    )
    record = DatasourceRecord(request=request, resources=resources)
    record.status = data.get("status", "succeeded")
    record.last_error = data.get("last_error")
    record.updated_at = _deserialize_datetime(data["updated_at"])
    return record


class StateStore:
    """Very small JSON file-backed state store."""

    def __init__(self, root_path: str | Path):
        self._root = Path(root_path).expanduser().resolve()
        self._root.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

    def _path_for(self, datasource_name: str) -> Path:
        safe_name = datasource_name.replace("/", "_")
        return self._root / f"{safe_name}.json"

    def get(self, datasource_name: str) -> Optional[DatasourceRecord]:
        path = self._path_for(datasource_name)
        if not path.exists():
            return None
        data = json.loads(path.read_text())
        return _json_to_record(data)

    def save(self, record: DatasourceRecord) -> None:
        path = self._path_for(record.request.name)
        with self._lock:
            payload = _record_to_json(record)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    def exists(self, datasource_name: str) -> bool:
        return self._path_for(datasource_name).exists()

    def list_records(self) -> list[DatasourceRecord]:
        records: list[DatasourceRecord] = []
        for path in self._root.glob("*.json"):
            data = json.loads(path.read_text())
            records.append(_json_to_record(data))
        return records
