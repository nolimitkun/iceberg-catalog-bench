from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from dam_automation.main import app as cli_app
from dam_automation.models import (
    DatasourceRecord,
    DatasourceRequest,
    DatasourceResources,
    DeletionOutcome,
    DatasourceDeletionResult,
)
from dam_automation.service import DatasourceAutomationService
from dam_automation.snowflake import SnowflakeDropSummary


def _sample_record(name: str = "example") -> DatasourceRecord:
    request = DatasourceRequest(name=name)
    resources = DatasourceResources(
        container_url="abfss://example@acct.dfs.core.windows.net/",
        managed_identity_id="identity",
        storage_credential_name="cred",
        external_location_name="location",
        catalog_name="catalog",
        group_name="group",
        service_principal_app_id="sp-app",
        service_principal_client_secret="secret",
        databricks_oauth_client_secret="db-secret",
        snowflake_external_volume_name="volume",
        snowflake_catalog_integration_name="integration",
        snowflake_database_name="database",
    )
    return DatasourceRecord(request=request, resources=resources)


def _patch_service(monkeypatch, factory):
    monkeypatch.setattr("dam_automation.main.DatasourceAutomationService", factory)


def _patch_config(monkeypatch, config_obj) -> None:
    monkeypatch.setattr("dam_automation.main.load_config", lambda _: config_obj)


def test_create_datasource_command_outputs_json(tmp_path, monkeypatch) -> None:
    runner = CliRunner()
    config_path = Path(tmp_path) / "config.yaml"
    config_path.write_text("dummy: true")

    record = _sample_record()

    class DummyService(DatasourceAutomationService):  # type: ignore[misc]
        def __init__(self, cfg) -> None:
            self.cfg = cfg

        def create_datasource(self, request: DatasourceRequest) -> DatasourceRecord:
            return record

    _patch_config(monkeypatch, config_obj={"dummy": True})
    _patch_service(monkeypatch, DummyService)

    result = runner.invoke(
        cli_app,
        ["create-datasource", "example", "--config", str(config_path)],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["datasource"] == "example"
    assert payload["catalog"] == "catalog"
    assert payload["storage_credential"] == "cred"


def test_drop_snowflake_command_reports_summary(tmp_path, monkeypatch) -> None:
    runner = CliRunner()
    config_path = Path(tmp_path) / "config.yaml"
    config_path.write_text("dummy: true")

    summary = SnowflakeDropSummary(
        external_volume_dropped=True,
        catalog_integration_dropped=False,
        database_dropped=True,
    )

    class DummyService(DatasourceAutomationService):  # type: ignore[misc]
        def __init__(self, cfg) -> None:
            self.cfg = cfg

        def drop_datasource(self, name: str) -> SnowflakeDropSummary:
            assert name == "example"
            return summary

    _patch_config(monkeypatch, config_obj={"dummy": True})
    _patch_service(monkeypatch, DummyService)

    result = runner.invoke(
        cli_app,
        ["drop-snowflake", "example", "--config", str(config_path)],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["datasource"] == "example"
    assert payload["snowflake_database_dropped"] is True
    assert payload["snowflake_catalog_integration_dropped"] is False
    assert payload["snowflake_external_volume_dropped"] is True


def test_delete_datasource_command_handles_state_missing(tmp_path, monkeypatch) -> None:
    runner = CliRunner()
    config_path = Path(tmp_path) / "config.yaml"
    config_path.write_text("dummy: true")

    deletion_result = DatasourceDeletionResult(
        input_name="example",
        normalized_name="example",
        state_record_name="example",
        state_deleted=True,
        state_found=False,
        azure=DeletionOutcome(True, None),
        identity=DeletionOutcome(True, None),
        databricks=DeletionOutcome(True, None),
        snowflake=DeletionOutcome(True, None),
    )

    class DummyService(DatasourceAutomationService):  # type: ignore[misc]
        def __init__(self, cfg) -> None:
            self.cfg = cfg

        def delete_datasource(self, name: str) -> DatasourceDeletionResult:
            assert name == "example"
            return deletion_result

    _patch_config(monkeypatch, config_obj={"dummy": True})
    _patch_service(monkeypatch, DummyService)

    result = runner.invoke(
        cli_app,
        ["delete-datasource", "example", "--config", str(config_path)],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["datasource"] == "example"
    assert payload["state_found"] is False
    assert "State record not found" in result.stderr
