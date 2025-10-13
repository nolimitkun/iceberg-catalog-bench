"""CLI entrypoint for DAM automation."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

import typer

from .config import load_config
from .models import DatasourceRequest
from .service import DatasourceAutomationService


def _configure_logging() -> None:
    env_level = os.getenv("DAM_AUTOMATION_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, env_level, None)
    if not isinstance(level, int):
        level = logging.INFO
        logging.warning(
            "Unrecognized DAM_AUTOMATION_LOG_LEVEL '%s'; defaulting to INFO",
            env_level,
        )
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


_configure_logging()

app = typer.Typer(help="Databricks & Azure lakehouse automation")


@app.command("create-datasource")
def create_datasource(
    name: str,
    config: Path = typer.Option(..., exists=True, readable=True, help="Path to automation config YAML"),
    description: Optional[str] = typer.Option(None, help="Optional datasource description"),
    owner: Optional[str] = typer.Option(None, help="Owner email or UPN"),
) -> None:
    """Provision all resources required for a new datasource."""

    automation_config = load_config(config)
    service = DatasourceAutomationService(automation_config)
    request = DatasourceRequest(name=name, description=description, owner=owner)
    record = service.create_datasource(request)
    typer.echo(
        json.dumps(
            {
                "datasource": record.request.name,
                "status": record.status,
                "catalog": record.resources.catalog_name,
                "group": record.resources.group_name,
                "external_location": record.resources.external_location_name,
                "storage_credential": record.resources.storage_credential_name,
            },
            indent=2,
        )
    )


@app.command("drop-snowflake")
def drop_snowflake_objects(
    name: str,
    config: Path = typer.Option(..., exists=True, readable=True, help="Path to automation config YAML"),
) -> None:
    """Drop Snowflake database, catalog integration, and external volume for a datasource."""

    automation_config = load_config(config)
    service = DatasourceAutomationService(automation_config)
    try:
        summary = service.drop_datasource(name)
    except ValueError as exc:  # pragma: no cover - CLI feedback
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(
        json.dumps(
            {
                "datasource": name,
                "snowflake_database_dropped": summary.database_dropped,
                "snowflake_catalog_integration_dropped": summary.catalog_integration_dropped,
                "snowflake_external_volume_dropped": summary.external_volume_dropped,
            },
            indent=2,
        )
    )


@app.command("delete-datasource")
def delete_datasource(
    name: str,
    config: Path = typer.Option(..., exists=True, readable=True, help="Path to automation config YAML"),
) -> None:
    """Tear down provisioned resources and remove the datasource from automation state."""

    automation_config = load_config(config)
    service = DatasourceAutomationService(automation_config)
    try:
        result = service.delete_datasource(name)
    except ValueError as exc:  # pragma: no cover - CLI feedback
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    except RuntimeError as exc:  # pragma: no cover - defensive path
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    if not result.state_found:
        typer.secho(
            "State record not found; attempting deletion with inferred resource names.",
            fg=typer.colors.YELLOW,
            err=True,
        )

    deletion_summary = {
        "datasource": result.input_name,
        "normalized_name": result.normalized_name,
        "state_record_name": result.state_record_name,
        "state_deleted": result.state_deleted,
        "state_found": result.state_found,
        "azure": {"succeeded": result.azure.succeeded, "message": result.azure.message},
        "identity": {"succeeded": result.identity.succeeded, "message": result.identity.message},
        "databricks": {"succeeded": result.databricks.succeeded, "message": result.databricks.message},
        "snowflake": {"succeeded": result.snowflake.succeeded, "message": result.snowflake.message},
    }
    typer.echo(json.dumps(deletion_summary, indent=2))

    all_subsystems_succeeded = all(
        outcome["succeeded"]
        for outcome in (
            deletion_summary["azure"],
            deletion_summary["identity"],
            deletion_summary["databricks"],
            deletion_summary["snowflake"],
        )
    )
    if not all_subsystems_succeeded:
        raise typer.Exit(code=1)

    if not result.state_deleted:
        typer.secho(
            "Datasource state was not removed; investigate and rerun deletion after resolving issues.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
