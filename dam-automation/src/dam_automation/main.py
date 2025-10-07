"""CLI entrypoint for DAM automation."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import typer

from .config import load_config
from .models import DatasourceRequest
from .service import DatasourceAutomationService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

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


if __name__ == "__main__":
    app()
