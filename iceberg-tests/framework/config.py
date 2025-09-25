import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError


class StorageConfig(BaseModel):
    warehouse_uri: Optional[str] = None
    staging_uri: Optional[str] = None
    account: Optional[str] = None
    container: Optional[str] = None


class CatalogConfig(BaseModel):
    name: str
    type: str
    description: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)


class EngineCatalogOverride(BaseModel):
    session_conf: Dict[str, Any] = Field(default_factory=dict)
    sql_variables: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)
    database: Optional[str] = None
    schema: Optional[str] = None


class EngineConfig(BaseModel):
    name: str
    type: str
    enabled: bool = True
    default_catalog: Optional[str] = None
    connection: Dict[str, Any] = Field(default_factory=dict)
    session_conf: Dict[str, Any] = Field(default_factory=dict)
    sql_variables: Dict[str, Any] = Field(default_factory=dict)
    catalog_overrides: Dict[str, EngineCatalogOverride] = Field(default_factory=dict)


class DatasetColumn(BaseModel):
    name: str
    type: str


class DatasetConfig(BaseModel):
    name: str
    rows: int
    columns: List[DatasetColumn]
    partition_spec: List[Dict[str, Any]] = Field(default_factory=list)
    sort_order: List[str] = Field(default_factory=list)
    table_properties: Dict[str, Any] = Field(default_factory=dict)


class TestCaseConfig(BaseModel):
    name: str
    description: Optional[str] = None
    scripts: Dict[str, Dict[str, str]]
    variables: Dict[str, Any] = Field(default_factory=dict)
    validations: List[Dict[str, Any]] = Field(default_factory=list)

    def resolve_script(self, engine: str, catalog: str) -> str:
        engine_map = self.scripts.get(engine) or self.scripts.get("*")
        if not engine_map:
            raise KeyError(f"No scripts registered for engine '{engine}' in test case '{self.name}'")
        script_path = engine_map.get(catalog) or engine_map.get("*")
        if not script_path:
            raise KeyError(
                f"No script for engine '{engine}' and catalog '{catalog}' in test case '{self.name}'"
            )
        return script_path


class PlanStepConfig(BaseModel):
    name: str
    test_case: str
    engine: str
    catalog: str
    dataset: Optional[str] = None
    variables: Dict[str, Any] = Field(default_factory=dict)
    continue_on_error: bool = False
    validations: List[Dict[str, Any]] = Field(default_factory=list)


class PlanConfig(BaseModel):
    name: str
    description: Optional[str] = None
    steps: List[PlanStepConfig]


class FrameworkConfig(BaseModel):
    storage: StorageConfig = Field(default_factory=StorageConfig)
    catalogs: Dict[str, CatalogConfig] = Field(default_factory=dict)
    engines: Dict[str, EngineConfig] = Field(default_factory=dict)
    datasets: Dict[str, DatasetConfig] = Field(default_factory=dict)
    test_cases: Dict[str, TestCaseConfig] = Field(default_factory=dict)
    plans: Dict[str, PlanConfig] = Field(default_factory=dict)


@dataclass
class ConfigBundle:
    root: Path
    framework: FrameworkConfig


def _expand_env_vars(raw_text: str) -> str:
    return os.path.expandvars(raw_text)


def load_framework_config(config_path: str) -> ConfigBundle:
    path = Path(config_path).expanduser().resolve()
    raw_text = path.read_text()
    expanded = _expand_env_vars(raw_text)
    data = yaml.safe_load(expanded) or {}

    try:
        framework = FrameworkConfig.model_validate(data)
    except ValidationError as exc:
        raise RuntimeError(f"Invalid framework configuration: {exc}") from exc

    return ConfigBundle(root=path.parent, framework=framework)
