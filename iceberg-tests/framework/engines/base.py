from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..config import CatalogConfig, EngineCatalogOverride, EngineConfig
from ..sql import render_sql_statements


logger = logging.getLogger(__name__)


@dataclass
class StatementResult:
    statement: str
    rows: Optional[List[Dict[str, Any]]] = None
    rowcount: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionResult:
    step_name: str
    statements: List[StatementResult]


class EngineAdapter:
    def __init__(
        self,
        name: str,
        engine_config: EngineConfig,
        catalog_config: CatalogConfig,
        catalog_override: Optional[EngineCatalogOverride],
        config_root: Path,
    ) -> None:
        self.name = name
        self.engine_config = engine_config
        self.catalog_config = catalog_config
        self.catalog_override = catalog_override or EngineCatalogOverride()
        self.config_root = config_root
        self.catalog_context: Dict[str, Any] = self.catalog_override.model_dump()
        self.base_variables: Dict[str, Any] = {}

    def configure(self, variables: Dict[str, Any]) -> None:
        self.base_variables = variables
        override_context = variables.get("catalog_override")
        if isinstance(override_context, dict):
            self.catalog_context = override_context
        else:
            self.catalog_context = self.catalog_override.model_dump()
        self.on_configure(variables)

    def on_configure(self, variables: Dict[str, Any]) -> None:
        """Hook for adapters that need to react to configuration changes."""

    def render_statements(self, sql_path: str, variables: Dict[str, Any]) -> List[str]:
        merged = {**self.base_variables, **variables}
        return render_sql_statements(self.config_root, sql_path, merged)

    def run(self, step_name: str, sql_path: str, variables: Dict[str, Any]) -> ExecutionResult:
        statements = self.render_statements(sql_path, variables)
        logger.debug("[%s] Executing %d statements from %s", self.name, len(statements), sql_path)
        results = self.execute(statements)
        return ExecutionResult(step_name=step_name, statements=results)

    def execute(self, statements: Iterable[str]) -> List[StatementResult]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class EngineFactory:
    def __init__(self, config_root: Path, engines: Dict[str, EngineConfig], catalogs: Dict[str, CatalogConfig]):
        self.config_root = config_root
        self.engines = engines
        self.catalogs = catalogs
        self._registry: Dict[str, type[EngineAdapter]] = {}
        self._cache: Dict[tuple[str, str], EngineAdapter] = {}

    def register(self, engine_type: str, adapter_cls: type[EngineAdapter]) -> None:
        self._registry[engine_type] = adapter_cls

    def get(self, engine_name: str, catalog_name: str) -> EngineAdapter:
        key = (engine_name, catalog_name)
        if key in self._cache:
            return self._cache[key]

        engine_config = self.engines.get(engine_name)
        if not engine_config:
            raise KeyError(f"Unknown engine '{engine_name}'")
        if not engine_config.enabled:
            raise RuntimeError(f"Engine '{engine_name}' is disabled in configuration")

        catalog_config = self.catalogs.get(catalog_name)
        if not catalog_config:
            raise KeyError(f"Unknown catalog '{catalog_name}'")

        adapter_cls = self._registry.get(engine_config.type)
        if not adapter_cls:
            raise KeyError(f"No adapter registered for engine type '{engine_config.type}'")

        adapter = adapter_cls(
            engine_name,
            engine_config,
            catalog_config,
            engine_config.catalog_overrides.get(catalog_name),
            self.config_root,
        )
        self._cache[key] = adapter
        return adapter

    def close_all(self) -> None:
        for adapter in self._cache.values():
            adapter.close()
        self._cache.clear()
