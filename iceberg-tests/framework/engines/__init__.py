from __future__ import annotations

from pathlib import Path

from .base import EngineFactory
from .databricks import DatabricksEngineAdapter
from .snowflake import SnowflakeEngineAdapter
from .spark import SparkEngineAdapter

__all__ = ["EngineFactory", "create_engine_factory"]


def create_engine_factory(config_root: Path, framework_config) -> EngineFactory:
    factory = EngineFactory(config_root, framework_config.engines, framework_config.catalogs)
    factory.register("spark", SparkEngineAdapter)
    factory.register("snowflake", SnowflakeEngineAdapter)
    factory.register("databricks", DatabricksEngineAdapter)
    return factory
