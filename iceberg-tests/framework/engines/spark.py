from __future__ import annotations

import logging
from typing import Iterable, List

from pyspark.sql import SparkSession

from .base import EngineAdapter, StatementResult

logger = logging.getLogger(__name__)


_OUTPUTLESS_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "CREATE",
    "DROP",
    "ALTER",
    "OPTIMIZE",
    "VACUUM",
    "TRUNCATE",
    "USE",
    "SET",
    "CALL",
    "CACHE",
    "UNCACHE",
}


class SparkEngineAdapter(EngineAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        connection = self.engine_config.connection or {}
        session_conf = dict(self.engine_config.session_conf)
        session_conf.update(self.catalog_override.session_conf)
        session_conf.update(connection.get("conf", {}))

        app_name = connection.get("app_name", "IcebergInteropSpark")
        master = connection.get("master")

        builder = SparkSession.builder.appName(app_name)
        if master:
            builder = builder.master(master)
        for key, value in session_conf.items():
            builder = builder.config(key, value)

        logger.info("[spark] Starting SparkSession app=%s master=%s", app_name, master or "(default)")
        self.spark = builder.getOrCreate()
        self.max_result_rows = int(connection.get("max_result_rows", 200))
        self.capture_rowcount = bool(connection.get("capture_rowcount", False))

    def _should_capture(self, statement: str) -> bool:
        stripped = statement.lstrip()
        keyword = stripped.split(" ", 1)[0].upper() if stripped else ""
        return keyword not in _OUTPUTLESS_KEYWORDS

    def execute(self, statements: Iterable[str]) -> List[StatementResult]:
        results: List[StatementResult] = []
        for statement in statements:
            logger.debug("[spark] Executing: %s", statement)
            df = self.spark.sql(statement)
            if self._should_capture(statement):
                rows_collected = df.take(self.max_result_rows)
                rows = [row.asDict(recursive=True) for row in rows_collected]
                rowcount = df.count() if self.capture_rowcount else len(rows)
            else:
                df.collect()
                rows = None
                rowcount = None
            results.append(StatementResult(statement=statement, rows=rows, rowcount=rowcount))
        return results

    def close(self) -> None:
        logger.info("[spark] Stopping SparkSession")
        self.spark.stop()
