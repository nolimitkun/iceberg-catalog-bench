from __future__ import annotations

import logging
from typing import Iterable, List, Optional

from databricks import sql

from .base import EngineAdapter, StatementResult

logger = logging.getLogger(__name__)


class DatabricksEngineAdapter(EngineAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        connection = dict(self.engine_config.connection)
        options = dict(self.catalog_override.options)

        server_hostname = connection.get("host") or connection.get("server_hostname")
        http_path = connection.get("http_path") or options.get("http_path")
        access_token = connection.get("token") or connection.get("access_token")
        catalog = options.get("catalog") or self.catalog_override.database
        schema = options.get("schema") or self.catalog_override.schema_name

        if not server_hostname or not http_path or not access_token:
            raise RuntimeError("Databricks connection requires host/http_path/token")

        kwargs = {
            "server_hostname": server_hostname,
            "http_path": http_path,
            "access_token": access_token,
        }
        if catalog:
            kwargs["catalog"] = catalog
        if schema:
            kwargs["schema"] = schema

        self.max_result_rows = int(connection.get("max_result_rows", 200))
        logger.info("[databricks] Connecting to %s http_path=%s", server_hostname, http_path)
        self.conn = sql.connect(**kwargs)

    def execute(self, statements: Iterable[str]) -> List[StatementResult]:
        results: List[StatementResult] = []
        cursor = self.conn.cursor()
        try:
            for statement in statements:
                logger.debug("[databricks] Executing: %s", statement)
                cursor.execute(statement)
                description = cursor.description
                if description:
                    columns = [col[0] for col in description]
                    fetched = cursor.fetchmany(self.max_result_rows)
                    rows = [dict(zip(columns, row)) for row in fetched]
                else:
                    rows = None
                rowcount = cursor.rowcount if cursor.rowcount >= 0 else None
                results.append(StatementResult(statement=statement, rows=rows, rowcount=rowcount))
        finally:
            cursor.close()
        return results

    def close(self) -> None:
        logger.info("[databricks] Closing connection")
        self.conn.close()
