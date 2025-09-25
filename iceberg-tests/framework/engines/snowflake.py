from __future__ import annotations

import logging
from typing import Iterable, List, Optional

import snowflake.connector
from snowflake.connector import DictCursor

from .base import EngineAdapter, StatementResult

logger = logging.getLogger(__name__)


class SnowflakeEngineAdapter(EngineAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        connection = dict(self.engine_config.connection)
        if self.catalog_override.database and "database" not in connection:
            connection["database"] = self.catalog_override.database
        if self.catalog_override.schema and "schema" not in connection:
            connection["schema"] = self.catalog_override.schema
        connection.setdefault("client_session_keep_alive", True)

        key_path = connection.pop("private_key_path", None)
        key_pass = connection.pop("private_key_passphrase", None)
        if key_path:
            logger.debug("[snowflake] Using key pair auth")
            from cryptography.hazmat.primitives import serialization

            with open(key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(), password=key_pass.encode() if key_pass else None
                )
            connection["private_key"] = private_key

        self.max_result_rows = int(connection.pop("max_result_rows", 200))
        self.ctx = snowflake.connector.connect(**connection)

    def execute(self, statements: Iterable[str]) -> List[StatementResult]:
        results: List[StatementResult] = []
        cursor: Optional[DictCursor] = None
        try:
            cursor = self.ctx.cursor(DictCursor)
            for statement in statements:
                logger.debug("[snowflake] Executing: %s", statement)
                cursor.execute(statement)
                if cursor.description:
                    fetched = cursor.fetchmany(self.max_result_rows)
                    rows = [dict(row) for row in fetched]
                else:
                    rows = None
                rowcount = cursor.rowcount if cursor.rowcount >= 0 else None
                results.append(StatementResult(statement=statement, rows=rows, rowcount=rowcount))
        finally:
            if cursor is not None:
                cursor.close()
        return results

    def close(self) -> None:
        logger.info("[snowflake] Closing connection")
        self.ctx.close()
