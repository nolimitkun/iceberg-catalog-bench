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
        for key, value in list(connection.items()):
            if isinstance(value, str) and not value:
                connection.pop(key)
        if self.catalog_override.database and "database" not in connection:
            connection["database"] = self.catalog_override.database
        if self.catalog_override.schema_name and "schema" not in connection:
            connection["schema"] = self.catalog_override.schema_name
        connection.setdefault("client_session_keep_alive", True)

        token = connection.get("token")
        if token:
            if token.startswith("${") or token == "your_personal_access_token":
                raise RuntimeError(
                    "SNOWFLAKE_TOKEN is not set to a real programmatic access token. "
                    "Update your environment/.env with a valid PAT."
                )
            logger.debug("[snowflake] Using personal access token authentication")
            connection.setdefault("authenticator", "PROGRAMMATIC_ACCESS_TOKEN")
            connection.pop("password", None)

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
