"""Snowflake provisioning helpers for external volumes and catalog integrations."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Iterable, Optional

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError

from .config import SnowflakeConfig

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SnowflakeExternalVolume:
    name: str


@dataclass(slots=True)
class SnowflakeCatalogIntegration:
    name: str


@dataclass(slots=True)
class SnowflakeCatalogLinkedDatabase:
    name: str


@dataclass(slots=True)
class SnowflakeDropSummary:
    external_volume_dropped: bool
    catalog_integration_dropped: bool
    database_dropped: bool


class SnowflakeAuthorizationError(RuntimeError):
    """Raised when Snowflake cannot authenticate against the Polaris REST endpoint."""


class SnowflakeIntegrationInUseError(RuntimeError):
    """Raised when a catalog integration cannot be replaced due to active dependencies."""


class SnowflakeProvisioner:
    """Executes Snowflake DDL for datasources."""

    def __init__(self, config: SnowflakeConfig) -> None:
        self._config = config

    @contextmanager
    def _cursor(self) -> Generator[SnowflakeCursor, None, None]:
        connect_args = {
            "account": self._config.account,
            "user": self._config.user,
            "password": self._config.password,
        }
        if self._config.role:
            connect_args["role"] = self._config.role
        if self._config.warehouse:
            connect_args["warehouse"] = self._config.warehouse
        if self._config.database:
            connect_args["database"] = self._config.database
        if self._config.default_schema:
            connect_args["schema"] = self._config.default_schema

        logger.debug("Connecting to Snowflake account '%s' as user '%s'", self._config.account, self._config.user)
        connection: SnowflakeConnection = snowflake.connector.connect(**connect_args)
        cursor: Optional[SnowflakeCursor] = None
        try:
            cursor = connection.cursor()
            yield cursor
        finally:
            if cursor is not None:
                cursor.close()
            connection.close()

    def ensure_external_volume(
        self,
        name: str,
        storage_base_url: str,
        tenant_id: str,
    ) -> SnowflakeExternalVolume:
        volume_identifier = name.upper()
        with self._cursor() as cursor:
            if self._external_volume_exists(cursor, volume_identifier):
                logger.info("Snowflake external volume '%s' already exists; skipping creation", volume_identifier)
                return SnowflakeExternalVolume(name=volume_identifier)

            logger.info("Creating Snowflake external volume '%s'", volume_identifier)
            ddl = "\n".join(
                [
                    f"CREATE EXTERNAL VOLUME {volume_identifier}",
                    "  STORAGE_LOCATIONS = (",
                    "    (",
                    f"      NAME = {self._quote_literal(volume_identifier)}",
                    "      STORAGE_PROVIDER = 'AZURE'",
                    f"      STORAGE_BASE_URL = {self._quote_literal(storage_base_url)}",
                    f"      AZURE_TENANT_ID = {self._quote_literal(tenant_id)}",
                    "    )",
                    "  )",
                ]
            )
            self._log_ddl(ddl, [])
            try:
                cursor.execute(ddl)
            except ProgrammingError as exc:
                if self._is_existing_object_error(exc):
                    logger.info("Snowflake external volume '%s' already exists (caught via exception); continuing", volume_identifier)
                    return SnowflakeExternalVolume(name=volume_identifier)
                raise
        return SnowflakeExternalVolume(name=volume_identifier)

    def ensure_catalog_integration(
        self,
        name: str,
        catalog_name: str,
        catalog_uri: str,
        client_id: str,
        client_secret: str,
        token_endpoint: str,
        scopes: Iterable[str],
        catalog_source: str,
        table_format: str,
    ) -> SnowflakeCatalogIntegration:
        integration_identifier = name.upper()
        with self._cursor() as cursor:
            if self._catalog_integration_exists(cursor, integration_identifier):
                logger.info("Snowflake catalog integration '%s' already exists; skipping creation", integration_identifier)
                return SnowflakeCatalogIntegration(name=integration_identifier)

            logger.info("Creating Snowflake catalog integration '%s'", integration_identifier)
            scope_literals = ", ".join(self._quote_literal(scope) for scope in scopes)
            rest_config_lines = [
                "  REST_CONFIG = (",
                f"    CATALOG_URI = {self._quote_literal(catalog_uri)}",
                f"    CATALOG_NAME = {self._quote_literal(catalog_name)}"
                "  )",
            ]
            rest_auth_lines = [
                "  REST_AUTHENTICATION = (",
                "    TYPE = OAUTH",
                f"    OAUTH_CLIENT_ID = {self._quote_literal(client_id)}",
                f"    OAUTH_CLIENT_SECRET = {self._quote_literal(client_secret)}",
                f"    OAUTH_ALLOWED_SCOPES = ({scope_literals})" if scope_literals else "    OAUTH_ALLOWED_SCOPES = ()",
                f"    OAUTH_TOKEN_URI = {self._quote_literal(token_endpoint)}",
                "  )",
            ]
            ddl = "\n".join(
                [
                    f"CREATE OR REPLACE CATALOG INTEGRATION {integration_identifier}",
                    f"  CATALOG_SOURCE = {catalog_source}",
                    f"  TABLE_FORMAT = {table_format}",
                    *rest_config_lines,
                    *rest_auth_lines,
                    "  ENABLED = TRUE"
                ]
            )
            logger.info("Executing Snowflake SQL:\n%s", ddl)
            self._log_ddl(ddl, [client_secret])
            try:
                cursor.execute(ddl)
            except ProgrammingError as exc:
                if self._is_existing_object_error(exc):
                    logger.info("Snowflake catalog integration '%s' already exists (via exception); continuing", integration_identifier)
                    return SnowflakeCatalogIntegration(name=integration_identifier)
                if self._is_integration_in_use_error(exc):
                    raise SnowflakeIntegrationInUseError(
                        f"Snowflake catalog integration '{integration_identifier}' is currently in use"
                    ) from exc
                raise
        return SnowflakeCatalogIntegration(name=integration_identifier)
 
    def cleanup_catalog_linked_artifacts(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
    ) -> None:
        db_identifier = database_name.upper()
        schema_identifier = schema_name.upper()
        table_identifier = table_name.upper()

        with self._cursor() as cursor:
            drop_table = f"DROP TABLE IF EXISTS {db_identifier}.{schema_identifier}.{table_identifier}"
            drop_schema = f"DROP SCHEMA IF EXISTS {db_identifier}.{schema_identifier}"
            drop_database = f"DROP DATABASE IF EXISTS {db_identifier}"

            for statement in (drop_table, drop_schema, drop_database):
                self._log_ddl(statement, [])
                try:
                    cursor.execute(statement)
                except ProgrammingError as exc:
                    logger.warning("Failed to execute cleanup statement '%s': %s", statement, exc)

    def ensure_catalog_linked_database(
        self,
        database_name: str,
        integration_name: str,
        external_volume_name: str,
        namespace_mode: str,
        namespace_delimiter: str,
        allowed_namespaces: Optional[Iterable[str]] = None,
    ) -> SnowflakeCatalogLinkedDatabase:
        with self._cursor() as cursor:
            if self._database_exists(cursor, database_name):
                logger.info("Snowflake catalog-linked database '%s' already exists", database_name)
                return SnowflakeCatalogLinkedDatabase(name=database_name)

            logger.info("Creating Snowflake catalog-linked database '%s'", database_name)
            linked_catalog_lines = [
                "  LINKED_CATALOG = (",
                f"    CATALOG = {integration_name}",
                f"    NAMESPACE_MODE = {namespace_mode}",
                f"    NAMESPACE_FLATTEN_DELIMITER = {self._quote_literal(namespace_delimiter)}",
            ]
            if allowed_namespaces:
                namespace_literals = ", ".join(self._quote_literal(ns) for ns in allowed_namespaces)
                linked_catalog_lines.append(f"    ALLOWED_NAMESPACES = ({namespace_literals})")
            linked_catalog_lines.append("  )")

            ddl_lines = [
                f"CREATE DATABASE {database_name}",
                *linked_catalog_lines,
                f"  EXTERNAL_VOLUME = {external_volume_name}",
            ]
            ddl = "\n".join(ddl_lines)
            self._log_ddl(ddl, [])
            try:
                cursor.execute(ddl)
            except ProgrammingError as exc:
                if self._is_existing_object_error(exc):
                    logger.info("Snowflake catalog-linked database '%s' already exists (via exception)", database_name)
                    return SnowflakeCatalogLinkedDatabase(name=database_name)
                if self._is_authorization_error(exc):
                    raise SnowflakeAuthorizationError(
                        f"Snowflake catalog integration '{integration_name}' could not authenticate with provided client credentials"
                    ) from exc
                raise
        return SnowflakeCatalogLinkedDatabase(name=database_name)

    def prime_catalog_linked_database(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
    ) -> None:
        statements = [
            f"USE DATABASE {database_name}",
            f"CREATE SCHEMA IF NOT EXISTS {schema_name}",
            f"USE SCHEMA {schema_name}",
            "\n".join(
                [
                    f"CREATE OR REPLACE ICEBERG TABLE {table_name}",
                    "  (",
                    "    first_name STRING,",
                    "    last_name STRING,",
                    "    amount INT,",
                    "    create_date DATE",
                    "  )",
                    "PARTITION BY (first_name)",
                    "TARGET_FILE_SIZE = '64MB'",
                ]
            ),
            (
                f"INSERT INTO {table_name} "
                "VALUES ('kun', 'xue', 100, '2025-05-06')"
            ),
        ]
        with self._cursor() as cursor:
            for statement in statements:
                self._log_ddl(statement, [])
                cursor.execute(statement)
            select_sql = f"SELECT * FROM {table_name}"
            self._log_ddl(select_sql, [])
            cursor.execute(select_sql)
            rows = cursor.fetchmany(5)
            if rows:
                logger.debug("Preview rows for '%s.%s.%s': %s", database_name, schema_name, table_name, rows)

    def drop_objects(
        self,
        database_name: str,
        catalog_integration_name: str,
        external_volume_name: str,
    ) -> SnowflakeDropSummary:
        dropped_database = False
        dropped_integration = False
        dropped_external_volume = False

        with self._cursor() as cursor:
            if self._database_exists(cursor, database_name):
                sql = f"DROP DATABASE IF EXISTS {database_name} CASCADE"
                self._log_ddl(sql, [])
                cursor.execute(sql)
                dropped_database = True
            else:
                logger.info("Snowflake catalog-linked database '%s' not found; skipping drop", database_name)

            if self._catalog_integration_exists(cursor, catalog_integration_name):
                sql = f"DROP CATALOG INTEGRATION IF EXISTS {catalog_integration_name}"
                self._log_ddl(sql, [])
                cursor.execute(sql)
                dropped_integration = True
            else:
                logger.info("Snowflake catalog integration '%s' not found; skipping drop", catalog_integration_name)

            if self._external_volume_exists(cursor, external_volume_name):
                sql = f"DROP EXTERNAL VOLUME IF EXISTS {external_volume_name}"
                self._log_ddl(sql, [])
                cursor.execute(sql)
                dropped_external_volume = True
            else:
                logger.info("Snowflake external volume '%s' not found; skipping drop", external_volume_name)

        return SnowflakeDropSummary(
            external_volume_dropped=dropped_external_volume,
            catalog_integration_dropped=dropped_integration,
            database_dropped=dropped_database,
        )

    def _external_volume_exists(self, cursor: SnowflakeCursor, name: str) -> bool:
        search = name.upper()
        cursor.execute(f"SHOW EXTERNAL VOLUMES LIKE {self._quote_literal(search)}")
        for row in cursor.fetchall():
            if str(row[1]).upper() == search:
                return True
        return False

    def _catalog_integration_exists(self, cursor: SnowflakeCursor, name: str) -> bool:
        search = name.upper()
        cursor.execute(f"SHOW CATALOG INTEGRATIONS LIKE {self._quote_literal(search)}")
        for row in cursor.fetchall():
            if str(row[1]).upper() == search:
                return True
        return False

    def _database_exists(self, cursor: SnowflakeCursor, name: str) -> bool:
        cursor.execute(f"SHOW DATABASES LIKE {self._quote_literal(name)}")
        for row in cursor.fetchall():
            if str(row[1]).upper() == name.upper():
                return True
        return False

    @staticmethod
    def _quote_literal(value: str) -> str:
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _is_existing_object_error(exc: ProgrammingError) -> bool:
        if exc.sqlstate == "42710":
            return True
        message = (exc.msg or "").lower()
        return "already exists" in message

    @staticmethod
    def _is_authorization_error(exc: ProgrammingError) -> bool:
        message = (exc.msg or "").lower()
        return "invalid_client" in message or "not authorized" in message

    @staticmethod
    def _is_integration_in_use_error(exc: ProgrammingError) -> bool:
        message = (exc.msg or "").lower()
        return "cannot be replaced" in message and "catalog integration" in message

    @staticmethod
    def _log_ddl(ddl: str, sensitive_values: Iterable[str]) -> None:
        sanitized = ddl
        for value in sensitive_values:
            if value:
                sanitized = sanitized.replace(value, "***")
        logger.debug("Executing Snowflake SQL:\n%s", sanitized)
