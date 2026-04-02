"""MySQL backend connector — production implementation using pymysql with pooling (F13)."""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Optional

from metamind.core.backends.connector import (
    BackendConnector,
    ConnectorCapabilities,
    ConnectorConnectionError,
    ConnectorExecutionError,
    ConnectorTimeoutError,
    ConnectionConfig,
    QueryResult,
)

logger = logging.getLogger(__name__)

_CAPABILITIES = ConnectorCapabilities(
    supports_aggregation=True,
    supports_window_functions=True,
    supports_cte=True,
    supports_lateral=False,
    supports_unnest=False,
    supports_hash_join=True,
    supports_merge_join=False,
    supports_json_ops=True,
    supports_materialized_views=False,
    is_distributed=False,
    dialect="mysql",
    max_concurrent_queries=150,
    cost_per_gb_scan=0.0,
)


class _SimpleConnectionPool:
    """Minimal connection pool for MySQL using pymysql."""

    def __init__(self, min_conn: int, max_conn: int, **connect_kwargs: Any) -> None:
        """Initialise pool with size bounds and connection parameters."""
        self._min = min_conn
        self._max = max_conn
        self._kwargs = connect_kwargs
        self._pool: list[Any] = []
        self._in_use: list[Any] = []

    def _create_conn(self) -> Any:
        """Create a new pymysql connection."""
        import pymysql  # lazy import
        return pymysql.connect(**self._kwargs)

    def initialise(self) -> None:
        """Pre-fill pool with min connections."""
        for _ in range(self._min):
            self._pool.append(self._create_conn())

    def acquire(self) -> Any:
        """Get a connection from the pool or create a new one."""
        if self._pool:
            conn = self._pool.pop()
        elif len(self._in_use) < self._max:
            conn = self._create_conn()
        else:
            raise ConnectorExecutionError("Connection pool exhausted")
        self._in_use.append(conn)
        return conn

    def release(self, conn: Any) -> None:
        """Return a connection to the pool."""
        if conn in self._in_use:
            self._in_use.remove(conn)
        try:
            conn.ping(reconnect=True)
            self._pool.append(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                logger.error("Unhandled exception in mysql_connector.py: %s", exc)

    def close_all(self) -> None:
        """Close all pooled and in-use connections."""
        for conn in self._pool + self._in_use:
            try:
                conn.close()
            except Exception:
                logger.error("Unhandled exception in mysql_connector.py: %s", exc)
        self._pool.clear()
        self._in_use.clear()


class MySQLConnector(BackendConnector):
    """Production-grade MySQL connector with connection pooling.

    Uses pymysql under the hood. Supports EXPLAIN FORMAT=JSON,
    information_schema statistics, and full QueryResult semantics.
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialise with connection config."""
        super().__init__(config)
        self._pool: Optional[_SimpleConnectionPool] = None

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return MySQL capabilities."""
        return _CAPABILITIES

    def connect(self) -> None:
        """Establish connection pool to MySQL."""
        try:
            c = self._config
            kwargs: dict[str, Any] = {
                "host": c.host or "localhost",
                "port": c.port or 3306,
                "user": c.username or "root",
                "password": c.password or "",
                "database": c.database or "",
                "connect_timeout": c.connect_timeout,
                "autocommit": True,
                "cursorclass_name": "DictCursor",
            }
            # Use DictCursor so rows come back as dicts
            import pymysql
            import pymysql.cursors
            kwargs["cursorclass"] = pymysql.cursors.DictCursor
            del kwargs["cursorclass_name"]

            self._pool = _SimpleConnectionPool(min_conn=2, max_conn=c.pool_size, **kwargs)
            self._pool.initialise()
            self._connected = True
            logger.info("MySQL connection pool ready: %s", self._config.backend_id)
        except Exception as exc:
            raise ConnectorConnectionError(
                f"Failed to connect to MySQL: {exc}",
                backend_id=self._config.backend_id,
            ) from exc

    def disconnect(self) -> None:
        """Close all pooled connections."""
        if self._pool is not None:
            self._pool.close_all()
        self._connected = False
        logger.info("MySQL connector disconnected: %s", self._config.backend_id)

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Execute SQL and return standardised QueryResult."""
        if self._pool is None:
            raise ConnectorExecutionError("Not connected", backend_id=self._config.backend_id)

        query_id = str(uuid.uuid4())
        start = time.monotonic()
        timeout = timeout_seconds or self._config.query_timeout

        conn = self._pool.acquire()
        try:
            with conn.cursor() as cur:
                if timeout:
                    cur.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
                try:
                    cur.execute(sql, params)
                except Exception as exc:
                    err_str = str(exc).lower()
                    if "timeout" in err_str or "max_execution_time" in err_str:
                        raise ConnectorTimeoutError(
                            f"Query timed out after {timeout}s",
                            backend_id=self._config.backend_id,
                            sql=sql,
                        ) from exc
                    raise ConnectorExecutionError(
                        f"MySQL query failed: {exc}",
                        backend_id=self._config.backend_id,
                        sql=sql,
                    ) from exc

                columns: list[str] = []
                rows: list[dict[str, Any]] = []
                row_count = 0

                if cur.description:
                    columns = [d[0] for d in cur.description]
                    raw = cur.fetchall()
                    rows = [dict(r) for r in raw] if raw else []
                    row_count = len(rows)
                elif cur.rowcount >= 0:
                    row_count = cur.rowcount
        finally:
            self._pool.release(conn)

        duration_ms = (time.monotonic() - start) * 1000
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=row_count,
            duration_ms=duration_ms,
            backend=self._config.backend_id,
            query_id=query_id,
        )

    def explain(self, sql: str) -> dict[str, Any]:
        """Run EXPLAIN FORMAT=JSON and return parsed plan dict."""
        explain_sql = f"EXPLAIN FORMAT=JSON {sql}"
        result = self.execute(explain_sql)
        if result.rows:
            raw = result.rows[0]
            # MySQL returns column named 'EXPLAIN'
            plan_str = raw.get("EXPLAIN") or raw.get("explain") or "{}"
            try:
                return json.loads(plan_str) if isinstance(plan_str, str) else plan_str
            except (json.JSONDecodeError, TypeError):
                return {"raw": str(plan_str)}
        return {}

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Fetch table and column statistics from information_schema."""
        table_sql = """
            SELECT
                TABLE_ROWS        AS row_count,
                DATA_LENGTH       AS data_bytes,
                INDEX_LENGTH      AS index_bytes,
                DATA_FREE         AS free_bytes,
                AVG_ROW_LENGTH    AS avg_row_length,
                CREATE_TIME       AS created_at,
                UPDATE_TIME       AS updated_at
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s
        """
        col_sql = """
            SELECT
                COLUMN_NAME        AS column_name,
                DATA_TYPE          AS data_type,
                IS_NULLABLE        AS is_nullable,
                COLUMN_DEFAULT     AS column_default,
                CHARACTER_MAXIMUM_LENGTH AS max_length
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s
            ORDER BY ORDINAL_POSITION
        """
        t_res = self.execute(table_sql, {"schema": schema, "table": table})
        c_res = self.execute(col_sql, {"schema": schema, "table": table})
        t_row = t_res.rows[0] if t_res.rows else {}
        return {
            "row_count": int(t_row.get("row_count") or 0),
            "data_bytes": int(t_row.get("data_bytes") or 0),
            "index_bytes": int(t_row.get("index_bytes") or 0),
            "avg_row_length": int(t_row.get("avg_row_length") or 0),
            "columns": c_res.rows,
        }
