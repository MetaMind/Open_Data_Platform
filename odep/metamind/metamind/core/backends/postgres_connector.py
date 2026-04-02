"""PostgreSQL backend connector — production implementation using psycopg2."""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Optional

import psycopg2
import psycopg2.extras
import psycopg2.pool

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


class PostgresConnector(BackendConnector):
    """Production-grade PostgreSQL connector with connection pooling.

    Supports statement timeout, EXPLAIN ANALYZE, and native statistics.
    """

    _CAPABILITIES = ConnectorCapabilities(
        supports_aggregation=True,
        supports_window_functions=True,
        supports_cte=True,
        supports_lateral=True,
        supports_unnest=True,
        supports_hash_join=True,
        supports_merge_join=True,
        supports_json_ops=True,
        supports_materialized_views=True,
        is_distributed=False,
        dialect="postgres",
        max_concurrent_queries=200,
    )

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialize with connection config."""
        super().__init__(config)
        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return Postgres capabilities."""
        return self._CAPABILITIES

    def connect(self) -> None:
        """Establish connection pool."""
        try:
            dsn = self._build_dsn()
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=self._config.pool_size,
                dsn=dsn,
                connect_timeout=self._config.connect_timeout,
                options="-c application_name=metamind",
            )
            self._connected = True
            logger.info("PostgreSQL connection pool established: %s", self._config.backend_id)
        except psycopg2.Error as exc:
            raise ConnectorConnectionError(
                f"Failed to connect to Postgres: {exc}",
                backend_id=self._config.backend_id,
            ) from exc

    def disconnect(self) -> None:
        """Close all pooled connections."""
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None
        self._connected = False
        logger.info("PostgreSQL pool closed: %s", self._config.backend_id)

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Execute SQL and return standardized QueryResult."""
        if not self._connected or self._pool is None:
            self.connect()

        query_id = str(uuid.uuid4())[:8]
        start = time.monotonic()
        timeout = timeout_seconds or self._config.query_timeout

        conn = self._pool.getconn()  # type: ignore[union-attr]
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Set statement timeout
                cur.execute(f"SET statement_timeout = {timeout * 1000}")

                try:
                    if params:
                        cur.execute(sql, params)
                    else:
                        cur.execute(sql)
                except psycopg2.extensions.QueryCanceledError as exc:
                    raise ConnectorTimeoutError(
                        f"Query timed out after {timeout}s",
                        backend_id=self._config.backend_id,
                        sql=sql,
                    ) from exc
                except psycopg2.Error as exc:
                    raise ConnectorExecutionError(
                        f"Query execution failed: {exc}",
                        backend_id=self._config.backend_id,
                        sql=sql,
                    ) from exc

                columns: list[str] = []
                rows: list[dict[str, Any]] = []
                row_count = 0

                if cur.description:
                    columns = [d[0] for d in cur.description]
                    raw_rows = cur.fetchall()
                    rows = [dict(row) for row in raw_rows]
                    row_count = len(rows)
                elif cur.rowcount >= 0:
                    row_count = cur.rowcount

                conn.commit()

        except (ConnectorTimeoutError, ConnectorExecutionError):
            conn.rollback()
            raise
        except Exception as exc:
            conn.rollback()
            raise ConnectorExecutionError(
                str(exc), backend_id=self._config.backend_id, sql=sql
            ) from exc
        finally:
            self._pool.putconn(conn)  # type: ignore[union-attr]

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
        """Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) and return parsed plan."""
        explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
        result = self.execute(explain_sql)
        if result.rows:
            plan_json = result.rows[0].get("QUERY PLAN", "[]")
            if isinstance(plan_json, list):
                return plan_json[0] if plan_json else {}
            return json.loads(plan_json)[0] if isinstance(plan_json, str) else {}
        return {}

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Fetch PostgreSQL native table statistics from pg_class and pg_stats."""
        stats_sql = """
            SELECT
                pgc.reltuples AS row_count,
                pg_total_relation_size(pgc.oid) AS total_bytes,
                pg_relation_size(pgc.oid) AS table_bytes,
                pgc.relpages AS pages
            FROM pg_class pgc
            JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace
            WHERE pgn.nspname = %(schema)s AND pgc.relname = %(table)s
        """
        col_sql = """
            SELECT
                attname AS column_name,
                n_distinct,
                null_frac,
                avg_width,
                most_common_vals,
                most_common_freqs,
                histogram_bounds,
                correlation
            FROM pg_stats
            WHERE schemaname = %(schema)s AND tablename = %(table)s
            ORDER BY attnum
        """
        table_result = self.execute(stats_sql, {"schema": schema, "table": table})
        col_result = self.execute(col_sql, {"schema": schema, "table": table})

        table_stats = table_result.rows[0] if table_result.rows else {}
        col_stats = col_result.rows

        return {
            "row_count": int(float(table_stats.get("row_count", 0))),
            "total_bytes": int(table_stats.get("total_bytes", 0)),
            "table_bytes": int(table_stats.get("table_bytes", 0)),
            "pages": int(table_stats.get("pages", 0)),
            "columns": col_stats,
        }

    def _build_dsn(self) -> str:
        """Build PostgreSQL DSN from config."""
        c = self._config
        if c.connection_string:
            return c.connection_string
        parts = ["postgresql://"]
        if c.username:
            parts.append(c.username)
            if c.password:
                parts.append(f":{c.password}")
            parts.append("@")
        parts.append(c.host or "localhost")
        if c.port:
            parts.append(f":{c.port}")
        parts.append(f"/{c.database or 'postgres'}")
        return "".join(parts)
