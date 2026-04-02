"""Snowflake backend connector — production implementation (F13)."""
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
    supports_lateral=True,
    supports_unnest=True,
    supports_hash_join=True,
    supports_merge_join=True,
    supports_broadcast_join=False,
    supports_json_ops=True,
    supports_materialized_views=True,
    is_distributed=True,
    is_serverless=True,
    dialect="snowflake",
    max_concurrent_queries=100,
    cost_per_gb_scan=5.0,
)

# Threshold above which we use async execution
_ASYNC_THRESHOLD_SECS = 30


class SnowflakeConnector(BackendConnector):
    """Snowflake connector using snowflake-connector-python.

    For long-running queries (>30 s by default) uses async execution with
    polling. Short queries execute synchronously for lower latency.

    Extra connection params (via ConnectionConfig.extra_params):
        ``account``  — Snowflake account identifier (required)
        ``warehouse`` — compute warehouse name
        ``role``      — Snowflake role to assume
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialise with connection config."""
        super().__init__(config)
        self._conn: Optional[Any] = None

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return Snowflake capabilities."""
        return _CAPABILITIES

    def connect(self) -> None:
        """Open a Snowflake connection."""
        try:
            import snowflake.connector  # type: ignore[import]

            c = self._config
            extra = c.extra_params
            self._conn = snowflake.connector.connect(
                account=extra.get("account", ""),
                user=c.username or "",
                password=c.password or "",
                database=c.database or "",
                schema=c.schema or "PUBLIC",
                warehouse=extra.get("warehouse", ""),
                role=extra.get("role", ""),
                login_timeout=c.connect_timeout,
                network_timeout=c.query_timeout,
                application="MetaMind",
            )
            self._connected = True
            logger.info("Snowflake connection established: %s", c.backend_id)
        except ImportError as exc:
            raise ConnectorConnectionError(
                "snowflake-connector-python not installed. Run: pip install snowflake-connector-python",
                backend_id=self._config.backend_id,
            ) from exc
        except Exception as exc:
            raise ConnectorConnectionError(
                f"Snowflake connect failed: {exc}",
                backend_id=self._config.backend_id,
            ) from exc

    def disconnect(self) -> None:
        """Close the Snowflake connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as exc:
                logger.warning("Error closing Snowflake connection: %s", exc)
        self._connected = False
        self._conn = None
        logger.info("Snowflake connector disconnected: %s", self._config.backend_id)

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Execute SQL on Snowflake, using async for long queries."""
        if self._conn is None:
            raise ConnectorExecutionError("Not connected", backend_id=self._config.backend_id)

        query_id = str(uuid.uuid4())
        start = time.monotonic()
        timeout = timeout_seconds or self._config.query_timeout

        try:
            cur = self._conn.cursor()
            use_async = timeout and timeout > _ASYNC_THRESHOLD_SECS
            if use_async:
                cur.execute_async(sql, params or {})
                qid = cur.sfqid
                import snowflake.connector  # type: ignore[import]
                while True:
                    status = self._conn.get_query_status_throw_if_error(qid)
                    from snowflake.connector import QueryStatus  # type: ignore[import]
                    if status in (QueryStatus.SUCCESS, QueryStatus.RUNNING):
                        if status == QueryStatus.SUCCESS:
                            break
                        elapsed = time.monotonic() - start
                        if elapsed > timeout:
                            raise ConnectorTimeoutError(
                                f"Snowflake query timed out after {timeout}s",
                                backend_id=self._config.backend_id,
                                sql=sql,
                            )
                        time.sleep(0.5)
                    else:
                        raise ConnectorExecutionError(
                            f"Snowflake async query failed with status {status}",
                            backend_id=self._config.backend_id,
                            sql=sql,
                        )
                cur.get_results_from_sfqid(qid)
            else:
                cur.execute(sql, params or {})

            columns: list[str] = []
            rows: list[dict[str, Any]] = []
            row_count = 0

            if cur.description:
                columns = [d[0] for d in cur.description]
                raw = cur.fetchall()
                rows = [dict(zip(columns, r)) for r in raw]
                row_count = len(rows)
            cur.close()
        except (ConnectorTimeoutError, ConnectorExecutionError):
            raise
        except Exception as exc:
            err_str = str(exc).lower()
            if "timeout" in err_str:
                raise ConnectorTimeoutError(
                    f"Snowflake query timed out: {exc}",
                    backend_id=self._config.backend_id,
                    sql=sql,
                ) from exc
            raise ConnectorExecutionError(
                f"Snowflake execution failed: {exc}",
                backend_id=self._config.backend_id,
                sql=sql,
            ) from exc

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
        """Run EXPLAIN USING JSON and return parsed plan."""
        explain_sql = f"EXPLAIN USING JSON {sql}"
        result = self.execute(explain_sql)
        if result.rows:
            raw = result.rows[0]
            plan_str = next(iter(raw.values()), "{}")
            try:
                return json.loads(plan_str) if isinstance(plan_str, str) else dict(raw)
            except (json.JSONDecodeError, TypeError):
                return {"raw": str(plan_str)}
        return {}

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Fetch row count and column info from INFORMATION_SCHEMA."""
        db = self._config.database or ""
        table_sql = f"""
            SELECT
                ROW_COUNT         AS row_count,
                BYTES             AS total_bytes,
                CREATED           AS created_at,
                LAST_ALTERED      AS updated_at,
                CLUSTERING_KEY    AS clustering_key
            FROM {db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s
        """
        col_sql = f"""
            SELECT
                COLUMN_NAME       AS column_name,
                DATA_TYPE         AS data_type,
                IS_NULLABLE       AS is_nullable,
                CHARACTER_MAXIMUM_LENGTH AS max_length,
                NUMERIC_PRECISION AS numeric_precision
            FROM {db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s
            ORDER BY ORDINAL_POSITION
        """
        t_res = self.execute(table_sql, {"schema": schema.upper(), "table": table.upper()})
        c_res = self.execute(col_sql, {"schema": schema.upper(), "table": table.upper()})
        t_row = t_res.rows[0] if t_res.rows else {}
        return {
            "row_count": int(t_row.get("ROW_COUNT") or t_row.get("row_count") or 0),
            "total_bytes": int(t_row.get("BYTES") or t_row.get("total_bytes") or 0),
            "clustering_key": t_row.get("CLUSTERING_KEY") or t_row.get("clustering_key"),
            "columns": c_res.rows,
        }
