"""DuckDB backend connector — in-process OLAP engine for local analytics."""
from __future__ import annotations

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


class DuckDBConnector(BackendConnector):
    """DuckDB in-process analytics connector.

    Supports Parquet, CSV, Arrow, and native DuckDB files.
    Excellent for local OLAP workloads and vector similarity search.
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
        supports_vector_search=True,    # via VSS extension
        is_distributed=False,
        dialect="duckdb",
        max_concurrent_queries=4,       # Single-process model
        cost_per_gb_scan=0.0,           # Local = free
    )

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialize with connection config."""
        super().__init__(config)
        self._conn: Optional[object] = None

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return DuckDB capabilities."""
        return self._CAPABILITIES

    def connect(self) -> None:
        """Open DuckDB connection (file or in-memory)."""
        try:
            import duckdb  # type: ignore[import]

            db_path = self._config.connection_string or self._config.database or ":memory:"
            self._conn = duckdb.connect(
                database=db_path,
                read_only=self._config.extra_params.get("read_only", False),
            )

            # Configure threads and memory
            threads = self._config.extra_params.get("threads", 4)
            memory_limit = self._config.extra_params.get("memory_limit", "4GB")
            self._conn.execute(f"PRAGMA threads={threads}")  # type: ignore[union-attr]
            self._conn.execute(f"PRAGMA memory_limit='{memory_limit}'")  # type: ignore[union-attr]

            self._connected = True
            logger.info("DuckDB connected: %s", db_path)
        except ImportError as exc:
            raise ConnectorConnectionError(
                "duckdb package not installed. Run: pip install duckdb",
                backend_id=self._config.backend_id,
            ) from exc
        except Exception as exc:
            raise ConnectorConnectionError(
                f"DuckDB connection failed: {exc}",
                backend_id=self._config.backend_id,
            ) from exc

    def disconnect(self) -> None:
        """Close DuckDB connection."""
        if self._conn is not None:
            self._conn.close()  # type: ignore[union-attr]
            self._conn = None
        self._connected = False
        logger.info("DuckDB connection closed: %s", self._config.backend_id)

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Execute SQL against DuckDB and return standardized result."""
        if not self._connected or self._conn is None:
            self.connect()

        query_id = str(uuid.uuid4())[:8]
        start = time.monotonic()

        try:
            if params:
                rel = self._conn.execute(sql, list(params.values()))  # type: ignore[union-attr]
            else:
                rel = self._conn.execute(sql)  # type: ignore[union-attr]

            if rel is None:
                return QueryResult(
                    columns=[], rows=[], row_count=0,
                    duration_ms=(time.monotonic() - start) * 1000,
                    backend=self._config.backend_id, query_id=query_id,
                )

            df = rel.fetchdf()
            columns = list(df.columns)
            rows = df.to_dict(orient="records")
            row_count = len(rows)

        except Exception as exc:
            error_str = str(exc).lower()
            if "timeout" in error_str or "interrupted" in error_str:
                raise ConnectorTimeoutError(
                    f"DuckDB query timed out: {exc}",
                    backend_id=self._config.backend_id,
                    sql=sql,
                ) from exc
            raise ConnectorExecutionError(
                f"DuckDB execution failed: {exc}",
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
        """Run DuckDB EXPLAIN and return plan dict."""
        result = self.execute(f"EXPLAIN ANALYZE {sql}")
        if result.rows:
            return {"plan": result.rows}
        return {}

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Fetch DuckDB table statistics via PRAGMA."""
        count_result = self.execute(f"SELECT COUNT(*) AS cnt FROM {schema}.{table}")
        row_count = count_result.rows[0].get("cnt", 0) if count_result.rows else 0

        col_result = self.execute(
            f"""SELECT column_name, data_type, null_percentage, approx_unique
                FROM duckdb_columns()
                WHERE schema_name='{schema}' AND table_name='{table}'"""
        )

        return {
            "row_count": int(row_count),
            "total_bytes": 0,
            "columns": col_result.rows,
        }

    def load_parquet(self, path: str, table_name: str) -> None:
        """Load a Parquet file as a virtual table."""
        self.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path}')"
        )
        logger.info("Loaded Parquet file %s as %s", path, table_name)

    def load_csv(self, path: str, table_name: str) -> None:
        """Load a CSV file as a virtual table."""
        self.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{path}')"
        )
        logger.info("Loaded CSV file %s as %s", path, table_name)
