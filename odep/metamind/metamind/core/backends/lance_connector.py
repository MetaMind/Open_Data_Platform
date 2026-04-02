"""LanceDB backend connector — vector-native tabular storage (F13, F19)."""
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
    ConnectionConfig,
    QueryResult,
)

logger = logging.getLogger(__name__)

_CAPABILITIES = ConnectorCapabilities(
    supports_aggregation=False,
    supports_window_functions=False,
    supports_cte=False,
    supports_lateral=False,
    supports_unnest=False,
    supports_hash_join=False,
    supports_merge_join=False,
    supports_vector_search=True,
    is_distributed=False,
    dialect="lance",
    max_concurrent_queries=50,
    cost_per_gb_scan=0.0,
)

# Simple SQL patterns we can translate
_SELECT_RE_SRC = (
    r"SELECT\s+(?P<cols>.+?)\s+FROM\s+(?P<table>\w+)"
    r"(?:\s+WHERE\s+(?P<where>.+?))?"
    r"(?:\s+LIMIT\s+(?P<limit>\d+))?\s*$"
)


class LanceConnector(BackendConnector):
    """LanceDB connector for vector-native tabular data.

    Connects to a LanceDB dataset directory.  SQL queries are translated
    to LanceDB Python API calls via a simple pattern-matching translator;
    complex SQL is not supported — use a full SQL engine for that.

    Extra params:
        ``uri`` — path or URI to the LanceDB dataset (overrides host/database)
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialise with connection config."""
        super().__init__(config)
        self._db: Optional[Any] = None
        self._uri: str = ""

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return Lance capabilities."""
        return _CAPABILITIES

    def connect(self) -> None:
        """Open LanceDB connection at the configured URI."""
        try:
            import lancedb  # type: ignore[import]
        except ImportError as exc:
            raise ConnectorConnectionError(
                "lancedb not installed. Run: pip install lancedb",
                backend_id=self._config.backend_id,
            ) from exc

        extra = self._config.extra_params
        self._uri = (
            extra.get("uri")
            or self._config.connection_string
            or self._config.database
            or "."
        )
        try:
            self._db = lancedb.connect(self._uri)
            self._connected = True
            logger.info("LanceDB connected: %s → %s", self._config.backend_id, self._uri)
        except Exception as exc:
            raise ConnectorConnectionError(
                f"LanceDB connect failed: {exc}",
                backend_id=self._config.backend_id,
            ) from exc

    def disconnect(self) -> None:
        """Disconnect LanceDB (no persistent session; just clear state)."""
        self._db = None
        self._connected = False
        logger.info("LanceDB connector disconnected: %s", self._config.backend_id)

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Translate simple SQL to LanceDB scan and execute.

        Supports:
        - ``SELECT col1, col2 FROM table [WHERE col = val] [LIMIT n]``
        - Vector queries embedded as ``_vector_search_`` annotations (see
          :meth:`execute_vector_search`)

        Raises:
            ConnectorExecutionError: For unsupported SQL constructs.
        """
        if self._db is None:
            raise ConnectorExecutionError("Not connected", backend_id=self._config.backend_id)

        query_id = str(uuid.uuid4())
        start = time.monotonic()

        try:
            rows, columns = self._translate_and_run(sql, params)
        except ConnectorExecutionError:
            raise
        except Exception as exc:
            raise ConnectorExecutionError(
                f"LanceDB execution failed: {exc}",
                backend_id=self._config.backend_id,
                sql=sql,
            ) from exc

        duration_ms = (time.monotonic() - start) * 1000
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            duration_ms=duration_ms,
            backend=self._config.backend_id,
            query_id=query_id,
        )

    def execute_vector_search(
        self,
        table_name: str,
        query_vector: list[float],
        top_k: int = 10,
        metric: str = "cosine",
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
    ) -> QueryResult:
        """Perform a native LanceDB vector search.

        Args:
            table_name: Lance table to search.
            query_vector: Query embedding.
            top_k: Number of results to return.
            metric: Distance metric — ``"cosine"`` (default), ``"l2"``, ``"dot"``.
            columns: Columns to return (default: all).
            where: Optional SQL-style filter predicate.

        Returns:
            QueryResult with ranked nearest neighbours.
        """
        if self._db is None:
            raise ConnectorExecutionError("Not connected", backend_id=self._config.backend_id)

        query_id = str(uuid.uuid4())
        start = time.monotonic()
        try:
            tbl = self._db.open_table(table_name)
            search = tbl.search(query_vector).metric(metric).limit(top_k)
            if where:
                search = search.where(where)
            if columns:
                search = search.select(columns)
            pdf = search.to_pandas()
            cols = list(pdf.columns)
            rows = pdf.to_dict(orient="records")
        except Exception as exc:
            raise ConnectorExecutionError(
                f"LanceDB vector search failed: {exc}",
                backend_id=self._config.backend_id,
            ) from exc

        duration_ms = (time.monotonic() - start) * 1000
        return QueryResult(
            columns=cols,
            rows=rows,
            row_count=len(rows),
            duration_ms=duration_ms,
            backend=self._config.backend_id,
            query_id=query_id,
        )

    def explain(self, sql: str) -> dict[str, Any]:
        """Return a simple scan descriptor for Lance (no cost model)."""
        parsed = self._parse_select(sql)
        table = parsed.get("table", "unknown")
        return {
            "plan_type": "lance_scan",
            "table": table,
            "uri": self._uri,
            "cols": parsed.get("cols", "*"),
        }

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Return row count and schema for a Lance table."""
        if self._db is None:
            return {}
        try:
            tbl = self._db.open_table(table)
            row_count = tbl.count_rows()
            schema_fields = []
            if hasattr(tbl, "schema"):
                for field in tbl.schema:
                    schema_fields.append({
                        "column_name": field.name,
                        "data_type": str(field.type),
                        "is_nullable": field.nullable,
                    })
            return {
                "table": table,
                "row_count": row_count,
                "columns": schema_fields,
            }
        except Exception as exc:
            logger.warning("LanceDB get_table_stats failed for %s: %s", table, exc)
            return {}

    # ── Internal helpers ──────────────────────────────────────

    def _parse_select(self, sql: str) -> dict[str, Any]:
        """Parse a minimal SELECT statement and return components as a dict."""
        import re
        pattern = re.compile(_SELECT_RE_SRC, re.IGNORECASE | re.DOTALL)
        m = pattern.match(sql.strip())
        if not m:
            return {}
        return {
            "cols": m.group("cols").strip(),
            "table": m.group("table").strip(),
            "where": (m.group("where") or "").strip(),
            "limit": int(m.group("limit")) if m.group("limit") else None,
        }

    def _translate_and_run(
        self, sql: str, params: Optional[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Translate simple SELECT SQL to LanceDB scan API."""
        parsed = self._parse_select(sql)
        if not parsed:
            raise ConnectorExecutionError(
                f"LanceDB only supports simple SELECT queries. Got: {sql[:200]}",
                backend_id=self._config.backend_id,
                sql=sql,
            )

        table_name = parsed["table"]
        try:
            tbl = self._db.open_table(table_name)
        except Exception as exc:
            raise ConnectorExecutionError(
                f"LanceDB table '{table_name}' not found: {exc}",
                backend_id=self._config.backend_id,
                sql=sql,
            ) from exc

        scanner = tbl.search()
        where_clause = parsed.get("where", "")
        if where_clause:
            # Apply basic substitution if params provided
            if params:
                for k, v in params.items():
                    where_clause = where_clause.replace(f"%({k})s", repr(v))
            scanner = scanner.where(where_clause)

        limit = parsed.get("limit")
        if limit:
            scanner = scanner.limit(limit)

        cols_str = parsed.get("cols", "*")
        selected_cols: Optional[list[str]] = None
        if cols_str and cols_str != "*":
            selected_cols = [c.strip() for c in cols_str.split(",")]
            scanner = scanner.select(selected_cols)

        pdf = scanner.to_pandas()
        columns = list(pdf.columns)
        rows = pdf.to_dict(orient="records")
        return rows, columns
