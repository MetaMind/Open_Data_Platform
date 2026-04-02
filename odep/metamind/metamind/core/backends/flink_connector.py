"""Apache Flink backend connector — uses SQL Gateway REST API (F13)."""
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

_CAPABILITIES = ConnectorCapabilities(
    supports_aggregation=True,
    supports_window_functions=True,
    supports_cte=True,
    supports_lateral=False,
    supports_unnest=True,
    supports_hash_join=True,
    supports_merge_join=False,
    supports_broadcast_join=True,
    is_distributed=True,
    dialect="flink",
    max_concurrent_queries=100,
    cost_per_gb_scan=0.0,
)

# Flink SQL Gateway REST paths
_BASE_V1 = "/v1"
_SESSIONS_PATH = f"{_BASE_V1}/sessions"
_POLL_INTERVAL_SECS = 0.5


class FlinkConnector(BackendConnector):
    """Flink SQL connector via the Flink SQL Gateway REST API.

    Requires Flink 1.16+ with the SQL Gateway enabled.
    Extra params:
        ``gateway_url``   — e.g. ``"http://flink-host:8083"`` (default localhost)
        ``session_props`` — dict of Flink session properties
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialise with connection config."""
        super().__init__(config)
        self._gateway_url: str = ""
        self._session_id: Optional[str] = None

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return Flink capabilities."""
        return _CAPABILITIES

    def _http(self) -> Any:
        """Return an httpx client (lazy import)."""
        try:
            import httpx  # type: ignore[import]
            return httpx
        except ImportError as exc:
            raise ConnectorConnectionError(
                "httpx is not installed. Run: pip install httpx",
                backend_id=self._config.backend_id,
            ) from exc

    def connect(self) -> None:
        """Create a Flink SQL Gateway session."""
        extra = self._config.extra_params
        host = self._config.host or "localhost"
        port = self._config.port or 8083
        self._gateway_url = extra.get("gateway_url") or f"http://{host}:{port}"

        session_props: dict[str, str] = extra.get("session_props", {})
        payload: dict[str, Any] = {}
        if session_props:
            payload["properties"] = session_props

        httpx = self._http()
        try:
            resp = httpx.post(
                f"{self._gateway_url}{_SESSIONS_PATH}",
                json=payload,
                timeout=self._config.connect_timeout,
            )
            resp.raise_for_status()
            self._session_id = resp.json().get("sessionHandle")
            self._connected = True
            logger.info(
                "Flink SQL Gateway session created: %s → %s",
                self._config.backend_id,
                self._session_id,
            )
        except Exception as exc:
            raise ConnectorConnectionError(
                f"Flink SQL Gateway connect failed: {exc}",
                backend_id=self._config.backend_id,
            ) from exc

    def disconnect(self) -> None:
        """Close the Flink SQL Gateway session."""
        if self._session_id is not None:
            httpx = self._http()
            try:
                httpx.delete(
                    f"{self._gateway_url}{_SESSIONS_PATH}/{self._session_id}",
                    timeout=10,
                )
            except Exception as exc:
                logger.warning("Error closing Flink session: %s", exc)
        self._session_id = None
        self._connected = False
        logger.info("Flink connector disconnected: %s", self._config.backend_id)

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Submit SQL to Flink Gateway and poll for results."""
        if not self._session_id:
            raise ConnectorExecutionError("Not connected", backend_id=self._config.backend_id)

        query_id = str(uuid.uuid4())
        start = time.monotonic()
        timeout = timeout_seconds or self._config.query_timeout
        httpx = self._http()

        # Submit statement
        stmt_url = (
            f"{self._gateway_url}{_SESSIONS_PATH}/{self._session_id}/statements"
        )
        try:
            submit_resp = httpx.post(
                stmt_url,
                json={"statement": sql},
                timeout=30,
            )
            submit_resp.raise_for_status()
            operation_handle = submit_resp.json().get("operationHandle")
        except Exception as exc:
            raise ConnectorExecutionError(
                f"Flink statement submit failed: {exc}",
                backend_id=self._config.backend_id,
                sql=sql,
            ) from exc

        # Poll for completion
        result_url = (
            f"{self._gateway_url}{_SESSIONS_PATH}/{self._session_id}"
            f"/operations/{operation_handle}/result/0"
        )
        columns: list[str] = []
        rows: list[dict[str, Any]] = []

        while True:
            elapsed = time.monotonic() - start
            if elapsed > timeout:
                raise ConnectorTimeoutError(
                    f"Flink query timed out after {timeout}s",
                    backend_id=self._config.backend_id,
                    sql=sql,
                )
            try:
                res = httpx.get(result_url, timeout=30)
                res.raise_for_status()
                data = res.json()
            except Exception as exc:
                raise ConnectorExecutionError(
                    f"Flink result poll failed: {exc}",
                    backend_id=self._config.backend_id,
                    sql=sql,
                ) from exc

            result_type = data.get("resultType", "")
            if result_type in ("EOS", "PAYLOAD"):
                schema = data.get("results", {}).get("columns", [])
                columns = [col["name"] for col in schema]
                data_rows = data.get("results", {}).get("data", [])
                for dr in data_rows:
                    field_vals = dr.get("fields", [])
                    rows.append(dict(zip(columns, field_vals)))
                if result_type == "EOS":
                    break
            elif result_type == "NOT_READY":
                time.sleep(_POLL_INTERVAL_SECS)
            else:
                raise ConnectorExecutionError(
                    f"Flink unexpected result type: {result_type}",
                    backend_id=self._config.backend_id,
                    sql=sql,
                )

        duration_ms = (time.monotonic() - start) * 1000
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            duration_ms=duration_ms,
            backend=self._config.backend_id,
            query_id=query_id,
        )

    def explain(self, sql: str) -> dict[str, Any]:
        """Run EXPLAIN via the SQL Gateway and return plan text."""
        explain_sql = f"EXPLAIN {sql}"
        try:
            result = self.execute(explain_sql)
            if result.rows:
                plan_lines = [str(list(r.values())[0]) for r in result.rows if r]
                return {"plan_type": "flink_explain", "plan": "\n".join(plan_lines)}
        except Exception as exc:
            logger.warning("Flink explain failed: %s", exc)
        return {"error": "explain failed"}

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Return CREATE TABLE DDL via Flink catalog for schema info."""
        qualified = f"{schema}.{table}" if schema else table
        try:
            result = self.execute(f"SHOW CREATE TABLE {qualified}")
            ddl = "\n".join(
                str(list(r.values())[0]) for r in result.rows if r
            )
            return {"table": qualified, "ddl": ddl}
        except Exception as exc:
            logger.warning("Flink get_table_stats failed for %s: %s", qualified, exc)
            return {}
