"""MetaMind Python Client — drop-in SDK for programmatic access."""
from __future__ import annotations

import logging
from typing import Any, Optional
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Result from MetaMind query execution."""

    query_id: str
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    duration_ms: float
    optimization_ms: float
    cache_hit: bool
    workload_type: str
    backend_used: str
    optimization_tier: int
    plan_cost: float

    def __repr__(self) -> str:
        return (
            f"QueryResult(rows={self.row_count}, backend={self.backend_used}, "
            f"cache_hit={self.cache_hit}, {self.duration_ms:.1f}ms)"
        )


class MetaMindClient:
    """Python client for the MetaMind Query Intelligence Platform.

    Pluggable anywhere — drop into any Python application to route
    queries through MetaMind's optimization pipeline.

    Example::

        client = MetaMindClient(
            base_url="http://localhost:8000",
            tenant_id="my-tenant",
            api_token="Bearer <token>",
        )
        result = client.query("SELECT * FROM orders WHERE status='pending'")
        print(result.rows)

    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        tenant_id: str = "default",
        api_token: Optional[str] = None,
        timeout: float = 120.0,
    ) -> None:
        """Initialize client."""
        self._base_url = base_url.rstrip("/")
        self._tenant_id = tenant_id
        self._timeout = timeout

        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "X-Tenant-ID": tenant_id,
        }
        if api_token:
            headers["Authorization"] = (
                api_token if api_token.startswith("Bearer ") else f"Bearer {api_token}"
            )

        self._http = httpx.Client(
            base_url=self._base_url,
            headers=headers,
            timeout=self._timeout,
        )

    def query(
        self,
        sql: str,
        backend: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        dry_run: bool = False,
    ) -> QueryResult:
        """Execute a SQL query through MetaMind.

        Args:
            sql: SQL string to execute
            backend: Optional backend override (e.g. 'duckdb', 'snowflake')
            timeout_seconds: Query timeout override
            dry_run: If True, optimize without executing

        Returns:
            QueryResult with rows and optimization metadata.

        Raises:
            MetaMindError: On API error or query failure.
        """
        payload: dict[str, Any] = {"sql": sql, "dry_run": dry_run}
        if backend:
            payload["backend"] = backend
        if timeout_seconds:
            payload["timeout_seconds"] = timeout_seconds

        resp = self._http.post("/api/v1/query", json=payload)
        self._check_response(resp)

        data = resp.json()
        return QueryResult(
            query_id=data["query_id"],
            columns=data["columns"],
            rows=data["rows"],
            row_count=data["row_count"],
            duration_ms=data["duration_ms"],
            optimization_ms=data["optimization_ms"],
            cache_hit=data["cache_hit"],
            workload_type=data["workload_type"],
            backend_used=data["backend_used"],
            optimization_tier=data["optimization_tier"],
            plan_cost=data["plan_cost"],
        )

    def nl_query(
        self,
        question: str,
        table_hints: Optional[list[str]] = None,
        execute: bool = True,
    ) -> dict[str, Any]:
        """Convert natural language to SQL and optionally execute (F28).

        Args:
            question: Natural language question
            table_hints: Relevant table names for schema context
            execute: If True, execute the generated SQL

        Returns:
            Dict with generated_sql, confidence, and optional execution_result.
        """
        payload: dict[str, Any] = {
            "nl_text": question,
            "execute": execute,
        }
        if table_hints:
            payload["table_hints"] = table_hints

        resp = self._http.post("/api/v1/nl/query", json=payload)
        self._check_response(resp)
        return resp.json()

    def list_tables(self, schema: Optional[str] = None) -> list[str]:
        """List registered tables for this tenant."""
        params: dict[str, str] = {}
        if schema:
            params["schema"] = schema
        resp = self._http.get("/api/v1/tables", params=params)
        self._check_response(resp)
        return resp.json()["tables"]

    def register_table(
        self,
        table_name: str,
        schema_name: str = "public",
        backend: str = "postgres",
        row_count: int = 0,
    ) -> dict[str, Any]:
        """Register a table in MetaMind's metadata catalog."""
        payload = {
            "table_name": table_name,
            "schema_name": schema_name,
            "backend": backend,
            "row_count": row_count,
        }
        resp = self._http.post("/api/v1/tables", json=payload)
        self._check_response(resp)
        return resp.json()

    def enable_feature(self, feature: str) -> None:
        """Enable a feature flag for this tenant."""
        resp = self._http.put(f"/api/v1/features/{feature}", params={"enabled": "true"})
        self._check_response(resp)

    def disable_feature(self, feature: str) -> None:
        """Disable a feature flag for this tenant."""
        resp = self._http.put(f"/api/v1/features/{feature}", params={"enabled": "false"})
        self._check_response(resp)

    def get_features(self) -> dict[str, bool]:
        """Get all feature flags for this tenant."""
        resp = self._http.get("/api/v1/features")
        self._check_response(resp)
        return resp.json()["flags"]

    def health(self) -> dict[str, Any]:
        """Check platform health."""
        resp = self._http.get("/api/v1/health")
        self._check_response(resp)
        return resp.json()

    def invalidate_cache(self, table_name: Optional[str] = None) -> int:
        """Invalidate plan cache (optionally for a specific table)."""
        params: dict[str, str] = {}
        if table_name:
            params["table_name"] = table_name
        resp = self._http.delete("/api/v1/cache", params=params)
        self._check_response(resp)
        return resp.json()["invalidated"]

    def _check_response(self, resp: httpx.Response) -> None:
        """Raise MetaMindError on non-2xx responses."""
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("detail", resp.text)
            except Exception:
                detail = resp.text
            raise MetaMindError(f"HTTP {resp.status_code}: {detail}")

    def close(self) -> None:
        """Close the HTTP client."""
        self._http.close()

    def __enter__(self) -> "MetaMindClient":
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()


class MetaMindError(Exception):
    """Base exception for MetaMind client errors."""
