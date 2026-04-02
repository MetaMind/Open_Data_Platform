"""MetaMindClient — REST client for MetaMind v2.0 via httpx."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import httpx

from odep.exceptions import AuthenticationError


@dataclass
class QueryResponse:
    query_id: str
    row_count: int
    duration_ms: float
    optimization_ms: float
    plan_cost: float
    cache_hit: bool
    backend_used: str
    optimization_tier: int
    workload_type: str
    flags_used: List[str] = field(default_factory=list)


class MetaMindClient:
    """REST client for MetaMind v2.0 API."""

    def __init__(
        self,
        base_url: str,
        tenant_id: str,
        api_token: Optional[str] = None,
        timeout: float = 120.0,
    ) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout)
        if api_token:
            self._client.headers["Authorization"] = f"Bearer {api_token}"
        self._tenant_id = tenant_id

    def query(self, sql: str, dry_run: bool = False) -> QueryResponse:
        """POST /api/v1/query and return a QueryResponse."""
        response = self._client.post(
            "/api/v1/query",
            json={"sql": sql, "tenant_id": self._tenant_id, "dry_run": dry_run},
        )
        if response.status_code in (401, 403):
            raise AuthenticationError("metamind", response.text)
        if response.status_code >= 400:
            raise RuntimeError(f"MetaMind query failed: {response.text}")
        data = response.json()
        return QueryResponse(
            query_id=data["query_id"],
            row_count=data["row_count"],
            duration_ms=data["duration_ms"],
            optimization_ms=data["optimization_ms"],
            plan_cost=data["plan_cost"],
            cache_hit=data["cache_hit"],
            backend_used=data["backend_used"],
            optimization_tier=data["optimization_tier"],
            workload_type=data["workload_type"],
            flags_used=data.get("flags_used", []),
        )

    def cancel(self, query_id: str) -> bool:
        """POST /api/v1/query/{query_id}/cancel. Returns True on success."""
        response = self._client.post(f"/api/v1/query/{query_id}/cancel")
        if response.status_code in (401, 403):
            raise AuthenticationError("metamind", response.text)
        return response.is_success

    def get_history(self, query_id: str) -> Optional[QueryResponse]:
        """GET /api/v1/query/history filtered by query_id. Returns QueryResponse or None."""
        response = self._client.get("/api/v1/query/history", params={"query_id": query_id})
        if response.status_code in (401, 403):
            raise AuthenticationError("metamind", response.text)
        if response.status_code == 404:
            return None
        data = response.json()
        if not data:
            return None
        return QueryResponse(
            query_id=data["query_id"],
            row_count=data["row_count"],
            duration_ms=data["duration_ms"],
            optimization_ms=data["optimization_ms"],
            plan_cost=data["plan_cost"],
            cache_hit=data["cache_hit"],
            backend_used=data["backend_used"],
            optimization_tier=data["optimization_tier"],
            workload_type=data["workload_type"],
            flags_used=data.get("flags_used", []),
        )
