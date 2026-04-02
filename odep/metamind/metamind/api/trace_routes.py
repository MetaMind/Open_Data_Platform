"""Distributed Query Tracing — API routes and TraceStore.

Provides a lightweight embedded trace viewer for ops teams without
a full Jaeger installation.

TraceStore: Redis-backed bounded list of recent spans (LPUSH + LTRIM).
Endpoints:
  GET /api/v1/traces                — list recent traces with filters
  GET /api/v1/traces/{trace_id}     — full span tree for one trace
  GET /traces                       — serve the single-page HTML UI
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

logger = logging.getLogger(__name__)

trace_router = APIRouter(tags=["tracing"])

_TRACE_LIST_KEY = "mm:traces"
_MAX_TRACES = 10_000


# ---------------------------------------------------------------------------
# TraceStore
# ---------------------------------------------------------------------------

class TraceStore:
    """Bounded Redis list of recent OpenTelemetry-style spans.

    Args:
        redis_client: Synchronous Redis client.
        max_traces: Maximum number of spans to retain (LTRIM after each push).
    """

    def __init__(self, redis_client: object, max_traces: int = _MAX_TRACES) -> None:
        self._redis = redis_client
        self._max = max_traces

    def record(self, span: dict) -> None:
        """Push a span to the bounded list.  Oldest spans are evicted automatically."""
        try:
            self._redis.lpush(_TRACE_LIST_KEY, json.dumps(span, default=str))  # type: ignore[union-attr]
            self._redis.ltrim(_TRACE_LIST_KEY, 0, self._max - 1)  # type: ignore[union-attr]
        except Exception as exc:
            logger.error("TraceStore.record failed: %s", exc)

    def query(
        self,
        tenant_id: Optional[str] = None,
        query_id: Optional[str] = None,
        min_duration_ms: Optional[float] = None,
        limit: int = 50,
    ) -> list[dict]:
        """Return matching spans, newest first.

        Args:
            tenant_id: Filter by tenant; None means all tenants.
            query_id: Filter by query_id; None means all queries.
            min_duration_ms: Minimum span duration to include.
            limit: Maximum number of results.

        Returns:
            List of span dicts sorted newest-first.
        """
        try:
            raw_items = self._redis.lrange(_TRACE_LIST_KEY, 0, self._max - 1)  # type: ignore[union-attr]
        except Exception as exc:
            logger.error("TraceStore.query Redis error: %s", exc)
            return []

        results: list[dict] = []
        for raw in raw_items:
            try:
                span = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue

            if tenant_id and span.get("tenant_id") != tenant_id:
                continue
            if query_id and span.get("query_id") != query_id:
                continue
            dur = span.get("duration_ms", 0)
            if min_duration_ms is not None and float(dur) < min_duration_ms:
                continue

            results.append(span)
            if len(results) >= limit:
                break

        return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_store(request: Request) -> TraceStore:
    ctx = getattr(request.app.state, "app_context", None)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialized")
    redis = getattr(ctx, "_redis_client", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis not available")
    return TraceStore(redis_client=redis)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@trace_router.get("/traces", response_class=HTMLResponse, include_in_schema=False)
async def trace_ui(request: Request) -> HTMLResponse:
    """Serve the embedded trace viewer single-page app.

    Uses importlib.resources so the path works correctly when the package
    is installed as a wheel, not just when run from source (fixes W-14).
    """
    import importlib.resources as pkg_resources
    import os

    # Try importlib.resources first (works in installed packages)
    try:
        # Python 3.9+ path
        ref = pkg_resources.files("metamind.frontend").joinpath("trace_ui.html")
        content = ref.read_text(encoding="utf-8")
        return HTMLResponse(content=content)
    except (AttributeError, ModuleNotFoundError, FileNotFoundError):
        pass

    # Fallback: resolve relative to this source file (development mode)
    fallback = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "frontend", "trace_ui.html")
    )
    try:
        with open(fallback, encoding="utf-8") as fh:
            return HTMLResponse(content=fh.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Trace UI not found</h1>", status_code=404)


@trace_router.get("/api/v1/traces")
async def list_traces(
    request: Request,
    tenant_id: Optional[str] = Query(default=None),
    query_id: Optional[str] = Query(default=None),
    min_duration_ms: Optional[float] = Query(default=None),
    limit: int = Query(default=50, le=500),
) -> list[dict]:
    """List recent traces with optional filters."""
    store = _get_store(request)
    return store.query(
        tenant_id=tenant_id,
        query_id=query_id,
        min_duration_ms=min_duration_ms,
        limit=limit,
    )


@trace_router.get("/api/v1/traces/{trace_id}")
async def get_trace(trace_id: str, request: Request) -> dict:
    """Return the full span tree for a single trace_id."""
    store = _get_store(request)
    spans = store.query(query_id=trace_id, limit=1000)
    if not spans:
        raise HTTPException(status_code=404, detail="Trace not found")
    return {
        "trace_id": trace_id,
        "span_count": len(spans),
        "spans": spans,
    }
