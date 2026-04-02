"""Metadata, cancel, and query-history routes — extracted from server.py.

File: metamind/api/metadata_routes.py
Role: API Engineer
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from metamind.api.security_middleware import QueryCancellationTracker

logger = logging.getLogger(__name__)


def register_metadata_routes(app: FastAPI, ctx_getter: Callable[[], Any]) -> None:
    """Register metadata, cancel, and history routes onto *app*."""

    @app.get("/api/v1/tables/search", tags=["Metadata"])
    async def search_tables(q: str, tenant_id: str = "default", limit: int = 100) -> Dict[str, Any]:
        """Search tables by name pattern."""
        ctx = ctx_getter()
        if ctx is None:
            raise HTTPException(status_code=503, detail="Application not initialized")
        tables = ctx.catalog.search_tables(q, tenant_id, limit)
        return {"tables": tables, "count": len(tables)}

    @app.get("/api/v1/tables/{table_name}", tags=["Metadata"])
    async def get_table_details(table_name: str, tenant_id: str = "default") -> Dict[str, Any]:
        """Get table metadata and columns."""
        ctx = ctx_getter()
        if ctx is None:
            raise HTTPException(status_code=503, detail="Application not initialized")
        table = ctx.catalog.get_table(table_name, tenant_id)
        if not table:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
        columns = ctx.catalog.get_table_columns(table.table_id)
        return {
            "table": {
                "table_id": table.table_id,
                "source_id": table.source_id,
                "schema_name": table.schema_name,
                "table_name": table.table_name,
                "row_count": table.row_count,
                "size_bytes": table.size_bytes,
                "is_partitioned": table.is_partitioned,
            },
            "columns": [
                {
                    "column_name": c.column_name,
                    "data_type": c.data_type,
                    "is_nullable": c.is_nullable,
                    "is_primary_key": c.is_primary_key,
                }
                for c in columns
            ],
        }

    @app.post(
        "/api/v1/query/{query_id}/cancel",
        response_model=Dict[str, Any],
        tags=["Query"],
    )
    async def cancel_query(
        query_id: str,
        tenant_id: str = "default",
    ) -> Dict[str, Any]:
        """Cancel a running query via QueryCancellationTracker."""
        ctx = ctx_getter()
        if ctx is None:
            raise HTTPException(status_code=503, detail="Application not initialised")
        tracker: QueryCancellationTracker = ctx.cancellation_tracker
        cancelled = tracker.cancel(query_id=query_id, tenant_id=tenant_id)
        if not cancelled:
            raise HTTPException(
                status_code=404,
                detail=f"Query {query_id!r} not found or already completed",
            )
        logger.info(
            "cancel_query: query_id=%s tenant=%s — cancellation registered",
            query_id,
            tenant_id,
        )
        return {"status": "cancellation_registered", "query_id": query_id, "tenant_id": tenant_id}

    @app.get("/api/v1/query/history", tags=["Query"])
    async def get_query_history(
        tenant_id: str = "default",
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return paginated query execution history for a tenant."""
        ctx = ctx_getter()
        if ctx is None:
            raise HTTPException(status_code=503, detail="Application not initialized")
        try:
            with ctx.sync_db_engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT query_id, user_id, original_sql, target_source,
                               status, total_time_ms, row_count, submitted_at
                        FROM mm_query_logs
                        WHERE tenant_id = :tenant_id
                        ORDER BY submitted_at DESC
                        LIMIT :limit OFFSET :offset
                        """
                    ),
                    {"tenant_id": tenant_id, "limit": limit, "offset": offset},
                ).fetchall()
            queries = [
                {
                    "query_id": r.query_id,
                    "user_id": r.user_id,
                    "sql": (
                        r.original_sql[:200] + "..."
                        if len(r.original_sql) > 200
                        else r.original_sql
                    ),
                    "target_source": r.target_source,
                    "status": r.status,
                    "execution_time_ms": r.total_time_ms,
                    "row_count": r.row_count,
                    "submitted_at": r.submitted_at.isoformat() if r.submitted_at else None,
                }
                for r in rows
            ]
            return {"queries": queries, "count": len(queries)}
        except Exception as exc:
            logger.error("get_query_history failed tenant=%s: %s", tenant_id, exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
