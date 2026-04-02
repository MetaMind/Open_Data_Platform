"""Extended admin routes — Phase 2 additions.

Registered in server.py alongside the core admin_router.
Includes:
  - GET /advisor/indexes  (Task 12)
  - GET /admin/mv/refresh-status  (Task 14)
  - GET /admin/failover/events  (Task 18)
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status

from metamind.core.advisor.index_recommender import (
    IndexRecommender,
    IndexRecommendation,
)

logger = logging.getLogger(__name__)

admin_ext_router = APIRouter(tags=["admin-ext"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db(request: Request):  # type: ignore[return]
    ctx = getattr(request.app.state, "app_context", None)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialized")
    db = getattr(ctx, "_sync_db_engine", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    return db


# ---------------------------------------------------------------------------
# Task 12: Index Recommender endpoint
# ---------------------------------------------------------------------------

@admin_ext_router.get("/advisor/indexes")
async def get_index_recommendations(
    request: Request,
    tenant_id: str = "default",
    lookback_days: int = 7,
) -> list[dict]:
    """Return top index recommendations for a tenant based on slow query analysis."""
    db = _db(request)
    recommender = IndexRecommender(db_engine=db)
    try:
        recs: list[IndexRecommendation] = await recommender.analyze(
            tenant_id=tenant_id,
            lookback_days=lookback_days,
        )
        return [
            {
                "table": r.table,
                "columns": r.columns,
                "index_type": r.index_type,
                "estimated_speedup_pct": r.estimated_speedup_pct,
                "query_count": r.query_count,
                "create_statement": r.create_statement,
            }
            for r in recs
        ]
    except Exception as exc:
        logger.error("get_index_recommendations failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Task 14: MV refresh status endpoint
# ---------------------------------------------------------------------------

@admin_ext_router.get("/admin/mv/refresh-status")
async def get_mv_refresh_status(
    request: Request,
    tenant_id: str = "default",
) -> list[dict]:
    """List materialized views with freshness and last_refreshed_at."""
    db = _db(request)
    try:
        from sqlalchemy import text
        with db.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT mv_id, mv_name, status, "
                    "CAST(last_refreshed_at AS TEXT) AS last_refreshed_at, "
                    "CAST(created_at AS TEXT) AS created_at "
                    "FROM mm_materialized_views "
                    "WHERE tenant_id = :tid "
                    "ORDER BY last_refreshed_at DESC NULLS LAST"
                ),
                {"tid": tenant_id},
            ).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("get_mv_refresh_status failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Task 18: Failover events endpoint
# ---------------------------------------------------------------------------

@admin_ext_router.get("/admin/failover/events")
async def get_failover_events(
    request: Request,
    limit: int = 100,
) -> list[dict]:
    """Return the last N failover events."""
    db = _db(request)
    try:
        from sqlalchemy import text
        with db.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT event_id, tenant_id, original_engine, failover_engine, "
                    "reason, CAST(occurred_at AS TEXT) AS occurred_at "
                    "FROM mm_failover_events "
                    "ORDER BY occurred_at DESC LIMIT :lim"
                ),
                {"lim": limit},
            ).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("get_failover_events failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
