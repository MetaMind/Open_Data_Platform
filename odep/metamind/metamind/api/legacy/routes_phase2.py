"""Phase 2 API extensions — budget, explain, backends, and advisor endpoints."""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import Depends, HTTPException

from metamind.api.auth import get_current_tenant
from metamind.api.models import ExecuteRequest
from metamind.api.server import app, get_bootstrap
from metamind.bootstrap import Bootstrap

logger = logging.getLogger(__name__)


# ── Budget management (F23) ───────────────────────────────────

@app.get("/v1/budget", tags=["Budget"])
async def get_budget(
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(get_bootstrap),
) -> dict:
    """F23: Get cloud budget usage for the current tenant.

    Returns monthly spend, budget limit, and per-backend breakdown.
    """
    flags = bs.get_feature_flags(tenant_id).get_flags()
    if not flags.F23_cloud_budget:
        raise HTTPException(
            status_code=403,
            detail="Feature F23_cloud_budget is not enabled for this tenant.",
        )
    engine = bs.get_query_engine()
    try:
        budget_data = engine._catalog.get_budget(tenant_id)
    except AttributeError:
        budget_data = {
            "tenant_id": tenant_id,
            "monthly_spend_usd": 0.0,
            "budget_limit_usd": None,
            "breakdown_by_backend": {},
        }
    return {"tenant_id": tenant_id, **budget_data}


@app.put("/v1/budget", tags=["Budget"])
async def set_budget(
    limit_usd: float,
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(get_bootstrap),
) -> dict:
    """F23: Set monthly cloud budget limit for the current tenant."""
    flags = bs.get_feature_flags(tenant_id).get_flags()
    if not flags.F23_cloud_budget:
        raise HTTPException(
            status_code=403,
            detail="Feature F23_cloud_budget is not enabled for this tenant.",
        )
    if limit_usd < 0:
        raise HTTPException(status_code=400, detail="Budget limit must be non-negative.")
    engine = bs.get_query_engine()
    try:
        engine._catalog.set_budget(tenant_id, limit_usd)
    except AttributeError:
        logger.error("Unhandled exception in routes_phase2.py: %s", exc)
    return {"tenant_id": tenant_id, "budget_limit_usd": limit_usd, "status": "updated"}


# ── Explain endpoint ──────────────────────────────────────────

@app.post("/v1/explain", tags=["Query"])
async def explain_query(
    request: ExecuteRequest,
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(get_bootstrap),
) -> dict:
    """Return the optimization plan tree as JSON without executing the query."""
    from metamind.core.query_engine import QueryContext
    engine = bs.get_query_engine()
    ctx = QueryContext(
        sql=request.sql,
        tenant_id=tenant_id,
        backend_id=request.backend_id or bs._settings.default_backend,
        dry_run=True,
    )
    try:
        result = engine.execute(ctx)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Explain failed: {exc}")
    return {
        "sql": request.sql,
        "plan": getattr(result, "plan_json", {}),
        "estimated_cost": getattr(result, "estimated_cost", None),
        "backend": ctx.backend_id,
    }


# ── Backend listing ───────────────────────────────────────────

@app.get("/v1/backends", tags=["Configuration"])
async def list_backends(
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(get_bootstrap),
) -> dict:
    """List all registered backend connectors and their health status."""
    from metamind.core.backends.registry import get_registry
    registry = get_registry()
    health = registry.health_check_all()
    registered_types = list(registry._connector_classes.keys())
    return {
        "backends": [
            {
                "backend_id": bid,
                "status": info.get("status", "unknown"),
                "connected": info.get("connected", "false"),
            }
            for bid, info in health.items()
        ],
        "registered_types": registered_types,
        "total": len(health),
    }


# ── Index advisor (F21) ───────────────────────────────────────

@app.get("/v1/advisor/indexes", tags=["Advisor"])
async def get_index_recommendations(
    limit: int = 10,
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(get_bootstrap),
) -> dict:
    """F21: Get index recommendations based on recent query workload."""
    flags = bs.get_feature_flags(tenant_id).get_flags()
    if not flags.F21_auto_advisor:
        raise HTTPException(
            status_code=403,
            detail="Feature F21_auto_advisor is not enabled for this tenant.",
        )
    engine = bs.get_query_engine()
    recommendations: list[dict] = []
    try:
        if hasattr(engine, "_advisor"):
            recommendations = engine._advisor.get_index_recommendations(
                tenant_id=tenant_id, limit=limit
            )
        else:
            recommendations = _generate_basic_index_recommendations(engine, tenant_id, limit)
    except Exception as exc:
        logger.warning("Index advisor failed: %s", exc)
    return {
        "tenant_id": tenant_id,
        "recommendations": recommendations,
        "count": len(recommendations),
    }


def _generate_basic_index_recommendations(
    engine: Any,
    tenant_id: str,
    limit: int,
) -> list[dict]:
    """Fallback index recommendations based on plan cache miss patterns."""
    recommendations: list[dict] = []
    try:
        if not hasattr(engine, "_plan_cache"):
            return recommendations
        stats = engine._plan_cache.stats()
        if isinstance(stats, dict) and stats.get("miss_count", 0) > 0:
            recommendations.append({
                "table": "unknown",
                "column": "unknown",
                "reason": "High cache miss rate suggests missing indexes",
                "estimated_benefit": "medium",
            })
    except Exception:
        logger.error("Unhandled exception in routes_phase2.py: %s", exc)
    return recommendations[:limit]
