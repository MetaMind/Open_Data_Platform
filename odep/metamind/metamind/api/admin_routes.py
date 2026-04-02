"""
Admin Routes — Tenant Management, Budget, Feature Flags, Routing Policies

File: metamind/api/admin_routes.py
Role: API Engineer
Dependencies: fastapi, sqlalchemy, metamind.bootstrap

Contains all /api/v1/admin/* and /api/v1/budget/* endpoints.
Registered via: app.include_router(admin_router, prefix="/api/v1")
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import text

logger = logging.getLogger(__name__)

admin_router = APIRouter(tags=["admin"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ctx(request: Request) -> Any:
    return request.app.state.context


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TenantQuotaUpdate(BaseModel):
    max_query_rate_per_minute: Optional[int] = None
    max_concurrent_queries: Optional[int] = None
    max_result_rows: Optional[int] = None
    max_storage_gb: Optional[float] = None


class RoutingPolicyCreate(BaseModel):
    policy_name: str
    description: str = ""
    priority: int = Field(default=50, ge=1, le=100)
    conditions: Dict[str, Any] = Field(default_factory=dict)
    target_engine: str


class FeatureFlagUpdate(BaseModel):
    tenant_id: str
    flag_name: str
    enabled: bool


# ---------------------------------------------------------------------------
# Tenant management
# ---------------------------------------------------------------------------

@admin_router.get("/admin/tenants", status_code=status.HTTP_200_OK)
async def list_tenants(req: Request) -> List[Dict[str, Any]]:
    """Return all tenants with their current quota settings."""
    ctx = _ctx(req)
    try:
        async with ctx.async_db_engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT t.tenant_id, t.tenant_name, t.is_active, t.created_at,
                               q.max_query_rate_per_minute, q.max_concurrent_queries,
                               q.max_result_rows
                        FROM mm_tenants t
                        LEFT JOIN mm_tenant_quotas q USING (tenant_id)
                        ORDER BY t.created_at
                        """
                    )
                )
            ).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("list_tenants failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@admin_router.put("/admin/tenants/{tenant_id}/quota", status_code=status.HTTP_200_OK)
async def update_tenant_quota(
    tenant_id: str,
    update: TenantQuotaUpdate,
    req: Request,
) -> Dict[str, Any]:
    """Update resource quotas for a tenant."""
    ctx = _ctx(req)
    try:
        sets = []
        params: Dict[str, Any] = {"tid": tenant_id}
        if update.max_query_rate_per_minute is not None:
            sets.append("max_query_rate_per_minute = :rate")
            params["rate"] = update.max_query_rate_per_minute
        if update.max_concurrent_queries is not None:
            sets.append("max_concurrent_queries = :concur")
            params["concur"] = update.max_concurrent_queries
        if update.max_result_rows is not None:
            sets.append("max_result_rows = :rows")
            params["rows"] = update.max_result_rows
        if not sets:
            raise HTTPException(status_code=400, detail="No fields to update")

        async with ctx.async_db_engine.begin() as conn:
            await conn.execute(
                text(
                    f"""
                    INSERT INTO mm_tenant_quotas (tenant_id, {', '.join(k.split(' =')[0].strip() for k in sets)})
                    VALUES (:tid, {', '.join(':' + p for p in params if p != 'tid')})
                    ON CONFLICT (tenant_id) DO UPDATE SET {', '.join(sets)}
                    """
                ),
                params,
            )
        return {"tenant_id": tenant_id, "updated": True}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("update_tenant_quota failed tenant=%s: %s", tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Routing policies
# ---------------------------------------------------------------------------

@admin_router.get("/admin/policies", status_code=status.HTTP_200_OK)
async def list_policies(req: Request, tenant_id: str = "default") -> List[Dict[str, Any]]:
    """Return all routing policies for a tenant."""
    ctx = _ctx(req)
    try:
        async with ctx.async_db_engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT policy_id, policy_name, description, priority,
                               conditions, target_engine, is_active, created_at
                        FROM mm_routing_policies
                        WHERE tenant_id = :tid
                        ORDER BY priority DESC, created_at
                        """
                    ),
                    {"tid": tenant_id},
                )
            ).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("list_policies failed tenant=%s: %s", tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@admin_router.post("/admin/policies", status_code=status.HTTP_201_CREATED)
async def create_policy(
    policy: RoutingPolicyCreate,
    req: Request,
    tenant_id: str = "default",
) -> Dict[str, Any]:
    """Create a new routing policy."""
    ctx = _ctx(req)
    import json
    policy_id = str(uuid.uuid4())
    try:
        async with ctx.async_db_engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO mm_routing_policies (
                        policy_id, tenant_id, policy_name, description,
                        priority, conditions, target_engine, is_active, created_at
                    ) VALUES (
                        :pid, :tid, :name, :desc,
                        :prio, :cond::jsonb, :engine, TRUE, NOW()
                    )
                    """
                ),
                {
                    "pid": policy_id,
                    "tid": tenant_id,
                    "name": policy.policy_name,
                    "desc": policy.description,
                    "prio": policy.priority,
                    "cond": json.dumps(policy.conditions),
                    "engine": policy.target_engine,
                },
            )
        return {"policy_id": policy_id, "created": True}
    except Exception as exc:
        logger.error("create_policy failed tenant=%s: %s", tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@admin_router.delete("/admin/policies/{policy_id}", status_code=status.HTTP_200_OK)
async def deactivate_policy(
    policy_id: str,
    req: Request,
    tenant_id: str = "default",
) -> Dict[str, Any]:
    """Soft-delete (deactivate) a routing policy."""
    ctx = _ctx(req)
    try:
        async with ctx.async_db_engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE mm_routing_policies
                    SET is_active = FALSE
                    WHERE policy_id = :pid AND tenant_id = :tid
                    """
                ),
                {"pid": policy_id, "tid": tenant_id},
            )
        return {"policy_id": policy_id, "deactivated": True}
    except Exception as exc:
        logger.error("deactivate_policy failed policy=%s: %s", policy_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------

@admin_router.get("/admin/feature-flags", status_code=status.HTTP_200_OK)
async def get_feature_flags(req: Request, tenant_id: str = "default") -> Dict[str, Any]:
    """Return F01–F30 feature flag status for a tenant."""
    ctx = _ctx(req)
    try:
        async with ctx.async_db_engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT flag_name, is_enabled, updated_at
                        FROM mm_feature_flags
                        WHERE tenant_id = :tid
                        ORDER BY flag_name
                        """
                    ),
                    {"tid": tenant_id},
                )
            ).fetchall()
        flags = {r.flag_name: {"enabled": r.is_enabled, "updated_at": str(r.updated_at)}
                 for r in rows}
        return {"tenant_id": tenant_id, "flags": flags}
    except Exception as exc:
        logger.error("get_feature_flags failed tenant=%s: %s", tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@admin_router.put("/admin/feature-flags", status_code=status.HTTP_200_OK)
async def toggle_feature_flag(
    update: FeatureFlagUpdate,
    req: Request,
) -> Dict[str, Any]:
    """Enable or disable a feature flag for a tenant."""
    ctx = _ctx(req)
    try:
        async with ctx.async_db_engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO mm_feature_flags (tenant_id, flag_name, is_enabled, updated_at)
                    VALUES (:tid, :flag, :enabled, NOW())
                    ON CONFLICT (tenant_id, flag_name) DO UPDATE
                    SET is_enabled = EXCLUDED.is_enabled, updated_at = NOW()
                    """
                ),
                {
                    "tid": update.tenant_id,
                    "flag": update.flag_name,
                    "enabled": update.enabled,
                },
            )
        return {"tenant_id": update.tenant_id, "flag": update.flag_name,
                "enabled": update.enabled}
    except Exception as exc:
        logger.error(
            "toggle_feature_flag failed tenant=%s flag=%s: %s",
            update.tenant_id,
            update.flag_name,
            exc,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Budget / F23
# ---------------------------------------------------------------------------

@admin_router.get("/budget/summary", status_code=status.HTTP_200_OK)
async def budget_summary(req: Request, tenant_id: str = "default") -> Dict[str, Any]:
    """Return current spend vs budget limit for a tenant."""
    ctx = _ctx(req)
    try:
        async with ctx.async_db_engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        """
                        SELECT bc.budget_name, bc.budget_limit_usd, bc.billing_cycle,
                               bc.alert_threshold_pct,
                               COALESCE(SUM(qc.actual_cost_usd), 0) AS current_spend
                        FROM mm_budget_configs bc
                        LEFT JOIN mm_query_costs qc ON qc.tenant_id = bc.tenant_id
                            AND qc.billed_at >= date_trunc('month', NOW())
                        WHERE bc.tenant_id = :tid AND bc.is_active = TRUE
                        GROUP BY 1,2,3,4
                        LIMIT 1
                        """
                    ),
                    {"tid": tenant_id},
                )
            ).fetchone()
        if not row:
            return {"tenant_id": tenant_id, "budget_configured": False}
        spend = float(row.current_spend)
        limit = float(row.budget_limit_usd)
        pct = round(spend / limit * 100, 2) if limit > 0 else 0.0
        alert_color = "green" if pct < 70 else ("yellow" if pct < 90 else "red")
        return {
            "tenant_id": tenant_id,
            "budget_name": row.budget_name,
            "budget_limit_usd": limit,
            "current_spend_usd": spend,
            "pct_used": pct,
            "billing_cycle": row.billing_cycle,
            "alert_threshold_pct": row.alert_threshold_pct,
            "alert_color": alert_color,
        }
    except Exception as exc:
        logger.error("budget_summary failed tenant=%s: %s", tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@admin_router.get("/budget/breakdown", status_code=status.HTTP_200_OK)
async def budget_breakdown(req: Request, tenant_id: str = "default") -> Dict[str, Any]:
    """Return cost breakdown by engine for the last 30 days."""
    ctx = _ctx(req)
    try:
        async with ctx.async_db_engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT engine,
                               SUM(actual_cost_usd) AS total_cost,
                               COUNT(*) AS query_count,
                               AVG(execution_time_ms) AS avg_ms
                        FROM mm_query_costs
                        WHERE tenant_id = :tid
                          AND billed_at >= NOW() - INTERVAL '30 days'
                        GROUP BY engine
                        ORDER BY total_cost DESC
                        """
                    ),
                    {"tid": tenant_id},
                )
            ).fetchall()
        return {
            "tenant_id": tenant_id,
            "period_days": 30,
            "by_engine": [dict(r._mapping) for r in rows],
        }
    except Exception as exc:
        logger.error("budget_breakdown failed tenant=%s: %s", tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@admin_router.get("/budget/alerts", status_code=status.HTTP_200_OK)
async def budget_alerts(req: Request, tenant_id: str = "default") -> List[Dict[str, Any]]:
    """Return active (unresolved) budget alerts for a tenant."""
    ctx = _ctx(req)
    try:
        async with ctx.async_db_engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT alert_id, budget_id, alert_type,
                               threshold_pct, current_spend, budget_limit,
                               pct_used, fired_at
                        FROM mm_budget_alerts
                        WHERE tenant_id = :tid AND is_resolved = FALSE
                        ORDER BY fired_at DESC
                        """
                    ),
                    {"tid": tenant_id},
                )
            ).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("budget_alerts failed tenant=%s: %s", tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Synthesis endpoints
# ---------------------------------------------------------------------------

@admin_router.get(
    "/synthesis/status",
    response_model=Dict[str, Any],
    summary="AI Synthesis Engine Status",
    tags=["Synthesis"],
)
async def get_synthesis_status(
    req: Request,
) -> Dict[str, Any]:
    """
    Return current synthesis engine metrics and background task status.

    Returns rules_generated_total, retrain_count, cycle_duration_ms_last,
    cycles_completed, cycles_errored, active_tenants, background_running.
    """
    ctx = _ctx(req)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialised")
    try:
        status = ctx.synthesis_engine.get_status()
        return {"ok": True, "synthesis": status}
    except Exception as exc:
        logger.error("GET /synthesis/status failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@admin_router.post(
    "/synthesis/run",
    response_model=Dict[str, Any],
    summary="Trigger Synthesis Cycle",
    tags=["Synthesis"],
)
async def run_synthesis_cycle(
    req: Request,
    tenant_id: str,
) -> Dict[str, Any]:
    """
    Manually trigger a synthesis cycle for a specific tenant.

    Runs the full pipeline: workload profiling → rule generation →
    rule registration → conditional retraining.
    Returns SynthesisCycleResult as JSON.
    """
    ctx = _ctx(req)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialised")
    try:
        result = await ctx.synthesis_engine.run_synthesis_cycle(tenant_id=tenant_id)
        return {
            "ok": True,
            "tenant_id": result.tenant_id,
            "rules_generated": result.rules_generated,
            "rules_retired": result.rules_retired,
            "retrained": result.retrained,
            "mae_before": result.mae_before,
            "mae_after": result.mae_after,
            "cycle_duration_ms": result.cycle_duration_ms,
            "error": result.error,
        }
    except Exception as exc:
        logger.error("POST /synthesis/run tenant=%s failed: %s", tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
