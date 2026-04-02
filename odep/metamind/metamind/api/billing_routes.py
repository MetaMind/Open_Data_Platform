"""Billing API routes.

GET  /api/v1/billing/summary?tenant_id=&month=YYYY-MM
POST /api/v1/billing/export
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from metamind.core.billing.usage_exporter import UsageBillingExporter

logger = logging.getLogger(__name__)

billing_router = APIRouter(tags=["billing"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_exporter(request: Request) -> UsageBillingExporter:
    ctx = getattr(request.app.state, "app_context", None)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialized")
    db = getattr(ctx, "_sync_db_engine", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    stripe_key = getattr(getattr(ctx, "settings", None), "stripe_api_key", None)
    return UsageBillingExporter(db_engine=db, stripe_api_key=stripe_key)


def _parse_month(month_str: str) -> tuple[datetime, datetime]:
    """Parse YYYY-MM into (period_start, period_end) datetimes."""
    import calendar
    dt = datetime.strptime(month_str, "%Y-%m")
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    start = datetime(dt.year, dt.month, 1)
    end = datetime(dt.year, dt.month, last_day, 23, 59, 59)
    return start, end


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class BillingExportRequest(BaseModel):
    tenant_id: str = Field(...)
    month: str = Field(..., pattern=r"^\d{4}-\d{2}$")
    export_type: str = Field(default="csv", pattern="^(csv|stripe)$")
    dest_path: Optional[str] = Field(default=None)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@billing_router.get("/billing/summary")
async def billing_summary(
    request: Request,
    tenant_id: str = "default",
    month: str = "2024-01",
) -> dict:
    """Return billing summary for a tenant+month."""
    exporter = _get_exporter(request)
    try:
        start, end = _parse_month(month)
        period = await exporter.aggregate(tenant_id, start, end)
        return {
            "tenant_id": period.tenant_id,
            "period_start": period.period_start.isoformat(),
            "period_end": period.period_end.isoformat(),
            "total_cost_usd": period.total_cost_usd,
            "line_items": [
                {
                    "engine": li.engine,
                    "query_count": li.query_count,
                    "cost_usd": li.total_cost_usd,
                    "result_rows": li.result_rows,
                }
                for li in period.line_items
            ],
        }
    except Exception as exc:
        logger.error("billing_summary failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@billing_router.post("/billing/export")
async def billing_export(
    payload: BillingExportRequest,
    request: Request,
) -> dict:
    """Trigger a Stripe export or CSV download for a billing period."""
    exporter = _get_exporter(request)
    try:
        start, end = _parse_month(payload.month)
        period = await exporter.aggregate(payload.tenant_id, start, end)

        if payload.export_type == "stripe":
            ref = await exporter.export_to_stripe(period)
            return {"status": "exported", "type": "stripe", "reference": ref}

        dest = payload.dest_path or f"/tmp/billing_{payload.tenant_id}_{payload.month}.csv"
        path = await exporter.export_to_csv(period, dest)
        return {"status": "exported", "type": "csv", "file_path": path}
    except Exception as exc:
        logger.error("billing_export failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
