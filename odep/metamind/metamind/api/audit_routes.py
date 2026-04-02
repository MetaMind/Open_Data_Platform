"""Audit export API routes.

POST /api/v1/audit/export  — trigger async export job
GET  /api/v1/audit/exports — list past exports for tenant
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, status
from pydantic import BaseModel, Field

from metamind.core.io.audit_exporter import AuditExporter

logger = logging.getLogger(__name__)

audit_router = APIRouter(tags=["audit"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class AuditExportRequest(BaseModel):
    """Request body for triggering an audit export."""

    tenant_id: str = Field(..., description="Tenant to export logs for")
    start_date: datetime = Field(..., description="Inclusive start datetime (ISO-8601)")
    end_date: datetime = Field(..., description="Inclusive end datetime (ISO-8601)")
    dest_path: str = Field(
        ...,
        description="Destination path: local file path, s3://bucket/key, or gs://bucket/blob",
    )
    storage_backend: str = Field(default="local", description="'local', 's3', or 'gcs'")


class AuditExportResponse(BaseModel):
    """Response after triggering an export."""

    status: str
    file_path: str
    row_count: int
    duration_ms: float


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _get_db(request: Request):  # type: ignore[return]
    """Extract db_engine from app state (set during bootstrap)."""
    ctx = getattr(request.app.state, "app_context", None)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialized")
    db = getattr(ctx, "_sync_db_engine", None) or getattr(ctx, "db_engine", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    return db


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@audit_router.post(
    "/audit/export",
    response_model=AuditExportResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def trigger_export(
    payload: AuditExportRequest,
    background_tasks: BackgroundTasks,
    request: Request,
) -> AuditExportResponse:
    """Trigger an async audit log export job."""
    db = _get_db(request)

    backend_map = {"local": "local", "s3": "s3", "gcs": "gcs"}
    backend = backend_map.get(payload.storage_backend, "local")

    exporter = AuditExporter(db_engine=db, storage_backend=backend)  # type: ignore[arg-type]

    result = await exporter.export(
        tenant_id=payload.tenant_id,
        start_date=payload.start_date,
        end_date=payload.end_date,
        dest_path=payload.dest_path,
    )

    return AuditExportResponse(
        status="complete",
        file_path=result.file_path,
        row_count=result.row_count,
        duration_ms=result.duration_ms,
    )


@audit_router.get("/audit/exports")
async def list_exports(
    request: Request,
    tenant_id: str = "default",
    limit: int = 50,
) -> list[dict]:
    """List past audit exports for a tenant."""
    db = _get_db(request)
    try:
        from sqlalchemy import text
        with db.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT tenant_id, file_path, row_count, exported_at "
                    "FROM mm_audit_exports "
                    "WHERE tenant_id = :tid "
                    "ORDER BY exported_at DESC "
                    "LIMIT :lim"
                ),
                {"tid": tenant_id, "lim": limit},
            ).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("list_exports failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
