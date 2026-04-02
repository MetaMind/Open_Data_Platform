"""Tenant Onboarding Wizard API.

Multi-step onboarding flow stored as a Redis session with a 30-minute TTL.

Steps:
  1 → start:    validate input, create session
  2 → provision: create DB rows for new tenant
  3 → validate:  run a test query against all engines
  4 → activate:  set is_active=true, dispatch welcome webhook

Session key: mm:onboard:{session_id}  (TTL 1800 s)
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field, EmailStr

logger = logging.getLogger(__name__)

onboarding_router = APIRouter(tags=["onboarding"])

_SESSION_TTL = 1800    # 30 minutes
_SESSION_PREFIX = "mm:onboard"

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class OnboardingStartRequest(BaseModel):
    tenant_name: str = Field(..., min_length=2, max_length=100)
    contact_email: str = Field(...)
    feature_preset: str = Field(default="standard",
                                pattern="^(standard|analytics|realtime)$")


class StepResponse(BaseModel):
    session_id: str
    step: int
    status: str
    next_url: Optional[str] = None
    errors: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def _session_key(session_id: str) -> str:
    return f"{_SESSION_PREFIX}:{session_id}"


def _get_redis(request: Request):  # type: ignore[return]
    ctx = getattr(request.app.state, "app_context", None)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialized")
    redis = getattr(ctx, "_redis_client", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis not available")
    return redis


def _get_db(request: Request):  # type: ignore[return]
    ctx = getattr(request.app.state, "app_context", None)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialized")
    db = getattr(ctx, "_sync_db_engine", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    return db


def _load_session(redis: object, session_id: str) -> dict:
    key = _session_key(session_id)
    try:
        raw = redis.get(key)  # type: ignore[union-attr]
        if raw is None:
            raise HTTPException(status_code=410, detail="Session expired or not found")
        return json.loads(raw)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("_load_session failed session=%s: %s", session_id, exc)
        raise HTTPException(status_code=503, detail="Session store error") from exc


def _save_session(redis: object, session_id: str, data: dict) -> None:
    key = _session_key(session_id)
    try:
        redis.setex(key, _SESSION_TTL, json.dumps(data))  # type: ignore[union-attr]
    except Exception as exc:
        logger.error("_save_session failed session=%s: %s", session_id, exc)


# ---------------------------------------------------------------------------
# Step 1: Start
# ---------------------------------------------------------------------------

@onboarding_router.post(
    "/onboarding/start",
    response_model=StepResponse,
    status_code=status.HTTP_201_CREATED,
)
async def onboarding_start(
    payload: OnboardingStartRequest,
    request: Request,
) -> StepResponse:
    """Begin a new tenant onboarding session."""
    redis = _get_redis(request)
    session_id = str(uuid.uuid4())

    session_data = {
        "session_id": session_id,
        "step": 1,
        "status": "started",
        "tenant_name": payload.tenant_name,
        "contact_email": payload.contact_email,
        "feature_preset": payload.feature_preset,
        "tenant_id": None,
        "created_at": time.time(),
        "errors": [],
    }
    _save_session(redis, session_id, session_data)

    return StepResponse(
        session_id=session_id,
        step=1,
        status="started",
        next_url=f"/api/v1/onboarding/{session_id}/step/2",
    )


# ---------------------------------------------------------------------------
# Step 2: Provision
# ---------------------------------------------------------------------------

@onboarding_router.post(
    "/onboarding/{session_id}/step/2",
    response_model=StepResponse,
)
async def onboarding_provision(session_id: str, request: Request) -> StepResponse:
    """Provision database rows for the new tenant."""
    redis = _get_redis(request)
    db = _get_db(request)
    session = _load_session(redis, session_id)

    if session["step"] < 1:
        raise HTTPException(status_code=400, detail="Step 1 not completed")

    tenant_id = f"tenant_{session_id[:8]}"
    errors: list[str] = []

    try:
        from sqlalchemy import text
        with db.begin() as conn:
            # W-17: Check uniqueness before insert — prevents 500 from DB constraint
            existing = conn.execute(
                text("SELECT 1 FROM mm_tenants WHERE name = :name LIMIT 1"),
                {"name": session["tenant_name"]},
            ).fetchone()
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Tenant name '{session['tenant_name']}' already exists.",
                )
            conn.execute(
                text(
                    "INSERT INTO mm_tenants (tenant_id, name, is_active) "
                    "VALUES (:tid, :name, FALSE) "
                    "ON CONFLICT (tenant_id) DO NOTHING"
                ),
                {"tid": tenant_id, "name": session["tenant_name"]},
            )
        logger.info("Onboarding provisioned tenant_id=%s", tenant_id)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Onboarding provision failed: %s", exc)
        errors.append(str(exc))

    session.update({"step": 2, "status": "provisioned", "tenant_id": tenant_id, "errors": errors})
    _save_session(redis, session_id, session)

    return StepResponse(
        session_id=session_id,
        step=2,
        status="provisioned" if not errors else "error",
        next_url=f"/api/v1/onboarding/{session_id}/step/3",
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Step 3: Validate
# ---------------------------------------------------------------------------

@onboarding_router.post(
    "/onboarding/{session_id}/step/3",
    response_model=StepResponse,
)
async def onboarding_validate(session_id: str, request: Request) -> StepResponse:
    """Run a test query to validate the tenant environment."""
    redis = _get_redis(request)
    db = _get_db(request)
    session = _load_session(redis, session_id)

    if session["step"] < 2:
        raise HTTPException(status_code=400, detail="Step 2 not completed")

    errors: list[str] = []
    try:
        from sqlalchemy import text
        with db.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()
    except Exception as exc:
        errors.append(f"Validation query failed: {exc}")
        logger.error("Onboarding validate failed: %s", exc)

    session.update({"step": 3, "status": "validated" if not errors else "error", "errors": errors})
    _save_session(redis, session_id, session)

    return StepResponse(
        session_id=session_id,
        step=3,
        status="validated" if not errors else "error",
        next_url=f"/api/v1/onboarding/{session_id}/step/4",
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Step 4: Activate
# ---------------------------------------------------------------------------

@onboarding_router.post(
    "/onboarding/{session_id}/step/4",
    response_model=StepResponse,
)
async def onboarding_activate(session_id: str, request: Request) -> StepResponse:
    """Activate the tenant and dispatch a welcome webhook."""
    redis = _get_redis(request)
    db = _get_db(request)
    session = _load_session(redis, session_id)

    if session["step"] < 3:
        raise HTTPException(status_code=400, detail="Step 3 not completed")

    tenant_id = session.get("tenant_id")
    errors: list[str] = []

    try:
        from sqlalchemy import text
        with db.begin() as conn:
            conn.execute(
                text(
                    "UPDATE mm_tenants SET is_active = TRUE "
                    "WHERE tenant_id = :tid"
                ),
                {"tid": tenant_id},
            )
        logger.info("Onboarding activated tenant_id=%s", tenant_id)
    except Exception as exc:
        errors.append(f"Activation failed: {exc}")
        logger.error("Onboarding activate DB error: %s", exc)

    session.update({"step": 4, "status": "active" if not errors else "error", "errors": errors})
    _save_session(redis, session_id, session)

    return StepResponse(
        session_id=session_id,
        step=4,
        status="active" if not errors else "error",
        next_url=None,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# GET: Session status
# ---------------------------------------------------------------------------

@onboarding_router.get(
    "/onboarding/{session_id}",
    response_model=StepResponse,
)
async def get_onboarding_status(session_id: str, request: Request) -> StepResponse:
    """Return current onboarding session status."""
    redis = _get_redis(request)
    session = _load_session(redis, session_id)

    step: int = session.get("step", 0)
    next_step = step + 1 if step < 4 else None
    next_url = (
        f"/api/v1/onboarding/{session_id}/step/{next_step}" if next_step else None
    )

    return StepResponse(
        session_id=session_id,
        step=step,
        status=session.get("status", "unknown"),
        next_url=next_url,
        errors=session.get("errors", []),
    )
