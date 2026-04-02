"""A/B Testing API routes.

POST /api/v1/ab/experiments          — create experiment
POST /api/v1/ab/experiments/{id}/run — run experiment
GET  /api/v1/ab/experiments/{id}     — fetch results
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from metamind.core.testing.ab_framework import ABTest

logger = logging.getLogger(__name__)

ab_router = APIRouter(tags=["ab-testing"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class CreateExperimentRequest(BaseModel):
    name: str = Field(..., description="Human-readable experiment name")
    sql_a: str = Field(..., description="Baseline SQL variant A")
    sql_b: str = Field(..., description="Candidate SQL variant B")
    tenant_id: str = Field(default="default")
    sample_pct: float = Field(default=0.1, ge=0.0, le=1.0)


class RunExperimentRequest(BaseModel):
    n_runs: int = Field(default=5, ge=1, le=20)


# ---------------------------------------------------------------------------
# Dependency helper
# ---------------------------------------------------------------------------

def _get_ab_test(request: Request) -> ABTest:
    ctx = getattr(request.app.state, "app_context", None)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialized")
    db = getattr(ctx, "_sync_db_engine", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    qe = getattr(ctx, "_query_engine", None)
    return ABTest(db_engine=db, query_engine=qe)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@ab_router.post(
    "/ab/experiments",
    status_code=status.HTTP_201_CREATED,
)
async def create_experiment(
    payload: CreateExperimentRequest,
    request: Request,
) -> dict:
    """Create a new A/B query experiment."""
    ab = _get_ab_test(request)
    experiment_id = await ab.create_experiment(
        name=payload.name,
        sql_a=payload.sql_a,
        sql_b=payload.sql_b,
        tenant_id=payload.tenant_id,
        sample_pct=payload.sample_pct,
    )
    return {"experiment_id": experiment_id, "status": "pending"}


@ab_router.post("/ab/experiments/{experiment_id}/run")
async def run_experiment(
    experiment_id: str,
    payload: RunExperimentRequest,
    request: Request,
) -> dict:
    """Execute an A/B experiment and return comparison results."""
    ab = _get_ab_test(request)
    try:
        result = await ab.run(experiment_id, n_runs=payload.n_runs)
        return result.to_dict()
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("run_experiment failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@ab_router.get("/ab/experiments/{experiment_id}")
async def get_experiment(
    experiment_id: str,
    request: Request,
) -> dict:
    """Fetch experiment definition and results."""
    ctx = getattr(request.app.state, "app_context", None)
    if ctx is None:
        raise HTTPException(status_code=503, detail="Application not initialized")
    db = getattr(ctx, "_sync_db_engine", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    try:
        import json
        from sqlalchemy import text
        with db.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT experiment_id, name, sql_a, sql_b, tenant_id, "
                    "sample_pct, status, result_json, "
                    "CAST(created_at AS TEXT) AS created_at, "
                    "CAST(completed_at AS TEXT) AS completed_at "
                    "FROM mm_ab_experiments WHERE experiment_id = :eid"
                ),
                {"eid": experiment_id},
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Experiment not found")
        data = dict(row._mapping)
        if data.get("result_json"):
            data["result"] = json.loads(data.pop("result_json"))
        return data
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("get_experiment failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
