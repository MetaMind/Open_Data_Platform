"""
Query Routes — All /api/v1/query/* FastAPI Endpoints

File: metamind/api/query_routes.py
Role: API Engineer
Dependencies: fastapi, metamind.bootstrap

Extracted from server.py (was 534 lines) to keep server.py ≤ 280 lines.
Note: This module is currently not mounted by `metamind.api.server`.
It is used by targeted tests and kept for backward compatibility.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

logger = logging.getLogger(__name__)

query_router = APIRouter(tags=["queries"])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=100_000)
    tenant_id: str = Field(default="default")
    user_id: str = Field(default="anonymous")
    freshness_requirement: str = Field(default="standard")
    stream: bool = Field(default=False)
    explain_only: bool = Field(default=False)


class QueryResponse(BaseModel):
    query_id: str
    status: str
    tenant_id: str
    target_source: str
    target_source_type: str
    execution_strategy: str
    estimated_cost_ms: float
    actual_cost_ms: Optional[float]
    cache_hit: bool
    rows_returned: int
    routing_reason: str
    columns: List[str]
    data: List[Dict[str, Any]]
    logical_plan: Optional[Dict]
    error: Optional[str]


# ---------------------------------------------------------------------------
# Dependency: get AppContext from request state
# ---------------------------------------------------------------------------

def _ctx(request: Request) -> Any:
    return request.app.state.context


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@query_router.post("/query/execute", response_model=QueryResponse, status_code=status.HTTP_200_OK)
async def execute_query(request: QueryRequest, req: Request) -> QueryResponse:
    """Execute a SQL query through the MetaMind router."""
    ctx = _ctx(req)
    query_id = str(uuid.uuid4())
    start = datetime.utcnow()

    try:
        router = ctx.query_router
        if router is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Query router not initialized",
            )

        decision = await router.route(
            sql=request.sql,
            tenant_id=request.tenant_id,
            user_id=request.user_id,
            freshness_requirement=request.freshness_requirement,
        )

        if request.explain_only:
            return QueryResponse(
                query_id=query_id,
                status="explained",
                tenant_id=request.tenant_id,
                target_source=decision.target_source,
                target_source_type=decision.target_source_type,
                execution_strategy=decision.execution_strategy.value,
                estimated_cost_ms=decision.estimated_cost_ms,
                actual_cost_ms=None,
                cache_hit=False,
                rows_returned=0,
                routing_reason=decision.reason,
                columns=[],
                data=[],
                logical_plan=None,
                error=None,
            )

        result, actual_ms = await _execute_decision(ctx, decision, request)
        columns = list(result.schema.names) if hasattr(result, "schema") else []
        rows = (
            result.to_pydict()
            if hasattr(result, "to_pydict")
            else {}
        )

        # Flatten pydict to list of row dicts
        data: List[Dict[str, Any]] = []
        if columns and rows:
            n = len(rows.get(columns[0], []))
            for i in range(min(n, 10_000)):
                data.append({col: rows[col][i] for col in columns})

        return QueryResponse(
            query_id=query_id,
            status="completed",
            tenant_id=request.tenant_id,
            target_source=decision.target_source,
            target_source_type=decision.target_source_type,
            execution_strategy=decision.execution_strategy.value,
            estimated_cost_ms=decision.estimated_cost_ms,
            actual_cost_ms=actual_ms,
            cache_hit=bool(decision.cache_key and decision.execution_strategy.value == "cached"),
            rows_returned=len(data),
            routing_reason=decision.reason,
            columns=columns,
            data=data,
            logical_plan=None,
            error=None,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("execute_query failed tenant=%s: %s", request.tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


async def _execute_decision(ctx: Any, decision: Any, req: QueryRequest) -> Any:
    """Dispatch to the correct execution engine based on routing decision."""
    import time
    t0 = time.monotonic()
    engine_type = decision.target_source_type

    try:
        if engine_type == "oracle":
            engine = ctx.oracle_connector
            result = await engine.execute(decision.rewritten_sql, req.tenant_id)
        elif engine_type == "trino":
            engine = ctx.trino_engine
            result = await engine.execute(decision.rewritten_sql, req.tenant_id)
        elif engine_type == "spark":
            engine = ctx.spark_engine
            result = await engine.submit_batch(decision.rewritten_sql, req.tenant_id)
        else:
            raise ValueError(f"Unknown engine type: {engine_type}")
    except AttributeError:
        # Engine not configured; return empty result
        import pyarrow as pa
        result = pa.table({})
    actual_ms = (time.monotonic() - t0) * 1000
    return result, actual_ms


@query_router.get("/query/history", status_code=status.HTTP_200_OK)
async def get_query_history(
    req: Request,
    tenant_id: str = "default",
    limit: int = 50,
    offset: int = 0,
    engine: str = "",
    from_ts: str = "",
    to_ts: str = "",
) -> Dict[str, Any]:
    """Return paginated query execution history."""
    ctx = _ctx(req)
    try:
        filters = "WHERE ql.tenant_id = :tid"
        params: Dict[str, Any] = {"tid": tenant_id, "lim": limit, "off": offset}
        if engine:
            filters += " AND ql.target_source = :eng"
            params["eng"] = engine
        if from_ts:
            filters += " AND ql.submitted_at >= :from_ts::timestamptz"
            params["from_ts"] = from_ts
        if to_ts:
            filters += " AND ql.submitted_at <= :to_ts::timestamptz"
            params["to_ts"] = to_ts

        async with ctx.async_db_engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        f"""
                        SELECT query_id,
                               original_sql AS sql_text,
                               target_source,
                               total_time_ms AS execution_time_ms,
                               row_count AS rows_returned,
                               status,
                               submitted_at
                        FROM mm_query_logs ql
                        {filters}
                        ORDER BY submitted_at DESC
                        LIMIT :lim OFFSET :off
                        """
                    ),
                    params,
                )
            ).fetchall()
        return {
            "items": [dict(r._mapping) for r in rows],
            "limit": limit,
            "offset": offset,
        }
    except Exception as exc:
        logger.error("get_query_history failed tenant=%s: %s", tenant_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@query_router.delete("/query/{query_id}/cancel", status_code=status.HTTP_200_OK)
async def cancel_query(query_id: str, req: Request) -> Dict[str, str]:
    """Request cancellation of an in-flight query."""
    ctx = _ctx(req)
    try:
        async with ctx.async_db_engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE mm_query_logs SET status = 'cancelled'
                    WHERE query_id = :qid AND status = 'running'
                    """
                ),
                {"qid": query_id},
            )
        return {"query_id": query_id, "status": "cancel_requested"}
    except Exception as exc:
        logger.error("cancel_query failed query_id=%s: %s", query_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@query_router.get("/synthesis/status", status_code=status.HTTP_200_OK)
async def synthesis_status(req: Request) -> Dict[str, Any]:
    """Return current AI synthesis engine status and metrics."""
    ctx = _ctx(req)
    synthesis_engine = getattr(ctx, "_synthesis_engine", None)
    if synthesis_engine is None:
        return {"status": "disabled", "metrics": {}}
    return {"status": "running", **synthesis_engine.get_status()}


@query_router.post("/synthesis/run", status_code=status.HTTP_202_ACCEPTED)
async def run_synthesis(req: Request, tenant_id: str = "default") -> Dict[str, Any]:
    """Trigger an immediate synthesis cycle for a tenant."""
    ctx = _ctx(req)
    synthesis_engine = getattr(ctx, "_synthesis_engine", None)
    if synthesis_engine is None:
        raise HTTPException(status_code=503, detail="Synthesis engine not initialized")
    result = await synthesis_engine.run_synthesis_cycle(tenant_id)
    return {
        "tenant_id": result.tenant_id,
        "rules_generated": result.rules_generated,
        "rules_retired": result.rules_retired,
        "retrained": result.retrained,
        "cycle_duration_ms": result.cycle_duration_ms,
        "error": result.error,
    }
