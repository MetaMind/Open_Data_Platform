"""Phase 3 federation API endpoints — mounted into the main FastAPI app."""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException

from metamind.api.auth import get_current_tenant
from metamind.api.models import (
    CreateFederatedMVRequest,
    CrossEnginePlanResponse,
    ExecuteRequest,
    FederatedMVResponse,
    PlacementRecommendationResponse,
    StatsSyncRequest,
    StatsSyncResponse,
)
from metamind.bootstrap import Bootstrap

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_bootstrap() -> Bootstrap:
    """Import lazily to avoid circular dependency at module load."""
    from metamind.api.server import get_bootstrap
    return get_bootstrap()


@router.post("/plan/federation", tags=["Federation"])
async def get_federation_plan(
    request: ExecuteRequest,
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(_get_bootstrap),
) -> CrossEnginePlanResponse:
    """F14: Return cross-engine plan analysis without executing."""
    from metamind.core.federation.planner import CrossEnginePlanner
    from metamind.core.costing.cost_model import CostModel
    from metamind.api.sql_parser import SQLParser
    from metamind.core.logical.nodes import ScanNode

    engine = bs.get_query_engine()
    flags = engine._flag_manager.get_flags()
    if not flags.F14_cross_engine_planning:
        raise HTTPException(status_code=403, detail="F14_cross_engine_planning not enabled")

    catalog = engine._catalog
    backend_registry = engine._backend_registry
    planner = CrossEnginePlanner(backend_registry, CostModel(), catalog)

    parser = SQLParser()
    logical_plan = parser.parse(request.sql)

    table_locations: dict[str, str] = {}
    try:
        def collect_scans(node: object) -> None:
            if isinstance(node, ScanNode):
                meta = catalog.get_table(tenant_id, node.schema_name, node.table_name)
                if meta:
                    table_locations[node.table_name] = meta.backend.value
            for child in getattr(node, "children", []):
                collect_scans(child)
        collect_scans(logical_plan)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not infer table locations: %s", exc)

    plan = planner.plan(logical_plan, tenant_id, table_locations)
    return CrossEnginePlanResponse(
        sub_plans=plan.sub_plans,
        transfer_count=len(plan.transfers),
        assembly_backend=plan.assembly_backend,
        estimated_total_cost_ms=plan.estimated_total_cost,
        estimated_transfer_bytes=plan.estimated_transfer_bytes,
        plan_explanation=plan.plan_explanation,
    )


@router.get("/federation/mvs", tags=["Federation"])
async def list_federated_mvs(
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(_get_bootstrap),
) -> list[FederatedMVResponse]:
    """F15: List all federated MVs for this tenant."""
    from metamind.core.mv.federated import FederatedMVManager
    engine = bs.get_query_engine()
    if not engine._flag_manager.get_flags().F15_federated_mvs:
        raise HTTPException(status_code=403, detail="F15_federated_mvs not enabled")
    manager = FederatedMVManager(bs.get_metadata_engine(), engine._backend_registry, None)
    return [
        FederatedMVResponse(
            mv_name=mv.mv_name, tenant_id=mv.tenant_id,
            source_backends=mv.source_backends, target_backend=mv.target_backend,
            refresh_policy=mv.refresh_policy,
            refresh_interval_minutes=mv.refresh_interval_minutes,
            last_refreshed=mv.last_refreshed,
            estimated_staleness_seconds=mv.estimated_staleness_seconds,
            row_count=mv.row_count,
        )
        for mv in manager.list_mvs(tenant_id)
    ]


@router.post("/federation/mvs", tags=["Federation"])
async def create_federated_mv(
    request: CreateFederatedMVRequest,
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(_get_bootstrap),
) -> FederatedMVResponse:
    """F15: Create a new federated MV definition."""
    from metamind.core.mv.federated import FederatedMVManager
    engine = bs.get_query_engine()
    if not engine._flag_manager.get_flags().F15_federated_mvs:
        raise HTTPException(status_code=403, detail="F15_federated_mvs not enabled")
    manager = FederatedMVManager(bs.get_metadata_engine(), engine._backend_registry, None)
    mv = manager.create(
        tenant_id=tenant_id, name=request.mv_name, source_sql=request.source_sql,
        source_backends=request.source_backends, target_backend=request.target_backend,
        refresh_policy=request.refresh_policy,
        refresh_interval_minutes=request.refresh_interval_minutes,
    )
    return FederatedMVResponse(
        mv_name=mv.mv_name, tenant_id=mv.tenant_id,
        source_backends=mv.source_backends, target_backend=mv.target_backend,
        refresh_policy=mv.refresh_policy,
        refresh_interval_minutes=mv.refresh_interval_minutes,
        last_refreshed=mv.last_refreshed,
        estimated_staleness_seconds=mv.estimated_staleness_seconds,
        row_count=mv.row_count,
    )


@router.post("/federation/mvs/{mv_name}/refresh", tags=["Federation"])
async def refresh_federated_mv(
    mv_name: str,
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(_get_bootstrap),
) -> dict[str, Any]:
    """F15: Trigger refresh of a federated MV."""
    from metamind.core.mv.federated import FederatedMVManager
    engine = bs.get_query_engine()
    if not engine._flag_manager.get_flags().F15_federated_mvs:
        raise HTTPException(status_code=403, detail="F15_federated_mvs not enabled")
    manager = FederatedMVManager(bs.get_metadata_engine(), engine._backend_registry, None)
    try:
        return manager.refresh(tenant_id, mv_name, engine._backend_registry)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/advisor/placement", tags=["Advisor"])
async def get_placement_recommendations(
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(_get_bootstrap),
) -> list[PlacementRecommendationResponse]:
    """F16: Get data placement recommendations based on query history."""
    from metamind.core.federation.placement_advisor import DataPlacementAdvisor
    from metamind.core.costing.cost_model import CostModel
    import sqlalchemy as sa

    engine = bs.get_query_engine()
    if not engine._flag_manager.get_flags().F16_data_placement_advisor:
        raise HTTPException(status_code=403, detail="F16_data_placement_advisor not enabled")

    advisor = DataPlacementAdvisor(bs.get_metadata_engine(), engine._catalog, CostModel())
    query_history: list[dict] = []
    try:
        with bs.get_metadata_engine().connect() as conn:
            rows = conn.execute(
                sa.text(
                    "SELECT sql_fingerprint, backend_used, tables_referenced "
                    "FROM mm_workload_patterns WHERE tenant_id = :tid LIMIT 1000"
                ),
                {"tid": tenant_id},
            ).fetchall()
        query_history = [
            {"sql": r[0], "backend_used": r[1], "tables": list(r[2] or [])}
            for r in rows
        ]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not load query history: %s", exc)

    recs = advisor.analyze(
        tenant_id, query_history, {}, engine._backend_registry.list_backends()
    )
    return [
        PlacementRecommendationResponse(
            table=r.table, current_backend=r.current_backend,
            recommended_backend=r.recommended_backend, reason=r.reason,
            estimated_query_speedup_x=r.estimated_query_speedup_x,
            estimated_cost_savings_monthly_usd=r.estimated_cost_savings_monthly_usd,
            migration_difficulty=r.migration_difficulty,
            affected_query_count=r.affected_query_count, confidence=r.confidence,
        )
        for r in recs
    ]


@router.post("/federation/stats/sync", tags=["Federation"])
async def sync_stats(
    request: StatsSyncRequest,
    tenant_id: str = Depends(get_current_tenant),
    bs: Bootstrap = Depends(_get_bootstrap),
) -> StatsSyncResponse:
    """F18: Trigger cross-engine statistics synchronization for a table."""
    from metamind.core.federation.stats_sync import StatsSynchronizer
    engine = bs.get_query_engine()
    if not engine._flag_manager.get_flags().F18_cross_engine_stats_sync:
        raise HTTPException(status_code=403, detail="F18_cross_engine_stats_sync not enabled")
    syncer = StatsSynchronizer(bs.get_metadata_engine(), engine._catalog, engine._backend_registry)
    result = syncer.sync(
        tenant_id=tenant_id,
        table_name=request.table_name,
        source_backend=request.source_backend,
        target_backends=request.target_backends,
    )
    return StatsSyncResponse(
        table_name=request.table_name,
        columns_synced=int(result.get("columns_synced", 0)),
        duration_ms=float(result.get("duration_ms", 0.0)),
        conflicts=list(result.get("conflicts", [])),
    )
