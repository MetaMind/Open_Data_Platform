"""Integration tests for UnifiedQueryPipeline (W-06) — W-04."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _make_pipeline(router_engine: str = "trino"):
    from metamind.core.pipeline import UnifiedQueryPipeline
    from metamind.core.query_engine import QueryPipelineResult
    from metamind.core.backends.connector import QueryResult as QR

    mock_result = QR(columns=["n"], rows=[{"n": 1}], row_count=1,
                     duration_ms=10.0, backend=router_engine)
    pipeline_result = QueryPipelineResult(
        result=mock_result, query_id="test-q", optimization_tier=1,
        cache_hit=False, workload_type="olap",
        backend_used=router_engine, optimization_ms=2.0,
        total_ms=12.0, plan_cost=50.0,
    )

    router = MagicMock()
    router_decision = MagicMock()
    router_decision.target_source = router_engine
    router.route = AsyncMock(return_value=router_decision)

    engine = MagicMock()
    engine.execute = AsyncMock(return_value=pipeline_result)

    return UnifiedQueryPipeline(query_router=router, query_engine=engine)


class TestUnifiedPipeline:
    @pytest.mark.asyncio
    async def test_execute_calls_router_then_engine(self) -> None:
        pipeline = _make_pipeline("trino")
        result = await pipeline.execute(
            sql="SELECT 1", tenant_id="acme",
        )
        assert result.backend_used == "trino"
        pipeline._router.route.assert_called_once()
        pipeline._engine.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_router_engine_name_passed_as_backend_hint(self) -> None:
        """Router decision flows into QueryEngine as backend_hint."""
        from metamind.core.query_engine import QueryContext

        pipeline = _make_pipeline("spark")
        await pipeline.execute(sql="SELECT COUNT(*) FROM big_table", tenant_id="globex")

        call_args = pipeline._engine.execute.call_args
        ctx: QueryContext = call_args[0][0]
        assert ctx.backend_hint == "spark"

    @pytest.mark.asyncio
    async def test_router_failure_falls_back_gracefully(self) -> None:
        """If router throws, engine still executes with no backend_hint."""
        from metamind.core.pipeline import UnifiedQueryPipeline
        from metamind.core.query_engine import QueryPipelineResult, QueryContext
        from metamind.core.backends.connector import QueryResult as QR

        mock_qr = QR(columns=[], rows=[], row_count=0, duration_ms=5.0, backend="internal")
        mock_pr = QueryPipelineResult(
            result=mock_qr, query_id="fb", optimization_tier=3,
            cache_hit=False, workload_type="unknown", backend_used="internal",
            optimization_ms=0.0, total_ms=5.0, plan_cost=0.0,
        )

        router = MagicMock()
        router.route = AsyncMock(side_effect=RuntimeError("router crashed"))
        engine = MagicMock()
        engine.execute = AsyncMock(return_value=mock_pr)

        pipeline = UnifiedQueryPipeline(query_router=router, query_engine=engine)
        result = await pipeline.execute(sql="SELECT 1", tenant_id="t1")

        # Engine was still called despite router failure
        engine.execute.assert_called_once()
        call_ctx: QueryContext = engine.execute.call_args[0][0]
        assert call_ctx.backend_hint is None

    @pytest.mark.asyncio
    async def test_user_roles_passed_to_engine_context(self) -> None:
        from metamind.core.query_engine import QueryContext

        pipeline = _make_pipeline()
        await pipeline.execute(
            sql="SELECT id FROM payments",
            tenant_id="acme",
            user_roles=["analyst"],
        )
        ctx: QueryContext = pipeline._engine.execute.call_args[0][0]
        assert "analyst" in ctx.metadata.get("user_roles", [])

    @pytest.mark.asyncio
    async def test_none_router_works(self) -> None:
        """Pipeline with no router (engine-only mode) still executes."""
        from metamind.core.pipeline import UnifiedQueryPipeline
        from metamind.core.query_engine import QueryPipelineResult
        from metamind.core.backends.connector import QueryResult as QR

        mock_qr = QR(columns=["x"], rows=[{"x": 1}], row_count=1,
                     duration_ms=7.0, backend="internal")
        mock_pr = QueryPipelineResult(
            result=mock_qr, query_id="nr", optimization_tier=1,
            cache_hit=False, workload_type="point_lookup",
            backend_used="internal", optimization_ms=1.0,
            total_ms=8.0, plan_cost=10.0,
        )
        engine = MagicMock()
        engine.execute = AsyncMock(return_value=mock_pr)

        pipeline = UnifiedQueryPipeline(query_router=None, query_engine=engine)
        result = await pipeline.execute(sql="SELECT 1", tenant_id="acme")
        assert result.backend_used == "internal"
