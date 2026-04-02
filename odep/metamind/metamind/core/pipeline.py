"""Unified Query Pipeline — W-06.

Collapses QueryRouter (ML routing) and QueryEngine (Cascades optimizer +
execution) into a single async pipeline that the API calls once per request.

Request flow:
  SQL in
    → 1. Auth / firewall (F36)
    → 2. RLS rewrite     (F31)
    → 3. ML routing      (QueryRouter.route)
    → 4. SLA enforcement (F32)
    → 5. Cascades opt    (QueryEngine.execute)
    → 6. Noisy-neighbor throttle (F33)
    → 7. Failover guard  (F34)
  QueryPipelineResult out
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Optional

logger = logging.getLogger(__name__)


class UnifiedQueryPipeline:
    """Single entry-point that wires QueryRouter → QueryEngine end-to-end.

    Both components are stored as references; the pipeline delegates to each
    in the correct order so the API layer no longer needs to know the
    two-router architecture.

    Args:
        query_router:  Initialised QueryRouter instance.
        query_engine:  Initialised QueryEngine instance.
    """

    def __init__(
        self,
        query_router: Any,
        query_engine: Any,
    ) -> None:
        self._router = query_router
        self._engine = query_engine

    async def execute(
        self,
        sql: str,
        tenant_id: str,
        user_id: Optional[str] = None,
        freshness_tolerance_seconds: Optional[int] = None,
        use_cache: bool = True,
        backend_hint: Optional[str] = None,
        user_roles: Optional[list[str]] = None,
    ) -> Any:
        """Run the full unified pipeline and return a QueryPipelineResult.

        Step 1 — ML routing: ask QueryRouter which backend fits best.
        Step 2 — Engine execution: run QueryEngine with the routed backend,
                 which internally handles firewall, RLS, Cascades, and feedback.

        If QueryRouter is unavailable the engine falls back to auto-selection.
        """
        query_id = str(uuid.uuid4())

        # Step 1: ML routing decision
        routed_backend: Optional[str] = backend_hint
        if self._router is not None:
            try:
                user_context: dict[str, Any] = {
                    "user_id": user_id or "anonymous",
                    "freshness_tolerance_seconds": freshness_tolerance_seconds,
                    "use_cache": use_cache,
                }
                decision = await self._router.route(sql, tenant_id, user_context)
                routed_backend = getattr(decision, "target_source", None) or routed_backend
                logger.debug(
                    "UnifiedPipeline: router selected backend=%s for query=%s",
                    routed_backend, query_id,
                )
            except Exception as exc:
                logger.warning(
                    "UnifiedPipeline: QueryRouter failed (using engine default): %s", exc
                )

        # Step 2: QueryEngine execution (firewall, RLS, Cascades, online learner)
        from metamind.core.query_engine import QueryContext
        ctx = QueryContext(
            query_id=query_id,
            tenant_id=tenant_id,
            sql=sql,
            backend_hint=routed_backend,
            metadata={"user_roles": user_roles or [], "user_id": user_id or "anonymous"},
        )
        return await self._engine.execute(ctx)
