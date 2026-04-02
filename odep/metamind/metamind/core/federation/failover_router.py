"""Multi-Region Failover Router.

Wraps backend dispatch with automatic failover logic.  On ConnectionError
or timeout, retries on the next healthy engine in the same region, then
cross-region if all local engines are unhealthy.

Failover events are logged to mm_failover_events.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional, TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from metamind.core.backends.connector import QueryResult

logger = logging.getLogger(__name__)


@dataclass
class FailoverEvent:
    """Record of a single failover occurrence."""

    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:16])
    tenant_id: str = ""
    original_engine: str = ""
    failover_engine: str = ""
    reason: str = ""


class QueryExecutionError(Exception):
    """Raised when all failover attempts are exhausted."""


class FailoverRouter:
    """Route queries with automatic failover across engines and regions.

    Args:
        health_registry: Object exposing is_healthy(engine_name) -> bool.
        region_map: Mapping of region → ordered list of engine names.
                    Example: {'us-east-1': ['trino-primary', 'spark-backup']}
        db_engine: SQLAlchemy Engine for persisting failover events.
        latency_oracle: Optional callable (engine_name) -> float returning
                        recent P95 ms.  When provided, failover candidates are
                        sorted by ascending latency so the fastest alternative
                        is chosen first (fixes W-15).
    """

    def __init__(
        self,
        health_registry: Any,
        region_map: dict[str, list[str]],
        db_engine: Optional[Engine] = None,
        latency_oracle: Optional[Any] = None,
    ) -> None:
        self._health = health_registry
        self._region_map = region_map
        self._db_engine = db_engine
        self._latency_oracle = latency_oracle
        # Reverse map: engine_name -> region
        self._engine_region: dict[str, str] = {
            engine: region
            for region, engines in region_map.items()
            for engine in engines
        }

    def _sort_by_latency(self, engines: list[str]) -> list[str]:
        """Sort engines ascending by P95 latency (fixes W-15)."""
        if self._latency_oracle is None:
            return engines
        def _p95(e: str) -> float:
            try:
                return float(self._latency_oracle(e))
            except Exception:
                return float("inf")
        return sorted(engines, key=_p95)

    # ------------------------------------------------------------------
    # Engine selection
    # ------------------------------------------------------------------

    def primary_engine(self, tenant_id: str) -> str:
        """Return the highest-priority healthy engine for a tenant.

        Uses the first region in region_map ordering as the preferred region.
        Falls back across regions if the primary region is fully down.
        """
        for region, engines in self._region_map.items():
            for engine in engines:
                if self._is_healthy(engine):
                    return engine
        # All engines unhealthy — return first as last resort
        first_region = next(iter(self._region_map.values()), [])
        return first_region[0] if first_region else "metamind-internal"

    def failover_engine(
        self, failed_engine: str, tenant_id: str
    ) -> Optional[str]:
        """Return the fastest healthy engine after failed_engine (fixes W-15).

        Candidates within each region are sorted ascending by P95 latency when
        a latency_oracle is available, so the fastest alternative is tried first.

        Search order:
        1. Healthy engines in the same region, sorted by latency.
        2. Healthy engines in other regions, sorted by latency.

        Returns None if no healthy alternative exists.
        """
        failed_region = self._engine_region.get(failed_engine)

        # Try same region first — sorted by P95 latency
        if failed_region:
            candidates = [
                e for e in self._region_map.get(failed_region, [])
                if e != failed_engine and self._is_healthy(e)
            ]
            for engine in self._sort_by_latency(candidates):
                logger.info(
                    "Failover: %s → %s (same region %s)",
                    failed_engine, engine, failed_region,
                )
                return engine

        # Cross-region fallback — sorted by P95 latency per region
        for region, engines in self._region_map.items():
            if region == failed_region:
                continue
            candidates = [e for e in engines if self._is_healthy(e)]
            for engine in self._sort_by_latency(candidates):
                logger.info(
                    "Failover: %s → %s (cross-region %s)",
                    failed_engine, engine, region,
                )
                return engine

        logger.error(
            "Failover: no healthy alternative for %s (tenant=%s)",
            failed_engine, tenant_id,
        )
        return None

    # ------------------------------------------------------------------
    # Execution with failover
    # ------------------------------------------------------------------

    async def execute_with_failover(
        self,
        query_ctx: Any,
        backends: dict[str, Any],
    ) -> Any:
        """Execute query_ctx on primary engine with automatic failover.

        Args:
            query_ctx: Object with .tenant_id, .sql, .query_id attributes.
            backends: Dict mapping engine_name -> callable that returns QueryResult.

        Returns:
            QueryResult from the first successful execution.

        Raises:
            QueryExecutionError: If all engines fail.
        """
        tenant_id: str = getattr(query_ctx, "tenant_id", "unknown")
        chosen = self.primary_engine(tenant_id)
        tried: list[str] = []

        while chosen and chosen not in tried:
            tried.append(chosen)
            backend_fn = backends.get(chosen)
            if backend_fn is None:
                logger.warning("No backend registered for engine=%s", chosen)
                alt = self.failover_engine(chosen, tenant_id)
                if alt and alt not in tried:
                    chosen = alt
                    continue
                break

            try:
                result = await self._call_backend(backend_fn, query_ctx)
                return result
            except (ConnectionError, TimeoutError, OSError) as exc:
                reason = str(exc)
                logger.error(
                    "Engine %s failed for query %s: %s — attempting failover",
                    chosen,
                    getattr(query_ctx, "query_id", "?"),
                    reason,
                )
                self._record_failover_event(
                    tenant_id=tenant_id,
                    original_engine=chosen,
                    failover_engine="",
                    reason=reason,
                )
                alt = self.failover_engine(chosen, tenant_id)
                if alt and alt not in tried:
                    self._record_failover_event(
                        tenant_id=tenant_id,
                        original_engine=chosen,
                        failover_engine=alt,
                        reason=reason,
                    )
                    chosen = alt
                else:
                    break

        raise QueryExecutionError(
            f"All engines exhausted for tenant={tenant_id}: tried={tried}"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_healthy(self, engine_name: str) -> bool:
        """Check engine health via health_registry, default True on error."""
        try:
            return bool(self._health.is_healthy(engine_name))
        except Exception as exc:
            logger.warning("Health check error for %s: %s", engine_name, exc)
            return True  # fail-open

    async def _call_backend(self, backend_fn: Any, query_ctx: Any) -> Any:
        """Invoke backend_fn (sync or async) with query_ctx."""
        import asyncio
        if asyncio.iscoroutinefunction(backend_fn):
            return await backend_fn(query_ctx)
        return backend_fn(query_ctx)

    def _record_failover_event(
        self,
        tenant_id: str,
        original_engine: str,
        failover_engine: str,
        reason: str,
    ) -> None:
        """Persist a failover event to mm_failover_events (best-effort)."""
        if self._db_engine is None:
            return
        try:
            with self._db_engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO mm_failover_events "
                        "(event_id, tenant_id, original_engine, failover_engine, "
                        " reason, occurred_at) "
                        "VALUES (:eid, :tid, :orig, :fail, :rsn, NOW())"
                    ),
                    {
                        "eid": str(uuid.uuid4())[:16],
                        "tid": tenant_id,
                        "orig": original_engine,
                        "fail": failover_engine,
                        "rsn": reason[:500],
                    },
                )
        except Exception as exc:
            logger.error("FailoverRouter._record_failover_event failed: %s", exc)
