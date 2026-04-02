"""
Tenant Quota Manager — Control Plane Component

File: metamind/core/quota_manager.py
Role: Platform Engineer
Split from: metamind/core/control_plane.py (Golden Rule: ≤500 lines)

Enforces per-tenant resource limits using Redis atomic counters:
- Concurrent query slots
- Per-minute / per-hour rate limiting
- Row, byte, and execution-time caps
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

_GB = 1024 * 1024 * 1024


@dataclass
class TenantQuota:
    """Resource limits for a single tenant."""
    tenant_id: str

    # Concurrency
    max_concurrent_queries: int = 10

    # Rate limits
    max_queries_per_minute: int = 1_000
    max_queries_per_hour: int = 10_000

    # Per-query resource caps
    max_rows_per_query: int = 1_000_000
    max_bytes_per_query: int = _GB           # 1 GB
    max_execution_time_seconds: int = 300    # 5 min

    # Cost cap (internal cost units)
    max_cost_per_query: float = 1_000.0

    # Cache allocation
    cache_quota_mb: int = 500


class TenantResourceManager:
    """
    Enforces tenant resource quotas via Redis atomic counters.

    All counters are stored in Redis with automatic TTL expiry so slots
    are reclaimed even when a worker crashes mid-query.

    Key schema::

        concurrent:<tenant_id>          INCR/DECR, TTL=300s
        queries:minute:<tenant_id>      INCR, TTL=60s
        queries:hour:<tenant_id>        INCR, TTL=3600s
        quota:<tenant_id>               JSON, TTL=300s
    """

    def __init__(self, redis_client: Any, db_engine: Any) -> None:
        self.redis = redis_client
        self.db_engine = db_engine
        logger.debug("TenantResourceManager initialised")

    # ------------------------------------------------------------------
    # Quota loading
    # ------------------------------------------------------------------

    async def get_quota(self, tenant_id: str) -> TenantQuota:
        """Return the quota for *tenant_id* (DB → default fallback)."""
        cached = await self.redis.get(f"quota:{tenant_id}")
        if cached:
            return TenantQuota(**json.loads(cached))

        quota = self._load_from_db(tenant_id)
        await self.redis.setex(f"quota:{tenant_id}", 300, json.dumps(quota.__dict__))
        return quota

    def _load_from_db(self, tenant_id: str) -> TenantQuota:
        try:
            with self.db_engine.connect() as conn:
                row = conn.execute(
                    text("SELECT * FROM mm_tenant_quotas WHERE tenant_id = :tid"),
                    {"tid": tenant_id},
                ).fetchone()
            if row:
                return TenantQuota(
                    tenant_id=tenant_id,
                    max_concurrent_queries=row.max_concurrent_queries,
                    max_queries_per_minute=row.max_queries_per_minute,
                    max_queries_per_hour=row.max_queries_per_hour,
                    max_rows_per_query=row.max_rows_per_query,
                    max_bytes_per_query=row.max_bytes_per_query,
                    max_execution_time_seconds=row.max_execution_time_seconds,
                )
        except Exception as exc:
            logger.warning("Could not load quota for tenant %s: %s", tenant_id, exc)
        return TenantQuota(tenant_id=tenant_id)

    # ------------------------------------------------------------------
    # Gate check (call before executing a query)
    # ------------------------------------------------------------------

    async def check_quota(
        self,
        tenant_id: str,
        estimated_rows: int = 0,
        estimated_bytes: int = 0,
    ) -> Dict[str, Any]:
        """
        Gate-check a pending query against all quota dimensions.

        Returns ``{"allowed": True}`` or ``{"allowed": False, "reason": str}``.
        """
        quota = await self.get_quota(tenant_id)

        concurrent = await self._get_concurrent(tenant_id)
        if concurrent >= quota.max_concurrent_queries:
            return {"allowed": False, "reason": (
                f"Concurrent limit reached ({concurrent}/{quota.max_concurrent_queries})"
            )}

        per_minute = await self._get_counter(f"queries:minute:{tenant_id}")
        if per_minute >= quota.max_queries_per_minute:
            return {"allowed": False, "reason": (
                f"Rate limit exceeded ({per_minute}/{quota.max_queries_per_minute}/min)"
            )}

        per_hour = await self._get_counter(f"queries:hour:{tenant_id}")
        if per_hour >= quota.max_queries_per_hour:
            return {"allowed": False, "reason": (
                f"Hourly limit exceeded ({per_hour}/{quota.max_queries_per_hour}/hr)"
            )}

        if estimated_rows and estimated_rows > quota.max_rows_per_query:
            return {"allowed": False, "reason": (
                f"Row cap exceeded (estimated {estimated_rows:,} > {quota.max_rows_per_query:,})"
            )}

        if estimated_bytes and estimated_bytes > quota.max_bytes_per_query:
            mb = estimated_bytes // (1024 * 1024)
            cap_mb = quota.max_bytes_per_query // (1024 * 1024)
            return {"allowed": False, "reason": f"Byte cap exceeded ({mb} MB > {cap_mb} MB)"}

        return {"allowed": True}

    # ------------------------------------------------------------------
    # Slot management (call on query start / end)
    # ------------------------------------------------------------------

    async def acquire_slot(self, tenant_id: str) -> None:
        """Increment the concurrent-query counter for *tenant_id*."""
        key = f"concurrent:{tenant_id}"
        new_val = await self.redis.incr(key)
        if new_val == 1:
            await self.redis.expire(key, 300)  # safety TTL

    async def release_slot(self, tenant_id: str) -> None:
        """Decrement the concurrent-query counter for *tenant_id*."""
        key = f"concurrent:{tenant_id}"
        val = await self.redis.decr(key)
        if val < 0:
            # Guard against negative counts caused by crash/restart
            await self.redis.set(key, 0)
            logger.warning("Concurrent counter went negative for tenant %s — reset to 0", tenant_id)

    async def record_query(self, tenant_id: str) -> None:
        """Increment the per-minute and per-hour rate-limit counters."""
        pipe = self.redis.pipeline()
        pipe.incr(f"queries:minute:{tenant_id}")
        pipe.expire(f"queries:minute:{tenant_id}", 60)
        pipe.incr(f"queries:hour:{tenant_id}")
        pipe.expire(f"queries:hour:{tenant_id}", 3600)
        await pipe.execute()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_concurrent(self, tenant_id: str) -> int:
        return await self._get_counter(f"concurrent:{tenant_id}")

    async def _get_counter(self, key: str) -> int:
        val = await self.redis.get(key)
        return int(val) if val else 0
