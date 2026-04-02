"""Query Plan Cache — Redis-backed, sqlglot-normalized, with Prometheus metrics.

Key format: mm:plan:{tenant_id}:{sha256_of_normalized_sql}
TTL: configurable (default 300 s).
Prometheus counters: PLAN_CACHE_HIT, PLAN_CACHE_MISS.
"""
from __future__ import annotations

import hashlib
import json
import logging
import pickle
from typing import Optional, TYPE_CHECKING

import sqlglot

if TYPE_CHECKING:
    from metamind.core.logical.nodes import LogicalNode

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus counters (optional — graceful no-op if prometheus_client absent)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter

    PLAN_CACHE_HIT = Counter(
        "metamind_plan_cache_hit_total",
        "Total plan cache hits",
        ["tenant_id"],
    )
    PLAN_CACHE_MISS = Counter(
        "metamind_plan_cache_miss_total",
        "Total plan cache misses",
        ["tenant_id"],
    )
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    PLAN_CACHE_HIT = None  # type: ignore[assignment]
    PLAN_CACHE_MISS = None  # type: ignore[assignment]


def _inc(counter: object, tenant_id: str) -> None:
    """Safely increment a Prometheus counter if available."""
    if _PROMETHEUS_AVAILABLE and counter is not None:
        try:
            counter.labels(tenant_id=tenant_id).inc()  # type: ignore[union-attr]
        except Exception as exc:
            logger.debug("Prometheus increment error: %s", exc)


class PlanCache:
    """Distributed query plan cache backed by Redis.

    Uses sqlglot to normalize SQL before hashing so parameterized
    variants of the same query share a single cache entry.

    Args:
        redis_client: An initialized redis.Redis (sync) client.
        ttl_seconds: Entry lifetime in Redis (default 300).
    """

    _KEY_PREFIX = "mm:plan"

    def __init__(
        self,
        redis_client: object,
        ttl_seconds: int = 300,
    ) -> None:
        self._redis = redis_client
        self._ttl = ttl_seconds

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def cache_key(self, sql: str, tenant_id: str) -> str:
        """Compute a deterministic Redis key for a SQL + tenant pair.

        Normalizes SQL with sqlglot (strips literals) then SHA-256 hashes.
        """
        normalized = self._normalize(sql)
        digest = hashlib.sha256(normalized.encode()).hexdigest()
        return f"{self._KEY_PREFIX}:{tenant_id}:{digest}"

    def get(self, sql: str, tenant_id: str) -> Optional[LogicalNode]:
        """Return a cached LogicalNode or None on miss / error."""
        key = self.cache_key(sql, tenant_id)
        try:
            raw = self._redis.get(key)  # type: ignore[union-attr]
            if raw is None:
                _inc(PLAN_CACHE_MISS, tenant_id)
                return None
            plan: LogicalNode = pickle.loads(raw)  # noqa: S301 — internal only
            _inc(PLAN_CACHE_HIT, tenant_id)
            logger.debug("PlanCache HIT key=%s tenant=%s", key[-12:], tenant_id)
            return plan
        except Exception as exc:
            logger.error(
                "PlanCache.get failed key=%s tenant=%s: %s", key[-12:], tenant_id, exc
            )
            _inc(PLAN_CACHE_MISS, tenant_id)
            return None

    def set(self, sql: str, tenant_id: str, plan: LogicalNode) -> None:
        """Serialize plan and store in Redis with TTL."""
        key = self.cache_key(sql, tenant_id)
        try:
            serialized = pickle.dumps(plan)
            self._redis.setex(key, self._ttl, serialized)  # type: ignore[union-attr]
            logger.debug(
                "PlanCache SET key=%s tenant=%s ttl=%ds",
                key[-12:],
                tenant_id,
                self._ttl,
            )
        except Exception as exc:
            logger.error(
                "PlanCache.set failed key=%s tenant=%s: %s", key[-12:], tenant_id, exc
            )

    def invalidate(
        self, tenant_id: str, table_name: Optional[str] = None
    ) -> int:
        """Delete matching cache keys; return count deleted.

        If table_name is supplied the prefix scan is unchanged but callers
        should invalidate per-tenant when a table is modified.
        """
        pattern = f"{self._KEY_PREFIX}:{tenant_id}:*"
        deleted = 0
        try:
            cursor = 0
            while True:
                cursor, keys = self._redis.scan(  # type: ignore[union-attr]
                    cursor, match=pattern, count=100
                )
                for key in keys:
                    self._redis.delete(key)  # type: ignore[union-attr]
                    deleted += 1
                if cursor == 0:
                    break
            logger.info(
                "PlanCache invalidated %d keys for tenant=%s table=%s",
                deleted,
                tenant_id,
                table_name,
            )
        except Exception as exc:
            logger.error(
                "PlanCache.invalidate error tenant=%s: %s", tenant_id, exc
            )
        return deleted

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _normalize(self, sql: str) -> str:
        """Normalize SQL with sqlglot; fall back to upper-stripped original."""
        try:
            statements = sqlglot.parse(sql)
            if statements and statements[0] is not None:
                return statements[0].sql(pretty=False).upper()
        except Exception as exc:
            logger.debug("sqlglot normalization error: %s", exc)
        return sql.strip().upper()
