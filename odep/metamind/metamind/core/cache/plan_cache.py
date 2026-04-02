"""F09 — Workload-Aware Plan Cache with fingerprinting and template extraction."""
from __future__ import annotations

import hashlib
import inspect
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Regex to normalize literals for cache key extraction
_INT_PATTERN = re.compile(r"\b\d+\b")
_FLOAT_PATTERN = re.compile(r"\b\d+\.\d+\b")
_STRING_PATTERN = re.compile(r"'[^']*'")
_IN_LIST_PATTERN = re.compile(r"IN\s*\([^)]+\)", re.IGNORECASE)


@dataclass
class CachedPlan:
    """A cached execution plan entry."""

    fingerprint: str
    tenant_id: str
    plan_json: str            # Serialized physical plan
    backend: str
    estimated_cost: float
    hit_count: int = 0
    created_at: float = field(default_factory=time.time)
    last_used_at: float = field(default_factory=time.time)
    plan_age_seconds: float = 0.0
    is_valid: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def record_hit(self) -> None:
        """Record a cache hit."""
        self.hit_count += 1
        self.last_used_at = time.time()

    @property
    def age_seconds(self) -> float:
        """How old this cache entry is."""
        return time.time() - self.created_at


class QueryFingerprint:
    """Generates stable cache keys from SQL queries.

    Normalizes literals, IN-list sizes, and whitespace to maximize cache hits
    for parameterized queries while maintaining safety.
    """

    def compute(self, sql: str, tenant_id: str) -> str:
        """Compute stable fingerprint for SQL + tenant combination.

        Normalizes:
        - String literals → '?'
        - Integer constants → ?
        - Float constants → ?
        - IN lists → IN (?, ...)
        - Whitespace normalization
        """
        normalized = self._normalize(sql)
        raw = f"{tenant_id}:{normalized}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def _normalize(self, sql: str) -> str:
        """Apply normalization rules to SQL."""
        # Order matters: normalize longer patterns first
        s = sql.strip()
        s = _FLOAT_PATTERN.sub("?", s)
        s = _INT_PATTERN.sub("?", s)
        s = _STRING_PATTERN.sub("'?'", s)
        s = _IN_LIST_PATTERN.sub("IN (?)", s)
        # Normalize whitespace
        s = re.sub(r"\s+", " ", s)
        return s.upper()

    def extract_template(self, sql: str) -> str:
        """Extract parameterized SQL template for workload analysis."""
        return self._normalize(sql)


class PlanCache:
    """In-memory + Redis plan cache for query optimization results.

    Cache hierarchy:
    1. L1: In-process dict (sub-millisecond)
    2. L2: Redis (distributed, shared across API instances)

    Cache key format: {tenant_id}:plan:{fingerprint}
    """

    def __init__(
        self,
        redis_client: Optional[object],
        ttl_seconds: int = 86400,
        max_local_entries: int = 10000,
    ) -> None:
        """Initialize plan cache with optional Redis backend."""
        self._redis = redis_client
        self._ttl = ttl_seconds
        self._local: dict[str, CachedPlan] = {}
        self._max_local = max_local_entries
        self._fingerprinter = QueryFingerprint()
        self._hits = 0
        self._misses = 0

    def get(self, sql: str, tenant_id: str) -> Optional[CachedPlan]:
        """Look up cached plan. Returns None on miss."""
        fp = self._fingerprinter.compute(sql, tenant_id)
        cache_key = f"{tenant_id}:plan:{fp}"

        # L1 lookup
        if cache_key in self._local:
            entry = self._local[cache_key]
            if entry.is_valid:
                entry.record_hit()
                self._hits += 1
                logger.debug("Plan cache L1 hit: %s", fp[:8])
                return entry

        # L2 Redis lookup
        if self._redis is not None:
            try:
                raw = self._redis.get(cache_key)  # type: ignore[union-attr]
                if inspect.isawaitable(raw):
                    logger.debug(
                        "PlanCache.get skipping async Redis client for key=%s tenant=%s",
                        fp[:8],
                        tenant_id,
                    )
                    raw = None
                if raw:
                    if isinstance(raw, (bytes, bytearray)):
                        raw = raw.decode("utf-8")
                    data = json.loads(raw)
                    entry = CachedPlan(**data)
                    entry.record_hit()
                    self._local[cache_key] = entry  # promote to L1
                    self._hits += 1
                    logger.debug("Plan cache L2 hit: %s", fp[:8])
                    return entry
            except Exception as exc:
                logger.warning("Redis plan cache error: %s", exc)

        self._misses += 1
        return None

    def put(self, sql: str, tenant_id: str, plan_json: str, backend: str, cost: float) -> str:
        """Store a plan in cache. Returns fingerprint."""
        fp = self._fingerprinter.compute(sql, tenant_id)
        cache_key = f"{tenant_id}:plan:{fp}"

        entry = CachedPlan(
            fingerprint=fp,
            tenant_id=tenant_id,
            plan_json=plan_json,
            backend=backend,
            estimated_cost=cost,
        )

        # L1 store (with eviction if full)
        if len(self._local) >= self._max_local:
            self._evict_lru()
        self._local[cache_key] = entry

        # L2 Redis store
        if self._redis is not None:
            try:
                ret = self._redis.setex(cache_key, self._ttl, json.dumps(entry.__dict__))  # type: ignore[union-attr]
                if inspect.isawaitable(ret):
                    logger.debug("PlanCache.put skipping async Redis client for key=%s", fp[:8])
            except Exception as exc:
                logger.warning("Failed to store plan in Redis: %s", exc)

        logger.debug("Cached plan %s for tenant %s", fp[:8], tenant_id)
        return fp

    def invalidate(self, tenant_id: str, table_name: Optional[str] = None) -> int:
        """Invalidate cache entries for a tenant (all or table-specific)."""
        prefix = f"{tenant_id}:plan:"
        removed = 0

        # L1 eviction
        keys = [k for k in self._local if k.startswith(prefix)]
        for k in keys:
            # If table_name specified, only evict plans referencing that table
            if table_name is None or table_name.upper() in self._local[k].plan_json.upper():
                del self._local[k]
                removed += 1

        # L2 Redis scan
        if self._redis is not None:
            try:
                for key in self._redis.scan_iter(f"{prefix}*"):  # type: ignore[union-attr]
                    if inspect.isawaitable(key):
                        break
                    if table_name is None:
                        ret = self._redis.delete(key)  # type: ignore[union-attr]
                        if inspect.isawaitable(ret):
                            break
                        removed += 1
            except Exception as exc:
                logger.warning("Redis invalidation error: %s", exc)

        logger.info("Invalidated %d plan cache entries for tenant %s", removed, tenant_id)
        return removed

    def stats(self) -> dict[str, object]:
        """Return cache performance statistics."""
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0.0,
            "local_entries": len(self._local),
        }

    def _evict_lru(self) -> None:
        """Evict least recently used entries to make room."""
        evict_count = max(1, self._max_local // 10)
        sorted_entries = sorted(
            self._local.items(), key=lambda x: x[1].last_used_at
        )
        for key, _ in sorted_entries[:evict_count]:
            del self._local[key]
        logger.debug("Evicted %d LRU plan cache entries", evict_count)
