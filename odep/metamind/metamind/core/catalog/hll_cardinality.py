"""HyperLogLog Cardinality Estimator.

Uses Redis HyperLogLog (PFADD / PFCOUNT) for real-time column cardinality
estimates, falling back to mm_table_stats when no HLL sketch exists.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_HLL_TTL_SECONDS = 3600  # 1 hour


def _hll_key(tenant_id: str, table_name: str, column_name: str) -> str:
    return f"mm:hll:{tenant_id}:{table_name}:{column_name}"


@dataclass
class CardinalityEstimate:
    """Result from the cardinality estimator."""

    estimate: int
    source: str       # 'hll' | 'stats' | 'fallback'
    age_seconds: int  # -1 when unknown


class HLLCardinalityEstimator:
    """Estimate column cardinality using Redis HyperLogLog sketches.

    Args:
        redis_client: Synchronous Redis client supporting HLL commands.
        db_engine: SQLAlchemy Engine used for table scans and stats lookups.
    """

    def __init__(self, redis_client: object, db_engine: Engine) -> None:
        self._redis = redis_client
        self._engine = db_engine

    # ------------------------------------------------------------------
    # Estimation
    # ------------------------------------------------------------------

    async def estimate(
        self,
        tenant_id: str,
        table_name: str,
        column_name: str,
    ) -> CardinalityEstimate:
        """Return cardinality estimate, preferring HLL over stale stats.

        Returns:
            CardinalityEstimate with estimate, source, and age_seconds.
        """
        key = _hll_key(tenant_id, table_name, column_name)
        try:
            count = self._redis.pfcount(key)  # type: ignore[union-attr]
            if count and int(count) > 0:
                ttl = self._redis.ttl(key)  # type: ignore[union-attr]
                age = _HLL_TTL_SECONDS - int(ttl or 0)
                logger.debug(
                    "HLL estimate %s.%s = %d (age=%ds)",
                    table_name, column_name, count, age,
                )
                return CardinalityEstimate(
                    estimate=int(count),
                    source="hll",
                    age_seconds=max(0, age),
                )
        except Exception as exc:
            logger.error(
                "HLLCardinalityEstimator.estimate Redis error: %s", exc
            )

        # Fallback: mm_table_stats
        return self._stats_fallback(tenant_id, table_name, column_name)

    # ------------------------------------------------------------------
    # Updating
    # ------------------------------------------------------------------

    async def update(
        self,
        tenant_id: str,
        table_name: str,
        column_name: str,
        sample: list,
    ) -> None:
        """Add sample values to the HLL sketch and reset TTL."""
        key = _hll_key(tenant_id, table_name, column_name)
        if not sample:
            return
        try:
            # Redis PFADD accepts multiple elements
            chunk_size = 500
            for i in range(0, len(sample), chunk_size):
                chunk = [str(v) for v in sample[i:i + chunk_size]]
                self._redis.pfadd(key, *chunk)  # type: ignore[union-attr]
            self._redis.expire(key, _HLL_TTL_SECONDS)  # type: ignore[union-attr]
            logger.debug(
                "HLL updated %s.%s with %d values",
                table_name, column_name, len(sample),
            )
        except Exception as exc:
            logger.error(
                "HLLCardinalityEstimator.update failed %s.%s: %s",
                table_name, column_name, exc,
            )

    async def build_from_table(
        self,
        tenant_id: str,
        table_name: str,
        column_name: str,
    ) -> None:
        """Build HLL sketch by scanning distinct column values (up to 100k).

        Runs in an executor to avoid blocking the event loop during the DB scan.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            self._build_sync,
            tenant_id,
            table_name,
            column_name,
        )

    def _build_sync(
        self, tenant_id: str, table_name: str, column_name: str
    ) -> None:
        """Synchronous table scan worker."""
        try:
            import time
            t0 = time.monotonic()
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT DISTINCT {column_name} "  # noqa: S608
                        f"FROM {table_name} LIMIT 100000"
                    )
                ).fetchall()
            values = [str(r[0]) for r in rows if r[0] is not None]
            dur = (time.monotonic() - t0) * 1000

            # Batch PFADD using a pipeline — reduces 100k round-trips to ~200 (fixes W-16)
            key = _hll_key(tenant_id, table_name, column_name)
            chunk_size = 500
            try:
                pipe = self._redis.pipeline(transaction=False)  # type: ignore[union-attr]
                for i in range(0, len(values), chunk_size):
                    chunk = values[i:i + chunk_size]
                    pipe.pfadd(key, *chunk)
                pipe.expire(key, _HLL_TTL_SECONDS)
                pipe.execute()
            except AttributeError:
                # Redis client does not support pipeline() — fall back to sequential
                for i in range(0, len(values), chunk_size):
                    self._redis.pfadd(key, *values[i:i + chunk_size])  # type: ignore[union-attr]
                self._redis.expire(key, _HLL_TTL_SECONDS)  # type: ignore[union-attr]

            est = self._redis.pfcount(key) or 0  # type: ignore[union-attr]
            logger.info(
                "HLL build_from_table %s.%s: scanned=%d estimate=%d dur=%.0fms",
                table_name, column_name, len(values), est, dur,
            )
        except Exception as exc:
            logger.error(
                "HLLCardinalityEstimator._build_sync failed %s.%s: %s",
                table_name, column_name, exc,
            )

    # ------------------------------------------------------------------
    # Stats fallback
    # ------------------------------------------------------------------

    def _stats_fallback(
        self, tenant_id: str, table_name: str, column_name: str
    ) -> CardinalityEstimate:
        """Look up cardinality from mm_table_stats."""
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT estimated_rows FROM mm_table_stats "
                        "WHERE tenant_id = :tid AND table_name = :tbl "
                        "ORDER BY updated_at DESC LIMIT 1"
                    ),
                    {"tid": tenant_id, "tbl": table_name},
                ).fetchone()
            if row:
                return CardinalityEstimate(
                    estimate=int(row.estimated_rows or 0),
                    source="stats",
                    age_seconds=-1,
                )
        except Exception as exc:
            logger.warning(
                "HLLCardinalityEstimator stats fallback failed: %s", exc
            )
        return CardinalityEstimate(estimate=1000, source="fallback", age_seconds=-1)
