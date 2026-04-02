"""Cross-Tenant Noisy Neighbor Detector.

Tracks per-tenant resource consumption in a Redis sliding-window sorted set.
Exposes throttle_factor() used by the router to apply backpressure.

Window implementation uses time-bucketed keys (e.g. mm:noisy:60:{bucket})
so that each window period is bounded and expires automatically — fixes W-11.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


class NoisyNeighborDetector:
    """Detect and throttle tenants that monopolize shared cluster resources.

    Uses a Redis sorted set (score = cumulative cost) to track consumption
    within a rolling time window.  The window key is time-bucketed so old
    windows expire naturally and a continuously active tenant's score resets
    each period (fixes W-11).

    All operations are best-effort; failures are logged and never surface to
    callers.

    Args:
        redis_client: Synchronous redis.Redis instance.
        window_seconds: Rolling window length in seconds (default 60).
    """

    def __init__(
        self,
        redis_client: object,
        window_seconds: int = 60,
    ) -> None:
        self._redis = redis_client
        self._window = window_seconds

    def _window_key(self) -> str:
        """Return a time-bucketed key for the current window.

        The bucket number advances every window_seconds so each window gets its
        own key.  Keys are set to expire after 2× the window, ensuring automatic
        cleanup without manual TTL refreshing (fixes W-11).
        """
        bucket = int(time.time() // self._window)
        return f"mm:noisy:{self._window}:{bucket}"

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_query(
        self,
        tenant_id: str,
        cpu_ms: float,
        mem_mb: float,
    ) -> None:
        """Record resource cost for a completed query.

        Cost score = cpu_ms + mem_mb * 10 (heuristic weighting).
        Uses ZINCRBY on a bounded sorted set; key expires after window_seconds.
        """
        cost = cpu_ms + mem_mb * 10.0
        try:
            self._redis.zincrby(self._window_key(), cost, tenant_id)  # type: ignore[union-attr]
            self._redis.expire(self._window_key(), self._window * 2)  # type: ignore[union-attr]
            logger.debug(
                "NoisyNeighbor recorded tenant=%s cpu_ms=%.1f mem_mb=%.1f cost=%.1f",
                tenant_id,
                cpu_ms,
                mem_mb,
                cost,
            )
        except Exception as exc:
            logger.error(
                "NoisyNeighborDetector.record_query failed tenant=%s: %s",
                tenant_id,
                exc,
            )

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def get_top_consumers(self, n: int = 5) -> list[dict]:
        """Return top-N tenants ordered by descending cost score.

        Returns:
            list of {"tenant_id": str, "cost_score": float}
        """
        try:
            raw = self._redis.zrevrange(  # type: ignore[union-attr]
                self._window_key(), 0, n - 1, withscores=True
            )
            return [
                {"tenant_id": tid.decode() if isinstance(tid, bytes) else tid,
                 "cost_score": float(score)}
                for tid, score in raw
            ]
        except Exception as exc:
            logger.error("NoisyNeighborDetector.get_top_consumers failed: %s", exc)
            return []

    def is_noisy(
        self,
        tenant_id: str,
        threshold_pct: float = 0.4,
    ) -> bool:
        """Return True if tenant exceeds threshold_pct of total window cost."""
        try:
            tenant_score = self._redis.zscore(self._window_key(), tenant_id)  # type: ignore[union-attr]
            if tenant_score is None:
                return False
            # Sum all scores for total cost
            all_scores = self._redis.zrange(  # type: ignore[union-attr]
                self._window_key(), 0, -1, withscores=True
            )
            total = sum(float(s) for _, s in all_scores)
            if total <= 0:
                return False
            share = float(tenant_score) / total
            return share > threshold_pct
        except Exception as exc:
            logger.error(
                "NoisyNeighborDetector.is_noisy failed tenant=%s: %s", tenant_id, exc
            )
            return False

    def throttle_factor(self, tenant_id: str) -> float:
        """Return a throttle factor in [0.0, 1.0] for the given tenant.

        1.0 means no throttle; lower values indicate increasing backpressure.
        Router should multiply sleep durations or queue depths by (1 - factor).

        Returns:
            1.0 if not noisy, else a value proportional to excess usage.
        """
        try:
            tenant_score = self._redis.zscore(self._window_key(), tenant_id)  # type: ignore[union-attr]
            if tenant_score is None:
                return 1.0

            all_scores = self._redis.zrange(  # type: ignore[union-attr]
                self._window_key(), 0, -1, withscores=True
            )
            total = sum(float(s) for _, s in all_scores)
            if total <= 0:
                return 1.0

            share = float(tenant_score) / total
            threshold = 0.4
            if share <= threshold:
                return 1.0

            # Linearly scale from 0.9 (just over threshold) to 0.1 (100% share)
            excess = (share - threshold) / (1.0 - threshold)  # 0..1
            factor = max(0.0, 0.9 - 0.8 * excess)
            logger.info(
                "NoisyNeighbor throttle tenant=%s share=%.2f factor=%.2f",
                tenant_id,
                share,
                factor,
            )
            return round(factor, 3)
        except Exception as exc:
            logger.error(
                "NoisyNeighborDetector.throttle_factor failed tenant=%s: %s",
                tenant_id,
                exc,
            )
            return 1.0
