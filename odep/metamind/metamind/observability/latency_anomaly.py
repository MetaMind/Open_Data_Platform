"""Query Latency Anomaly Detector.

Detects p99 latency spikes per tenant+engine using Z-score over a rolling window.
Fires webhook alerts and writes to mm_anomaly_events.
Throttled to one alert per tenant/engine per 10 minutes.
"""
from __future__ import annotations

import asyncio
import logging
import math
import statistics
import time
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_TICK_INTERVAL_SECONDS = 60
_THROTTLE_SECONDS = 600   # 10 minutes between alerts per tenant+engine
_THROTTLE_KEY_PREFIX = "mm:anomaly:throttle"


@dataclass
class AnomalyAlert:
    """Represents a detected latency anomaly."""

    tenant_id: str
    engine: str
    p99_ms: float
    baseline_ms: float
    z_score: float
    detected_at: float = 0.0

    def __post_init__(self) -> None:
        if not self.detected_at:
            self.detected_at = time.time()


class LatencyAnomalyDetector:
    """Rolling Z-score anomaly detector for query latency.

    Args:
        db_engine: SQLAlchemy Engine for querying mm_query_logs.
        redis_client: Redis client for throttle state.
        webhook_dispatcher: Optional dispatcher to forward alerts.
        window_minutes: Rolling baseline window length.
        z_threshold: Z-score threshold above which an alert fires.
    """

    def __init__(
        self,
        db_engine: Engine,
        redis_client: Optional[object],
        webhook_dispatcher: Optional[Any],
        window_minutes: int = 10,
        z_threshold: float = 3.0,
    ) -> None:
        self._engine = db_engine
        self._redis = redis_client
        self._webhook = webhook_dispatcher
        self._window = window_minutes
        self._z_threshold = z_threshold
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "LatencyAnomalyDetector started (window=%dm z=%.1f)",
            self._window,
            self._z_threshold,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("LatencyAnomalyDetector stopped")

    # ------------------------------------------------------------------
    # Detection loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._tick()
            except Exception as exc:
                logger.error("LatencyAnomalyDetector._tick error: %s", exc, exc_info=True)
            await asyncio.sleep(_TICK_INTERVAL_SECONDS)

    async def _tick(self) -> None:
        """Check each active tenant+engine combination for anomalies."""
        combos = self._fetch_active_combos()
        for tenant_id, engine in combos:
            try:
                durations = self._fetch_window_durations(tenant_id, engine)
                if len(durations) < 5:
                    continue  # Not enough data for a reliable baseline

                p99 = self._percentile(durations, 0.99)
                baseline = statistics.mean(durations)
                stddev = statistics.stdev(durations) if len(durations) > 1 else 0.0

                if stddev < 1.0:
                    continue  # Essentially constant latency

                z_score = (p99 - baseline) / stddev
                if z_score > self._z_threshold:
                    await self._fire_alert(tenant_id, engine, p99, baseline, z_score)
            except Exception as exc:
                logger.error(
                    "LatencyAnomalyDetector combo=%s/%s error: %s",
                    tenant_id, engine, exc,
                )

    async def _fire_alert(
        self,
        tenant_id: str,
        engine: str,
        p99_ms: float,
        baseline_ms: float,
        z_score: float,
    ) -> None:
        """Fire an anomaly alert if not throttled."""
        throttle_key = f"{_THROTTLE_KEY_PREFIX}:{tenant_id}:{engine}"

        # Check throttle
        if self._redis is not None:
            try:
                if self._redis.exists(throttle_key):  # type: ignore[union-attr]
                    logger.debug(
                        "AnomalyDetector throttled tenant=%s engine=%s", tenant_id, engine
                    )
                    return
                self._redis.setex(throttle_key, _THROTTLE_SECONDS, "1")  # type: ignore[union-attr]
            except Exception as exc:
                logger.error("AnomalyDetector throttle check failed: %s", exc)

        alert = AnomalyAlert(
            tenant_id=tenant_id,
            engine=engine,
            p99_ms=p99_ms,
            baseline_ms=baseline_ms,
            z_score=z_score,
        )

        logger.warning(
            "LATENCY ANOMALY tenant=%s engine=%s p99=%.0fms baseline=%.0fms z=%.2f",
            tenant_id, engine, p99_ms, baseline_ms, z_score,
        )

        self._persist_alert(alert)

        if self._webhook is not None:
            try:
                from metamind.cdc.outbound_webhook import CDCEvent
                event = CDCEvent(
                    table_name="mm_anomaly_events",
                    operation="INSERT",
                    tenant_id=tenant_id,
                    after={
                        "tenant_id": tenant_id,
                        "engine": engine,
                        "p99_ms": p99_ms,
                        "baseline_ms": baseline_ms,
                        "z_score": z_score,
                    },
                )
                await self._webhook.dispatch(event)
            except Exception as exc:
                logger.error("AnomalyDetector webhook dispatch failed: %s", exc)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_active_combos(self) -> list[tuple[str, str]]:
        """Return (tenant_id, engine) pairs active in the last hour."""
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT DISTINCT tenant_id, target_source AS engine "
                        "FROM mm_query_logs "
                        "WHERE submitted_at >= NOW() - INTERVAL '1 hour' "
                        "  AND target_source IS NOT NULL"
                    )
                ).fetchall()
            return [(r.tenant_id, r.engine) for r in rows]
        except Exception as exc:
            logger.error("_fetch_active_combos failed: %s", exc)
            return []

    def _fetch_window_durations(
        self, tenant_id: str, engine: str
    ) -> list[float]:
        """Fetch duration_ms values for the rolling window."""
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT total_time_ms AS duration_ms FROM mm_query_logs "
                        "WHERE tenant_id = :tid AND target_source = :eng "
                        "  AND submitted_at >= NOW() - :win * INTERVAL '1 minute' "
                        "  AND total_time_ms IS NOT NULL "
                        "ORDER BY submitted_at DESC LIMIT 1000"
                    ),
                    {"tid": tenant_id, "eng": engine, "win": self._window},
                ).fetchall()
            return [float(r.duration_ms) for r in rows]
        except Exception as exc:
            logger.error("_fetch_window_durations failed: %s", exc)
            return []

    def _persist_alert(self, alert: AnomalyAlert) -> None:
        """Write the anomaly event to mm_anomaly_events."""
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO mm_anomaly_events "
                        "(tenant_id, engine, p99_ms, baseline_ms, z_score, detected_at) "
                        "VALUES (:tid, :eng, :p99, :base, :z, NOW()) "
                        "ON CONFLICT DO NOTHING"
                    ),
                    {
                        "tid": alert.tenant_id,
                        "eng": alert.engine,
                        "p99": alert.p99_ms,
                        "base": alert.baseline_ms,
                        "z": alert.z_score,
                    },
                )
        except Exception as exc:
            logger.error("AnomalyDetector._persist_alert failed: %s", exc)

    @staticmethod
    def _percentile(data: list[float], pct: float) -> float:
        if not data:
            return 0.0
        s = sorted(data)
        idx = int(math.ceil(pct * len(s))) - 1
        return s[max(0, idx)]
