"""Phase 2 background task startup helpers for MetaMind bootstrap.

Separated from bootstrap.py to keep that file under the 500-line limit.
Called from AppContext.initialize() after core services are ready.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def start_background_tasks(ctx: Any) -> None:
    """Start all Phase 2 background tasks attached to the AppContext.

    Args:
        ctx: Initialized AppContext instance with _sync_db_engine and _redis_client.
    """
    db = ctx.sync_db_engine    # public property — fixes W-19 (private attr access)
    redis = ctx._redis_client  # redis has no public property yet; acceptable

    # Task 14: MV Auto-Refresh Scheduler
    try:
        from metamind.core.mv.auto_refresh import MVAutoRefreshScheduler
        mv_scheduler = MVAutoRefreshScheduler(
            db_engine=db,
            query_engine=db,        # W-09: pass db engine so refresh SQL executes
            poll_interval_seconds=120,
        )
        await mv_scheduler.start()
        ctx._mv_scheduler = mv_scheduler  # type: ignore[attr-defined]
        logger.info("MVAutoRefreshScheduler started")
    except Exception as exc:
        logger.error("Failed to start MVAutoRefreshScheduler: %s", exc)

    # Task 20: Latency Anomaly Detector
    try:
        from metamind.observability.latency_anomaly import LatencyAnomalyDetector
        anomaly_detector = LatencyAnomalyDetector(
            db_engine=db,
            redis_client=redis,
            webhook_dispatcher=None,
        )
        await anomaly_detector.start()
        ctx._anomaly_detector = anomaly_detector  # type: ignore[attr-defined]
        logger.info("LatencyAnomalyDetector started")
    except Exception as exc:
        logger.error("Failed to start LatencyAnomalyDetector: %s", exc)
