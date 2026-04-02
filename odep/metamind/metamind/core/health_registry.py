"""
Engine Health Registry — Control Plane Component

File: metamind/core/health_registry.py
Role: Platform Engineer
Split from: metamind/core/control_plane.py (Golden Rule: ≤500 lines)

Tracks health of all registered execution engines with periodic
background monitoring and Redis-backed result caching.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class EngineStatus(Enum):
    """Engine health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    OFFLINE = "offline"


@dataclass
class EngineHealth:
    """Snapshot of engine health at a point in time."""
    engine_name: str
    status: EngineStatus
    last_check: datetime
    response_time_ms: int
    active_connections: int
    max_connections: int
    error_rate: float
    cpu_percent: float
    memory_percent: float
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "status": self.status.value,
        }

    @classmethod
    def offline(cls, engine_name: str, reason: str) -> EngineHealth:
        """Create an OFFLINE health record for a failed check."""
        return cls(
            engine_name=engine_name,
            status=EngineStatus.OFFLINE,
            last_check=datetime.now(),
            response_time_ms=0,
            active_connections=0,
            max_connections=0,
            error_rate=1.0,
            cpu_percent=0.0,
            memory_percent=0.0,
            details={"error": reason},
        )


class EngineHealthRegistry:
    """
    Registry tracking live health of all execution engines.

    Performs periodic health checks (default every 30 s) and caches
    results in Redis so multiple API instances share the same view.

    Usage::

        registry = EngineHealthRegistry(redis_client)
        registry.register_engine("trino", trino_engine)
        registry.register_engine("oracle", oracle_connector)
        await registry.start_monitoring()
    """

    REDIS_TTL_MULTIPLIER = 2  # Cache TTL = check_interval * this

    def __init__(
        self,
        redis_client: Any,
        check_interval_seconds: int = 30,
    ) -> None:
        self.redis = redis_client
        self.check_interval = check_interval_seconds
        self._engines: Dict[str, Any] = {}
        self._health_cache: Dict[str, EngineHealth] = {}
        self._check_task: Optional[asyncio.Task] = None
        logger.debug("EngineHealthRegistry initialised")

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_engine(self, name: str, connector: Any) -> None:
        """Register an engine connector for health monitoring."""
        self._engines[name] = connector
        logger.info("Registered engine for health monitoring: %s", name)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start_monitoring(self) -> None:
        """Start the background health-check loop (idempotent)."""
        if self._check_task is not None:
            return
        self._check_task = asyncio.create_task(self._health_check_loop())
        logger.info("Engine health monitoring started (interval=%ds)", self.check_interval)

    async def stop_monitoring(self) -> None:
        """Cancel the background health-check loop."""
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError as exc:
                logger.error("HealthRegistry: health check task cancelled for engine=%s: %s",
                    "health_check_loop", exc)
            self._check_task = None
            logger.info("Engine health monitoring stopped")

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def get_health(self, engine_name: str) -> Optional[EngineHealth]:
        """Return the latest health for *engine_name*, or None."""
        raw = await self.redis.get(f"health:{engine_name}")
        if raw:
            data = json.loads(raw)
            data["last_check"] = datetime.fromisoformat(data["last_check"])
            data["status"] = EngineStatus(data["status"])
            return EngineHealth(**data)
        return self._health_cache.get(engine_name)

    async def get_all_health(self) -> Dict[str, EngineHealth]:
        """Return health for all registered engines."""
        return dict(self._health_cache)

    def is_healthy(self, engine_name: str) -> bool:
        """Return True if the engine is HEALTHY or DEGRADED."""
        h = self._health_cache.get(engine_name)
        if not h:
            return False
        return h.status in (EngineStatus.HEALTHY, EngineStatus.DEGRADED)

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _health_check_loop(self) -> None:
        while True:
            try:
                await self._check_all_engines()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Unexpected error in health-check loop: %s", exc)
                await asyncio.sleep(self.check_interval)

    async def _check_all_engines(self) -> None:
        tasks = {
            name: asyncio.create_task(self._check_engine(name, connector))
            for name, connector in self._engines.items()
        }
        for name, task in tasks.items():
            try:
                health = await task
            except Exception as exc:
                logger.warning("Health check failed for %s: %s", name, exc)
                health = EngineHealth.offline(name, str(exc))

            self._health_cache[name] = health
            ttl = self.check_interval * self.REDIS_TTL_MULTIPLIER
            await self.redis.setex(f"health:{name}", ttl, json.dumps(health.to_dict()))

    async def _check_engine(self, name: str, connector: Any) -> EngineHealth:
        """Probe a single engine and return an EngineHealth snapshot."""
        t0 = time.time()
        if hasattr(connector, "health_check"):
            data: Dict[str, Any] = await connector.health_check()
        else:
            data = {"status": "unknown"}
        response_ms = int((time.time() - t0) * 1000)

        _status_map = {
            "healthy": EngineStatus.HEALTHY,
            "degraded": EngineStatus.DEGRADED,
            "offline": EngineStatus.OFFLINE,
        }
        status = _status_map.get(data.get("status", ""), EngineStatus.UNHEALTHY)

        return EngineHealth(
            engine_name=name,
            status=status,
            last_check=datetime.now(),
            response_time_ms=response_ms,
            active_connections=data.get("active_connections", 0),
            max_connections=data.get("max_connections", 100),
            error_rate=data.get("error_rate", 0.0),
            cpu_percent=data.get("cpu_percent", 0.0),
            memory_percent=data.get("memory_percent", 0.0),
            details=data,
        )
