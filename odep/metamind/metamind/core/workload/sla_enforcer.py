"""SLA/SLO Enforcement Engine.

Loads per-tenant latency targets from DB (cached in Redis) and overrides
engine selection when a query is at risk of breaching its SLA.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_SLA_CACHE_TTL = 60   # seconds


class RiskLevel(str, Enum):
    SAFE = "safe"
    AT_RISK = "at_risk"
    BREACH = "breach"


@dataclass
class SLAConfig:
    """Per-tenant SLA targets in milliseconds."""

    tenant_id: str
    p50_target_ms: float
    p95_target_ms: float
    p99_target_ms: float
    breach_action: str = "reroute"  # reroute | queue | alert


@dataclass
class SLADecision:
    """Result of an SLA enforcement decision."""

    original_engine: str
    final_engine: str
    risk_level: RiskLevel
    reason: str


class SLAEnforcer:
    """Enforce per-tenant query latency budgets.

    Args:
        db_engine: SQLAlchemy Engine for loading SLA configs.
        redis_client: Redis for caching SLA configs.
    """

    def __init__(self, db_engine: Engine, redis_client: object) -> None:
        self._engine = db_engine
        self._redis = redis_client

    # ------------------------------------------------------------------
    # SLA loading
    # ------------------------------------------------------------------

    async def load_sla(self, tenant_id: str) -> SLAConfig:
        """Load SLA config for tenant, using Redis cache first.

        Falls back to DB on cache miss; falls back to generous defaults if absent.
        """
        cache_key = f"mm:sla:{tenant_id}"
        try:
            raw = self._redis.get(cache_key)  # type: ignore[union-attr]
            if raw:
                data = json.loads(raw)
                return SLAConfig(**data)
        except Exception as exc:
            logger.error("SLAEnforcer.load_sla Redis error: %s", exc)

        return self._load_from_db(tenant_id, cache_key)

    def _load_from_db(self, tenant_id: str, cache_key: str) -> SLAConfig:
        """Load from mm_sla_configs and populate Redis cache."""
        row = None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT p50_target_ms, p95_target_ms, p99_target_ms, "
                        "breach_action FROM mm_sla_configs "
                        "WHERE tenant_id = :tid AND is_active = TRUE "
                        "ORDER BY updated_at DESC LIMIT 1"
                    ),
                    {"tid": tenant_id},
                ).fetchone()
        except Exception as exc:
            logger.error("SLAEnforcer DB load failed: %s", exc)

        if row:
            cfg = SLAConfig(
                tenant_id=tenant_id,
                p50_target_ms=float(row.p50_target_ms),
                p95_target_ms=float(row.p95_target_ms),
                p99_target_ms=float(row.p99_target_ms),
                breach_action=row.breach_action or "reroute",
            )
        else:
            # Generous defaults when no SLA is configured
            cfg = SLAConfig(
                tenant_id=tenant_id,
                p50_target_ms=2000.0,
                p95_target_ms=10000.0,
                p99_target_ms=30000.0,
            )

        try:
            self._redis.setex(  # type: ignore[union-attr]
                cache_key, _SLA_CACHE_TTL, json.dumps(cfg.__dict__)
            )
        except Exception as exc:
            logger.error("SLAEnforcer cache write failed: %s", exc)

        return cfg

    # ------------------------------------------------------------------
    # Risk estimation
    # ------------------------------------------------------------------

    def estimate_risk(
        self,
        query_complexity: int,
        engine: str,
        historical_p95_ms: float,
        sla: SLAConfig,
    ) -> RiskLevel:
        """Estimate latency risk for a query on the given engine.

        Args:
            query_complexity: 1–10 score from the query classifier.
            engine: Engine being considered.
            historical_p95_ms: p95 latency for this engine from recent history.
            sla: SLA config for the tenant.

        Returns:
            RiskLevel.SAFE | AT_RISK | BREACH
        """
        # Scale historical p95 by complexity factor (1x–2x)
        complexity_factor = 1.0 + max(0, (query_complexity - 5)) / 10.0
        estimated_p95 = historical_p95_ms * complexity_factor

        if estimated_p95 > sla.p95_target_ms:
            return RiskLevel.BREACH
        if estimated_p95 > sla.p95_target_ms * 0.8:
            return RiskLevel.AT_RISK
        return RiskLevel.SAFE

    # ------------------------------------------------------------------
    # Real-time P95 lookup (fixes W-13)
    # ------------------------------------------------------------------

    def _fetch_engine_p95(self, engine: str, window_minutes: int = 10) -> float:
        """Query the recent p95 latency for an engine from mm_query_logs.

        Args:
            engine: Backend engine name (e.g. 'trino', 'spark').
            window_minutes: How far back to look.

        Returns:
            p95 latency in ms, or 0.0 if no data is available.
        """
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT PERCENTILE_CONT(0.95) WITHIN GROUP "
                        "    (ORDER BY total_time_ms) AS p95 "
                        "FROM mm_query_logs "
                        "WHERE target_source = :engine "
                        "  AND submitted_at > NOW() - (:mins * INTERVAL '1 minute') "
                        "  AND total_time_ms IS NOT NULL"
                    ),
                    {"engine": engine, "mins": window_minutes},
                ).fetchone()
            if row and row.p95 is not None:
                return float(row.p95)
        except Exception as exc:
            logger.warning("SLAEnforcer._fetch_engine_p95 failed engine=%s: %s", engine, exc)
        return 0.0

    # ------------------------------------------------------------------
    # Enforcement
    # ------------------------------------------------------------------

    async def enforce(
        self,
        tenant_id: str,
        query_id: str,
        chosen_engine: str,
        available_engines: Optional[list[str]] = None,
        query_complexity: int = 5,
        historical_p95_ms: float = -1.0,  # -1 = auto-fetch from DB
    ) -> str:
        """Apply SLA enforcement and return the final engine to use.

        If historical_p95_ms is not supplied (or is -1), the method queries
        mm_query_logs for the real recent P95 so risk estimation is accurate —
        fixes W-13 where the default of 0.0 always produced SAFE risk.

        Args:
            tenant_id: Tenant identifier.
            query_id: Query identifier (for logging).
            chosen_engine: Engine selected by the router.
            available_engines: All healthy engines the router can use.
            query_complexity: Complexity score 1–10 from classifier.
            historical_p95_ms: Override p95 (ms). Pass -1 to auto-fetch.

        Returns:
            Final engine name to dispatch to.
        """
        sla = await self.load_sla(tenant_id)

        # Auto-fetch real P95 when not provided (fixes W-13)
        if historical_p95_ms < 0:
            historical_p95_ms = self._fetch_engine_p95(chosen_engine)

        risk = self.estimate_risk(
            query_complexity, chosen_engine, historical_p95_ms, sla
        )

        if risk != RiskLevel.BREACH:
            self._log_decision(
                query_id, tenant_id, chosen_engine, chosen_engine, risk,
                "No SLA risk; using chosen engine."
            )
            return chosen_engine

        # Try to find a faster alternative
        final_engine = chosen_engine
        reason = "SLA BREACH risk; no faster engine available — queued."

        if available_engines:
            # Prefer any engine with lower historical p95 (simple heuristic:
            # try engines in order; first one that differs from chosen is used)
            alternatives = [e for e in available_engines if e != chosen_engine]
            if alternatives:
                final_engine = alternatives[0]
                reason = (
                    f"SLA BREACH risk on {chosen_engine}; "
                    f"rerouted to {final_engine}."
                )

        self._log_decision(
            query_id, tenant_id, chosen_engine, final_engine, risk, reason
        )
        return final_engine

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _log_decision(
        self,
        query_id: str,
        tenant_id: str,
        original: str,
        final: str,
        risk: RiskLevel,
        reason: str,
    ) -> None:
        """Persist SLA decision to mm_sla_decisions (best-effort)."""
        log_level = logging.WARNING if risk == RiskLevel.BREACH else logging.DEBUG
        logger.log(
            log_level,
            "SLA decision query=%s tenant=%s original=%s final=%s risk=%s: %s",
            query_id, tenant_id, original, final, risk.value, reason,
        )
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO mm_sla_decisions "
                        "(query_id, tenant_id, original_engine, final_engine, "
                        " risk_level, reason, decided_at) "
                        "VALUES (:qid, :tid, :orig, :fin, :risk, :rsn, NOW())"
                    ),
                    {
                        "qid": query_id,
                        "tid": tenant_id,
                        "orig": original,
                        "fin": final,
                        "risk": risk.value,
                        "rsn": reason,
                    },
                )
        except Exception as exc:
            logger.error("SLAEnforcer._log_decision DB write failed: %s", exc)
