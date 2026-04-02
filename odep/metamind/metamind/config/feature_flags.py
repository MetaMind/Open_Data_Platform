"""Feature flag system for MetaMind F01–F30 roadmap features."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, fields
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


@dataclass
class FeatureFlags:
    """Toggle for all roadmap features F01–F30. All default False for backward compat."""

    # Priority 1: Plan Quality
    F01_learned_cardinality: bool = False
    F02_correlation_stats: bool = False
    F03_skew_detection: bool = False
    F04_bushy_join_dp: bool = False
    F05_predicate_inference: bool = False
    F06_cost_calibration: bool = False
    F07_subquery_decorrelation: bool = False
    F08_stats_aware_pushdown: bool = False

    # Priority 2: Optimization Speed
    F09_plan_caching: bool = False
    F10_incremental_memo: bool = False
    F11_compiled_execution: bool = False
    F12_optimization_tiering: bool = False

    # Priority 3: Cross-Engine Federation
    F13_universal_connectors: bool = False
    F14_cross_engine_planning: bool = False
    F15_federated_mvs: bool = False
    F16_data_placement_advisor: bool = False
    F17_dialect_aware_sql: bool = False
    F18_cross_engine_stats_sync: bool = False
    F19_vector_search: bool = False

    # Priority 4: Self-Tuning
    F20_regret_minimization: bool = False
    F21_auto_advisor: bool = False
    F22_auto_stats: bool = False
    F23_cloud_budget: bool = False
    F24_workload_classification: bool = False
    F25_query_queuing: bool = False
    F26_predictive_pre_warming: bool = False

    # Bonus Tier
    F27_multi_objective_cost: bool = False
    F28_nl_interface: bool = False
    F29_rewrite_suggestions: bool = False
    F30_optimization_replay: bool = False

    # Phase 2 — per-tenant toggle for each new sub-system (fixes W-07)
    F31_rls_enforcement: bool = False       # Row-level security rewriting
    F32_sla_enforcement: bool = False       # SLA/SLO latency-budget enforcement
    F33_noisy_neighbor: bool = True         # Noisy-neighbor detection & throttle
    F34_anomaly_detection: bool = True      # Z-score latency anomaly alerts
    F35_ai_tuner: bool = False              # AI-powered slow-query rewriter
    F36_query_firewall: bool = True         # Fingerprint denylist/allowlist

    def to_dict(self) -> dict[str, bool]:
        """Serialize all flags to dictionary."""
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, data: dict[str, bool]) -> FeatureFlags:
        """Deserialize from dictionary, ignoring unknown keys."""
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)

    @classmethod
    def all_enabled(cls) -> FeatureFlags:
        """Create flags with all features enabled (for testing/dev)."""
        kwargs = {f.name: True for f in fields(cls)}
        return cls(**kwargs)

    @classmethod
    def phase1(cls) -> FeatureFlags:
        """Create flags for Phase 1 (Foundation) features."""
        return cls(
            F03_skew_detection=True,
            F05_predicate_inference=True,
            F06_cost_calibration=True,
            F08_stats_aware_pushdown=True,
            F09_plan_caching=True,
            F12_optimization_tiering=True,
            F22_auto_stats=True,
        )


class FeatureFlagManager:
    """Manages per-tenant feature flags stored in PostgreSQL."""

    _FLAGS_TABLE = "mm_feature_flags"

    def __init__(self, engine: Engine, tenant_id: str) -> None:
        """Initialize with database engine and tenant ID."""
        self._engine = engine
        self._tenant_id = tenant_id
        self._cache: Optional[FeatureFlags] = None

    def get_flags(self) -> FeatureFlags:
        """Load feature flags for tenant from database (cached in-process)."""
        if self._cache is not None:
            return self._cache

        stmt = sa.text(
            f"SELECT flags FROM {self._FLAGS_TABLE} WHERE tenant_id = :tid"
        )
        try:
            with self._engine.connect() as conn:
                row = conn.execute(stmt, {"tid": self._tenant_id}).fetchone()
        except SQLAlchemyError as exc:
            # Fail-open for bootstrap environments where feature-flag migrations
            # have not been applied yet.
            logger.warning(
                "FeatureFlagManager fallback to defaults; table unavailable for tenant=%s: %s",
                self._tenant_id,
                exc,
            )
            flags = FeatureFlags()
            self._cache = flags
            return flags

        if row is None:
            # Tenant not found: return defaults and persist
            flags = FeatureFlags()
            self._persist(flags)
        else:
            flags = FeatureFlags.from_dict(row[0] or {})

        self._cache = flags
        return flags

    def enable(self, feature: str) -> None:
        """Enable a single feature flag for this tenant."""
        flags = self.get_flags()
        if not hasattr(flags, feature):
            raise ValueError(f"Unknown feature flag: {feature}")
        object.__setattr__(flags, feature, True)
        self._persist(flags)
        self._cache = flags
        logger.info("Enabled feature %s for tenant %s", feature, self._tenant_id)

    def disable(self, feature: str) -> None:
        """Disable a single feature flag for this tenant."""
        flags = self.get_flags()
        if not hasattr(flags, feature):
            raise ValueError(f"Unknown feature flag: {feature}")
        object.__setattr__(flags, feature, False)
        self._persist(flags)
        self._cache = flags
        logger.info("Disabled feature %s for tenant %s", feature, self._tenant_id)

    def is_enabled(self, feature: str) -> bool:
        """Check if a feature is enabled for this tenant."""
        flags = self.get_flags()
        if not hasattr(flags, feature):
            raise ValueError(f"Unknown feature flag: {feature}")
        return getattr(flags, feature)

    def invalidate_cache(self) -> None:
        """Clear in-memory flag cache, forcing reload on next access."""
        self._cache = None

    def _persist(self, flags: FeatureFlags) -> None:
        """Upsert feature flags row for this tenant."""
        import json

        stmt = sa.text(
            f"""INSERT INTO {self._FLAGS_TABLE} (tenant_id, flags, updated_at)
            VALUES (:tid, CAST(:flags AS jsonb), NOW())
            ON CONFLICT (tenant_id) DO UPDATE
            SET flags = EXCLUDED.flags, updated_at = NOW()"""
        )
        try:
            with self._engine.begin() as conn:
                conn.execute(stmt, {"tid": self._tenant_id, "flags": json.dumps(flags.to_dict())})
        except SQLAlchemyError as exc:
            logger.warning(
                "FeatureFlagManager persist skipped for tenant=%s: %s",
                self._tenant_id,
                exc,
            )
