"""Unit tests for SLAEnforcer, NoisyNeighborDetector, OnlineCostLearner — W-04."""
from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock, patch

from metamind.core.workload.sla_enforcer import SLAEnforcer, SLAConfig, RiskLevel
from metamind.core.workload.noisy_neighbor import NoisyNeighborDetector
from metamind.ml.online_learner import OnlineCostLearner


# ────────────────────────────────────────────────────────────────────────
# SLAEnforcer
# ────────────────────────────────────────────────────────────────────────

def _make_sla_config(p95: float = 1000.0) -> SLAConfig:
    return SLAConfig(
        tenant_id="t1",
        p50_target_ms=200.0,
        p95_target_ms=p95,
        p99_target_ms=p95 * 3,
        breach_action="reroute",
    )


def _make_enforcer(cached_sla: SLAConfig | None = None) -> SLAEnforcer:
    redis = MagicMock()
    if cached_sla is not None:
        redis.get.return_value = json.dumps(cached_sla.__dict__).encode()
    else:
        redis.get.return_value = None
    db = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = None  # no DB row
    db.connect.return_value.__enter__ = MagicMock(return_value=conn)
    db.connect.return_value.__exit__ = MagicMock(return_value=False)
    return SLAEnforcer(db_engine=db, redis_client=redis)


class TestEstimateRisk:
    def test_safe_when_p95_well_below_target(self) -> None:
        sla = _make_sla_config(p95=1000.0)
        enforcer = _make_enforcer()
        risk = enforcer.estimate_risk(
            query_complexity=3, engine="trino",
            historical_p95_ms=100.0, sla=sla
        )
        assert risk == RiskLevel.SAFE

    def test_at_risk_when_approaching_target(self) -> None:
        sla = _make_sla_config(p95=1000.0)
        enforcer = _make_enforcer()
        # 850ms / 1000ms = 85% > 80% threshold
        risk = enforcer.estimate_risk(
            query_complexity=5, engine="trino",
            historical_p95_ms=850.0, sla=sla
        )
        assert risk == RiskLevel.AT_RISK

    def test_breach_when_exceeding_target(self) -> None:
        sla = _make_sla_config(p95=1000.0)
        enforcer = _make_enforcer()
        risk = enforcer.estimate_risk(
            query_complexity=5, engine="trino",
            historical_p95_ms=1200.0, sla=sla
        )
        assert risk == RiskLevel.BREACH


class TestEnforceWithRealP95:
    """enforce() queries real P95 when historical_p95_ms is -1 (fixes W-13)."""

    @pytest.mark.asyncio
    async def test_auto_fetch_used_when_default(self) -> None:
        enforcer = _make_enforcer(_make_sla_config(p95=5000.0))
        with patch.object(enforcer, "_fetch_engine_p95", return_value=100.0) as mock_fetch:
            engine = await enforcer.enforce(
                tenant_id="t1", query_id="q1",
                chosen_engine="trino",
                available_engines=["trino", "spark"],
                historical_p95_ms=-1.0,
            )
        mock_fetch.assert_called_once_with("trino")
        assert engine == "trino"  # p95=100 is safe vs target=5000

    @pytest.mark.asyncio
    async def test_reroute_when_breach(self) -> None:
        enforcer = _make_enforcer(_make_sla_config(p95=500.0))
        with patch.object(enforcer, "_fetch_engine_p95", return_value=900.0):
            engine = await enforcer.enforce(
                tenant_id="t1", query_id="q2",
                chosen_engine="trino",
                available_engines=["trino", "spark"],
                historical_p95_ms=-1.0,
            )
        assert engine == "spark"  # rerouted


# ────────────────────────────────────────────────────────────────────────
# NoisyNeighborDetector
# ────────────────────────────────────────────────────────────────────────

def _make_nn(scores: dict[str, float]) -> NoisyNeighborDetector:
    """Build a detector with a Redis mock whose sorted set has given scores."""
    redis = MagicMock()
    encoded = {(k.encode() if isinstance(k, str) else k): v for k, v in scores.items()}

    redis.zscore.side_effect = lambda key, tenant: scores.get(
        tenant.decode() if isinstance(tenant, bytes) else tenant
    )
    redis.zrange.return_value = list(encoded.items())
    redis.zincrby.return_value = None
    redis.expire.return_value = None
    return NoisyNeighborDetector(redis_client=redis, window_seconds=60)


class TestThrottleFactor:
    def test_no_throttle_for_small_tenant(self) -> None:
        nn = _make_nn({"small": 10.0, "big": 1000.0})
        factor = nn.throttle_factor("small")
        assert factor == 1.0

    def test_throttle_for_dominant_tenant(self) -> None:
        nn = _make_nn({"hog": 900.0, "other": 100.0})
        factor = nn.throttle_factor("hog")
        assert factor < 1.0
        assert factor >= 0.0

    def test_unknown_tenant_no_throttle(self) -> None:
        nn = _make_nn({"other": 500.0})
        factor = nn.throttle_factor("ghost")
        assert factor == 1.0


class TestTimeBucketedKey:
    """Window key is time-bucketed, not a static string (fixes W-11)."""

    def test_key_contains_bucket_number(self) -> None:
        import time
        redis = MagicMock()
        nn = NoisyNeighborDetector(redis_client=redis, window_seconds=60)
        key = nn._window_key()
        bucket = int(time.time() // 60)
        assert str(bucket) in key

    def test_different_windows_different_keys(self) -> None:
        redis = MagicMock()
        nn30 = NoisyNeighborDetector(redis_client=redis, window_seconds=30)
        nn60 = NoisyNeighborDetector(redis_client=redis, window_seconds=60)
        assert nn30._window_key() != nn60._window_key()


# ────────────────────────────────────────────────────────────────────────
# OnlineCostLearner
# ────────────────────────────────────────────────────────────────────────

def _make_learner(buf_len: int = 0) -> tuple[OnlineCostLearner, MagicMock]:
    redis = MagicMock()
    redis.llen.return_value = buf_len
    redis.rpush.return_value = 1

    model = MagicMock()
    model.partial_fit.return_value = None
    model.predict.return_value = [1.0, 1.0]
    model.model_id = "test-model"
    model.model_path = None

    learner = OnlineCostLearner(cost_model=model, redis_client=redis, batch_size=5)
    return learner, redis


class TestShouldUpdate:
    def test_false_when_buffer_small(self) -> None:
        learner, _ = _make_learner(buf_len=3)
        assert learner.should_update() is False

    def test_true_when_buffer_full(self) -> None:
        learner, _ = _make_learner(buf_len=5)
        assert learner.should_update() is True


class TestDistributedLock:
    """partial_fit() acquires Redis lock and releases it (fixes W-10)."""

    def test_lock_acquired_and_released(self) -> None:
        learner, redis = _make_learner(buf_len=5)
        redis.set.return_value = True  # lock acquired
        redis.pipeline.return_value.__enter__ = MagicMock(return_value=redis)
        redis.pipeline.return_value.__exit__ = MagicMock(return_value=False)
        redis.pipeline.return_value.lpop.return_value = None
        redis.pipeline.return_value.execute.return_value = [None] * 5

        # Simulate empty buffer after lock (no items to pop)
        learner.partial_fit()
        redis.set.assert_called_once()
        # Lock should be deleted even if buffer was empty
        redis.delete.assert_called_once()

    def test_skips_when_lock_not_acquired(self) -> None:
        learner, redis = _make_learner(buf_len=5)
        redis.set.return_value = False  # another worker holds lock
        result = learner.partial_fit()
        assert result.samples_used == 0
        learner._model.partial_fit.assert_not_called()
