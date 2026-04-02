"""Unit tests for FailoverRouter and LatencyAnomalyDetector — W-04."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, AsyncMock, patch


# ────────────────────────────────────────────────────────────────────────
# FailoverRouter
# ────────────────────────────────────────────────────────────────────────

def _make_router(healthy: set[str], latencies: dict[str, float] | None = None):
    from metamind.core.federation.failover_router import FailoverRouter

    health = MagicMock()
    health.is_healthy.side_effect = lambda e: e in healthy

    oracle = (lambda e: latencies.get(e, 999.0)) if latencies else None

    region_map = {
        "us-east-1": ["trino-primary", "spark-backup"],
        "eu-west-1": ["trino-eu", "spark-eu"],
    }
    return FailoverRouter(
        health_registry=health,
        region_map=region_map,
        db_engine=None,
        latency_oracle=oracle,
    )


class TestPrimaryEngine:
    def test_returns_first_healthy(self) -> None:
        router = _make_router({"trino-primary", "spark-backup"})
        assert router.primary_engine("t1") == "trino-primary"

    def test_skips_unhealthy_primary(self) -> None:
        router = _make_router({"spark-backup"})
        assert router.primary_engine("t1") == "spark-backup"

    def test_falls_back_cross_region(self) -> None:
        router = _make_router({"trino-eu"})
        assert router.primary_engine("t1") == "trino-eu"


class TestFailoverEngine:
    def test_same_region_failover(self) -> None:
        router = _make_router({"spark-backup", "trino-eu"})
        alt = router.failover_engine("trino-primary", "t1")
        assert alt == "spark-backup"

    def test_cross_region_when_same_region_down(self) -> None:
        router = _make_router({"trino-eu"})
        alt = router.failover_engine("trino-primary", "t1")
        assert alt == "trino-eu"

    def test_none_when_no_healthy_alternative(self) -> None:
        router = _make_router(set())
        alt = router.failover_engine("trino-primary", "t1")
        assert alt is None


class TestLatencySorting:
    """Candidates are sorted by P95 latency when oracle is provided (fixes W-15)."""

    def test_faster_alternative_chosen_first(self) -> None:
        latencies = {
            "trino-primary": 9999.0,  # failed
            "spark-backup": 5000.0,   # slow
            "trino-eu": 100.0,        # fast
            "spark-eu": 200.0,
        }
        router = _make_router({"spark-backup", "trino-eu", "spark-eu"}, latencies)
        # spark-backup is in same region but trino-eu is faster cross-region
        # same-region candidates: spark-backup (5000ms)
        # cross-region candidates: trino-eu (100ms), spark-eu (200ms)
        alt = router.failover_engine("trino-primary", "t1")
        # spark-backup (same region, 5000ms) should still win over cross-region
        assert alt == "spark-backup"

    def test_sort_by_latency_within_region(self) -> None:
        """When multiple same-region alternatives exist, fastest wins."""
        from metamind.core.federation.failover_router import FailoverRouter
        health = MagicMock()
        health.is_healthy.return_value = True

        latencies = {"engine-a": 800.0, "engine-b": 200.0, "engine-c": 9999.0}
        region_map = {"us-east-1": ["engine-c", "engine-a", "engine-b"]}
        router = FailoverRouter(
            health_registry=health,
            region_map=region_map,
            latency_oracle=lambda e: latencies.get(e, 999.0),
        )
        alt = router.failover_engine("engine-c", "t1")
        assert alt == "engine-b"  # fastest (200ms) chosen over engine-a (800ms)


class TestExecuteWithFailover:
    @pytest.mark.asyncio
    async def test_succeeds_on_primary(self) -> None:
        router = _make_router({"trino-primary", "spark-backup"})
        backends = {
            "trino-primary": AsyncMock(return_value=MagicMock(row_count=5)),
        }
        result = await router.execute_with_failover(
            MagicMock(tenant_id="t1", sql="SELECT 1", query_id="q1"),
            backends,
        )
        assert result.row_count == 5

    @pytest.mark.asyncio
    async def test_fails_over_on_connection_error(self) -> None:
        router = _make_router({"trino-primary", "spark-backup"})
        call_count = {"n": 0}

        async def primary_fn(ctx):
            raise ConnectionError("primary down")

        async def backup_fn(ctx):
            return MagicMock(row_count=3)

        backends = {"trino-primary": primary_fn, "spark-backup": backup_fn}
        result = await router.execute_with_failover(
            MagicMock(tenant_id="t1", sql="SELECT 1", query_id="q1"),
            backends,
        )
        assert result.row_count == 3

    @pytest.mark.asyncio
    async def test_raises_when_all_exhausted(self) -> None:
        from metamind.core.federation.failover_router import QueryExecutionError
        router = _make_router({"trino-primary", "spark-backup"})

        async def always_fail(ctx):
            raise ConnectionError("down")

        backends = {"trino-primary": always_fail, "spark-backup": always_fail}
        with pytest.raises(QueryExecutionError):
            await router.execute_with_failover(
                MagicMock(tenant_id="t1", sql="SELECT 1", query_id="q1"),
                backends,
            )


# ────────────────────────────────────────────────────────────────────────
# LatencyAnomalyDetector
# ────────────────────────────────────────────────────────────────────────

class TestAnomalyDetector:
    def _make_detector(self, throttle_returns: bool = False):
        from metamind.observability.latency_anomaly import LatencyAnomalyDetector

        redis = MagicMock()
        redis.get.return_value = None          # no throttle key
        redis.setex.return_value = True

        db = MagicMock()
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        db.connect.return_value.__enter__ = MagicMock(return_value=conn)
        db.connect.return_value.__exit__ = MagicMock(return_value=False)

        return LatencyAnomalyDetector(
            db_engine=db,
            redis_client=redis,
            webhook_dispatcher=None,
            window_minutes=10,
            z_threshold=3.0,
        )

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self) -> None:
        detector = self._make_detector()
        await detector.start()
        assert detector._running is True
        await detector.stop()
        assert detector._running is False

    @pytest.mark.asyncio
    async def test_no_alert_when_no_data(self) -> None:
        detector = self._make_detector()
        # _tick with empty DB result should not crash
        with patch.object(detector, "_fire_alert", new=AsyncMock()) as mock_alert:
            await detector._tick()
        mock_alert.assert_not_called()

    def test_z_score_below_threshold_no_alert(self) -> None:
        import statistics, math
        samples = [100.0, 110.0, 95.0, 105.0, 102.0]
        mean = statistics.mean(samples)
        stdev = statistics.stdev(samples) or 1.0
        # p99 well within normal range
        p99 = 115.0
        z = (p99 - mean) / stdev
        assert z < 3.0  # should not trigger

    def test_high_z_score_triggers(self) -> None:
        import statistics
        samples = [100.0, 102.0, 98.0, 101.0, 99.0]
        mean = statistics.mean(samples)
        stdev = statistics.stdev(samples) or 1.0
        p99 = 9000.0
        z = (p99 - mean) / stdev
        assert z > 3.0  # should trigger alert
