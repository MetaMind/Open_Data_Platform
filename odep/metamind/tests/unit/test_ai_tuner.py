"""Unit tests for AIQueryTuner (Task 01) — W-04."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from metamind.core.rewrite.ai_tuner import AIQueryTuner, TuneResult


def _make_tuner(threshold_ms: int = 5000) -> tuple[AIQueryTuner, MagicMock]:
    llm = MagicMock()
    tuner = AIQueryTuner(llm_client=llm, latency_threshold_ms=threshold_ms)
    return tuner, llm


class TestThresholdGuard:
    """Queries below threshold are never sent to the LLM."""

    @pytest.mark.asyncio
    async def test_below_threshold_returns_original(self) -> None:
        tuner, llm = _make_tuner(threshold_ms=5000)
        result = await tuner.tune(
            "SELECT 1", {"duration_ms": 4999}, "tenant1"
        )
        assert result.was_changed is False
        assert result.rewritten_sql == "SELECT 1"
        llm.messages.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_exactly_at_threshold_is_tuned(self) -> None:
        """Boundary fix: queries at exactly threshold_ms should be tuned (W-18)."""
        tuner, llm = _make_tuner(threshold_ms=5000)
        llm.messages.create = AsyncMock(
            return_value=MagicMock(
                content=[MagicMock(text="<rewritten_sql>SELECT 1</rewritten_sql>")]
            )
        )
        # 5000ms == threshold → should attempt rewrite (< not <=)
        result = await tuner.tune("SELECT 1", {"duration_ms": 5000}, "t1")
        llm.messages.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_above_threshold_calls_llm(self) -> None:
        tuner, llm = _make_tuner(threshold_ms=1000)
        llm.messages.create = AsyncMock(
            return_value=MagicMock(
                content=[MagicMock(text="<rewritten_sql>SELECT id FROM t</rewritten_sql>")]
            )
        )
        result = await tuner.tune(
            "SELECT * FROM t", {"duration_ms": 2000}, "tenant2"
        )
        assert isinstance(result, TuneResult)


class TestLLMFallback:
    """On LLM failure the original SQL is always returned."""

    @pytest.mark.asyncio
    async def test_llm_exception_returns_original(self) -> None:
        tuner, llm = _make_tuner(threshold_ms=100)
        llm.messages.create = AsyncMock(side_effect=RuntimeError("timeout"))
        result = await tuner.tune("SELECT * FROM t", {"duration_ms": 9999}, "t1")
        assert result.was_changed is False
        assert result.rewritten_sql == "SELECT * FROM t"
        assert "timeout" in result.explanation

    @pytest.mark.asyncio
    async def test_invalid_sql_from_llm_returns_original(self) -> None:
        tuner, llm = _make_tuner(threshold_ms=100)
        llm.messages.create = AsyncMock(
            return_value=MagicMock(
                content=[MagicMock(text="<rewritten_sql>NOT VALID SQL !!!###</rewritten_sql>")]
            )
        )
        result = await tuner.tune("SELECT 1", {"duration_ms": 9999}, "t1")
        assert result.was_changed is False


class TestTuneResult:
    """TuneResult dataclass hashes correctly."""

    def test_hashes_populated(self) -> None:
        r = TuneResult(
            original_sql="SELECT 1",
            rewritten_sql="SELECT 2",
            explanation="test",
            was_changed=True,
        )
        assert len(r.original_sql_hash) == 16
        assert len(r.rewritten_sql_hash) == 16
        assert r.original_sql_hash != r.rewritten_sql_hash
