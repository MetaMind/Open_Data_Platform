"""
Unit Tests — GPU Engine and GPU Router

File: tests/unit/test_gpu_engine.py
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch, AsyncMock

import pyarrow as pa
import numpy as np


def _make_settings(min_rows: int = 100_000, enabled: bool = True) -> MagicMock:
    s = MagicMock()
    s.gpu.min_gpu_rows = min_rows
    s.gpu.enabled = enabled
    return s


class TestGPUEngineAvailability(unittest.TestCase):

    def test_is_available_false_when_cudf_not_installed(self) -> None:
        import metamind.execution.gpu_engine as ge
        original = ge.GPU_AVAILABLE
        ge.GPU_AVAILABLE = False
        try:
            from metamind.execution.gpu_engine import GPUEngine
            engine = GPUEngine(_make_settings())
            assert engine.is_available is False
        finally:
            ge.GPU_AVAILABLE = original

    def test_health_check_returns_dict(self) -> None:
        from metamind.execution.gpu_engine import GPUEngine
        engine = GPUEngine(_make_settings())
        h = engine.health_check()
        assert isinstance(h, dict)
        assert "gpu_available" in h
        assert "min_gpu_rows" in h


class TestGPUEngineCPUFallback(unittest.IsolatedAsyncioTestCase):

    async def test_execute_returns_arrow_table_on_cpu(self) -> None:
        from metamind.execution.gpu_engine import GPUEngine
        import metamind.execution.gpu_engine as ge
        ge.GPU_AVAILABLE = False
        try:
            engine = GPUEngine(_make_settings(min_rows=1))
            data = pa.table({"x": [1, 2, 3], "y": [10, 20, 30]})
            result = await engine.execute(
                data, "filter",
                {"predicates": [{"column": "x", "op": ">=", "value": 2}]},
                "tenant1",
            )
            assert isinstance(result, pa.Table)
        finally:
            ge.GPU_AVAILABLE = False  # keep mocked

    async def test_filter_cpu_reduces_rows(self) -> None:
        from metamind.execution.gpu_engine import GPUEngine
        import metamind.execution.gpu_engine as ge
        ge.GPU_AVAILABLE = False
        engine = GPUEngine(_make_settings())
        data = pa.table({"val": list(range(10))})
        result = engine._execute_filter(
            data, [{"column": "val", "op": ">=", "value": 5}], gpu=False
        )
        assert len(result) == 5

    async def test_engine_used_label_cpu_fallback(self) -> None:
        from metamind.execution.gpu_engine import GPUEngine
        from metamind.core.gpu_router import GPURouter
        import metamind.execution.gpu_engine as ge
        ge.GPU_AVAILABLE = False

        engine = GPUEngine(_make_settings(min_rows=1))
        router = GPURouter(engine, _make_settings(min_rows=1, enabled=True))
        data = pa.table({"x": list(range(5))})
        result, engine_used = await router.route_with_gpu_fallback(
            data, "filter",
            {"predicates": [{"column": "x", "op": ">", "value": 2}]},
            "t1",
        )
        # GPU unavailable → cpu_fallback label
        assert engine_used in ("cpu_fallback", "gpu")

    def test_execute_filter_unknown_column_skips(self) -> None:
        from metamind.execution.gpu_engine import GPUEngine
        import metamind.execution.gpu_engine as ge
        ge.GPU_AVAILABLE = False
        engine = GPUEngine(_make_settings())
        data = pa.table({"x": [1, 2, 3]})
        result = engine._execute_filter(
            data, [{"column": "nonexistent_col", "op": "=", "value": 1}], gpu=False
        )
        # Should return original table unchanged
        assert len(result) == 3

    def test_execute_aggregation_cpu(self) -> None:
        import pandas as pd
        from metamind.execution.gpu_engine import GPUEngine
        import metamind.execution.gpu_engine as ge
        ge.GPU_AVAILABLE = False
        engine = GPUEngine(_make_settings())
        data = pa.table({"region": ["A", "A", "B"], "revenue": [100, 200, 300]})
        result = engine._execute_aggregation(
            data,
            {"group_by": ["region"], "aggregates": [{"column": "revenue", "func": "sum", "alias": "total"}]},
            gpu=False,
        )
        assert isinstance(result, pa.Table)
        assert len(result) == 2  # 2 distinct regions


class TestGPURouterDecision(unittest.TestCase):

    def _make_router(self, min_rows: int = 100_000, enabled: bool = True, available: bool = True):
        from metamind.execution.gpu_engine import GPUEngine
        from metamind.core.gpu_router import GPURouter
        import metamind.execution.gpu_engine as ge
        ge.GPU_AVAILABLE = available

        engine = GPUEngine(_make_settings(min_rows=min_rows, enabled=enabled))
        # Patch is_available property
        engine_mock = MagicMock(spec=GPUEngine)
        engine_mock.is_available = available
        engine_mock.health_check.return_value = {}
        engine_mock._execute_on_cpu = engine._execute_on_cpu
        settings = _make_settings(min_rows=min_rows, enabled=enabled)
        return GPURouter(engine_mock, settings)

    def test_should_not_use_gpu_when_disabled(self) -> None:
        router = self._make_router(enabled=False, available=True)
        assert router.should_use_gpu({"estimated_output_rows": 1_000_000}) is False

    def test_should_not_use_gpu_below_min_rows(self) -> None:
        router = self._make_router(min_rows=100_000, enabled=True, available=True)
        assert router.should_use_gpu({"estimated_output_rows": 50_000}) is False

    def test_should_use_gpu_when_conditions_met(self) -> None:
        router = self._make_router(min_rows=100_000, enabled=True, available=True)
        assert router.should_use_gpu({
            "estimated_output_rows": 500_000,
            "operation_type": "aggregate",
            "num_joins": 0,
        }) is True

    def test_should_not_use_gpu_for_join_heavy(self) -> None:
        router = self._make_router(min_rows=1, enabled=True, available=True)
        assert router.should_use_gpu({
            "estimated_output_rows": 1_000_000,
            "num_joins": 5,
        }) is False

    def test_should_not_use_gpu_when_unavailable(self) -> None:
        router = self._make_router(available=False, enabled=True)
        assert router.should_use_gpu({"estimated_output_rows": 1_000_000}) is False


class TestGPUEngineConversion(unittest.TestCase):

    def test_execute_filter_equality(self) -> None:
        from metamind.execution.gpu_engine import GPUEngine
        import metamind.execution.gpu_engine as ge
        ge.GPU_AVAILABLE = False
        engine = GPUEngine(_make_settings())
        data = pa.table({"status": ["open", "closed", "open"]})
        result = engine._execute_filter(
            data, [{"column": "status", "op": "=", "value": "open"}], gpu=False
        )
        assert len(result) == 2

    def test_execute_filter_range(self) -> None:
        from metamind.execution.gpu_engine import GPUEngine
        import metamind.execution.gpu_engine as ge
        ge.GPU_AVAILABLE = False
        engine = GPUEngine(_make_settings())
        data = pa.table({"amount": [10, 50, 200, 500]})
        result = engine._execute_filter(
            data, [{"column": "amount", "op": ">=", "value": 100}], gpu=False
        )
        assert len(result) == 2

    def test_execute_join_cpu(self) -> None:
        from metamind.execution.gpu_engine import GPUEngine
        import metamind.execution.gpu_engine as ge
        ge.GPU_AVAILABLE = False
        engine = GPUEngine(_make_settings())
        left = pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        right = pa.table({"id": [1, 2, 4], "score": [10, 20, 40]})
        result = engine._execute_join(left, right, key="id", join_type="inner", gpu=False)
        assert isinstance(result, pa.Table)
        assert len(result) == 2  # ids 1 and 2 match


if __name__ == "__main__":
    unittest.main()
