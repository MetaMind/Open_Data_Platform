"""
Unit Tests — Neural Cost Model

File: tests/unit/test_neural_cost_model.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

import numpy as np

_FEATURES = {k: 1.0 for k in [
    "num_tables", "num_joins", "join_depth", "num_aggregates", "num_filters",
    "has_subquery", "estimated_output_rows", "scan_selectivity", "cross_engine_flag",
    "partition_pruning_possible", "has_window_function", "num_sort_nodes",
    "has_limit", "avg_table_size_rows", "complexity_score",
]}

def _has_torch() -> bool:
    try:
        import torch  # noqa: F401
        return True
    except ImportError:
        return False


class TestNeuralCostModelPredict(unittest.TestCase):

    def test_predict_returns_cost_prediction_shape(self) -> None:
        from metamind.ml.neural_cost_model import NeuralCostModel
        m = NeuralCostModel()
        pred = m.predict(_FEATURES)
        assert hasattr(pred, "estimated_cost")
        assert hasattr(pred, "lower_bound")
        assert hasattr(pred, "upper_bound")
        assert hasattr(pred, "confidence")

    def test_predict_untrained_returns_zero_cost(self) -> None:
        from metamind.ml.neural_cost_model import NeuralCostModel
        m = NeuralCostModel()
        pred = m.predict(_FEATURES)
        assert pred.estimated_cost == 0.0

    def test_bounds_ordering_after_training(self) -> None:
        if not _has_torch():
            self.skipTest("torch not installed")
        from metamind.ml.neural_cost_model import NeuralCostModel
        m = NeuralCostModel()
        X = np.random.rand(200, 15).astype(np.float32)
        y = np.log1p(np.random.rand(200) * 1000).astype(np.float32)
        m.train(X, y, epochs=10)
        pred = m.predict(_FEATURES)
        assert pred.lower_bound <= pred.estimated_cost + 1e-3
        assert pred.estimated_cost <= pred.upper_bound + 1e-3

    def test_confidence_between_0_and_1(self) -> None:
        if not _has_torch():
            self.skipTest("torch not installed")
        from metamind.ml.neural_cost_model import NeuralCostModel
        m = NeuralCostModel()
        X = np.random.rand(100, 15).astype(np.float32)
        y = np.random.rand(100).astype(np.float32)
        m.train(X, y, epochs=5)
        pred = m.predict(_FEATURES)
        assert 0.0 <= pred.confidence <= 1.0


class TestNeuralCostModelTrain(unittest.TestCase):

    def test_train_returns_train_metrics(self) -> None:
        if not _has_torch():
            self.skipTest("torch not installed")
        from metamind.ml.neural_cost_model import NeuralCostModel, TrainMetrics
        m = NeuralCostModel()
        X = np.random.rand(200, 15).astype(np.float32)
        y = (X[:, 0] * 500 + 100).astype(np.float32)
        metrics = m.train(X, y, epochs=20)
        assert isinstance(metrics, TrainMetrics)
        assert metrics.mae >= 0.0
        assert metrics.epochs_trained == 20
        assert metrics.training_time_ms >= 0

    def test_train_noop_without_torch(self) -> None:
        import metamind.ml.neural_cost_model as ncm
        original = ncm.TORCH_AVAILABLE
        ncm.TORCH_AVAILABLE = False
        try:
            from metamind.ml.neural_cost_model import NeuralCostModel
            m = NeuralCostModel()
            X = np.random.rand(100, 15).astype(np.float32)
            y = np.random.rand(100).astype(np.float32)
            metrics = m.train(X, y)
            assert metrics.mae == 0.0
        finally:
            ncm.TORCH_AVAILABLE = original

    def test_is_available_false_without_torch(self) -> None:
        import metamind.ml.neural_cost_model as ncm
        original = ncm.TORCH_AVAILABLE
        ncm.TORCH_AVAILABLE = False
        try:
            from metamind.ml.neural_cost_model import NeuralCostModel
            assert not NeuralCostModel().is_available
        finally:
            ncm.TORCH_AVAILABLE = original

    def test_batch_predict_length(self) -> None:
        if not _has_torch():
            self.skipTest("torch not installed")
        from metamind.ml.neural_cost_model import NeuralCostModel
        m = NeuralCostModel()
        X = np.random.rand(50, 15).astype(np.float32)
        y = np.random.rand(50).astype(np.float32)
        m.train(X, y, epochs=3)
        preds = m.predict_batch([_FEATURES] * 8)
        assert len(preds) == 8


class TestNeuralCostModelPersistence(unittest.TestCase):

    def test_save_load_roundtrip(self) -> None:
        if not _has_torch():
            self.skipTest("torch not installed")
        from metamind.ml.neural_cost_model import NeuralCostModel
        m = NeuralCostModel()
        X = np.random.rand(100, 15).astype(np.float32)
        y = np.random.rand(100).astype(np.float32)
        m.train(X, y, epochs=5)
        pred_before = m.predict(_FEATURES)

        with tempfile.NamedTemporaryFile(suffix=".pt", delete=False) as f:
            path = f.name
        try:
            m.save(path)
            m2 = NeuralCostModel()
            m2.load(path)
            pred_after = m2.predict(_FEATURES)
            if pred_before.estimated_cost > 1e-6:
                diff = abs(pred_after.estimated_cost - pred_before.estimated_cost)
                tol = pred_before.estimated_cost * 0.01
                assert diff <= tol, f"Round-trip diff {diff:.4f} > 1% of {pred_before.estimated_cost:.4f}"
        finally:
            os.unlink(path)

    def test_save_noop_without_training(self) -> None:
        from metamind.ml.neural_cost_model import NeuralCostModel
        m = NeuralCostModel()
        with tempfile.NamedTemporaryFile(suffix=".pt", delete=False) as f:
            path = f.name
        try:
            m.save(path)  # should not raise
        finally:
            if os.path.exists(path):
                os.unlink(path)


class TestMCDropoutUncertainty(unittest.TestCase):

    def test_uncertainty_non_negative(self) -> None:
        if not _has_torch():
            self.skipTest("torch not installed")
        from metamind.ml.neural_cost_model import NeuralCostModel
        m = NeuralCostModel()
        X = np.random.rand(300, 15).astype(np.float32)
        y = np.random.rand(300).astype(np.float32)
        m.train(X, y, epochs=10)
        pred = m.predict(_FEATURES)
        interval = pred.upper_bound - pred.lower_bound
        assert interval >= 0.0

    def test_predict_batch_bounds_per_sample(self) -> None:
        if not _has_torch():
            self.skipTest("torch not installed")
        from metamind.ml.neural_cost_model import NeuralCostModel
        m = NeuralCostModel()
        X = np.random.rand(100, 15).astype(np.float32)
        y = np.random.rand(100).astype(np.float32)
        m.train(X, y, epochs=5)
        preds = m.predict_batch([_FEATURES] * 5)
        for p in preds:
            assert p.lower_bound <= p.estimated_cost + 1e-3
            assert p.estimated_cost <= p.upper_bound + 1e-3


if __name__ == "__main__":
    unittest.main()
