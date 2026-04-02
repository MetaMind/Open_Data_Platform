"""
Neural Cost Model — PyTorch-Based Query Cost Prediction with MC Dropout

File: metamind/ml/neural_cost_model.py
Role: ML Engineer
Dependencies: torch (optional), numpy, scikit-learn

Implements a three-layer fully-connected neural network for query cost
prediction.  Monte Carlo Dropout provides calibrated uncertainty estimates
without requiring a separate Bayesian inference framework.  Falls back
transparently to a no-op stub when PyTorch is unavailable so that the
rest of the platform continues to function with the XGBoost model.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PyTorch import guard
# ---------------------------------------------------------------------------
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset

    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    logger.warning(
        "NeuralCostModel: torch not installed — falling back to XGBoost predictions"
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_MC_SAMPLES = 30          # MC Dropout forward passes per prediction
_DEFAULT_INPUT_DIM = 15   # matches PlanFeatures.to_dict() key count
_HIDDEN_DIMS = (256, 128, 64)
_DROPOUT_RATE = 0.1


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------

@dataclass
class CostPrediction:
    """Point estimate + uncertainty interval from a cost model inference."""
    estimated_cost: float
    lower_bound: float
    upper_bound: float
    confidence: float        # 0–1; higher = tighter interval
    model_type: str = "neural"


@dataclass
class TrainMetrics:
    """Metrics returned after a training run."""
    mae: float = 0.0
    rmse: float = 0.0
    r2_score: float = 0.0
    epochs_trained: int = 0
    training_time_ms: int = 0


# ---------------------------------------------------------------------------
# PyTorch model architecture (only defined when torch is importable)
# ---------------------------------------------------------------------------

if TORCH_AVAILABLE:

    class _CostNet(nn.Module):
        """Three-layer MLP with dropout at every hidden layer."""

        def __init__(self, input_dim: int) -> None:
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(input_dim, _HIDDEN_DIMS[0]),
                nn.ReLU(),
                nn.Dropout(_DROPOUT_RATE),
                nn.Linear(_HIDDEN_DIMS[0], _HIDDEN_DIMS[1]),
                nn.ReLU(),
                nn.Dropout(_DROPOUT_RATE),
                nn.Linear(_HIDDEN_DIMS[1], _HIDDEN_DIMS[2]),
                nn.ReLU(),
                nn.Linear(_HIDDEN_DIMS[2], 1),
            )

        def forward(self, x: "torch.Tensor") -> "torch.Tensor":
            return self.net(x).squeeze(-1)

        def predict_mc(
            self, x: "torch.Tensor", n_samples: int = _MC_SAMPLES
        ) -> Tuple["torch.Tensor", "torch.Tensor"]:
            """
            Run *n_samples* stochastic forward passes with dropout active.
            Returns (mean, std) over the sample dimension.
            """
            self.train()  # keep dropout active during inference
            with torch.no_grad():
                samples = torch.stack(
                    [self.forward(x) for _ in range(n_samples)], dim=0
                )
            self.eval()
            return samples.mean(dim=0), samples.std(dim=0)

else:
    _CostNet = None  # type: ignore[assignment,misc]


# ---------------------------------------------------------------------------
# NeuralCostModel
# ---------------------------------------------------------------------------

class NeuralCostModel:
    """
    Wraps _CostNet with a sklearn-like predict / train API.

    All inputs are expected to be log1p-transformed runtimes (matching
    TrainingDatasetBuilder.export_to_numpy).
    """

    def __init__(self, input_dim: int = _DEFAULT_INPUT_DIM) -> None:
        self._input_dim = input_dim
        self._model: Optional[Any] = None   # _CostNet instance
        self._is_trained = False
        self._feature_mean: Optional[np.ndarray] = None
        self._feature_std: Optional[np.ndarray] = None

        if TORCH_AVAILABLE:
            self._model = _CostNet(input_dim)
            self._model.eval()

    # ------------------------------------------------------------------
    # Public inference API
    # ------------------------------------------------------------------

    def predict(self, features: Dict[str, Any]) -> CostPrediction:
        """Single-sample prediction with uncertainty bounds."""
        x = self._dict_to_array(features)
        return self._predict_array(x.reshape(1, -1))[0]

    def predict_batch(self, features_list: List[Dict[str, Any]]) -> List[CostPrediction]:
        """Vectorised batch prediction."""
        if not features_list:
            return []
        X = np.stack([self._dict_to_array(f) for f in features_list]).astype(np.float32)
        return self._predict_array(X)

    def predict_raw(self, x: np.ndarray) -> np.ndarray:
        """Return raw log-space predictions for a numpy matrix."""
        if not TORCH_AVAILABLE or not self._is_trained:
            return np.zeros(len(x), dtype=np.float32)
        x_norm = self._normalise(x)
        tensor = torch.tensor(x_norm, dtype=torch.float32)
        with torch.no_grad():
            self._model.eval()
            out = self._model(tensor).numpy()
        return out

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train(
        self,
        X: np.ndarray,
        y: np.ndarray,
        epochs: int = 100,
        batch_size: int = 256,
        lr: float = 1e-3,
    ) -> TrainMetrics:
        """
        Train the neural network on (X, y) where y = log1p(runtime_ms).

        Stores feature normalization statistics for inference.
        Returns TrainMetrics with final-epoch MAE, RMSE, R².
        """
        if not TORCH_AVAILABLE:
            logger.warning(
                "NeuralCostModel.train: torch unavailable, skipping training"
            )
            return TrainMetrics()

        t0 = time.monotonic()
        X = X.astype(np.float32)
        y = y.astype(np.float32)

        # Fit normalizer
        self._feature_mean = X.mean(axis=0)
        self._feature_std = X.std(axis=0) + 1e-8
        X_norm = (X - self._feature_mean) / self._feature_std

        dataset = TensorDataset(
            torch.tensor(X_norm),
            torch.tensor(y),
        )
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

        optimizer = optim.Adam(self._model.parameters(), lr=lr)
        loss_fn = nn.MSELoss()

        self._model.train()
        final_loss = 0.0
        for epoch in range(epochs):
            for xb, yb in loader:
                optimizer.zero_grad()
                pred = self._model(xb)
                loss = loss_fn(pred, yb)
                loss.backward()
                optimizer.step()
            if epoch == epochs - 1:
                final_loss = float(loss.item())

        self._is_trained = True
        self._model.eval()

        # Compute metrics on full dataset
        with torch.no_grad():
            preds = self._model(torch.tensor(X_norm)).numpy()
        residuals = preds - y
        mae = float(np.mean(np.abs(residuals)))
        rmse = float(np.sqrt(np.mean(residuals ** 2)))
        ss_res = float(np.sum(residuals ** 2))
        ss_tot = float(np.sum((y - y.mean()) ** 2))
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            "NeuralCostModel.train complete epochs=%d mae=%.4f rmse=%.4f r2=%.4f ms=%d",
            epochs,
            mae,
            rmse,
            r2,
            elapsed_ms,
        )
        return TrainMetrics(mae=mae, rmse=rmse, r2_score=r2,
                            epochs_trained=epochs, training_time_ms=elapsed_ms)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        """Persist model state dict and normalization stats."""
        if not TORCH_AVAILABLE or not self._is_trained:
            logger.warning("NeuralCostModel.save: nothing to save")
            return
        torch.save(
            {
                "state_dict": self._model.state_dict(),
                "input_dim": self._input_dim,
                "feature_mean": self._feature_mean,
                "feature_std": self._feature_std,
            },
            path,
        )
        logger.info("NeuralCostModel saved to %s", path)

    def load(self, path: str) -> None:
        """Restore model from a previously saved checkpoint."""
        if not TORCH_AVAILABLE:
            logger.warning("NeuralCostModel.load: torch unavailable")
            return
        checkpoint = torch.load(path, map_location="cpu")
        input_dim = checkpoint.get("input_dim", self._input_dim)
        if self._model is None or input_dim != self._input_dim:
            self._input_dim = input_dim
            self._model = _CostNet(input_dim)
        self._model.load_state_dict(checkpoint["state_dict"])
        self._model.eval()
        self._feature_mean = checkpoint.get("feature_mean")
        self._feature_std = checkpoint.get("feature_std")
        self._is_trained = True
        logger.info("NeuralCostModel loaded from %s", path)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _predict_array(self, X: np.ndarray) -> List[CostPrediction]:
        """MC Dropout inference on a 2-D numpy array."""
        if not TORCH_AVAILABLE or not self._is_trained:
            return [
                CostPrediction(
                    estimated_cost=0.0,
                    lower_bound=0.0,
                    upper_bound=0.0,
                    confidence=0.0,
                    model_type="neural_untrained",
                )
                for _ in range(len(X))
            ]
        X_norm = self._normalise(X.astype(np.float32))
        tensor = torch.tensor(X_norm, dtype=torch.float32)
        means, stds = self._model.predict_mc(tensor, _MC_SAMPLES)
        means_np = means.numpy()
        stds_np = stds.numpy()

        predictions: List[CostPrediction] = []
        for mu, sigma in zip(means_np, stds_np):
            lower = float(np.expm1(max(0.0, float(mu) - 1.96 * float(sigma))))
            upper = float(np.expm1(float(mu) + 1.96 * float(sigma)))
            point = float(np.expm1(float(mu)))
            conf = float(max(0.0, 1.0 - float(sigma)))
            predictions.append(
                CostPrediction(
                    estimated_cost=max(0.0, point),
                    lower_bound=max(0.0, lower),
                    upper_bound=max(0.0, upper),
                    confidence=round(min(1.0, conf), 4),
                    model_type="neural",
                )
            )
        return predictions

    def _normalise(self, X: np.ndarray) -> np.ndarray:
        if self._feature_mean is not None and self._feature_std is not None:
            return (X - self._feature_mean) / self._feature_std
        return X

    def _dict_to_array(self, features: Dict[str, Any]) -> np.ndarray:
        """Convert a feature dict to an ordered numpy vector."""
        keys = [
            "num_tables", "num_joins", "join_depth", "num_aggregates",
            "num_filters", "has_subquery", "estimated_output_rows",
            "scan_selectivity", "cross_engine_flag", "partition_pruning_possible",
            "has_window_function", "num_sort_nodes", "has_limit",
            "avg_table_size_rows", "complexity_score",
        ]
        return np.array([float(features.get(k, 0)) for k in keys], dtype=np.float32)

    @property
    def is_available(self) -> bool:
        """True when torch is installed and the model has been trained."""
        return TORCH_AVAILABLE and self._is_trained
