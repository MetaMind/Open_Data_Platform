"""F01 XGBoost cardinality model trainer with cross-validation and versioning.

Trains per-table cardinality estimation models from observed query feedback.
Supports hyperparameter search, k-fold cross-validation, early stopping,
and model version management.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class TrainingResult:
    """Results from a model training run."""

    tenant_id: str
    table_name: str
    model_version: int
    sample_count: int
    train_rmse: float
    val_rmse: float
    cv_rmse_mean: float
    cv_rmse_std: float
    training_time_ms: float
    model_key: str
    is_improvement: bool  # True if better than previous version


class CardinalityTrainer:
    """Trains XGBoost cardinality models with cross-validation.

    Features:
    - K-fold cross-validation with early stopping
    - Automatic hyperparameter selection based on dataset size
    - Model versioning (only saves if new model improves over previous)
    - Log-space training (models log1p(cardinality) for numeric stability)
    """

    def __init__(
        self,
        model_store: Any,
        n_folds: int = 5,
        min_samples: int = 50,
        improvement_threshold: float = 0.05,
    ) -> None:
        """Initialize trainer.

        Args:
            model_store: ModelStore for saving/loading models.
            n_folds: Number of cross-validation folds.
            min_samples: Minimum samples required for training.
            improvement_threshold: Min RMSE reduction to replace existing model.
        """
        self._store = model_store
        self._n_folds = n_folds
        self._min_samples = min_samples
        self._improvement_threshold = improvement_threshold

    def train(
        self,
        tenant_id: str,
        table_name: str,
        X: np.ndarray,
        y: np.ndarray,
    ) -> Optional[TrainingResult]:
        """Train a cardinality model with cross-validation.

        Args:
            tenant_id: Tenant identifier.
            table_name: Table the model is for.
            X: Feature matrix (N x F).
            y: Target cardinalities (N,).

        Returns:
            TrainingResult if training succeeded, None otherwise.
        """
        if len(X) < self._min_samples:
            logger.warning(
                "Insufficient training data for %s.%s (%d < %d)",
                tenant_id, table_name, len(X), self._min_samples,
            )
            return None

        try:
            import xgboost as xgb
        except ImportError:
            logger.warning("xgboost not installed; skipping training")
            return None

        start = time.monotonic()
        log_y = np.log1p(y.astype(float))

        # Select hyperparameters based on dataset size
        params = self._select_hyperparams(len(X))

        # K-fold cross-validation
        cv_scores = self._cross_validate(X, log_y, params)

        # Train final model on all data
        model = xgb.XGBRegressor(**params)
        model.fit(
            X, log_y,
            eval_set=[(X, log_y)],
            verbose=False,
        )

        # Calculate training RMSE
        train_preds = model.predict(X)
        train_rmse = float(np.sqrt(np.mean((train_preds - log_y) ** 2)))

        # Check if this improves over existing model
        model_key = self._model_key(tenant_id, table_name)
        current_version = self._store.get_version(model_key)
        new_version = current_version + 1

        is_improvement = self._check_improvement(
            model_key, cv_scores["mean"], current_version
        )

        if is_improvement:
            self._store.save(model_key, model, version=new_version, metadata={
                "tenant_id": tenant_id,
                "table_name": table_name,
                "sample_count": len(X),
                "train_rmse": train_rmse,
                "cv_rmse_mean": cv_scores["mean"],
                "cv_rmse_std": cv_scores["std"],
                "params": params,
            })
            logger.info(
                "Saved improved model v%d for %s.%s (cv_rmse=%.4f)",
                new_version, tenant_id, table_name, cv_scores["mean"],
            )
        else:
            logger.info(
                "Model for %s.%s not improved (cv_rmse=%.4f vs existing)",
                tenant_id, table_name, cv_scores["mean"],
            )

        elapsed_ms = (time.monotonic() - start) * 1000
        return TrainingResult(
            tenant_id=tenant_id,
            table_name=table_name,
            model_version=new_version if is_improvement else current_version,
            sample_count=len(X),
            train_rmse=train_rmse,
            val_rmse=cv_scores["mean"],
            cv_rmse_mean=cv_scores["mean"],
            cv_rmse_std=cv_scores["std"],
            training_time_ms=elapsed_ms,
            model_key=model_key,
            is_improvement=is_improvement,
        )

    def _select_hyperparams(self, n_samples: int) -> dict[str, Any]:
        """Select XGBoost hyperparameters based on dataset size."""
        if n_samples < 200:
            return {
                "n_estimators": 50,
                "max_depth": 4,
                "learning_rate": 0.1,
                "min_child_weight": 5,
                "subsample": 1.0,
                "colsample_bytree": 1.0,
            }
        elif n_samples < 2000:
            return {
                "n_estimators": 100,
                "max_depth": 6,
                "learning_rate": 0.1,
                "min_child_weight": 3,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
            }
        else:
            return {
                "n_estimators": 200,
                "max_depth": 8,
                "learning_rate": 0.05,
                "min_child_weight": 1,
                "subsample": 0.7,
                "colsample_bytree": 0.7,
                "reg_alpha": 0.1,
                "reg_lambda": 1.0,
            }

    def _cross_validate(
        self, X: np.ndarray, y: np.ndarray, params: dict[str, Any]
    ) -> dict[str, float]:
        """Run k-fold cross-validation."""
        import xgboost as xgb

        n = len(X)
        fold_size = n // self._n_folds
        fold_scores: list[float] = []

        indices = np.random.permutation(n)

        for fold in range(self._n_folds):
            val_start = fold * fold_size
            val_end = val_start + fold_size if fold < self._n_folds - 1 else n

            val_idx = indices[val_start:val_end]
            train_idx = np.concatenate([indices[:val_start], indices[val_end:]])

            X_train, y_train = X[train_idx], y[train_idx]
            X_val, y_val = X[val_idx], y[val_idx]

            model = xgb.XGBRegressor(**params)
            model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

            val_preds = model.predict(X_val)
            rmse = float(np.sqrt(np.mean((val_preds - y_val) ** 2)))
            fold_scores.append(rmse)

        return {
            "mean": float(np.mean(fold_scores)),
            "std": float(np.std(fold_scores)),
            "folds": fold_scores,
        }

    def _check_improvement(
        self, model_key: str, new_cv_rmse: float, current_version: int
    ) -> bool:
        """Check if new model improves over the current stored model."""
        if current_version == 0:
            return True

        metadata = self._store.get_metadata(model_key)
        if metadata is None:
            return True

        prev_rmse = metadata.get("cv_rmse_mean", float("inf"))
        improvement = (prev_rmse - new_cv_rmse) / max(0.001, prev_rmse)
        return improvement >= self._improvement_threshold

    def _model_key(self, tenant_id: str, table_name: str) -> str:
        """Generate a model store key."""
        return f"card_{tenant_id}_{table_name}"[:48]
