"""
ML Cost Model - Query Cost Prediction

File: metamind/ml/cost_model.py
Role: ML Engineer
Phase: 2
Dependencies: xgboost, scikit-learn

XGBoost-based cost prediction for query routing decisions.
Predicts execution time for different sources (Oracle, S3, etc.)
"""

from __future__ import annotations

import json
import logging
import os
import pickle
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ModelMetrics:
    """Model performance metrics."""
    mae: float
    rmse: float
    r2: float
    mape: float


class QueryCostModel:
    """
    XGBoost-based cost prediction for query routing.
    
    Predicts execution time for different sources:
    - Oracle (OLTP)
    - S3/Iceberg (OLAP via Trino)
    - Spark (Batch)
    """
    
    # Feature names for the model
    FEATURE_NAMES = [
        "num_tables",
        "num_joins",
        "num_aggregates",
        "has_where",
        "has_group_by",
        "has_order_by",
        "has_limit",
        "has_subquery",
        "query_length",
        "complexity_score"
    ]
    
    def __init__(
        self,
        model_path: str = "./models",
        model_type: str = "xgboost"
    ):
        """
        Initialize cost model.
        
        Args:
            model_path: Path to model storage
            model_type: Model algorithm type
        """
        self.model_path = model_path
        self.model_type = model_type
        self._models: Dict[str, Any] = {}
        self._is_loaded = False
        
        # Ensure model directory exists
        os.makedirs(model_path, exist_ok=True)
        
        logger.debug(f"QueryCostModel initialized: {model_type}")
    
    def _load_model(self, source: str) -> Optional[Any]:
        """
        Load model for a specific source.
        
        Args:
            source: Source type (oracle, s3_iceberg, spark)
            
        Returns:
            Loaded model or None
        """
        if source in self._models:
            return self._models[source]
        
        model_file = os.path.join(self.model_path, f"{source}_model.pkl")
        
        if not os.path.exists(model_file):
            logger.warning(f"Model not found for {source}, using heuristic")
            return None
        
        try:
            with open(model_file, "rb") as f:
                model = pickle.load(f)
            self._models[source] = model
            logger.debug(f"Loaded model for {source}")
            return model
        except Exception as e:
            logger.error(f"Failed to load model for {source}: {e}")
            return None
    
    def predict(self, features: Dict[str, Any], source: str) -> float:
        """
        Predict query cost for a source.
        
        Args:
            features: Query features
            source: Target source (oracle, s3_iceberg, spark)
            
        Returns:
            Predicted cost in milliseconds
        """
        model = self._load_model(source)
        
        if model is None:
            # Fallback to heuristic
            return self._heuristic_predict(features, source)
        
        try:
            # Extract features in correct order
            X = self._extract_features(features)
            
            # Predict
            prediction = model.predict(X)[0]
            
            # Ensure positive
            return max(1.0, float(prediction))
            
        except Exception as e:
            logger.warning(f"Model prediction failed: {e}, using heuristic")
            return self._heuristic_predict(features, source)
    
    def predict_with_confidence(
        self,
        features: Dict[str, Any],
        source: str
    ) -> Tuple[float, float]:
        """
        Predict with confidence interval.
        
        Args:
            features: Query features
            source: Target source
            
        Returns:
            Tuple of (prediction, confidence)
        """
        prediction = self.predict(features, source)
        
        # Simple confidence based on feature complexity
        complexity = features.get("complexity_score", 0)
        if complexity < 5:
            confidence = 0.9
        elif complexity < 15:
            confidence = 0.8
        else:
            confidence = 0.7
        
        return prediction, confidence
    
    def _extract_features(self, features: Dict[str, Any]) -> np.ndarray:
        """
        Extract numerical features from feature dict.
        
        Args:
            features: Query features
            
        Returns:
            Feature array
        """
        return np.array([[
            features.get("num_tables", 0),
            features.get("num_joins", 0),
            features.get("num_aggregates", 0),
            1.0 if features.get("has_where") else 0.0,
            1.0 if features.get("has_group_by") else 0.0,
            1.0 if features.get("has_order_by") else 0.0,
            1.0 if features.get("has_limit") else 0.0,
            1.0 if features.get("has_subquery") else 0.0,
            features.get("query_length", 0),
            features.get("complexity_score", 0)
        ]])
    
    def _heuristic_predict(
        self,
        features: Dict[str, Any],
        source: str
    ) -> float:
        """
        Heuristic cost prediction when model unavailable.
        
        Args:
            features: Query features
            source: Target source
            
        Returns:
            Estimated cost in milliseconds
        """
        # Base costs by source
        base_costs = {
            "oracle": 500.0,
            "s3_iceberg": 200.0,
            "spark": 1000.0,
            "trino": 200.0
        }
        
        base = base_costs.get(source, 300.0)
        
        # Add complexity penalty
        complexity = features.get("complexity_score", 0)
        complexity_penalty = complexity * 20
        
        # Join penalty
        joins = features.get("num_joins", 0)
        join_penalty = joins * 100
        
        # Aggregate penalty
        aggregates = features.get("num_aggregates", 0)
        agg_penalty = aggregates * 50
        
        total = base + complexity_penalty + join_penalty + agg_penalty
        
        return max(10.0, total)
    
    def train(
        self,
        training_data: List[Dict[str, Any]],
        source: str
    ) -> ModelMetrics:
        """
        Train model for a source.
        
        Args:
            training_data: List of {features, actual_cost} dicts
            source: Source type
            
        Returns:
            Model metrics
        """
        try:
            from xgboost import XGBRegressor
            from sklearn.model_selection import train_test_split
            from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
        except ImportError:
            logger.error("XGBoost not installed, cannot train model")
            raise
        
        # Prepare data
        X = []
        y = []
        
        for sample in training_data:
            features = self._extract_features(sample["features"])
            X.append(features[0])
            y.append(sample["actual_cost"])
        
        X = np.array(X)
        y = np.array(y)
        
        # Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Train
        model = XGBRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42
        )
        model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = model.predict(X_test)
        
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)
        mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
        
        metrics = ModelMetrics(mae=mae, rmse=rmse, r2=r2, mape=mape)
        
        # Save model
        model_file = os.path.join(self.model_path, f"{source}_model.pkl")
        with open(model_file, "wb") as f:
            pickle.dump(model, f)
        
        # Save metrics
        metrics_file = os.path.join(self.model_path, f"{source}_metrics.json")
        with open(metrics_file, "w") as f:
            json.dump({
                "mae": mae,
                "rmse": rmse,
                "r2": r2,
                "mape": mape,
                "training_samples": len(training_data)
            }, f, indent=2)
        
        self._models[source] = model
        
        logger.info(
            f"Trained model for {source}: MAE={mae:.2f}ms, "
            f"RMSE={rmse:.2f}ms, R2={r2:.3f}"
        )
        
        return metrics
    
    def get_metrics(self, source: str) -> Optional[Dict[str, Any]]:
        """
        Get model metrics.
        
        Args:
            source: Source type
            
        Returns:
            Metrics dictionary or None
        """
        metrics_file = os.path.join(self.model_path, f"{source}_metrics.json")
        
        if not os.path.exists(metrics_file):
            return None
        
        try:
            with open(metrics_file, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load metrics: {e}")
            return None


# ---------------------------------------------------------------------------
# Neural net extension — appended by Task 2
# ---------------------------------------------------------------------------

from dataclasses import dataclass as _dc2
from typing import Optional as _Opt2


@_dc2
class ModelComparison:
    """A/B comparison between XGBoost and neural predictions."""
    features_snapshot: dict
    actual_runtime_ms: float
    xgboost_prediction_ms: float
    neural_prediction_ms: float
    xgboost_error_pct: float
    neural_error_pct: float
    winner: str


def _get_neural_model(model_path: str) -> "_Opt2[Any]":
    """Lazily load the NeuralCostModel singleton."""
    try:
        from metamind.ml.neural_cost_model import NeuralCostModel
        neural_path = os.path.join(model_path, "neural_cost_model.pt")
        m = NeuralCostModel()
        if os.path.exists(neural_path):
            m.load(neural_path)
        return m
    except Exception as exc:
        logger.error("_get_neural_model: failed to load neural model: %s", exc)
        return None


def predict_neural(
    self: "QueryCostModel",
    features: Dict[str, Any],
) -> "Optional[Any]":
    """
    Predict using NeuralCostModel; returns CostPrediction or None on failure.
    """
    neural = _get_neural_model(self.model_path)
    if neural is None or not neural.is_available:
        return None
    try:
        return neural.predict(features)
    except Exception as exc:
        logger.error("QueryCostModel.predict_neural failed: %s", exc)
        return None


async def compare_models(
    self: "QueryCostModel",
    features: Dict[str, Any],
    actual_runtime_ms: float,
    source: str = "trino",
) -> "ModelComparison":
    """A/B test XGBoost vs neural cost predictions against a known runtime."""
    xgb_pred = self.predict(features, source)
    neural_pred_obj = predict_neural(self, features)
    neural_ms = float(neural_pred_obj.estimated_cost) if neural_pred_obj else xgb_pred

    def _err(pred: float) -> float:
        if actual_runtime_ms <= 0:
            return 0.0
        return abs(pred - actual_runtime_ms) / actual_runtime_ms * 100.0

    xgb_err = _err(xgb_pred)
    nn_err = _err(neural_ms)
    winner = "neural" if nn_err < xgb_err else "xgboost"

    return ModelComparison(
        features_snapshot=features,
        actual_runtime_ms=actual_runtime_ms,
        xgboost_prediction_ms=xgb_pred,
        neural_prediction_ms=neural_ms,
        xgboost_error_pct=round(xgb_err, 2),
        neural_error_pct=round(nn_err, 2),
        winner=winner,
    )


def predict_batch_raw(self: "QueryCostModel", X: "Any") -> "Any":
    """Return raw log-space predictions array (used by FeedbackTrainer)."""
    import numpy as _np
    results = _np.zeros(len(X), dtype=_np.float32)
    for i, row in enumerate(X):
        feat = {
            "num_tables": float(row[0]),
            "num_joins": float(row[1]),
            "num_aggregates": float(row[3]),
            "has_where": 1.0 if row[4] > 0 else 0.0,
        }
        try:
            results[i] = float(self.predict(feat, "trino"))
        except Exception:
            results[i] = 0.0
    return results


def train_from_numpy(self: "QueryCostModel", X: "Any", y: "Any") -> "Any":
    """Train from numpy arrays (used by FeedbackTrainer)."""
    import numpy as _np
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    try:
        from xgboost import XGBRegressor
    except ImportError:
        logger.error("QueryCostModel.train: xgboost not installed")
        return None

    if len(X) < 10:
        logger.warning("QueryCostModel.train: insufficient samples %d", len(X))
        return None

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = XGBRegressor(n_estimators=100, max_depth=6, learning_rate=0.1, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = float(_np.sqrt(mean_squared_error(y_test, y_pred)))
    r2 = r2_score(y_test, y_pred)
    self._models["trino"] = model

    from metamind.ml.neural_cost_model import TrainMetrics
    return TrainMetrics(mae=mae, rmse=rmse, r2_score=r2, epochs_trained=100, training_time_ms=0)


# Monkey-patch new methods onto QueryCostModel
QueryCostModel.predict_neural = predict_neural         # type: ignore[attr-defined]
QueryCostModel.compare_models = compare_models         # type: ignore[attr-defined]
QueryCostModel.predict_batch_raw = predict_batch_raw   # type: ignore[attr-defined]
QueryCostModel.train = train_from_numpy                # type: ignore[attr-defined]
