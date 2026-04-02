"""F01 — Learned Cardinality Estimation with XGBoost hybrid estimator."""
from __future__ import annotations

import hashlib
import logging
import os
import pickle
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from metamind.core.logical.nodes import Predicate
from metamind.core.metadata.models import ColumnMeta, TableMeta

logger = logging.getLogger(__name__)


@dataclass
class CardinalityFeatures:
    """Feature vector for cardinality estimation model."""

    table_row_count: float
    num_predicates: int
    eq_predicate_count: int
    range_predicate_count: int
    in_predicate_count: int
    null_predicate_count: int
    histogram_sel_product: float    # Product of histogram selectivities
    ndv_sel_product: float          # Product of NDV-based selectivities
    null_fraction_sum: float
    col_avg_width_sum: float

    def to_array(self) -> np.ndarray:
        """Convert to numpy feature array."""
        return np.array([
            np.log1p(self.table_row_count),
            self.num_predicates,
            self.eq_predicate_count,
            self.range_predicate_count,
            self.in_predicate_count,
            self.null_predicate_count,
            np.log1p(-np.log1p(max(1e-10, self.histogram_sel_product))),
            np.log1p(-np.log1p(max(1e-10, self.ndv_sel_product))),
            self.null_fraction_sum,
            self.col_avg_width_sum,
        ], dtype=np.float32)


class CardinalityFeatureExtractor:
    """Extracts feature vectors for cardinality ML models."""

    def extract(
        self,
        predicates: list[Predicate],
        table_meta: TableMeta,
    ) -> CardinalityFeatures:
        """Extract features from predicates and table metadata."""
        from metamind.core.costing.histograms import HistogramEstimator

        estimator = HistogramEstimator()
        col_map: dict[str, ColumnMeta] = {
            c.column_name.lower(): c for c in table_meta.columns
        }

        hist_sel = 1.0
        ndv_sel = 1.0
        null_frac_sum = 0.0
        avg_width_sum = 0.0
        eq_count = range_count = in_count = null_count = 0

        for pred in predicates:
            col_name = pred.column.split(".")[-1].lower()
            col_meta = col_map.get(col_name)

            sel = estimator.estimate_selectivity(pred, col_meta)
            hist_sel *= sel

            # NDV-based selectivity
            if col_meta and col_meta.ndv > 0:
                ndv_sel *= 1.0 / col_meta.ndv
            else:
                ndv_sel *= 0.01

            # Stats
            if col_meta:
                null_frac_sum += col_meta.null_fraction
                avg_width_sum += col_meta.avg_width

            # Predicate type counts
            op = pred.operator
            if op == "=":
                eq_count += 1
            elif op in ("<", ">", "<=", ">=", "BETWEEN"):
                range_count += 1
            elif op == "IN":
                in_count += 1
            elif op == "IS NULL":
                null_count += 1

        return CardinalityFeatures(
            table_row_count=table_meta.row_count,
            num_predicates=len(predicates),
            eq_predicate_count=eq_count,
            range_predicate_count=range_count,
            in_predicate_count=in_count,
            null_predicate_count=null_count,
            histogram_sel_product=hist_sel,
            ndv_sel_product=ndv_sel,
            null_fraction_sum=null_frac_sum,
            col_avg_width_sum=avg_width_sum,
        )


class LearnedCardinalityPredictor:
    """XGBoost-based cardinality predictor per tenant/table (F01).

    Uses a hybrid approach: if the model has low confidence, falls back
    to histogram estimation.
    """

    def __init__(
        self,
        tenant_id: str,
        table_name: str,
        model_path: str,
        confidence_threshold: float = 0.8,
    ) -> None:
        """Initialize predictor."""
        self._tenant_id = tenant_id
        self._table_name = table_name
        self._model_path = model_path
        self._confidence_threshold = confidence_threshold
        self._model: Optional[object] = None
        self._feature_extractor = CardinalityFeatureExtractor()

    def load(self) -> bool:
        """Load model from disk. Returns True if successful."""
        path = self._model_file_path()
        if not os.path.exists(path):
            return False
        try:
            with open(path, "rb") as f:
                self._model = pickle.load(f)
            logger.info("Loaded cardinality model: %s", path)
            return True
        except Exception as exc:
            logger.warning("Failed to load cardinality model %s: %s", path, exc)
            return False

    def predict(
        self,
        predicates: list[Predicate],
        table_meta: TableMeta,
    ) -> tuple[float, float]:
        """Predict cardinality. Returns (row_estimate, confidence).

        If model not loaded or confidence too low, returns histogram estimate.
        """
        features = self._feature_extractor.extract(predicates, table_meta)
        feature_vec = features.to_array()

        if self._model is not None:
            try:
                import xgboost as xgb  # type: ignore[import]

                dmatrix = xgb.DMatrix(feature_vec.reshape(1, -1))
                log_pred = self._model.predict(dmatrix)[0]  # type: ignore[union-attr]
                pred_rows = float(np.expm1(log_pred))
                confidence = self._compute_confidence(feature_vec)

                if confidence >= self._confidence_threshold:
                    logger.debug(
                        "Learned cardinality: %.0f rows (confidence=%.2f)",
                        pred_rows, confidence
                    )
                    return max(1.0, pred_rows), confidence

            except Exception as exc:
                logger.warning("Learned model prediction failed: %s", exc)

        # Fallback: histogram-based estimate
        hist_rows = table_meta.row_count * features.histogram_sel_product
        return max(1.0, hist_rows), 0.5

    def _compute_confidence(self, features: np.ndarray) -> float:
        """Estimate prediction confidence based on feature validity."""
        # Confidence is higher when histogram selectivities are not extreme
        hist_sel = features[6]  # log-transformed
        if abs(hist_sel) > 10:  # extreme selectivity
            return 0.3
        # Base confidence from training data coverage
        row_count_bucket = features[0]  # log1p(row_count)
        if row_count_bucket < 3:  # very small tables
            return 0.4
        return 0.85

    def _model_file_path(self) -> str:
        """Compute model file path for this tenant/table combination."""
        key = hashlib.md5(f"{self._tenant_id}:{self._table_name}".encode()).hexdigest()[:16]
        return os.path.join(self._model_path, f"card_{key}.pkl")


class HybridCardinalityEstimator:
    """Hybrid estimator combining learned models with histogram fallback (F01).

    Manages per-tenant, per-table learned models transparently.
    """

    def __init__(self, model_base_path: str, confidence_threshold: float = 0.8) -> None:
        """Initialize hybrid estimator."""
        self._model_path = model_base_path
        self._confidence_threshold = confidence_threshold
        self._predictors: dict[str, LearnedCardinalityPredictor] = {}

    def estimate(
        self,
        predicates: list[Predicate],
        table_meta: TableMeta,
    ) -> float:
        """Estimate output cardinality for a table scan with predicates."""
        if not predicates:
            return float(table_meta.row_count)

        predictor = self._get_predictor(table_meta.tenant_id, table_meta.table_name)
        rows, confidence = predictor.predict(predicates, table_meta)

        logger.debug(
            "Cardinality estimate: %s.%s → %.0f rows (confidence=%.2f)",
            table_meta.schema_name, table_meta.table_name, rows, confidence
        )
        return rows

    def _get_predictor(self, tenant_id: str, table_name: str) -> LearnedCardinalityPredictor:
        """Get or create predictor for a tenant/table combination."""
        key = f"{tenant_id}:{table_name}"
        if key not in self._predictors:
            predictor = LearnedCardinalityPredictor(
                tenant_id=tenant_id,
                table_name=table_name,
                model_path=self._model_path,
                confidence_threshold=self._confidence_threshold,
            )
            predictor.load()  # Load if available
            self._predictors[key] = predictor
        return self._predictors[key]
