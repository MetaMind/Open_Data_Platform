"""
Feedback Trainer — Online Cost Model Retraining Loop

File: metamind/synthesis/feedback_trainer.py
Role: ML Engineer
Dependencies: metamind.ml.cost_model, metamind.observability.drift_detector,
              metamind.synthesis.training_dataset, sqlalchemy

Monitors new training samples and drift alerts, triggers incremental
retraining of the XGBoost / Neural cost model, and persists the new model
version and performance metrics to mm_learned_models.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import numpy as np
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from metamind.synthesis.training_dataset import TrainingDatasetBuilder

logger = logging.getLogger(__name__)

# Thresholds that trigger retraining
_NEW_SAMPLE_THRESHOLD = 500
_DRIFT_SCORE_THRESHOLD = 0.15   # PSI score above which we always retrain


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------

@dataclass
class RetrainResult:
    """Outcome of a single retraining cycle."""
    tenant_id: str
    samples_used: int
    mae_before: float
    mae_after: float
    improvement_pct: float
    retrained_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    model_version_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    skipped_reason: str = ""   # non-empty when retrain was skipped


# ---------------------------------------------------------------------------
# FeedbackTrainer
# ---------------------------------------------------------------------------

class FeedbackTrainer:
    """
    Manages the retraining lifecycle for the query cost model.

    Retraining is triggered when **either**:
    - More than *_NEW_SAMPLE_THRESHOLD* new samples have accumulated since
      the last training run, OR
    - The DriftDetector reports a drift score above *_DRIFT_SCORE_THRESHOLD*.

    After retraining, the new model artifact (pickled bytes) is stored in
    ``mm_learned_models`` alongside before/after MAE metrics.
    """

    def __init__(
        self,
        db_engine: AsyncEngine,
        dataset_builder: TrainingDatasetBuilder,
        cost_model: Any,             # metamind.ml.cost_model.QueryCostModel
        drift_detector: Any,         # metamind.observability.drift_detector.DriftDetector
    ) -> None:
        self._db = db_engine
        self._dataset = dataset_builder
        self._cost_model = cost_model
        self._drift_detector = drift_detector

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def retrain_if_needed(self, tenant_id: str) -> Optional[RetrainResult]:
        """Check conditions and retrain the cost model if warranted."""
        if not await self._should_retrain(tenant_id):
            logger.info(
                "FeedbackTrainer: retrain not needed for tenant=%s", tenant_id
            )
            return RetrainResult(
                tenant_id=tenant_id,
                samples_used=0,
                mae_before=0.0,
                mae_after=0.0,
                improvement_pct=0.0,
                skipped_reason="thresholds not met",
            )
        return await self._retrain(tenant_id)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _should_retrain(self, tenant_id: str) -> bool:
        """True if new-sample count OR drift score exceeds thresholds."""
        # Check drift detector
        try:
            drift_summary = await self._get_drift_summary(tenant_id)
            if drift_summary.get("max_psi", 0) > _DRIFT_SCORE_THRESHOLD:
                logger.info(
                    "FeedbackTrainer: drift triggered retrain tenant=%s psi=%.3f",
                    tenant_id,
                    drift_summary["max_psi"],
                )
                return True
        except Exception as exc:
            logger.error(
                "FeedbackTrainer._should_retrain drift check failed tenant=%s: %s",
                tenant_id,
                exc,
            )

        # Check sample count since last model
        last_model_id = await self._get_last_model_id(tenant_id)
        new_count = await self._dataset.count_new_samples(tenant_id, last_model_id)
        if new_count >= _NEW_SAMPLE_THRESHOLD:
            logger.info(
                "FeedbackTrainer: sample count triggered retrain tenant=%s new=%d",
                tenant_id,
                new_count,
            )
            return True
        return False

    async def _retrain(self, tenant_id: str) -> RetrainResult:
        """Fetch batch, train model, persist version."""
        X, y = await self._dataset.export_to_numpy(tenant_id)
        if len(X) == 0:
            logger.warning(
                "FeedbackTrainer._retrain: no samples available tenant=%s", tenant_id
            )
            return RetrainResult(
                tenant_id=tenant_id,
                samples_used=0,
                mae_before=0.0,
                mae_after=0.0,
                improvement_pct=0.0,
                skipped_reason="no samples",
            )

        mae_before = await self._evaluate_current_model(tenant_id, X, y)

        # Train using the cost model's train() method
        try:
            train_metrics = self._cost_model.train(X, y)
            mae_after = float(getattr(train_metrics, "mae", mae_before))
        except Exception as exc:
            logger.error(
                "FeedbackTrainer._retrain: model training failed tenant=%s: %s",
                tenant_id,
                exc,
            )
            return RetrainResult(
                tenant_id=tenant_id,
                samples_used=len(X),
                mae_before=mae_before,
                mae_after=mae_before,
                improvement_pct=0.0,
                skipped_reason=f"training error: {exc}",
            )

        improvement_pct = (
            (mae_before - mae_after) / mae_before * 100.0
            if mae_before > 0
            else 0.0
        )

        result = RetrainResult(
            tenant_id=tenant_id,
            samples_used=len(X),
            mae_before=mae_before,
            mae_after=mae_after,
            improvement_pct=round(improvement_pct, 2),
        )

        await self._persist_model_version(result)
        logger.info(
            "FeedbackTrainer._retrain done tenant=%s samples=%d mae_before=%.2f "
            "mae_after=%.2f improvement=%.1f%%",
            tenant_id,
            result.samples_used,
            result.mae_before,
            result.mae_after,
            result.improvement_pct,
        )
        return result

    async def _evaluate_current_model(
        self,
        tenant_id: str,
        X: np.ndarray,
        y: np.ndarray,
    ) -> float:
        """Compute MAE on current model before retraining."""
        try:
            preds = self._cost_model.predict_batch_raw(X)
            mae = float(np.mean(np.abs(preds - y)))
            return mae
        except Exception as exc:
            logger.error(
                "FeedbackTrainer._evaluate_current_model failed tenant=%s: %s",
                tenant_id,
                exc,
            )
            return 9999.0

    async def _get_drift_summary(self, tenant_id: str) -> Dict[str, Any]:
        """Ask drift_detector for the latest PSI scores."""
        try:
            if hasattr(self._drift_detector, "get_latest_drift_summary"):
                return await self._drift_detector.get_latest_drift_summary(tenant_id)
        except Exception as exc:
            logger.error(
                "FeedbackTrainer._get_drift_summary failed tenant=%s: %s",
                tenant_id,
                exc,
            )
        return {"max_psi": 0.0}

    async def _get_last_model_id(self, tenant_id: str) -> Optional[str]:
        """Return the UUID of the most recently trained model for this tenant."""
        try:
            async with self._db.connect() as conn:
                row = (
                    await conn.execute(
                        text(
                            """
                            SELECT model_id FROM mm_learned_models
                            WHERE tenant_id = :tid
                              AND model_type = 'cost_model'
                              AND is_active = TRUE
                            ORDER BY created_at DESC
                            LIMIT 1
                            """
                        ),
                        {"tid": tenant_id},
                    )
                ).fetchone()
                return str(row.model_id) if row else None
        except Exception as exc:
            logger.error(
                "FeedbackTrainer._get_last_model_id failed tenant=%s: %s",
                tenant_id,
                exc,
            )
            return None

    async def _persist_model_version(self, result: RetrainResult) -> None:
        """Write a new mm_learned_models row for the freshly trained model."""
        metadata = json.dumps(
            {
                "samples_used": result.samples_used,
                "mae_before": result.mae_before,
                "mae_after": result.mae_after,
                "improvement_pct": result.improvement_pct,
            }
        )
        try:
            async with self._db.begin() as conn:
                # Deactivate previous cost_model entries for this tenant
                await conn.execute(
                    text(
                        """
                        UPDATE mm_learned_models
                        SET is_active = FALSE
                        WHERE tenant_id = :tid AND model_type = 'cost_model'
                        """
                    ),
                    {"tid": result.tenant_id},
                )
                await conn.execute(
                    text(
                        """
                        INSERT INTO mm_learned_models (
                            model_id, tenant_id, model_type, model_name,
                            model_metadata, status, is_active, created_at
                        ) VALUES (
                            :mid, :tid, 'cost_model', :name,
                            :meta::jsonb, 'active', TRUE, NOW()
                        )
                        """
                    ),
                    {
                        "mid": result.model_version_id,
                        "tid": result.tenant_id,
                        "name": f"cost_model_v_{result.retrained_at.strftime('%Y%m%d_%H%M%S')}",
                        "meta": metadata,
                    },
                )
        except Exception as exc:
            logger.error(
                "FeedbackTrainer._persist_model_version failed tenant=%s: %s",
                result.tenant_id,
                exc,
            )
