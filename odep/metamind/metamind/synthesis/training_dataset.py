"""
Training Dataset Builder — ML Cost Model Training Data Management

File: metamind/synthesis/training_dataset.py
Role: ML Engineer
Dependencies: sqlalchemy, numpy, metamind.synthesis.plan_feature_extractor

Manages the lifecycle of training samples used by the cost model.
Provides insert, retrieval, and numpy export APIs so that FeedbackTrainer
and NeuralCostModel can work with a consistent data contract.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from metamind.synthesis.plan_feature_extractor import PlanFeatures

logger = logging.getLogger(__name__)

# Ordered list of numeric feature columns — must match PlanFeatures.to_dict() keys
_FEATURE_COLUMNS: List[str] = [
    "num_tables",
    "num_joins",
    "join_depth",
    "num_aggregates",
    "num_filters",
    "has_subquery",
    "estimated_output_rows",
    "scan_selectivity",
    "cross_engine_flag",
    "partition_pruning_possible",
    "has_window_function",
    "num_sort_nodes",
    "has_limit",
    "avg_table_size_rows",
    "complexity_score",
]

# Engine name → integer label for classification targets
_ENGINE_LABELS: Dict[str, int] = {
    "oracle": 0,
    "trino": 1,
    "spark": 2,
    "gpu": 3,
    "s3": 1,      # alias for trino/iceberg
    "iceberg": 1,
}


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------

@dataclass
class TrainingSample:
    """One training observation: plan features + observed outcome."""
    # Plan features (mirrors PlanFeatures)
    num_tables: int = 0
    num_joins: int = 0
    join_depth: int = 0
    num_aggregates: int = 0
    num_filters: int = 0
    has_subquery: bool = False
    estimated_output_rows: float = 0.0
    scan_selectivity: float = 1.0
    cross_engine_flag: bool = False
    partition_pruning_possible: bool = False
    has_window_function: bool = False
    num_sort_nodes: int = 0
    has_limit: bool = False
    avg_table_size_rows: float = 0.0
    complexity_score: float = 0.0
    # Outcome
    runtime_ms: float = 0.0
    engine_label: str = "trino"
    tenant_id: str = ""

    def to_feature_vector(self) -> List[float]:
        """Return ordered numeric feature vector."""
        return [
            float(self.num_tables),
            float(self.num_joins),
            float(self.join_depth),
            float(self.num_aggregates),
            float(self.num_filters),
            float(self.has_subquery),
            float(self.estimated_output_rows),
            float(self.scan_selectivity),
            float(self.cross_engine_flag),
            float(self.partition_pruning_possible),
            float(self.has_window_function),
            float(self.num_sort_nodes),
            float(self.has_limit),
            float(self.avg_table_size_rows),
            float(self.complexity_score),
        ]


# ---------------------------------------------------------------------------
# TrainingDatasetBuilder
# ---------------------------------------------------------------------------

class TrainingDatasetBuilder:
    """
    Persists training samples to ``mm_learned_models`` (training_samples JSONB)
    and provides batch retrieval for ML training pipelines.

    Uses a dedicated JSONB column in mm_learned_models to avoid schema
    coupling with the model weights storage.  Each row represents one
    tenant's accumulated training batch.
    """

    _TABLE = "mm_training_samples"

    def __init__(self, db_engine: AsyncEngine) -> None:
        self._db = db_engine

    async def add_sample(
        self,
        features: PlanFeatures,
        runtime_ms: float,
        engine: str,
        tenant_id: str,
    ) -> None:
        """Persist one training sample.  Inserts into mm_training_samples."""
        payload = features.to_dict()
        payload["runtime_ms"] = runtime_ms
        payload["engine"] = engine
        try:
            async with self._db.begin() as conn:
                await conn.execute(
                    text(
                        """
                        INSERT INTO mm_training_samples
                            (tenant_id, features, runtime_ms, engine, recorded_at)
                        VALUES
                            (:tid, :feat::jsonb, :rt, :eng, NOW())
                        """
                    ),
                    {
                        "tid": tenant_id,
                        "feat": json.dumps(payload),
                        "rt": runtime_ms,
                        "eng": engine,
                    },
                )
        except Exception as exc:
            logger.error(
                "TrainingDatasetBuilder.add_sample failed tenant=%s engine=%s: %s",
                tenant_id,
                engine,
                exc,
            )

    async def get_training_batch(
        self,
        tenant_id: str,
        limit: int = 10_000,
    ) -> List[TrainingSample]:
        """Return the most recent *limit* samples for *tenant_id*."""
        samples: List[TrainingSample] = []
        try:
            async with self._db.connect() as conn:
                rows = (
                    await conn.execute(
                        text(
                            """
                            SELECT features, runtime_ms, engine
                            FROM mm_training_samples
                            WHERE tenant_id = :tid
                            ORDER BY recorded_at DESC
                            LIMIT :lim
                            """
                        ),
                        {"tid": tenant_id, "lim": limit},
                    )
                ).fetchall()

            for row in rows:
                feat = row.features if isinstance(row.features, dict) else json.loads(row.features)
                sample = TrainingSample(
                    num_tables=int(feat.get("num_tables", 0)),
                    num_joins=int(feat.get("num_joins", 0)),
                    join_depth=int(feat.get("join_depth", 0)),
                    num_aggregates=int(feat.get("num_aggregates", 0)),
                    num_filters=int(feat.get("num_filters", 0)),
                    has_subquery=bool(feat.get("has_subquery", 0)),
                    estimated_output_rows=float(feat.get("estimated_output_rows", 0)),
                    scan_selectivity=float(feat.get("scan_selectivity", 1.0)),
                    cross_engine_flag=bool(feat.get("cross_engine_flag", 0)),
                    partition_pruning_possible=bool(feat.get("partition_pruning_possible", 0)),
                    has_window_function=bool(feat.get("has_window_function", 0)),
                    num_sort_nodes=int(feat.get("num_sort_nodes", 0)),
                    has_limit=bool(feat.get("has_limit", 0)),
                    avg_table_size_rows=float(feat.get("avg_table_size_rows", 0)),
                    complexity_score=float(feat.get("complexity_score", 0)),
                    runtime_ms=float(row.runtime_ms),
                    engine_label=str(row.engine),
                    tenant_id=tenant_id,
                )
                samples.append(sample)
        except Exception as exc:
            logger.error(
                "TrainingDatasetBuilder.get_training_batch failed tenant=%s: %s",
                tenant_id,
                exc,
            )
        return samples

    async def export_to_numpy(
        self,
        tenant_id: str,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Return (X, y) matrices for scikit-learn / PyTorch training.

        X shape: (n_samples, n_features)
        y shape: (n_samples,)  — log-transformed runtime_ms
        """
        samples = await self.get_training_batch(tenant_id)
        if not samples:
            return np.empty((0, len(_FEATURE_COLUMNS))), np.empty((0,))

        X = np.array([s.to_feature_vector() for s in samples], dtype=np.float32)
        y = np.log1p(np.array([s.runtime_ms for s in samples], dtype=np.float32))
        return X, y

    async def count_new_samples(
        self,
        tenant_id: str,
        since_model_id: Optional[str],
    ) -> int:
        """Count samples recorded after the last model training run."""
        try:
            async with self._db.connect() as conn:
                if since_model_id is None:
                    result = await conn.execute(
                        text(
                            "SELECT COUNT(*) FROM mm_training_samples WHERE tenant_id = :tid"
                        ),
                        {"tid": tenant_id},
                    )
                else:
                    result = await conn.execute(
                        text(
                            """
                            SELECT COUNT(*) FROM mm_training_samples ts
                            WHERE ts.tenant_id = :tid
                              AND ts.recorded_at > (
                                  SELECT created_at FROM mm_learned_models
                                  WHERE model_id = :mid AND tenant_id = :tid
                                  LIMIT 1
                              )
                            """
                        ),
                        {"tid": tenant_id, "mid": since_model_id},
                    )
                row = result.fetchone()
                return int(row[0]) if row else 0
        except Exception as exc:
            logger.error(
                "TrainingDatasetBuilder.count_new_samples failed tenant=%s: %s",
                tenant_id,
                exc,
            )
            return 0
