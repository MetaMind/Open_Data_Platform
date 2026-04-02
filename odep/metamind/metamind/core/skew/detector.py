"""F03 — Skew Detector: identifies data skew in join key columns."""
from __future__ import annotations

import logging
import math
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from metamind.core.metadata.models import ColumnMeta, TableMeta
from metamind.core.skew.compensator import SkewInfo

logger = logging.getLogger(__name__)

SKEW_THRESHOLD = 0.05   # Key is skewed if any value > 5% of total rows


class SkewDetector:
    """Detects data skew in join key columns using MCV statistics (F03)."""

    def detect(self, col_meta: ColumnMeta, table_meta: TableMeta) -> SkewInfo:
        """Detect skew for a single column."""
        top_values: list[dict[str, object]] = []
        max_freq = 0.0

        if col_meta.most_common_vals and col_meta.most_common_freqs:
            for val, freq in zip(col_meta.most_common_vals, col_meta.most_common_freqs):
                row_count = int(table_meta.row_count * float(freq))
                top_values.append({"value": val, "frequency": float(freq), "count": row_count})
                max_freq = max(max_freq, float(freq))

        is_skewed = max_freq >= SKEW_THRESHOLD
        ideal_freq = 1.0 / max(1, col_meta.ndv) if col_meta.ndv > 0 else 0.01
        skew_factor = max_freq / ideal_freq if (is_skewed and ideal_freq > 0) else 1.0

        compensation = self._recommend_compensation(
            is_skewed, max_freq, table_meta.row_count, skew_factor
        )

        if is_skewed:
            logger.info(
                "Skew detected: %s.%s skew_ratio=%.3f compensation=%s",
                table_meta.table_name, col_meta.column_name, max_freq, compensation
            )

        return SkewInfo(
            table_name=table_meta.table_name,
            column_name=col_meta.column_name,
            skew_ratio=max_freq,
            is_skewed=is_skewed,
            top_values=top_values[:10],
            compensation=compensation,
            estimated_skew_factor=skew_factor,
        )

    def detect_all(self, table_meta: TableMeta) -> list[SkewInfo]:
        """Detect skew across all columns in a table."""
        return [
            self.detect(col, table_meta)
            for col in table_meta.columns
            if col.most_common_freqs
        ]

    def detect_join_key(
        self,
        join_column: str,
        table_meta: TableMeta,
    ) -> Optional[SkewInfo]:
        """Detect skew specifically for a join key column."""
        col = table_meta.get_column(join_column)
        if col is None:
            return None
        return self.detect(col, table_meta)

    def persist(
        self, engine: Engine, tenant_id: str, skew_info: SkewInfo
    ) -> None:
        """Persist skew tracking record to database."""
        import json
        stmt = sa.text(
            """INSERT INTO mm_skew_tracking
               (tenant_id, table_name, column_name, skew_ratio,
                top_k_values, is_skewed, compensation, analyzed_at)
               VALUES (:tid, :tbl, :col, :ratio, :topk::jsonb,
                       :skewed, :comp, NOW())
               ON CONFLICT (tenant_id, table_name, column_name) DO UPDATE SET
               skew_ratio=EXCLUDED.skew_ratio, top_k_values=EXCLUDED.top_k_values,
               is_skewed=EXCLUDED.is_skewed, compensation=EXCLUDED.compensation,
               analyzed_at=NOW()"""
        )
        with engine.begin() as conn:
            conn.execute(stmt, {
                "tid": tenant_id,
                "tbl": skew_info.table_name,
                "col": skew_info.column_name,
                "ratio": skew_info.skew_ratio,
                "topk": json.dumps(skew_info.top_values),
                "skewed": skew_info.is_skewed,
                "comp": skew_info.compensation,
            })

    def _recommend_compensation(
        self, is_skewed: bool, max_freq: float, row_count: int, skew_factor: float
    ) -> str:
        """Recommend the best skew compensation strategy."""
        if not is_skewed:
            return "none"
        estimated_bytes = row_count * 100  # ~100 bytes/row
        if estimated_bytes <= 100 * 1024 * 1024:  # 100MB threshold
            return "broadcast"
        if skew_factor >= 10:
            return "salt"
        return "none"
