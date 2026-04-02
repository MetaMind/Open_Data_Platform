"""MetaMind histogram-based selectivity estimation."""
from __future__ import annotations

import bisect
import logging
import math
from typing import Optional

from metamind.core.logical.nodes import Predicate
from metamind.core.metadata.models import ColumnMeta

logger = logging.getLogger(__name__)

# Selectivity defaults (Postgres-compatible)
DEFAULT_EQ_SEL = 0.005
DEFAULT_RANGE_SEL = 0.3333
DEFAULT_IN_SEL_PER_VALUE = 0.005
DEFAULT_LIKE_SEL = 0.1
DEFAULT_NULL_SEL = 0.01


class HistogramEstimator:
    """Estimates predicate selectivity using column histograms and MCV lists.

    Uses a combination of:
    - Most Common Values (MCV) for equality predicates
    - Histogram bounds for range predicates
    - NDV for IN predicates
    - Defaults for unsupported operators
    """

    def estimate_selectivity(
        self,
        predicate: Predicate,
        col_meta: Optional[ColumnMeta],
    ) -> float:
        """Estimate selectivity [0,1] for a single predicate.

        Returns a value in (0, 1].
        """
        if col_meta is None:
            return self._default_sel(predicate.operator)

        op = predicate.operator
        if op == "IS NULL":
            return col_meta.null_fraction or DEFAULT_NULL_SEL

        if op in ("=", "!="):
            sel = self._eq_selectivity(predicate.value, col_meta)
            return sel if op == "=" else max(0.0001, 1.0 - sel - col_meta.null_fraction)

        if op in ("<", "<=", ">", ">="):
            return self._range_selectivity(predicate, col_meta)

        if op == "IN":
            return self._in_selectivity(predicate.value, col_meta)

        if op == "LIKE":
            return DEFAULT_LIKE_SEL

        if op == "BETWEEN":
            # Approx as two range predicates
            return DEFAULT_RANGE_SEL * 0.5

        return DEFAULT_EQ_SEL

    def estimate_cardinality(
        self,
        table_rows: int,
        predicates: list[Predicate],
        col_metas: dict[str, ColumnMeta],
    ) -> float:
        """Estimate output cardinality after applying multiple predicates.

        Uses independence assumption (product of selectivities).
        """
        if table_rows <= 0:
            return 1.0

        combined_sel = 1.0
        for pred in predicates:
            col_meta = col_metas.get(pred.column.split(".")[-1])
            sel = self.estimate_selectivity(pred, col_meta)
            combined_sel *= sel

        # Clamp to at least 1 row
        return max(1.0, combined_sel * table_rows)

    # ── Private Helpers ───────────────────────────────────────

    def _eq_selectivity(
        self, value: object, col: ColumnMeta
    ) -> float:
        """Estimate equality predicate selectivity using MCV or NDV."""
        val_str = str(value)

        # Check MCV list first
        if col.most_common_vals and col.most_common_freqs:
            for mcv, mcf in zip(col.most_common_vals, col.most_common_freqs):
                if str(mcv) == val_str:
                    return float(mcf)

        # Fall back to uniform distribution over NDV
        if col.ndv > 0:
            # Fraction not in MCV
            mcv_total = sum(col.most_common_freqs) if col.most_common_freqs else 0.0
            non_mcv_frac = max(0.0, 1.0 - mcv_total - col.null_fraction)
            non_mcv_ndv = max(1, col.ndv - len(col.most_common_vals))
            return non_mcv_frac / non_mcv_ndv

        return DEFAULT_EQ_SEL

    def _range_selectivity(
        self, predicate: Predicate, col: ColumnMeta
    ) -> float:
        """Estimate range selectivity using histogram bounds."""
        bounds = col.histogram_bounds
        if not bounds:
            return DEFAULT_RANGE_SEL

        try:
            value = float(str(predicate.value))
        except (ValueError, TypeError):
            return DEFAULT_RANGE_SEL

        try:
            float_bounds = [float(b) for b in bounds]
        except (ValueError, TypeError):
            return DEFAULT_RANGE_SEL

        n_buckets = len(float_bounds) - 1
        if n_buckets <= 0:
            return DEFAULT_RANGE_SEL

        op = predicate.operator
        if op in ("<", "<="):
            # Fraction of bounds less than value
            pos = bisect.bisect_right(float_bounds, value)
            fraction = pos / len(float_bounds)
        else:  # >, >=
            pos = bisect.bisect_left(float_bounds, value)
            fraction = (len(float_bounds) - pos) / len(float_bounds)

        # MCV correction: subtract fraction already in MCVs
        mcv_total = sum(col.most_common_freqs) if col.most_common_freqs else 0.0
        non_mcv_frac = max(0.0, 1.0 - mcv_total - col.null_fraction)
        return max(0.0001, min(1.0, fraction * non_mcv_frac))

    def _in_selectivity(
        self, values: object, col: ColumnMeta
    ) -> float:
        """Estimate IN predicate selectivity."""
        if not isinstance(values, list):
            return DEFAULT_EQ_SEL

        total = 0.0
        for val in values:
            total += self._eq_selectivity(val, col)
        return min(1.0, total)

    def _default_sel(self, operator: str) -> float:
        """Default selectivity for operators with no statistics."""
        defaults = {
            "=": DEFAULT_EQ_SEL,
            "!=": 1.0 - DEFAULT_EQ_SEL,
            "<": DEFAULT_RANGE_SEL,
            "<=": DEFAULT_RANGE_SEL,
            ">": DEFAULT_RANGE_SEL,
            ">=": DEFAULT_RANGE_SEL,
            "IN": DEFAULT_IN_SEL_PER_VALUE * 5,
            "LIKE": DEFAULT_LIKE_SEL,
            "IS NULL": DEFAULT_NULL_SEL,
        }
        return defaults.get(operator, DEFAULT_EQ_SEL)
