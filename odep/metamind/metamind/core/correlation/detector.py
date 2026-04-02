"""F02 Column correlation detector."""
from __future__ import annotations
import logging, numpy as np
logger = logging.getLogger(__name__)
class CorrelationDetector:
    def detect(self, col_a: list[object], col_b: list[object]) -> float:
        try:
            a, b = np.array(col_a, dtype=float), np.array(col_b, dtype=float)
            if len(a) < 3: return 0.0
            corr = float(np.corrcoef(a, b)[0, 1])
            return 0.0 if np.isnan(corr) else corr
        except (ValueError, TypeError): return 0.0
