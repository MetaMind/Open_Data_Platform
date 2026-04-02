"""Cardinality deviation detector."""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
class DeviationDetector:
    def __init__(self, threshold: float = 2.0) -> None:
        self._threshold = threshold
    def is_significant(self, estimated: float, actual: float) -> bool:
        if actual == 0: return False
        ratio = max(estimated, actual) / max(1, min(estimated, actual))
        return ratio > self._threshold
