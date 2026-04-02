"""Cache admission policy."""
from __future__ import annotations
class AdmissionPolicy:
    def __init__(self, min_duration_ms: float = 10.0) -> None:
        self._min = min_duration_ms
    def should_cache(self, duration_ms: float, row_count: int) -> bool:
        return duration_ms >= self._min and row_count > 0
