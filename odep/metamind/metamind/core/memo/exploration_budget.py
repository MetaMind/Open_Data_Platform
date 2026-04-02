"""Exploration budget limiter for Cascades."""
from __future__ import annotations
class ExplorationBudget:
    def __init__(self, max_steps: int = 10000) -> None:
        self._max = max_steps; self._used = 0
    def consume(self, steps: int = 1) -> bool:
        self._used += steps
        return self._used < self._max
    @property
    def exhausted(self) -> bool:
        return self._used >= self._max
