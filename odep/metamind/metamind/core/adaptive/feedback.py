"""Adaptive feedback collection."""
from __future__ import annotations
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
logger = logging.getLogger(__name__)
@dataclass
class ExecutionFeedback:
    query_id: str; tenant_id: str; estimated_rows: int; actual_rows: int
    estimated_cost: float; actual_duration_ms: float; backend: str
    executed_at: datetime; error: Optional[str] = None
class FeedbackCollector:
    def __init__(self, engine: object) -> None:
        self._engine = engine; self._buffer: list[ExecutionFeedback] = []
    def record(self, feedback: ExecutionFeedback) -> None:
        self._buffer.append(feedback)
        if len(self._buffer) >= 100: self.flush()
    def flush(self) -> None:
        logger.debug("Flushing %d feedback records", len(self._buffer))
        self._buffer.clear()
