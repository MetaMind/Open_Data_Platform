"""F25 — Query Queuing and Fair-Share Scheduling."""
from __future__ import annotations

import asyncio
import heapq
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from metamind.core.workload.classifier import WorkloadType

logger = logging.getLogger(__name__)


@dataclass
class QueuedQuery:
    """A query waiting for execution in the queue."""

    query_id: str
    tenant_id: str
    sql: str
    workload_type: WorkloadType
    priority: int                  # 0=highest, 10=lowest
    submitted_at: datetime
    estimated_cost: float
    timeout_seconds: int = 300
    result_callback: Optional[object] = field(default=None, repr=False)

    def __lt__(self, other: "QueuedQuery") -> bool:
        """Compare by priority then submission time for heap ordering."""
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.submitted_at < other.submitted_at


class QueryQueue:
    """Priority-based query queue with admission control (F25).

    Uses a min-heap ordered by (priority, submitted_at).
    Supports tenant-level isolation and query cancellation.
    """

    def __init__(self, max_concurrent: int = 50, max_queue_depth: int = 500) -> None:
        """Initialize queue with concurrency limits."""
        self._max_concurrent = max_concurrent
        self._max_queue = max_queue_depth
        self._heap: list[QueuedQuery] = []
        self._active: dict[str, QueuedQuery] = {}
        self._cancelled: set[str] = set()
        self._by_id: dict[str, QueuedQuery] = {}
        self._lock = asyncio.Lock()

    async def submit(self, query: QueuedQuery) -> str:
        """Submit query to the queue. Returns query_id.

        Raises:
            RuntimeError: If queue is full.
        """
        async with self._lock:
            if len(self._heap) >= self._max_queue:
                raise RuntimeError(
                    f"Queue full ({self._max_queue} entries). Try again later."
                )
            heapq.heappush(self._heap, query)
            self._by_id[query.query_id] = query
            logger.debug(
                "Queued query %s (tenant=%s, priority=%d)",
                query.query_id, query.tenant_id, query.priority
            )
            return query.query_id

    def submit_sync(self, query: QueuedQuery) -> str:
        """Synchronous version of submit for non-async contexts."""
        if len(self._heap) >= self._max_queue:
            raise RuntimeError("Queue full")
        heapq.heappush(self._heap, query)
        self._by_id[query.query_id] = query
        return query.query_id

    async def get_next(self) -> Optional[QueuedQuery]:
        """Get next query to execute (respects concurrency limit)."""
        async with self._lock:
            if len(self._active) >= self._max_concurrent:
                return None

            while self._heap:
                query = heapq.heappop(self._heap)
                if query.query_id in self._cancelled:
                    self._cancelled.discard(query.query_id)
                    continue

                # Check timeout
                elapsed = (datetime.utcnow() - query.submitted_at).total_seconds()
                if elapsed > query.timeout_seconds:
                    logger.warning("Query %s timed out in queue", query.query_id)
                    continue

                self._active[query.query_id] = query
                logger.debug("Dequeued query %s for execution", query.query_id)
                return query

            return None

    def complete(self, query_id: str) -> None:
        """Mark a query as completed, freeing its concurrency slot."""
        self._active.pop(query_id, None)
        self._by_id.pop(query_id, None)

    def cancel(self, query_id: str) -> bool:
        """Cancel a queued (not yet executing) query."""
        if query_id in self._active:
            logger.warning("Cannot cancel active query %s", query_id)
            return False
        if query_id in self._by_id:
            self._cancelled.add(query_id)
            del self._by_id[query_id]
            logger.info("Cancelled query %s", query_id)
            return True
        return False

    def get_position(self, query_id: str) -> int:
        """Get queue position for a query_id (1-indexed, 0 if active)."""
        if query_id in self._active:
            return 0
        for i, q in enumerate(sorted(self._heap)):
            if q.query_id == query_id:
                return i + 1
        return -1

    @property
    def queue_depth(self) -> int:
        """Current number of waiting queries."""
        return len(self._heap) - len(self._cancelled)

    @property
    def active_count(self) -> int:
        """Number of currently executing queries."""
        return len(self._active)

    def stats(self) -> dict[str, object]:
        """Return queue statistics."""
        tenant_counts: dict[str, int] = {}
        for q in self._heap:
            tenant_counts[q.tenant_id] = tenant_counts.get(q.tenant_id, 0) + 1
        return {
            "queue_depth": self.queue_depth,
            "active_count": self.active_count,
            "max_concurrent": self._max_concurrent,
            "tenant_distribution": tenant_counts,
        }


class FairShareScheduler:
    """Weighted fair-share scheduler for multi-tenant query execution (F25).

    Uses virtual time (deficit round-robin) to ensure fair resource sharing
    while respecting tenant weight priorities.
    """

    def __init__(self, tenant_weights: Optional[dict[str, float]] = None) -> None:
        """Initialize with optional tenant weight configuration."""
        self._weights: dict[str, float] = tenant_weights or {}
        self._virtual_time: dict[str, float] = {}
        self._default_weight = 1.0

    def select_next(
        self, tenant_queues: dict[str, list[QueuedQuery]]
    ) -> Optional[QueuedQuery]:
        """Select the next query to execute using weighted fair-share.

        Selects the tenant with the lowest normalized virtual time
        (smallest virtual_time / weight ratio), ensuring fair allocation.
        """
        eligible: list[tuple[float, str, QueuedQuery]] = []

        for tenant_id, queue in tenant_queues.items():
            if not queue:
                continue
            vt = self._virtual_time.get(tenant_id, 0.0)
            weight = self._weights.get(tenant_id, self._default_weight)
            normalized_vt = vt / max(0.001, weight)
            # Add priority boost for high-priority queries
            next_query = min(queue, key=lambda q: q.priority)
            priority_boost = next_query.priority * 0.01
            eligible.append((normalized_vt + priority_boost, tenant_id, next_query))

        if not eligible:
            return None

        _, selected_tenant, query = min(eligible, key=lambda x: x[0])
        logger.debug(
            "Fair-share selected tenant %s (vt=%.2f)",
            selected_tenant, self._virtual_time.get(selected_tenant, 0.0)
        )
        return query

    def update_virtual_time(self, tenant_id: str, query_duration_ms: float) -> None:
        """Update virtual time after query completion.

        Virtual time increases proportional to actual duration and inversely
        proportional to tenant weight, implementing fair-share accounting.
        """
        weight = self._weights.get(tenant_id, self._default_weight)
        increment = query_duration_ms / max(0.001, weight)
        self._virtual_time[tenant_id] = self._virtual_time.get(tenant_id, 0.0) + increment

    def set_weight(self, tenant_id: str, weight: float) -> None:
        """Update tenant scheduling weight (higher = more resources)."""
        if weight <= 0:
            raise ValueError(f"Weight must be positive, got {weight}")
        self._weights[tenant_id] = weight
        logger.info("Set scheduling weight for tenant %s: %.2f", tenant_id, weight)

    def reset_virtual_time(self) -> None:
        """Reset all virtual times (e.g., at scheduling epoch boundary)."""
        # Normalize relative to minimum to prevent starvation
        if self._virtual_time:
            min_vt = min(self._virtual_time.values())
            self._virtual_time = {
                tid: vt - min_vt for tid, vt in self._virtual_time.items()
            }
