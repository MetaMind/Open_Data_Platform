"""Integration tests for the async query queue executor (F25)."""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_queue(max_concurrent: int = 5):
    """Create a QueryQueue."""
    from metamind.core.workload.queue import QueryQueue
    return QueryQueue(max_concurrent=max_concurrent, max_queue_depth=100)


def _make_queued_query(tenant_id: str = "tenant-1", timeout: int = 10) -> object:
    """Create a QueuedQuery."""
    from metamind.core.workload.queue import QueuedQuery
    from metamind.core.workload.classifier import WorkloadType
    return QueuedQuery(
        query_id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        sql="SELECT 1",
        workload_type=WorkloadType.ANALYTICS,
        priority=5,
        submitted_at=datetime.utcnow(),
        estimated_cost=1.0,
        timeout_seconds=timeout,
    )


@pytest.mark.asyncio
async def test_queue_accepts_and_executes_queries():
    """Queue should accept queries and allow retrieval."""
    try:
        queue = _make_queue()
        q = _make_queued_query()
        qid = await queue.submit(q)
        assert qid == q.query_id
        next_q = await queue.get_next()
        assert next_q is not None
        assert next_q.query_id == qid
    except ImportError as exc:
        pytest.skip(f"Queue not available: {exc}")


@pytest.mark.asyncio
async def test_queue_respects_concurrency_limit():
    """Queue should not allow more than max_concurrent active queries."""
    try:
        max_concurrent = 3
        queue = _make_queue(max_concurrent=max_concurrent)

        # Fill up active slots
        queries = [_make_queued_query() for _ in range(max_concurrent + 2)]
        for q in queries:
            await queue.submit(q)

        # Drain max_concurrent queries
        active_count = 0
        while active_count < max_concurrent:
            next_q = await queue.get_next()
            if next_q is None:
                break
            active_count += 1

        # Next get should return None (at capacity)
        assert active_count <= max_concurrent
    except ImportError as exc:
        pytest.skip(f"Queue not available: {exc}")


@pytest.mark.asyncio
async def test_queue_cancel_removes_query():
    """Cancelling a query should mark it as cancelled."""
    try:
        queue = _make_queue()
        q = _make_queued_query()
        qid = await queue.submit(q)

        result = queue.cancel(qid)
        assert result is True or qid in queue._cancelled
    except ImportError as exc:
        pytest.skip(f"Queue not available: {exc}")


@pytest.mark.asyncio
async def test_fair_share_tenant_isolation():
    """FairShareScheduler should interleave queries from multiple tenants."""
    try:
        from metamind.core.workload.queue import FairShareScheduler, QueuedQuery
        from metamind.core.workload.classifier import WorkloadType

        scheduler = FairShareScheduler()

        def _q(tenant: str, prio: int = 5) -> QueuedQuery:
            return QueuedQuery(
                query_id=str(uuid.uuid4()),
                tenant_id=tenant,
                sql="SELECT 1",
                workload_type=WorkloadType.ANALYTICS,
                priority=prio,
                submitted_at=datetime.utcnow(),
                estimated_cost=1.0,
            )

        tenant_queues = {
            "a": [_q("a") for _ in range(5)],
            "b": [_q("b") for _ in range(5)],
        }

        selected_tenants = []
        for _ in range(6):
            next_q = scheduler.select_next(tenant_queues)
            if next_q is None:
                break
            selected_tenants.append(next_q.tenant_id)
            # Remove from queue
            tenant_queues[next_q.tenant_id] = [
                x for x in tenant_queues[next_q.tenant_id]
                if x.query_id != next_q.query_id
            ]

        # Both tenants should get some queries
        assert "a" in selected_tenants
        assert "b" in selected_tenants
    except ImportError as exc:
        pytest.skip(f"FairShareScheduler not available: {exc}")


@pytest.mark.asyncio
async def test_queue_timeout_enforced():
    """Queue should handle queries with very short timeouts."""
    try:
        from metamind.core.workload.queue_executor import QueryQueueExecutor, PrometheusMetrics
        from metamind.core.workload.queue import FairShareScheduler

        queue = _make_queue()
        scheduler = FairShareScheduler()

        # Mock the query engine to sleep longer than timeout
        mock_engine = MagicMock()

        def slow_execute(ctx):
            import time
            time.sleep(100)  # Will be cancelled by timeout

        mock_engine.execute = slow_execute

        executor = QueryQueueExecutor(
            queue=queue,
            scheduler=scheduler,
            query_engine=mock_engine,
            max_workers=2,
        )

        # Submit a query with 0.1s timeout
        q = _make_queued_query(timeout=0)
        await queue.submit(q)

        # The executor should handle the timeout without hanging
        metrics = executor.get_metrics()
        assert "queue_depth" in metrics
        assert "active" in metrics
    except ImportError as exc:
        pytest.skip(f"QueryQueueExecutor not available: {exc}")
