"""F25 — Production-hardened query queue async execution loop."""
from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Optional

from metamind.core.workload.queue import QueuedQuery, QueryQueue

if TYPE_CHECKING:
    from metamind.core.query_engine import QueryEngine
    from metamind.core.workload.queue import FairShareScheduler

logger = logging.getLogger(__name__)

# Number of worker threads for blocking query execution
_DEFAULT_THREAD_WORKERS = 8
_POLL_INTERVAL_SECONDS = 0.05  # 50 ms


class PrometheusMetrics:
    """Thin wrapper around Prometheus counters/histograms for queue metrics.

    Deferred import so the module compiles even without prometheus_client.
    """

    def __init__(self) -> None:
        """Initialize metrics lazily."""
        self._ready = False
        self._queue_depth: Optional[object] = None
        self._active_queries: Optional[object] = None
        self._wait_time: Optional[object] = None
        self._timeout_counter: Optional[object] = None
        self._error_counter: Optional[object] = None
        self._duration_histogram: Optional[object] = None
        self._try_init()

    def _try_init(self) -> None:
        """Try to initialize prometheus metrics."""
        try:
            from prometheus_client import Counter, Gauge, Histogram
            self._queue_depth = Gauge(
                "metamind_queue_depth", "Current queue depth"
            )
            self._active_queries = Gauge(
                "metamind_active_queries_queue",
                "Active queries in queue executor",
                ["tenant"],
            )
            self._wait_time = Histogram(
                "metamind_queue_wait_ms",
                "Queue wait time in milliseconds",
                ["tenant"],
                buckets=[10, 50, 100, 500, 1000, 5000, 30000],
            )
            self._timeout_counter = Counter(
                "metamind_queue_timeouts_total",
                "Query execution timeouts",
                ["tenant"],
            )
            self._error_counter = Counter(
                "metamind_queue_errors_total",
                "Query execution errors",
                ["tenant"],
            )
            self._duration_histogram = Histogram(
                "metamind_queue_duration_ms",
                "Query execution duration in ms",
                ["tenant", "workload"],
                buckets=[1, 5, 20, 100, 500, 1000, 5000, 30000],
            )
            self._ready = True
        except ImportError:
            logger.debug("prometheus_client not available; metrics disabled")

    def record_queue_depth(self, depth: int) -> None:
        """Record current queue depth."""
        if self._ready and self._queue_depth is not None:
            self._queue_depth.set(depth)  # type: ignore[attr-defined]

    def record_active(self, tenant_id: str, delta: int) -> None:
        """Increment/decrement active query gauge."""
        if self._ready and self._active_queries is not None:
            if delta > 0:
                self._active_queries.labels(tenant=tenant_id).inc()  # type: ignore[attr-defined]
            else:
                self._active_queries.labels(tenant=tenant_id).dec()  # type: ignore[attr-defined]

    def record_wait(self, tenant_id: str, wait_ms: float) -> None:
        """Record queue wait time."""
        if self._ready and self._wait_time is not None:
            self._wait_time.labels(tenant=tenant_id).observe(wait_ms)  # type: ignore[attr-defined]

    def record_timeout(self, tenant_id: str) -> None:
        """Increment timeout counter."""
        if self._ready and self._timeout_counter is not None:
            self._timeout_counter.labels(tenant=tenant_id).inc()  # type: ignore[attr-defined]

    def record_error(self, tenant_id: str) -> None:
        """Increment error counter."""
        if self._ready and self._error_counter is not None:
            self._error_counter.labels(tenant=tenant_id).inc()  # type: ignore[attr-defined]

    def record_duration(
        self, tenant_id: str, workload: str, duration_ms: float
    ) -> None:
        """Record query execution duration."""
        if self._ready and self._duration_histogram is not None:
            self._duration_histogram.labels(
                tenant=tenant_id, workload=workload
            ).observe(duration_ms)  # type: ignore[attr-defined]


class QueryQueueExecutor:
    """Async execution loop that drives the QueryQueue.

    Runs as a background task in the FastAPI app.
    Every 50ms, dequeues the next query, dispatches to thread pool,
    emits Prometheus metrics, and enforces timeouts.
    """

    def __init__(
        self,
        queue: QueryQueue,
        scheduler: "FairShareScheduler",
        query_engine: "QueryEngine",
        metrics: Optional[PrometheusMetrics] = None,
        max_workers: int = _DEFAULT_THREAD_WORKERS,
    ) -> None:
        """Initialize executor with queue, scheduler, engine, and metrics."""
        self._queue = queue
        self._scheduler = scheduler
        self._engine = query_engine
        self._metrics = metrics or PrometheusMetrics()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="mm-query-worker"
        )
        self._running = False
        self._active_tasks: dict[str, asyncio.Task] = {}  # type: ignore[type-arg]

        # Stats for get_metrics()
        self._total_wait_ms: float = 0.0
        self._completed: int = 0
        self._timeouts: int = 0
        self._errors: int = 0

    # ── Main Loop ─────────────────────────────────────────────

    async def run_forever(self) -> None:
        """Main loop: every 50ms, call queue.get_next(), dispatch to thread pool."""
        self._running = True
        logger.info("QueryQueueExecutor started")
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                logger.info("QueryQueueExecutor cancelled, stopping")
                break
            except Exception as exc:
                logger.exception("Unexpected error in queue executor tick: %s", exc)
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    async def _tick(self) -> None:
        """Single tick: dequeue and dispatch all dispatchable queries."""
        queue_depth = len(self._queue._heap)
        active_count = len(self._queue._active)
        self._metrics.record_queue_depth(queue_depth)

        # Dispatch as many as concurrency allows
        for _ in range(10):  # max dispatches per tick
            query = await self._queue.get_next()
            if query is None:
                break

            # Skip cancelled queries
            if query.query_id in self._queue._cancelled:
                self._queue._cancelled.discard(query.query_id)
                logger.debug("Skipping cancelled query %s", query.query_id)
                continue

            wait_ms = (
                time.monotonic() * 1000
                - query.submitted_at.timestamp() * 1000
            )
            self._metrics.record_wait(query.tenant_id, wait_ms)
            self._total_wait_ms += wait_ms

            task = asyncio.create_task(
                self._dispatch(query), name=f"query-{query.query_id[:8]}"
            )
            self._active_tasks[query.query_id] = task
            task.add_done_callback(
                lambda t, qid=query.query_id: self._active_tasks.pop(qid, None)
            )

    # ── Dispatch ──────────────────────────────────────────────

    async def _dispatch(self, query: QueuedQuery) -> None:
        """Run query in thread pool executor."""
        from metamind.core.query_engine import QueryContext

        ctx = QueryContext(
            query_id=query.query_id,
            tenant_id=query.tenant_id,
            sql=query.sql,
            timeout_seconds=query.timeout_seconds,
        )

        self._metrics.record_active(query.tenant_id, 1)
        start_ms = time.monotonic() * 1000

        loop = asyncio.get_running_loop()
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(
                    self._executor,
                    self._engine.execute,
                    ctx,
                ),
                timeout=float(query.timeout_seconds),
            )
            duration_ms = time.monotonic() * 1000 - start_ms
            self._metrics.record_duration(
                query.tenant_id,
                query.workload_type.value if hasattr(query.workload_type, "value") else str(query.workload_type),
                duration_ms,
            )
            self._completed += 1
            self._queue.complete(query.query_id)

            if query.result_callback is not None:
                try:
                    query.result_callback(result)  # type: ignore[call-arg]
                except Exception as cb_exc:
                    logger.warning(
                        "Result callback failed for query %s: %s",
                        query.query_id,
                        cb_exc,
                    )

        except asyncio.TimeoutError:
            logger.warning(
                "Query %s timed out after %ss (tenant=%s)",
                query.query_id,
                query.timeout_seconds,
                query.tenant_id,
            )
            self._metrics.record_timeout(query.tenant_id)
            self._timeouts += 1
            self._queue.cancel(query.query_id)

            if query.result_callback is not None:
                try:
                    query.result_callback(
                        TimeoutError(
                            f"Query exceeded {query.timeout_seconds}s timeout"
                        )
                    )
                except Exception:
                    logger.error("Unhandled exception in queue_executor.py: %s", exc)

        except Exception as exc:
            logger.exception(
                "Query %s failed (tenant=%s): %s",
                query.query_id,
                query.tenant_id,
                exc,
            )
            self._metrics.record_error(query.tenant_id)
            self._errors += 1
            self._queue.complete(query.query_id)

            if query.result_callback is not None:
                try:
                    query.result_callback(exc)  # type: ignore[call-arg]
                except Exception:
                    logger.error("Unhandled exception in queue_executor.py: %s", exc)

        finally:
            self._metrics.record_active(query.tenant_id, -1)

    # ── Lifecycle ─────────────────────────────────────────────

    async def drain(self, timeout_seconds: float = 30.0) -> None:
        """Graceful shutdown: wait for active queries to complete."""
        self._running = False
        logger.info(
            "Draining %d active tasks (timeout=%.1fs)",
            len(self._active_tasks),
            timeout_seconds,
        )
        if not self._active_tasks:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._active_tasks.values(), return_exceptions=True),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Drain timed out; %d tasks still active",
                len(self._active_tasks),
            )
        finally:
            self._executor.shutdown(wait=False)
            logger.info("QueryQueueExecutor drained")

    def stop(self) -> None:
        """Signal the run_forever loop to stop."""
        self._running = False

    # ── Metrics ───────────────────────────────────────────────

    def get_metrics(self) -> dict[str, object]:
        """Return snapshot: {queue_depth, active, avg_wait_ms, timeouts_total}."""
        avg_wait = (
            self._total_wait_ms / self._completed
            if self._completed > 0
            else 0.0
        )
        return {
            "queue_depth": len(self._queue._heap),
            "active": len(self._queue._active),
            "avg_wait_ms": round(avg_wait, 2),
            "timeouts_total": self._timeouts,
            "errors_total": self._errors,
            "completed_total": self._completed,
        }
