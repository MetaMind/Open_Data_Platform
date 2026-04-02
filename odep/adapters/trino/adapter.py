"""TrinoAdapter — ExecutionEngine implementation backed by Trino.

Trino is a distributed SQL query engine. This adapter connects via the
trino-python-client (DBAPI2) and submits SQL queries asynchronously using
background threads, tracking state in an in-process job registry.

Docker Compose usage:
  Start Trino:   docker compose --profile trino up -d
  HTTP UI:       http://localhost:8082/ui
  Config:        ODEP_EXECUTION__TRINO_HOST=localhost
                 ODEP_EXECUTION__TRINO_PORT=8082
                 ODEP_EXECUTION__TRINO_CATALOG=tpch
                 ODEP_EXECUTION__TRINO_SCHEMA=tiny

The built-in `tpch` catalog is available immediately with no extra setup.
To query your own data, mount a catalog config file into the container.

Example SQL (works out of the box):
  SELECT * FROM tpch.tiny.orders LIMIT 10
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, Iterator, List, Optional

from odep.models import JobConfig, JobResult

_TRINO_AVAILABLE = False
try:
    import trino  # noqa: F401
    _TRINO_AVAILABLE = True
except ImportError:
    pass


class TrinoAdapter:
    """ExecutionEngine implementation backed by Trino.

    Submits SQL queries to a Trino cluster via the DBAPI2 interface.
    Supports sync and async execution, cancellation, and metrics.
    """

    def __init__(self, config: Any) -> None:
        self.config = config
        self._host: str = getattr(config, "trino_host", "localhost")
        self._port: int = int(getattr(config, "trino_port", 8082))
        self._user: str = getattr(config, "trino_user", "odep")
        self._catalog: str = getattr(config, "trino_catalog", "tpch")
        self._schema: str = getattr(config, "trino_schema", "tiny")
        self._jobs: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # ExecutionEngine Protocol
    # ------------------------------------------------------------------

    def submit(self, config: JobConfig, async_run: bool = False) -> str:
        """Submit a SQL query to Trino and return a job_handle UUID.

        Only EngineType.SQL is supported. For async_run=True the query
        runs in a background thread and the handle is returned immediately.
        """
        if not _TRINO_AVAILABLE:
            raise RuntimeError(
                "trino-python-client is not installed. Run: pip install trino"
            )

        job_handle = str(uuid.uuid4())
        cancel_event = threading.Event()

        self._jobs[job_handle] = {
            "status": "PENDING",
            "result": None,
            "logs": [],
            "start_time": time.time(),
            "end_time": None,
            "row_count": 0,
            "error": None,
            "cancel_event": cancel_event,
            "thread": None,
            "query_id": None,       # Trino query ID for cancellation
            "stats": {},
        }

        if async_run:
            thread = threading.Thread(
                target=self._run_query,
                args=(job_handle, config.code, cancel_event),
                daemon=True,
            )
            self._jobs[job_handle]["thread"] = thread
            self._jobs[job_handle]["status"] = "RUNNING"
            thread.start()
        else:
            self._jobs[job_handle]["status"] = "RUNNING"
            self._run_query(job_handle, config.code, cancel_event)

        return job_handle

    def get_status(self, job_handle: str) -> Dict[str, Any]:
        """Return status dict for the given job_handle."""
        job = self._jobs[job_handle]
        return {
            "status": job["status"],
            "job_handle": job_handle,
            "query_id": job.get("query_id"),
        }

    def wait_for_completion(self, job_handle: str, timeout_sec: int = 3600) -> JobResult:
        """Block until the Trino query completes or timeout_sec is exceeded."""
        job = self._jobs[job_handle]
        thread = job.get("thread")

        if thread is not None:
            thread.join(timeout=timeout_sec)
            if thread.is_alive():
                self.cancel(job_handle)
                raise TimeoutError(
                    f"Trino job {job_handle!r} exceeded timeout of {timeout_sec}s"
                )

        start_time = job["start_time"]
        end_time = job.get("end_time") or time.time()
        execution_time_ms = int((end_time - start_time) * 1000)
        stats = job.get("stats", {})

        return JobResult(
            success=job["status"] == "SUCCESS",
            records_processed=job.get("row_count", 0),
            execution_time_ms=execution_time_ms,
            error_message=job.get("error"),
            metrics={
                "backend_used": "trino",
                "trino_host": self._host,
                "trino_port": self._port,
                "query_id": job.get("query_id"),
                "catalog": self._catalog,
                "schema": self._schema,
                "elapsed_ms": stats.get("elapsedTimeMillis", execution_time_ms),
                "cpu_ms": stats.get("cpuTimeMillis", 0),
                "peak_memory_bytes": stats.get("peakMemoryBytes", 0),
            },
        )

    def stream_logs(self, job_handle: str) -> Iterator[str]:
        """Yield log lines captured during query execution."""
        for line in self._jobs[job_handle]["logs"]:
            yield line

    def cancel(self, job_handle: str) -> bool:
        """Cancel a running Trino query via the REST API."""
        job = self._jobs[job_handle]
        job["cancel_event"].set()
        job["status"] = "CANCELLED"
        query_id = job.get("query_id")
        if query_id:
            self._cancel_trino_query(query_id)
        return True

    def get_metrics(self, job_handle: str) -> Dict[str, Any]:
        """Return execution metrics for the given job_handle."""
        job = self._jobs[job_handle]
        start_time = job["start_time"]
        end_time = job.get("end_time") or time.time()
        stats = job.get("stats", {})
        return {
            "execution_time_ms": int((end_time - start_time) * 1000),
            "rows_processed": job.get("row_count", 0),
            "backend_used": "trino",
            "trino_host": self._host,
            "trino_port": self._port,
            "query_id": job.get("query_id"),
            "catalog": self._catalog,
            "schema": self._schema,
            "elapsed_ms": stats.get("elapsedTimeMillis", 0),
            "cpu_ms": stats.get("cpuTimeMillis", 0),
            "peak_memory_bytes": stats.get("peakMemoryBytes", 0),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_connection(self):
        """Create and return a Trino DBAPI2 connection."""
        import trino
        return trino.dbapi.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            catalog=self._catalog,
            schema=self._schema,
        )

    def _run_query(
        self, job_handle: str, sql: str, cancel_event: threading.Event
    ) -> None:
        """Execute the SQL query against Trino and update the job registry."""
        job = self._jobs[job_handle]

        if cancel_event.is_set():
            job["status"] = "CANCELLED"
            job["end_time"] = time.time()
            return

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Execute the query
            cursor.execute(sql)

            # Capture the Trino query ID for cancellation support
            if hasattr(cursor, "query_id"):
                job["query_id"] = cursor.query_id
                job["logs"].append(f"Trino query_id: {cursor.query_id}")

            # Fetch all results
            rows = cursor.fetchall()
            job["row_count"] = len(rows)
            job["result"] = rows

            # Capture query stats if available
            if hasattr(cursor, "stats") and cursor.stats:
                job["stats"] = cursor.stats

            job["status"] = "SUCCESS"
            job["logs"].append(f"Query completed: {len(rows)} rows returned")

        except Exception as exc:
            job["status"] = "FAILED"
            job["error"] = str(exc)
            job["logs"].append(f"Query failed: {exc}")
        finally:
            job["end_time"] = time.time()

    def _cancel_trino_query(self, query_id: str) -> None:
        """Cancel a Trino query via the coordinator REST API."""
        try:
            import httpx
            httpx.delete(
                f"http://{self._host}:{self._port}/v1/query/{query_id}",
                headers={"X-Trino-User": self._user},
                timeout=5.0,
            )
        except Exception:
            pass  # Best-effort

    def health_check(self) -> bool:
        """Return True if the Trino coordinator is reachable."""
        try:
            import httpx
            resp = httpx.get(
                f"http://{self._host}:{self._port}/v1/info",
                timeout=5.0,
            )
            return resp.status_code == 200
        except Exception:
            return False
