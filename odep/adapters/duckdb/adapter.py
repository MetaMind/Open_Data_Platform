"""DuckDbAdapter — full implementation of ExecutionEngine using DuckDB."""

from __future__ import annotations

import contextlib
import io
import threading
import time
import uuid
from typing import Any, Dict, Iterator

from odep.interfaces import ExecutionEngine
from odep.models import JobConfig, JobResult


class DuckDbAdapter:
    """ExecutionEngine implementation backed by DuckDB."""

    def __init__(self, config: Any) -> None:
        self.config = config
        self._jobs: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(self, config: JobConfig, async_run: bool = False) -> str:
        """Submit a SQL job and return a job_handle.

        If async_run=False (default), blocks until the job completes.
        If async_run=True, runs in a background daemon thread and returns immediately.
        """
        job_handle = str(uuid.uuid4())
        cancel_event = threading.Event()

        self._jobs[job_handle] = {
            "status": "RUNNING",
            "result": None,
            "logs": [],
            "start_time": time.time(),
            "cancel_event": cancel_event,
            "thread": None,
        }

        if async_run:
            thread = threading.Thread(
                target=self._execute_sql,
                args=(job_handle, config.code, cancel_event),
                daemon=True,
            )
            self._jobs[job_handle]["thread"] = thread
            thread.start()
        else:
            self._execute_sql(job_handle, config.code, cancel_event)

        return job_handle

    def get_status(self, job_handle: str) -> Dict[str, Any]:
        """Return status dict for the given job_handle.

        Raises KeyError if the handle is not found.
        """
        job = self._jobs[job_handle]  # raises KeyError if missing
        return {"status": job["status"], "job_handle": job_handle}

    def wait_for_completion(self, job_handle: str, timeout_sec: int = 3600) -> JobResult:
        """Block until the job completes or timeout_sec is exceeded.

        Raises TimeoutError if the job does not finish in time.
        """
        job = self._jobs[job_handle]
        thread = job.get("thread")

        if thread is not None:
            thread.join(timeout=timeout_sec)
            if thread.is_alive():
                self.cancel(job_handle)
                raise TimeoutError(
                    f"Job {job_handle!r} exceeded timeout of {timeout_sec}s"
                )

        start_time = job["start_time"]
        end_time = job.get("end_time", time.time())
        execution_time_ms = int((end_time - start_time) * 1000)

        return JobResult(
            success=job["status"] == "SUCCESS",
            execution_time_ms=execution_time_ms,
            error_message=job.get("error"),
            records_processed=job.get("row_count", 0),
        )

    def stream_logs(self, job_handle: str) -> Iterator[str]:
        """Yield each log line captured during job execution."""
        for line in self._jobs[job_handle]["logs"]:
            yield line

    def cancel(self, job_handle: str) -> bool:
        """Signal cancellation for the given job and mark it CANCELLED."""
        job = self._jobs[job_handle]
        job["cancel_event"].set()
        job["status"] = "CANCELLED"
        return True

    def get_metrics(self, job_handle: str) -> Dict[str, Any]:
        """Return execution metrics for the given job_handle."""
        job = self._jobs[job_handle]
        start_time = job["start_time"]
        end_time = job.get("end_time", time.time())
        execution_time_ms = int((end_time - start_time) * 1000)

        return {
            "execution_time_ms": execution_time_ms,
            "rows_processed": job.get("row_count", 0),
            "backend_used": "duckdb",
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _execute_sql(
        self, job_handle: str, sql: str, cancel_event: threading.Event
    ) -> None:
        """Execute *sql* via DuckDB and update the job registry with the result."""
        job = self._jobs[job_handle]

        # Honour cancellation requested before execution starts
        if cancel_event.is_set():
            job["status"] = "CANCELLED"
            job["end_time"] = time.time()
            return

        stdout_capture = io.StringIO()
        try:
            import duckdb  # local import so the rest of odep works without duckdb

            with contextlib.redirect_stdout(stdout_capture):
                conn = duckdb.connect()
                rows = conn.execute(sql).fetchall()

            captured = stdout_capture.getvalue()
            if captured:
                job["logs"].extend(captured.splitlines())

            job["status"] = "SUCCESS"
            job["row_count"] = len(rows)
            job["result"] = rows
        except Exception as exc:  # noqa: BLE001
            captured = stdout_capture.getvalue()
            if captured:
                job["logs"].extend(captured.splitlines())

            job["status"] = "FAILED"
            job["error"] = str(exc)
        finally:
            job["end_time"] = time.time()
