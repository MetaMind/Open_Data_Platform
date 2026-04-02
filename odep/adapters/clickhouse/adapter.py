"""ClickHouseAdapter — ExecutionEngine implementation backed by ClickHouse.

Connects via the clickhouse-connect Python client (HTTP interface).
Supports SQL queries with async execution via background threads.

Docker Compose usage:
  docker compose --profile clickhouse up -d
  ClickHouse HTTP:  http://localhost:8123
  ClickHouse UI:    http://localhost:8123/play

Config (.odep.env):
  ODEP_EXECUTION__DEFAULT_ENGINE=clickhouse
  ODEP_EXECUTION__CLICKHOUSE_HOST=localhost
  ODEP_EXECUTION__CLICKHOUSE_PORT=8123
  ODEP_EXECUTION__CLICKHOUSE_USER=default
  ODEP_EXECUTION__CLICKHOUSE_PASSWORD=
  ODEP_EXECUTION__CLICKHOUSE_DATABASE=default

Install: pip install clickhouse-connect
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, Iterator, List, Optional

from odep.models import JobConfig, JobResult

_CH_AVAILABLE = False
try:
    import clickhouse_connect  # noqa: F401
    _CH_AVAILABLE = True
except ImportError:
    pass


class ClickHouseAdapter:
    """ExecutionEngine implementation backed by ClickHouse.

    Submits SQL queries via clickhouse-connect (HTTP interface).
    Supports sync and async execution, cancellation via query_id,
    and rich metrics from ClickHouse query_log.
    """

    def __init__(self, config: Any) -> None:
        self.config = config
        self._host: str = getattr(config, "clickhouse_host", "localhost")
        self._port: int = int(getattr(config, "clickhouse_port", 8123))
        self._user: str = getattr(config, "clickhouse_user", "default")
        self._password: str = getattr(config, "clickhouse_password", "")
        self._database: str = getattr(config, "clickhouse_database", "default")
        self._jobs: Dict[str, Dict[str, Any]] = {}

    def _get_client(self):
        """Create and return a ClickHouse client."""
        import clickhouse_connect
        return clickhouse_connect.get_client(
            host=self._host,
            port=self._port,
            username=self._user,
            password=self._password,
            database=self._database,
        )

    # ------------------------------------------------------------------
    # ExecutionEngine Protocol
    # ------------------------------------------------------------------

    def submit(self, config: JobConfig, async_run: bool = False) -> str:
        """Submit a SQL query to ClickHouse and return a job_handle UUID."""
        if not _CH_AVAILABLE:
            raise RuntimeError(
                "clickhouse-connect is not installed. Run: pip install clickhouse-connect"
            )

        job_handle = str(uuid.uuid4())
        # Use the job_handle as the ClickHouse query_id for cancellation support
        query_id = job_handle

        self._jobs[job_handle] = {
            "status": "PENDING",
            "query_id": query_id,
            "start_time": time.time(),
            "end_time": None,
            "row_count": 0,
            "error": None,
            "logs": [],
            "stats": {},
            "thread": None,
        }

        if async_run:
            t = threading.Thread(
                target=self._run_query,
                args=(job_handle, config.code, query_id),
                daemon=True,
            )
            self._jobs[job_handle]["thread"] = t
            self._jobs[job_handle]["status"] = "RUNNING"
            t.start()
        else:
            self._jobs[job_handle]["status"] = "RUNNING"
            self._run_query(job_handle, config.code, query_id)

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
        """Block until the ClickHouse query completes or timeout_sec is exceeded."""
        job = self._jobs[job_handle]
        thread = job.get("thread")
        if thread is not None:
            thread.join(timeout=timeout_sec)
            if thread.is_alive():
                self.cancel(job_handle)
                raise TimeoutError(
                    f"ClickHouse job {job_handle!r} exceeded timeout of {timeout_sec}s"
                )

        end_time = job.get("end_time") or time.time()
        execution_time_ms = int((end_time - job["start_time"]) * 1000)
        stats = job.get("stats", {})

        return JobResult(
            success=job["status"] == "SUCCESS",
            records_processed=job.get("row_count", 0),
            execution_time_ms=execution_time_ms,
            error_message=job.get("error"),
            metrics={
                "backend_used": "clickhouse",
                "clickhouse_host": self._host,
                "clickhouse_port": self._port,
                "query_id": job.get("query_id"),
                "database": self._database,
                "elapsed_ns": stats.get("elapsed", 0),
                "read_rows": stats.get("read_rows", 0),
                "read_bytes": stats.get("read_bytes", 0),
                "written_rows": stats.get("written_rows", 0),
            },
        )

    def stream_logs(self, job_handle: str) -> Iterator[str]:
        """Yield captured log lines."""
        for line in self._jobs[job_handle]["logs"]:
            yield line

    def cancel(self, job_handle: str) -> bool:
        """Cancel a running ClickHouse query via KILL QUERY."""
        job = self._jobs[job_handle]
        query_id = job.get("query_id")
        if query_id and _CH_AVAILABLE:
            try:
                client = self._get_client()
                client.command(f"KILL QUERY WHERE query_id = '{query_id}' SYNC")
            except Exception:
                pass
        job["status"] = "CANCELLED"
        return True

    def get_metrics(self, job_handle: str) -> Dict[str, Any]:
        """Return execution metrics, enriched from system.query_log if available."""
        job = self._jobs[job_handle]
        end_time = job.get("end_time") or time.time()
        stats = job.get("stats", {})

        # Try to enrich from system.query_log
        if _CH_AVAILABLE and job.get("query_id"):
            try:
                client = self._get_client()
                result = client.query(
                    "SELECT read_rows, read_bytes, written_rows, memory_usage, query_duration_ms "
                    "FROM system.query_log "
                    f"WHERE query_id = '{job['query_id']}' AND type = 'QueryFinish' "
                    "LIMIT 1"
                )
                if result.result_rows:
                    row = result.result_rows[0]
                    stats = {
                        "read_rows": row[0],
                        "read_bytes": row[1],
                        "written_rows": row[2],
                        "memory_usage": row[3],
                        "query_duration_ms": row[4],
                    }
            except Exception:
                pass

        return {
            "execution_time_ms": int((end_time - job["start_time"]) * 1000),
            "rows_processed": job.get("row_count", 0),
            "backend_used": "clickhouse",
            "clickhouse_host": self._host,
            "clickhouse_port": self._port,
            "query_id": job.get("query_id"),
            "database": self._database,
            **stats,
        }

    def health_check(self) -> bool:
        """Return True if ClickHouse is reachable."""
        if not _CH_AVAILABLE:
            return False
        try:
            client = self._get_client()
            result = client.command("SELECT 1")
            return result == 1
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_query(self, job_handle: str, sql: str, query_id: str) -> None:
        """Execute the SQL query against ClickHouse."""
        job = self._jobs[job_handle]
        try:
            client = self._get_client()
            result = client.query(sql, settings={"query_id": query_id})
            job["row_count"] = len(result.result_rows)
            job["stats"] = {
                "read_rows": getattr(result, "read_rows", 0),
                "read_bytes": getattr(result, "read_bytes", 0),
                "written_rows": getattr(result, "written_rows", 0),
                "elapsed": getattr(result, "elapsed", 0),
            }
            job["status"] = "SUCCESS"
            job["logs"].append(f"Query completed: {job['row_count']} rows returned")
        except Exception as exc:
            job["status"] = "FAILED"
            job["error"] = str(exc)
            job["logs"].append(f"ClickHouse query failed: {exc}")
        finally:
            job["end_time"] = time.time()
