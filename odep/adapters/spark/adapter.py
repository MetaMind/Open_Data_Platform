"""SparkAdapter — ExecutionEngine implementation backed by Apache Spark.

Supports two submission modes:
  - SQL/Python via pyspark in client mode (spark_master = "spark://host:7077" or "local[*]")
  - Job status tracking via the Spark REST API (spark_rest_url)

Docker Compose usage:
  Start the cluster:  docker compose --profile full up -d spark-master spark-worker
  Master URL:         spark://localhost:7077
  REST API:           http://localhost:8081/api/v1
  Config:             ODEP_EXECUTION__SPARK_MASTER=spark://localhost:7077
                      ODEP_EXECUTION__SPARK_REST_URL=http://localhost:8081
"""

from __future__ import annotations

import io
import threading
import time
import uuid
from contextlib import redirect_stdout
from typing import Any, Dict, Iterator, List, Optional

from odep.models import JobConfig, JobResult

_PYSPARK_AVAILABLE = False
try:
    import pyspark  # noqa: F401
    _PYSPARK_AVAILABLE = True
except ImportError:
    pass


class SparkAdapter:
    """ExecutionEngine implementation backed by Apache Spark.

    Submits SQL or Python jobs to a Spark cluster (local or remote).
    Tracks job state in an in-process registry and optionally polls the
    Spark REST API for richer status information.
    """

    def __init__(self, config: Any) -> None:
        self.config = config
        self._master: str = getattr(config, "spark_master", "local[*]")
        self._rest_url: str = getattr(config, "spark_rest_url", "http://localhost:8081")
        self._jobs: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # ExecutionEngine Protocol
    # ------------------------------------------------------------------

    def submit(self, config: JobConfig, async_run: bool = False) -> str:
        """Submit a SQL or Python job to Spark.

        For SQL jobs (EngineType.SQL): executes via SparkSession.sql().
        For Python jobs (EngineType.PYTHON): executes via exec() with a
        SparkSession available as `spark` in the local namespace.

        Returns a job_handle UUID.
        """
        if not _PYSPARK_AVAILABLE:
            raise RuntimeError(
                "pyspark is not installed. Run: pip install pyspark"
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
            "app_id": None,
        }

        if async_run:
            thread = threading.Thread(
                target=self._run_spark_job,
                args=(job_handle, config, cancel_event),
                daemon=True,
            )
            self._jobs[job_handle]["thread"] = thread
            self._jobs[job_handle]["status"] = "RUNNING"
            thread.start()
        else:
            self._jobs[job_handle]["status"] = "RUNNING"
            self._run_spark_job(job_handle, config, cancel_event)

        return job_handle

    def get_status(self, job_handle: str) -> Dict[str, Any]:
        """Return status dict for the given job_handle."""
        job = self._jobs[job_handle]
        return {
            "status": job["status"],
            "job_handle": job_handle,
            "app_id": job.get("app_id"),
        }

    def wait_for_completion(self, job_handle: str, timeout_sec: int = 3600) -> JobResult:
        """Block until the Spark job completes or timeout_sec is exceeded."""
        job = self._jobs[job_handle]
        thread = job.get("thread")

        if thread is not None:
            thread.join(timeout=timeout_sec)
            if thread.is_alive():
                self.cancel(job_handle)
                raise TimeoutError(
                    f"Spark job {job_handle!r} exceeded timeout of {timeout_sec}s"
                )

        start_time = job["start_time"]
        end_time = job.get("end_time") or time.time()
        execution_time_ms = int((end_time - start_time) * 1000)

        return JobResult(
            success=job["status"] == "SUCCESS",
            records_processed=job.get("row_count", 0),
            execution_time_ms=execution_time_ms,
            error_message=job.get("error"),
            metrics={
                "backend_used": "spark",
                "spark_master": self._master,
                "app_id": job.get("app_id"),
            },
        )

    def stream_logs(self, job_handle: str) -> Iterator[str]:
        """Yield captured log lines from the Spark job."""
        for line in self._jobs[job_handle]["logs"]:
            yield line

    def cancel(self, job_handle: str) -> bool:
        """Signal cancellation and mark the job as CANCELLED."""
        job = self._jobs[job_handle]
        job["cancel_event"].set()
        job["status"] = "CANCELLED"
        # Attempt to kill via REST API if we have an app_id
        app_id = job.get("app_id")
        if app_id:
            self._kill_spark_app(app_id)
        return True

    def get_metrics(self, job_handle: str) -> Dict[str, Any]:
        """Return execution metrics for the given job_handle."""
        job = self._jobs[job_handle]
        start_time = job["start_time"]
        end_time = job.get("end_time") or time.time()
        return {
            "execution_time_ms": int((end_time - start_time) * 1000),
            "rows_processed": job.get("row_count", 0),
            "backend_used": "spark",
            "spark_master": self._master,
            "app_id": job.get("app_id"),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_spark_job(
        self, job_handle: str, config: JobConfig, cancel_event: threading.Event
    ) -> None:
        """Execute the Spark job and update the job registry."""
        from pyspark.sql import SparkSession

        job = self._jobs[job_handle]

        if cancel_event.is_set():
            job["status"] = "CANCELLED"
            job["end_time"] = time.time()
            return

        stdout_buf = io.StringIO()
        try:
            spark = (
                SparkSession.builder
                .master(self._master)
                .appName(f"odep-{job_handle[:8]}")
                .config("spark.ui.enabled", "true")
                .getOrCreate()
            )

            # Capture the Spark app ID for REST API tracking
            job["app_id"] = spark.sparkContext.applicationId

            with redirect_stdout(stdout_buf):
                from odep.models import EngineType
                if config.engine == EngineType.SQL:
                    df = spark.sql(config.code)
                    rows = df.collect()
                    job["row_count"] = len(rows)
                    job["result"] = rows
                elif config.engine == EngineType.PYTHON:
                    # Execute Python code with `spark` in scope
                    local_ns: Dict[str, Any] = {"spark": spark}
                    exec(config.code, local_ns)  # noqa: S102
                    job["row_count"] = local_ns.get("_row_count", 0)
                else:
                    raise ValueError(
                        f"SparkAdapter supports SQL and PYTHON engine types, got {config.engine}"
                    )

            captured = stdout_buf.getvalue()
            if captured:
                job["logs"].extend(captured.splitlines())

            job["status"] = "SUCCESS"

        except Exception as exc:
            captured = stdout_buf.getvalue()
            if captured:
                job["logs"].extend(captured.splitlines())
            job["status"] = "FAILED"
            job["error"] = str(exc)
        finally:
            job["end_time"] = time.time()

    def _kill_spark_app(self, app_id: str) -> None:
        """Attempt to kill a Spark application via the REST API."""
        try:
            import httpx
            httpx.post(
                f"{self._rest_url}/api/v1/applications/{app_id}/kill",
                timeout=5.0,
            )
        except Exception:
            pass  # Best-effort; don't raise on kill failure

    def _get_running_apps(self) -> List[Dict[str, Any]]:
        """Query the Spark REST API for running applications."""
        try:
            import httpx
            resp = httpx.get(f"{self._rest_url}/api/v1/applications", timeout=5.0)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return []
