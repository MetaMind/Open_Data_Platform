"""FlinkAdapter — ExecutionEngine implementation backed by Apache Flink.

Uses the Flink REST API (JobManager HTTP API) to submit and monitor jobs.
Supports SQL jobs via the Flink SQL Gateway REST API (Flink 1.16+).

Docker Compose usage:
  docker compose --profile flink up -d
  Flink UI:         http://localhost:8083
  SQL Gateway:      http://localhost:8084

Config (.odep.env):
  ODEP_EXECUTION__DEFAULT_ENGINE=flink
  ODEP_EXECUTION__FLINK_JOBMANAGER_URL=http://localhost:8083
  ODEP_EXECUTION__FLINK_SQL_GATEWAY_URL=http://localhost:8084
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, Iterator, List, Optional

import httpx

from odep.models import JobConfig, JobResult

_STATUS_MAP: Dict[str, str] = {
    "CREATED": "PENDING",
    "RUNNING": "RUNNING",
    "FAILING": "RUNNING",
    "FAILED": "FAILED",
    "CANCELLING": "RUNNING",
    "CANCELED": "CANCELLED",
    "FINISHED": "SUCCESS",
    "RESTARTING": "RUNNING",
    "SUSPENDED": "PENDING",
    "RECONCILING": "RUNNING",
}


class FlinkAdapter:
    """ExecutionEngine implementation backed by Apache Flink REST API.

    Supports:
    - SQL jobs via Flink SQL Gateway (EngineType.SQL)
    - JAR jobs via JobManager REST API (EngineType.PYTHON — submits a pre-uploaded JAR)
    """

    def __init__(self, config: Any) -> None:
        self.config = config
        self._jm_url: str = getattr(config, "flink_jobmanager_url", "http://localhost:8083")
        self._gw_url: str = getattr(config, "flink_sql_gateway_url", "http://localhost:8084")
        self._jm_client = httpx.Client(base_url=self._jm_url, timeout=30.0)
        self._gw_client = httpx.Client(base_url=self._gw_url, timeout=30.0)
        self._jobs: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # ExecutionEngine Protocol
    # ------------------------------------------------------------------

    def submit(self, config: JobConfig, async_run: bool = False) -> str:
        """Submit a SQL or JAR job to Flink.

        SQL jobs use the Flink SQL Gateway.
        PYTHON jobs are treated as JAR submissions (code = JAR path or entry class).
        """
        from odep.models import EngineType
        job_handle = str(uuid.uuid4())
        self._jobs[job_handle] = {
            "status": "PENDING",
            "flink_job_id": None,
            "session_handle": None,
            "operation_handle": None,
            "start_time": time.time(),
            "end_time": None,
            "row_count": 0,
            "error": None,
            "logs": [],
        }

        if config.engine == EngineType.SQL:
            if async_run:
                t = threading.Thread(
                    target=self._run_sql, args=(job_handle, config.code), daemon=True
                )
                self._jobs[job_handle]["status"] = "RUNNING"
                t.start()
            else:
                self._jobs[job_handle]["status"] = "RUNNING"
                self._run_sql(job_handle, config.code)
        else:
            # JAR submission — config.code is the JAR path or entry class
            if async_run:
                t = threading.Thread(
                    target=self._run_jar, args=(job_handle, config.code), daemon=True
                )
                self._jobs[job_handle]["status"] = "RUNNING"
                t.start()
            else:
                self._jobs[job_handle]["status"] = "RUNNING"
                self._run_jar(job_handle, config.code)

        return job_handle

    def get_status(self, job_handle: str) -> Dict[str, Any]:
        """Return status dict, polling Flink if a flink_job_id is known."""
        job = self._jobs[job_handle]
        flink_job_id = job.get("flink_job_id")
        if flink_job_id and job["status"] == "RUNNING":
            try:
                resp = self._jm_client.get(f"/jobs/{flink_job_id}")
                if resp.status_code == 200:
                    flink_state = resp.json().get("state", "RUNNING")
                    job["status"] = _STATUS_MAP.get(flink_state, "RUNNING")
            except Exception:
                pass
        return {"status": job["status"], "job_handle": job_handle, "flink_job_id": flink_job_id}

    def wait_for_completion(self, job_handle: str, timeout_sec: int = 3600) -> JobResult:
        """Poll until the Flink job completes or timeout_sec is exceeded."""
        start = time.time()
        while True:
            status = self.get_status(job_handle)["status"]
            if status in ("SUCCESS", "FAILED", "CANCELLED"):
                break
            if time.time() - start > timeout_sec:
                self.cancel(job_handle)
                raise TimeoutError(f"Flink job {job_handle!r} exceeded timeout of {timeout_sec}s")
            time.sleep(2)

        job = self._jobs[job_handle]
        end_time = job.get("end_time") or time.time()
        execution_time_ms = int((end_time - job["start_time"]) * 1000)

        return JobResult(
            success=job["status"] == "SUCCESS",
            records_processed=job.get("row_count", 0),
            execution_time_ms=execution_time_ms,
            error_message=job.get("error"),
            metrics={
                "backend_used": "flink",
                "flink_jobmanager_url": self._jm_url,
                "flink_job_id": job.get("flink_job_id"),
            },
        )

    def stream_logs(self, job_handle: str) -> Iterator[str]:
        """Yield captured log lines."""
        for line in self._jobs[job_handle]["logs"]:
            yield line

    def cancel(self, job_handle: str) -> bool:
        """Cancel a running Flink job."""
        job = self._jobs[job_handle]
        flink_job_id = job.get("flink_job_id")
        if flink_job_id:
            try:
                self._jm_client.patch(f"/jobs/{flink_job_id}", json={"status": "CANCELED"})
            except Exception:
                pass
        job["status"] = "CANCELLED"
        return True

    def get_metrics(self, job_handle: str) -> Dict[str, Any]:
        """Return execution metrics."""
        job = self._jobs[job_handle]
        end_time = job.get("end_time") or time.time()
        return {
            "execution_time_ms": int((end_time - job["start_time"]) * 1000),
            "rows_processed": job.get("row_count", 0),
            "backend_used": "flink",
            "flink_jobmanager_url": self._jm_url,
            "flink_job_id": job.get("flink_job_id"),
        }

    def health_check(self) -> bool:
        """Return True if the Flink JobManager is reachable."""
        try:
            resp = self._jm_client.get("/overview", timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_sql(self, job_handle: str, sql: str) -> None:
        """Execute SQL via Flink SQL Gateway."""
        job = self._jobs[job_handle]
        try:
            # 1. Open a session
            sess_resp = self._gw_client.post("/v1/sessions", json={
                "properties": {"execution.runtime-mode": "BATCH"}
            })
            sess_resp.raise_for_status()
            session_handle = sess_resp.json()["sessionHandle"]
            job["session_handle"] = session_handle

            # 2. Submit the statement
            stmt_resp = self._gw_client.post(
                f"/v1/sessions/{session_handle}/statements",
                json={"statement": sql},
            )
            stmt_resp.raise_for_status()
            operation_handle = stmt_resp.json()["operationHandle"]
            job["operation_handle"] = operation_handle

            # 3. Poll until complete
            while True:
                status_resp = self._gw_client.get(
                    f"/v1/sessions/{session_handle}/operations/{operation_handle}/status"
                )
                status_resp.raise_for_status()
                op_status = status_resp.json().get("status", {}).get("statusCode", "RUNNING")
                if op_status in ("FINISHED", "ERROR", "CLOSED", "CANCELED"):
                    break
                time.sleep(1)

            if op_status == "FINISHED":
                # Fetch results
                results_resp = self._gw_client.get(
                    f"/v1/sessions/{session_handle}/operations/{operation_handle}/result/0"
                )
                if results_resp.status_code == 200:
                    data = results_resp.json()
                    job["row_count"] = len(data.get("results", {}).get("data", []))
                job["status"] = "SUCCESS"
            else:
                job["status"] = "FAILED"
                job["error"] = f"SQL Gateway operation ended with status: {op_status}"

        except Exception as exc:
            job["status"] = "FAILED"
            job["error"] = str(exc)
            job["logs"].append(f"Flink SQL error: {exc}")
        finally:
            job["end_time"] = time.time()

    def _run_jar(self, job_handle: str, jar_path_or_class: str) -> None:
        """Submit a JAR job to Flink JobManager."""
        job = self._jobs[job_handle]
        try:
            # Upload JAR if it's a file path
            if jar_path_or_class.endswith(".jar"):
                with open(jar_path_or_class, "rb") as f:
                    upload_resp = self._jm_client.post(
                        "/jars/upload",
                        files={"jarfile": (jar_path_or_class, f, "application/java-archive")},
                    )
                    upload_resp.raise_for_status()
                    jar_id = upload_resp.json()["filename"].split("/")[-1]
            else:
                jar_id = jar_path_or_class  # treat as already-uploaded jar ID

            # Run the JAR
            run_resp = self._jm_client.post(f"/jars/{jar_id}/run", json={})
            run_resp.raise_for_status()
            flink_job_id = run_resp.json()["jobid"]
            job["flink_job_id"] = flink_job_id
            job["logs"].append(f"Flink job submitted: {flink_job_id}")
            job["status"] = "SUCCESS"

        except Exception as exc:
            job["status"] = "FAILED"
            job["error"] = str(exc)
            job["logs"].append(f"Flink JAR error: {exc}")
        finally:
            job["end_time"] = time.time()
