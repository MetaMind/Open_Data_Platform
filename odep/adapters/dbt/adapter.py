"""DbtAdapter — ExecutionEngine implementation backed by dbt Core.

Invokes dbt CLI commands (dbt run, dbt test, dbt compile) via subprocess.
The `code` field in JobConfig is treated as the dbt project directory path.

Config (.odep.env):
  ODEP_EXECUTION__DEFAULT_ENGINE=dbt
  ODEP_EXECUTION__DBT_PROJECT_DIR=/path/to/dbt/project
  ODEP_EXECUTION__DBT_PROFILES_DIR=~/.dbt
  ODEP_EXECUTION__DBT_TARGET=dev

Install: pip install dbt-core dbt-duckdb  (or dbt-bigquery, dbt-snowflake, etc.)
"""

from __future__ import annotations

import io
import subprocess
import threading
import time
import uuid
from typing import Any, Dict, Iterator, List, Optional

from odep.models import JobConfig, JobResult


class DbtAdapter:
    """ExecutionEngine implementation that invokes dbt CLI commands.

    JobConfig.code is interpreted as the dbt command to run, e.g.:
      "run"                    -> dbt run
      "run --select my_model"  -> dbt run --select my_model
      "test"                   -> dbt test
      "build"                  -> dbt build
      "compile"                -> dbt compile
    """

    def __init__(self, config: Any) -> None:
        self.config = config
        self._project_dir: str = getattr(config, "dbt_project_dir", ".")
        self._profiles_dir: str = getattr(config, "dbt_profiles_dir", "~/.dbt")
        self._target: str = getattr(config, "dbt_target", "dev")
        self._jobs: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # ExecutionEngine Protocol
    # ------------------------------------------------------------------

    def submit(self, config: JobConfig, async_run: bool = False) -> str:
        """Run a dbt command. config.code is the dbt subcommand (e.g. 'run', 'test')."""
        job_handle = str(uuid.uuid4())
        self._jobs[job_handle] = {
            "status": "PENDING",
            "start_time": time.time(),
            "end_time": None,
            "row_count": 0,
            "error": None,
            "logs": [],
            "returncode": None,
            "thread": None,
        }

        if async_run:
            t = threading.Thread(
                target=self._run_dbt, args=(job_handle, config.code), daemon=True
            )
            self._jobs[job_handle]["thread"] = t
            self._jobs[job_handle]["status"] = "RUNNING"
            t.start()
        else:
            self._jobs[job_handle]["status"] = "RUNNING"
            self._run_dbt(job_handle, config.code)

        return job_handle

    def get_status(self, job_handle: str) -> Dict[str, Any]:
        """Return status dict for the given job_handle."""
        job = self._jobs[job_handle]
        return {"status": job["status"], "job_handle": job_handle}

    def wait_for_completion(self, job_handle: str, timeout_sec: int = 3600) -> JobResult:
        """Block until the dbt command completes or timeout_sec is exceeded."""
        job = self._jobs[job_handle]
        thread = job.get("thread")
        if thread is not None:
            thread.join(timeout=timeout_sec)
            if thread.is_alive():
                self.cancel(job_handle)
                raise TimeoutError(f"dbt job {job_handle!r} exceeded timeout of {timeout_sec}s")

        end_time = job.get("end_time") or time.time()
        execution_time_ms = int((end_time - job["start_time"]) * 1000)

        return JobResult(
            success=job["status"] == "SUCCESS",
            records_processed=job.get("row_count", 0),
            execution_time_ms=execution_time_ms,
            error_message=job.get("error"),
            metrics={
                "backend_used": "dbt",
                "dbt_project_dir": self._project_dir,
                "dbt_target": self._target,
                "returncode": job.get("returncode"),
            },
        )

    def stream_logs(self, job_handle: str) -> Iterator[str]:
        """Yield captured dbt output lines."""
        for line in self._jobs[job_handle]["logs"]:
            yield line

    def cancel(self, job_handle: str) -> bool:
        """Mark the job as cancelled (dbt subprocess cannot be easily killed mid-run)."""
        self._jobs[job_handle]["status"] = "CANCELLED"
        return True

    def get_metrics(self, job_handle: str) -> Dict[str, Any]:
        """Return execution metrics."""
        job = self._jobs[job_handle]
        end_time = job.get("end_time") or time.time()
        return {
            "execution_time_ms": int((end_time - job["start_time"]) * 1000),
            "rows_processed": job.get("row_count", 0),
            "backend_used": "dbt",
            "dbt_project_dir": self._project_dir,
            "dbt_target": self._target,
            "returncode": job.get("returncode"),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_dbt(self, job_handle: str, dbt_command: str) -> None:
        """Invoke dbt CLI and capture output."""
        job = self._jobs[job_handle]
        cmd = [
            "dbt", *dbt_command.split(),
            "--project-dir", self._project_dir,
            "--profiles-dir", self._profiles_dir,
            "--target", self._target,
            "--no-use-colors",
        ]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,
            )
            stdout_lines = result.stdout.splitlines()
            stderr_lines = result.stderr.splitlines()
            job["logs"].extend(stdout_lines)
            job["logs"].extend(stderr_lines)
            job["returncode"] = result.returncode

            # Parse row count from dbt output (e.g. "Completed successfully" or "X of Y models")
            for line in stdout_lines:
                if "of" in line and "model" in line.lower():
                    parts = line.strip().split()
                    for i, p in enumerate(parts):
                        if p.isdigit() and i + 1 < len(parts) and parts[i + 1] in ("of", "models"):
                            job["row_count"] = int(p)
                            break

            if result.returncode == 0:
                job["status"] = "SUCCESS"
            else:
                job["status"] = "FAILED"
                job["error"] = f"dbt exited with code {result.returncode}"

        except subprocess.TimeoutExpired:
            job["status"] = "FAILED"
            job["error"] = "dbt command timed out"
        except FileNotFoundError:
            job["status"] = "FAILED"
            job["error"] = "dbt not found. Install with: pip install dbt-core"
        except Exception as exc:
            job["status"] = "FAILED"
            job["error"] = str(exc)
        finally:
            job["end_time"] = time.time()
