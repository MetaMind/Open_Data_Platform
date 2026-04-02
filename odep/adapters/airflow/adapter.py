"""AirflowAdapter — full implementation of Orchestrator using Airflow REST API v1."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

from odep.config import OrchestrationConfig
from odep.interfaces import Orchestrator
from odep.models import JobDefinition, JobStatus


# Mapping from Airflow DAG run states to ODEP JobStatus
_STATE_MAP: Dict[str, JobStatus] = {
    "queued": JobStatus.PENDING,
    "scheduled": JobStatus.PENDING,
    "running": JobStatus.RUNNING,
    "success": JobStatus.SUCCESS,
    "failed": JobStatus.FAILED,
    "upstream_failed": JobStatus.FAILED,
    "up_for_retry": JobStatus.RETRYING,
    "removed": JobStatus.CANCELLED,
}


class AirflowAdapter:
    """Orchestrator implementation backed by Apache Airflow REST API v1."""

    def __init__(self, config: OrchestrationConfig) -> None:
        self.config = config
        self._client = httpx.Client(
            base_url=config.airflow_url,
            auth=(config.airflow_username, config.airflow_password),
            timeout=30.0,
        )
        # Maps run_id (dag_run_id) -> dag_id for status lookups
        self._dag_run_map: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Orchestrator Protocol methods
    # ------------------------------------------------------------------

    def deploy_job(self, job: JobDefinition) -> str:
        """Register a DAG in Airflow and return the job_id."""
        payload = {
            "dag_id": job.job_id,
            "description": job.name,
            "tags": [],
            "schedule_interval": job.schedule,
            "is_paused_upon_creation": False,
        }
        response = self._client.post("/api/v1/dags", json=payload)
        if response.is_error:
            raise RuntimeError(
                f"Failed to deploy job {job.job_id!r}: {response.text}"
            )
        return job.job_id

    def trigger_job(
        self,
        job_id: str,
        execution_date: Optional[datetime] = None,
        conf: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Trigger a DAG run and return the dag_run_id."""
        payload: Dict[str, Any] = {"conf": conf or {}}
        if execution_date is not None:
            payload["execution_date"] = execution_date.isoformat()

        response = self._client.post(
            f"/api/v1/dags/{job_id}/dagRuns", json=payload
        )
        response.raise_for_status()
        dag_run_id: str = response.json()["dag_run_id"]
        self._dag_run_map[dag_run_id] = job_id
        return dag_run_id

    def get_status(self, run_id: str) -> JobStatus:
        """Return the current JobStatus for a given run_id."""
        dag_id = self._dag_run_map.get(run_id, run_id)
        response = self._client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}"
        )
        response.raise_for_status()
        state: str = response.json().get("state", "")
        return _STATE_MAP.get(state, JobStatus.PENDING)

    def get_logs(self, run_id: str, tail: int = 100) -> List[str]:
        """Return the last `tail` log lines for the first task in the run."""
        dag_id = self._dag_run_map.get(run_id, run_id)
        try:
            # Get task instances for this dag run
            ti_response = self._client.get(
                f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
            )
            ti_response.raise_for_status()
            task_instances = ti_response.json().get("task_instances", [])
            if not task_instances:
                return [f"No task instances found for run_id={run_id!r}"]

            task_id = task_instances[0]["task_id"]

            # Fetch logs for the first task instance, attempt 1
            log_response = self._client.get(
                f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"
            )
            log_response.raise_for_status()
            content = log_response.text
            lines = content.splitlines()
            return lines[-tail:] if len(lines) > tail else lines
        except Exception as e:
            return [f"Could not retrieve logs for run_id={run_id!r}: {e}"]

    def cancel_run(self, run_id: str) -> bool:
        """Cancel a running DAG run by patching its state to 'failed'."""
        dag_id = self._dag_run_map.get(run_id, run_id)
        try:
            response = self._client.patch(
                f"/api/v1/dags/{dag_id}/dagRuns/{run_id}",
                json={"state": "failed"},
            )
            response.raise_for_status()
            return True
        except Exception:
            return False

    def backfill(
        self, job_id: str, start_date: datetime, end_date: datetime
    ) -> List[str]:
        """Enqueue one DAG run per day in [start_date, end_date) and return run_ids."""
        run_ids: List[str] = []
        current = start_date
        while current < end_date:
            payload = {
                "execution_date": current.isoformat(),
                "conf": {},
            }
            response = self._client.post(
                f"/api/v1/dags/{job_id}/dagRuns", json=payload
            )
            response.raise_for_status()
            dag_run_id: str = response.json()["dag_run_id"]
            self._dag_run_map[dag_run_id] = job_id
            run_ids.append(dag_run_id)
            current += timedelta(days=1)
        return run_ids

    def list_jobs(self, tags: Optional[List[str]] = None) -> List[JobDefinition]:
        """Return all DAGs as JobDefinition objects."""
        response = self._client.get("/api/v1/dags")
        response.raise_for_status()
        dags = response.json().get("dags", [])
        return [
            JobDefinition(
                job_id=dag["dag_id"],
                name=dag.get("description", ""),
                schedule=dag.get("schedule_interval"),
            )
            for dag in dags
        ]

    def delete_job(self, job_id: str) -> bool:
        """Delete a DAG. Returns True on success, False if not found."""
        response = self._client.delete(f"/api/v1/dags/{job_id}")
        if response.status_code in (200, 204):
            return True
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return False

    def health_check(self) -> bool:
        """Return True if Airflow metadata database is healthy."""
        try:
            response = self._client.get("/api/v1/health")
            if response.status_code != 200:
                return False
            data = response.json()
            return data.get("metadatabase", {}).get("status") == "healthy"
        except Exception:
            return False
