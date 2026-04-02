"""PrefectAdapter — Orchestrator implementation backed by Prefect REST API.

Prefect 2/3 exposes a REST API at /api (default port 4200).
This adapter uses httpx to call the Prefect API for all operations.

Docker Compose usage:
  docker compose --profile prefect up -d
  Prefect UI: http://localhost:4200

Config (.odep.env):
  ODEP_ORCHESTRATION__ENGINE=prefect
  ODEP_ORCHESTRATION__PREFECT_URL=http://localhost:4200
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

from odep.models import JobDefinition, JobStatus

_STATUS_MAP: Dict[str, JobStatus] = {
    "Scheduled": JobStatus.PENDING,
    "Pending": JobStatus.PENDING,
    "Running": JobStatus.RUNNING,
    "Completed": JobStatus.SUCCESS,
    "Failed": JobStatus.FAILED,
    "Crashed": JobStatus.FAILED,
    "Cancelling": JobStatus.CANCELLED,
    "Cancelled": JobStatus.CANCELLED,
    "Paused": JobStatus.PENDING,
}


class PrefectAdapter:
    """Orchestrator implementation backed by Prefect REST API."""

    def __init__(self, config: Any) -> None:
        self.config = config
        self._url: str = getattr(config, "prefect_url", "http://localhost:4200")
        self._client = httpx.Client(
            base_url=self._url,
            headers={"Content-Type": "application/json"},
            timeout=30.0,
        )
        self._run_map: Dict[str, str] = {}  # flow_run_id -> deployment_name

    def deploy_job(self, job: JobDefinition) -> str:
        """Create a Prefect deployment for the given job."""
        payload = {
            "name": job.job_id,
            "flow_name": job.name,
            "schedule": {"cron": job.schedule} if job.schedule else None,
            "tags": [],
            "parameter_openapi_schema": {},
        }
        resp = self._client.post("/api/deployments/", json=payload)
        if resp.is_error:
            raise RuntimeError(f"Failed to create Prefect deployment {job.job_id!r}: {resp.text}")
        return job.job_id

    def trigger_job(
        self,
        job_id: str,
        execution_date: Optional[datetime] = None,
        conf: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Create a flow run for the given deployment and return the flow_run_id."""
        # First find the deployment by name
        resp = self._client.post(
            "/api/deployments/filter",
            json={"deployments": {"name": {"any_": [job_id]}}},
        )
        resp.raise_for_status()
        deployments = resp.json()
        if not deployments:
            raise RuntimeError(f"Prefect deployment {job_id!r} not found")
        deployment_id = deployments[0]["id"]

        # Create a flow run
        payload: Dict[str, Any] = {"parameters": conf or {}}
        if execution_date:
            payload["scheduled_start_time"] = execution_date.isoformat()

        run_resp = self._client.post(
            f"/api/deployments/{deployment_id}/create_flow_run",
            json=payload,
        )
        run_resp.raise_for_status()
        flow_run_id: str = run_resp.json()["id"]
        self._run_map[flow_run_id] = job_id
        return flow_run_id

    def get_status(self, run_id: str) -> JobStatus:
        """Return the current JobStatus for a Prefect flow run."""
        resp = self._client.get(f"/api/flow_runs/{run_id}")
        resp.raise_for_status()
        state_name: str = resp.json().get("state", {}).get("name", "Pending")
        return _STATUS_MAP.get(state_name, JobStatus.PENDING)

    def get_logs(self, run_id: str, tail: int = 100) -> List[str]:
        """Return the last `tail` log lines for a Prefect flow run."""
        try:
            resp = self._client.post(
                "/api/logs/filter",
                json={
                    "logs": {"flow_run_id": {"any_": [run_id]}},
                    "sort": "TIMESTAMP_DESC",
                    "limit": tail,
                },
            )
            resp.raise_for_status()
            logs = resp.json()
            lines = [entry.get("message", "") for entry in reversed(logs)]
            return lines
        except Exception as e:
            return [f"Could not retrieve logs for run_id={run_id!r}: {e}"]

    def cancel_run(self, run_id: str) -> bool:
        """Cancel a Prefect flow run."""
        try:
            resp = self._client.post(f"/api/flow_runs/{run_id}/set_state", json={
                "state": {"type": "CANCELLED", "name": "Cancelled"},
                "force": True,
            })
            return resp.status_code in (200, 201)
        except Exception:
            return False

    def backfill(self, job_id: str, start_date: datetime, end_date: datetime) -> List[str]:
        """Launch one flow run per day in [start_date, end_date) and return run_ids."""
        run_ids: List[str] = []
        current = start_date
        while current < end_date:
            run_id = self.trigger_job(job_id, execution_date=current)
            run_ids.append(run_id)
            current += timedelta(days=1)
        return run_ids

    def list_jobs(self, tags: Optional[List[str]] = None) -> List[JobDefinition]:
        """Return all Prefect deployments as JobDefinition objects."""
        try:
            payload: Dict[str, Any] = {}
            if tags:
                payload["deployments"] = {"tags": {"all_": tags}}
            resp = self._client.post("/api/deployments/filter", json=payload)
            resp.raise_for_status()
            return [
                JobDefinition(
                    job_id=d["name"],
                    name=d.get("description", d["name"]),
                    schedule=d.get("schedule", {}).get("cron") if d.get("schedule") else None,
                )
                for d in resp.json()
            ]
        except Exception:
            return []

    def delete_job(self, job_id: str) -> bool:
        """Delete a Prefect deployment by name."""
        try:
            resp = self._client.post(
                "/api/deployments/filter",
                json={"deployments": {"name": {"any_": [job_id]}}},
            )
            resp.raise_for_status()
            deployments = resp.json()
            if not deployments:
                return False
            deployment_id = deployments[0]["id"]
            del_resp = self._client.delete(f"/api/deployments/{deployment_id}")
            return del_resp.status_code in (200, 204)
        except Exception:
            return False

    def health_check(self) -> bool:
        """Return True if the Prefect API server is reachable."""
        try:
            resp = self._client.get("/api/health", timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False
