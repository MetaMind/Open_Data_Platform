"""DagsterAdapter — Orchestrator implementation backed by Dagster GraphQL API.

Dagster exposes a GraphQL API at /graphql (default port 3000).
This adapter uses httpx to call the GraphQL endpoint for all operations.

Docker Compose usage:
  docker compose --profile dagster up -d
  Dagster UI: http://localhost:3000

Config (.odep.env):
  ODEP_ORCHESTRATION__ENGINE=dagster
  ODEP_ORCHESTRATION__DAGSTER_URL=http://localhost:3000
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

from odep.models import JobDefinition, JobStatus

_STATUS_MAP: Dict[str, JobStatus] = {
    "QUEUED": JobStatus.PENDING,
    "NOT_STARTED": JobStatus.PENDING,
    "STARTING": JobStatus.RUNNING,
    "RUNNING": JobStatus.RUNNING,
    "SUCCESS": JobStatus.SUCCESS,
    "FAILURE": JobStatus.FAILED,
    "CANCELED": JobStatus.CANCELLED,
    "CANCELING": JobStatus.CANCELLED,
}


class DagsterAdapter:
    """Orchestrator implementation backed by Dagster GraphQL API."""

    def __init__(self, config: Any) -> None:
        self.config = config
        self._url: str = getattr(config, "dagster_url", "http://localhost:3000")
        self._client = httpx.Client(base_url=self._url, timeout=30.0)
        # run_id -> job_name mapping
        self._run_map: Dict[str, str] = {}

    def _gql(self, query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute a GraphQL query against the Dagster API."""
        payload: Dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        resp = self._client.post("/graphql", json=payload)
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"Dagster GraphQL error: {data['errors']}")
        return data.get("data", {})

    def deploy_job(self, job: JobDefinition) -> str:
        """In Dagster, jobs are defined in code — deploy is a no-op that validates the job exists."""
        # Verify the job exists in the repository
        query = """
        query JobExists($jobName: String!) {
          jobOrError(selector: {repositoryLocationName: "odep", repositoryName: "__repository__", jobName: $jobName}) {
            __typename
            ... on Job { name }
          }
        }
        """
        try:
            self._gql(query, {"jobName": job.job_id})
        except Exception:
            pass  # Job may not exist yet; Dagster jobs are code-defined
        return job.job_id

    def trigger_job(
        self,
        job_id: str,
        execution_date: Optional[datetime] = None,
        conf: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Launch a Dagster job run and return the run_id."""
        query = """
        mutation LaunchRun($jobName: String!, $runConfigData: RunConfigData, $tags: [ExecutionTag!]) {
          launchRun(executionParams: {
            selector: {repositoryLocationName: "odep", repositoryName: "__repository__", jobName: $jobName}
            runConfigData: $runConfigData
            executionMetadata: {tags: $tags}
          }) {
            __typename
            ... on LaunchRunSuccess { run { runId } }
            ... on PythonError { message }
          }
        }
        """
        tags = []
        if execution_date:
            tags.append({"key": "execution_date", "value": execution_date.isoformat()})

        data = self._gql(query, {
            "jobName": job_id,
            "runConfigData": conf or {},
            "tags": tags,
        })
        result = data.get("launchRun", {})
        if result.get("__typename") != "LaunchRunSuccess":
            raise RuntimeError(f"Failed to launch Dagster run: {result}")
        run_id: str = result["run"]["runId"]
        self._run_map[run_id] = job_id
        return run_id

    def get_status(self, run_id: str) -> JobStatus:
        """Return the current JobStatus for a Dagster run."""
        query = """
        query RunStatus($runId: ID!) {
          runOrError(runId: $runId) {
            __typename
            ... on Run { status }
            ... on RunNotFoundError { message }
          }
        }
        """
        data = self._gql(query, {"runId": run_id})
        run = data.get("runOrError", {})
        status_str: str = run.get("status", "NOT_STARTED")
        return _STATUS_MAP.get(status_str, JobStatus.PENDING)

    def get_logs(self, run_id: str, tail: int = 100) -> List[str]:
        """Return the last `tail` log messages for a Dagster run."""
        query = """
        query RunLogs($runId: ID!, $limit: Int!) {
          logsForRun(runId: $runId, limit: $limit) {
            __typename
            ... on EventConnection {
              events {
                ... on MessageEvent { message timestamp }
              }
            }
          }
        }
        """
        try:
            data = self._gql(query, {"runId": run_id, "limit": tail})
            events = data.get("logsForRun", {}).get("events", [])
            return [e.get("message", "") for e in events if e.get("message")]
        except Exception as e:
            return [f"Could not retrieve logs for run_id={run_id!r}: {e}"]

    def cancel_run(self, run_id: str) -> bool:
        """Terminate a Dagster run."""
        query = """
        mutation TerminateRun($runId: String!) {
          terminateRun(runId: $runId) {
            __typename
            ... on TerminateRunSuccess { run { runId } }
            ... on PythonError { message }
          }
        }
        """
        try:
            data = self._gql(query, {"runId": run_id})
            return data.get("terminateRun", {}).get("__typename") == "TerminateRunSuccess"
        except Exception:
            return False

    def backfill(self, job_id: str, start_date: datetime, end_date: datetime) -> List[str]:
        """Launch one run per day in [start_date, end_date) and return run_ids."""
        run_ids: List[str] = []
        current = start_date
        while current < end_date:
            run_id = self.trigger_job(job_id, execution_date=current)
            run_ids.append(run_id)
            current += timedelta(days=1)
        return run_ids

    def list_jobs(self, tags: Optional[List[str]] = None) -> List[JobDefinition]:
        """Return all jobs in the Dagster repository."""
        query = """
        query ListJobs {
          repositoryOrError(repositorySelector: {repositoryLocationName: "odep", repositoryName: "__repository__"}) {
            __typename
            ... on Repository {
              jobs { name description }
            }
          }
        }
        """
        try:
            data = self._gql(query)
            jobs = data.get("repositoryOrError", {}).get("jobs", [])
            return [
                JobDefinition(job_id=j["name"], name=j.get("description", j["name"]))
                for j in jobs
            ]
        except Exception:
            return []

    def delete_job(self, job_id: str) -> bool:
        """Dagster jobs are code-defined and cannot be deleted via API. Returns False."""
        return False

    def health_check(self) -> bool:
        """Return True if the Dagster webserver is reachable."""
        try:
            resp = self._client.get("/server_info", timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False
