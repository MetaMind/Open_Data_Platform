"""TemporalAdapter — Orchestrator implementation backed by Temporal.

Temporal uses gRPC natively, but also exposes an HTTP API via the
Temporal Web UI and the temporal-http-bridge. This adapter uses the
Temporal Python SDK (temporalio) for workflow execution.

Docker Compose usage:
  docker compose --profile temporal up -d
  Temporal UI: http://localhost:8233

Config (.odep.env):
  ODEP_ORCHESTRATION__ENGINE=temporal
  ODEP_ORCHESTRATION__TEMPORAL_HOST=localhost:7233
  ODEP_ORCHESTRATION__TEMPORAL_NAMESPACE=default
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from odep.models import JobDefinition, JobStatus

_TEMPORAL_AVAILABLE = False
try:
    import temporalio  # noqa: F401
    _TEMPORAL_AVAILABLE = True
except ImportError:
    pass

_STATUS_MAP: Dict[str, JobStatus] = {
    "WORKFLOW_EXECUTION_STATUS_RUNNING": JobStatus.RUNNING,
    "WORKFLOW_EXECUTION_STATUS_COMPLETED": JobStatus.SUCCESS,
    "WORKFLOW_EXECUTION_STATUS_FAILED": JobStatus.FAILED,
    "WORKFLOW_EXECUTION_STATUS_CANCELED": JobStatus.CANCELLED,
    "WORKFLOW_EXECUTION_STATUS_TERMINATED": JobStatus.CANCELLED,
    "WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW": JobStatus.RUNNING,
    "WORKFLOW_EXECUTION_STATUS_TIMED_OUT": JobStatus.FAILED,
}


class TemporalAdapter:
    """Orchestrator implementation backed by Temporal workflow engine.

    Uses the temporalio Python SDK for workflow execution.
    Install: pip install temporalio
    """

    def __init__(self, config: Any) -> None:
        self.config = config
        self._host: str = getattr(config, "temporal_host", "localhost:7233")
        self._namespace: str = getattr(config, "temporal_namespace", "default")
        self._task_queue: str = getattr(config, "temporal_task_queue", "odep-task-queue")
        self._run_map: Dict[str, str] = {}  # run_id -> workflow_id

    def _require_sdk(self) -> None:
        if not _TEMPORAL_AVAILABLE:
            raise RuntimeError(
                "temporalio is not installed. Run: pip install temporalio"
            )

    def _run_async(self, coro):
        """Run an async coroutine from sync context."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(asyncio.run, coro)
                    return future.result()
            return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)

    async def _get_client(self):
        from temporalio.client import Client
        return await Client.connect(self._host, namespace=self._namespace)

    def deploy_job(self, job: JobDefinition) -> str:
        """In Temporal, workflows are code-defined. Deploy registers the job_id."""
        # Temporal workflows are registered via workers — no API call needed
        return job.job_id

    def trigger_job(
        self,
        job_id: str,
        execution_date: Optional[datetime] = None,
        conf: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Start a Temporal workflow execution and return the workflow run_id."""
        self._require_sdk()

        async def _start():
            from temporalio.client import Client
            client = await Client.connect(self._host, namespace=self._namespace)
            workflow_id = f"{job_id}-{uuid.uuid4().hex[:8]}"
            handle = await client.start_workflow(
                job_id,
                conf or {},
                id=workflow_id,
                task_queue=self._task_queue,
            )
            return handle.id, handle.result_run_id

        workflow_id, run_id = self._run_async(_start())
        self._run_map[run_id] = workflow_id
        return run_id

    def get_status(self, run_id: str) -> JobStatus:
        """Return the current JobStatus for a Temporal workflow run."""
        self._require_sdk()
        workflow_id = self._run_map.get(run_id, run_id)

        async def _status():
            from temporalio.client import Client
            client = await Client.connect(self._host, namespace=self._namespace)
            handle = client.get_workflow_handle(workflow_id, run_id=run_id)
            desc = await handle.describe()
            return str(desc.status)

        try:
            status_str = self._run_async(_status())
            return _STATUS_MAP.get(status_str, JobStatus.PENDING)
        except Exception:
            return JobStatus.PENDING

    def get_logs(self, run_id: str, tail: int = 100) -> List[str]:
        """Return workflow history events as log lines."""
        self._require_sdk()
        workflow_id = self._run_map.get(run_id, run_id)

        async def _logs():
            from temporalio.client import Client
            client = await Client.connect(self._host, namespace=self._namespace)
            handle = client.get_workflow_handle(workflow_id, run_id=run_id)
            lines = []
            async for event in handle.fetch_history_events():
                lines.append(f"{event.event_type}: {event.event_id}")
            return lines[-tail:]

        try:
            return self._run_async(_logs())
        except Exception as e:
            return [f"Could not retrieve logs for run_id={run_id!r}: {e}"]

    def cancel_run(self, run_id: str) -> bool:
        """Cancel a running Temporal workflow."""
        self._require_sdk()
        workflow_id = self._run_map.get(run_id, run_id)

        async def _cancel():
            from temporalio.client import Client
            client = await Client.connect(self._host, namespace=self._namespace)
            handle = client.get_workflow_handle(workflow_id, run_id=run_id)
            await handle.cancel()

        try:
            self._run_async(_cancel())
            return True
        except Exception:
            return False

    def backfill(self, job_id: str, start_date: datetime, end_date: datetime) -> List[str]:
        """Launch one workflow per day in [start_date, end_date) and return run_ids."""
        run_ids: List[str] = []
        current = start_date
        while current < end_date:
            run_id = self.trigger_job(job_id, execution_date=current)
            run_ids.append(run_id)
            current += timedelta(days=1)
        return run_ids

    def list_jobs(self, tags: Optional[List[str]] = None) -> List[JobDefinition]:
        """List open workflow executions in the namespace."""
        self._require_sdk()

        async def _list():
            from temporalio.client import Client
            client = await Client.connect(self._host, namespace=self._namespace)
            results = []
            async for wf in client.list_workflows():
                results.append(JobDefinition(
                    job_id=wf.id,
                    name=wf.workflow_type,
                ))
            return results

        try:
            return self._run_async(_list())
        except Exception:
            return []

    def delete_job(self, job_id: str) -> bool:
        """Temporal workflows cannot be deleted via API — terminate instead."""
        return False

    def health_check(self) -> bool:
        """Return True if the Temporal server is reachable."""
        if not _TEMPORAL_AVAILABLE:
            return False

        async def _health():
            from temporalio.client import Client
            await Client.connect(self._host, namespace=self._namespace)
            return True

        try:
            return self._run_async(_health())
        except Exception:
            return False
