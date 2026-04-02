"""MetaMindAdapter — ExecutionEngine implementation backed by MetaMind v2.0."""

from __future__ import annotations

from typing import Any, Dict, Iterator

from odep.adapters.metamind.client import MetaMindClient
from odep.config import MetaMindConfig
from odep.models import JobConfig, JobResult


class MetaMindAdapter:
    """ExecutionEngine implementation that wraps MetaMind v2.0's synchronous REST API."""

    def __init__(self, config: MetaMindConfig) -> None:
        self._client = MetaMindClient(
            base_url=config.metamind_url,
            tenant_id=config.tenant_id,
            api_token=config.api_token,
            timeout=config.timeout,
        )
        self._last_result: Dict[str, Any] = {}  # maps job_handle → last QueryResponse

    def submit(self, config: JobConfig, async_run: bool = False) -> str:
        """POST SQL to MetaMind and return the query_id as job_handle."""
        result = self._client.query(config.code, dry_run=async_run)
        self._last_result[result.query_id] = result
        return result.query_id

    def get_status(self, job_handle: str) -> Dict[str, Any]:
        """Return status dict by checking MetaMind query history."""
        result = self._client.get_history(job_handle)
        if result is not None:
            return {"status": "SUCCESS", "job_handle": job_handle}
        return {"status": "UNKNOWN", "job_handle": job_handle}

    def wait_for_completion(self, job_handle: str, timeout_sec: int = 3600) -> JobResult:
        """Return JobResult from stored synchronous result (MetaMind is synchronous)."""
        result = self._last_result.get(job_handle)
        if result is None:
            raise KeyError(f"No result found for job_handle={job_handle!r}")
        return JobResult(
            success=True,
            records_processed=result.row_count,
            execution_time_ms=int(result.duration_ms),
            metrics={
                "optimization_ms": result.optimization_ms,
                "plan_cost": result.plan_cost,
                "cache_hit": result.cache_hit,
                "backend_used": result.backend_used,
            },
        )

    def cancel(self, job_handle: str) -> bool:
        """Cancel a running MetaMind query."""
        self._client.cancel(job_handle)
        return True

    def get_metrics(self, job_handle: str) -> Dict[str, Any]:
        """Return full MetaMind optimization metrics for a job."""
        result = self._last_result.get(job_handle)
        if result is None:
            return {
                "optimization_tier": None,
                "cache_hit": None,
                "workload_type": None,
                "backend_used": None,
                "optimization_ms": None,
                "plan_cost": None,
                "flags_used": [],
            }
        return {
            "optimization_tier": result.optimization_tier,
            "cache_hit": result.cache_hit,
            "workload_type": result.workload_type,
            "backend_used": result.backend_used,
            "optimization_ms": result.optimization_ms,
            "plan_cost": result.plan_cost,
            "flags_used": result.flags_used,
        }

    def stream_logs(self, job_handle: str) -> Iterator[str]:
        """MetaMind does not support log streaming — yields nothing."""
        return
        yield  # makes this a generator
