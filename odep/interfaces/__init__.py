"""ODEP Protocol interfaces for MetadataService, Orchestrator, and ExecutionEngine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional
from typing import runtime_checkable, Protocol
from datetime import datetime

if TYPE_CHECKING:
    from odep.models import (
        DatasetMetadata,
        LineageEdge,
        JobDefinition,
        JobConfig,
        JobResult,
        JobStatus,
    )


@runtime_checkable
class MetadataService(Protocol):
    """Unified interface for catalog, lineage, quality, and governance operations."""

    # Catalog
    def register_dataset(self, dataset: "DatasetMetadata") -> str:
        """Persist a dataset and return its canonical URN."""
        ...

    def get_dataset(self, urn: str) -> "Optional[DatasetMetadata]":
        """Return the DatasetMetadata for the given URN, or None if not found."""
        ...

    def search_catalog(
        self, query: str, filters: Optional[Dict[str, Any]] = None
    ) -> "List[DatasetMetadata]":
        """Return datasets whose name, description, or tags match the query."""
        ...

    def delete_dataset(self, urn: str) -> bool:
        """Soft-delete a dataset by URN. Returns True if found, False otherwise."""
        ...

    # Lineage
    def create_lineage(self, edges: "List[LineageEdge]") -> None:
        """Persist a list of lineage edges in the lineage graph."""
        ...

    def get_upstream(self, urn: str, depth: int = 1) -> "List[LineageEdge]":
        """Return all LineageEdge objects reachable within *depth* hops upstream."""
        ...

    def get_downstream(self, urn: str, depth: int = 1) -> "List[LineageEdge]":
        """Return all LineageEdge objects reachable within *depth* hops downstream."""
        ...

    # Quality
    def record_quality_check(
        self,
        urn: str,
        check_name: str,
        passed: bool,
        metrics: Dict[str, float],
    ) -> None:
        """Persist a quality check result associated with the given URN."""
        ...

    def get_quality_score(self, urn: str) -> float:
        """Return quality score in [0.0, 100.0] as (passed / total) * 100."""
        ...

    # Governance
    def apply_tag(self, urn: str, tag: str) -> None:
        """Associate a governance tag with the dataset identified by URN."""
        ...

    def check_access(self, user: str, urn: str, action: str) -> bool:
        """Return True if *user* has permission to perform *action* on *urn*."""
        ...


@runtime_checkable
class Orchestrator(Protocol):
    """Abstract interface for pipeline scheduling, triggering, monitoring, and backfill."""

    def deploy_job(self, job: "JobDefinition") -> str:
        """Register a job in the underlying engine and return a job_id."""
        ...

    def trigger_job(
        self,
        job_id: str,
        execution_date: Optional[datetime] = None,
        conf: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Enqueue a run for *job_id* and return a run_id."""
        ...

    def get_status(self, run_id: str) -> "JobStatus":
        """Return the current JobStatus for the given run_id."""
        ...

    def get_logs(self, run_id: str, tail: int = 100) -> List[str]:
        """Return the last *tail* log lines for the given run_id."""
        ...

    def cancel_run(self, run_id: str) -> bool:
        """Cancel a running job. Returns True on success."""
        ...

    def backfill(
        self, job_id: str, start_date: datetime, end_date: datetime
    ) -> List[str]:
        """Enqueue one run per scheduled interval in [start_date, end_date).

        Returns run_id values in chronological order.
        """
        ...

    def list_jobs(self, tags: Optional[List[str]] = None) -> "List[JobDefinition]":
        """Return all deployed job definitions, optionally filtered by tags."""
        ...

    def delete_job(self, job_id: str) -> bool:
        """Remove a job from the orchestrator. Returns True if found."""
        ...

    def health_check(self) -> bool:
        """Return True if the orchestrator is reachable, False otherwise."""
        ...


@runtime_checkable
class ExecutionEngine(Protocol):
    """Abstract interface for submitting and monitoring compute jobs."""

    def submit(self, config: "JobConfig", async_run: bool = False) -> str:
        """Submit a job and return a job_handle.

        If async_run=False, blocks until completion before returning.
        If async_run=True, returns immediately with a handle for polling.
        """
        ...

    def get_status(self, job_handle: str) -> Dict[str, Any]:
        """Return a status dict for the given job_handle."""
        ...

    def wait_for_completion(
        self, job_handle: str, timeout_sec: int = 3600
    ) -> "JobResult":
        """Block until the job completes or timeout_sec is exceeded.

        Raises TimeoutError if the job does not finish within timeout_sec.
        """
        ...

    def stream_logs(self, job_handle: str) -> Iterator[str]:
        """Yield log lines from the running or completed job."""
        ...

    def cancel(self, job_handle: str) -> bool:
        """Cancel a running job. Returns True on success."""
        ...

    def get_metrics(self, job_handle: str) -> Dict[str, Any]:
        """Return execution metrics for the given job_handle."""
        ...


__all__ = ["MetadataService", "Orchestrator", "ExecutionEngine"]
