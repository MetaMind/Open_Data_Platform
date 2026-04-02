"""execute_with_quality_gate — Algorithm 3 from the ODEP design document."""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from odep.config import OdepConfig
from odep.exceptions import PipelineExecutionError, QualityGateFailure
from odep.factory import get_execution_adapter, get_metadata_adapter, get_orchestrator_adapter
from odep.models import JobResult, JobStatus


def execute_with_quality_gate(
    job_id: str,
    run_conf: Optional[Dict[str, Any]] = None,
    config: Optional[OdepConfig] = None,
) -> JobResult:
    """Trigger a job, poll for completion, evaluate quality gates, and return the result.

    Implements Algorithm 3 from the design document.

    Raises:
        TimeoutError: if the job exceeds the configured timeout.
        PipelineExecutionError: if the orchestrator run reaches FAILED status.
        QualityGateFailure: if a blocking quality rule evaluates to False.
    """
    if config is None:
        config = OdepConfig()

    orchestrator = get_orchestrator_adapter(config.orchestration.engine, config.orchestration)
    execution = get_execution_adapter(config.execution.default_engine, config.execution)
    metadata = get_metadata_adapter(config.metadata.engine, config.metadata)

    run_id = orchestrator.trigger_job(job_id, conf=run_conf or {})

    start = time.time()
    timeout_sec = 3600
    poll_interval = 5

    while True:
        status = orchestrator.get_status(run_id)
        if status not in (JobStatus.RUNNING, JobStatus.PENDING):
            break
        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Job {job_id!r} run {run_id!r} exceeded timeout")
        time.sleep(poll_interval)

    if status == JobStatus.FAILED:
        logs = orchestrator.get_logs(run_id)
        raise PipelineExecutionError(run_id, logs)

    result = execution.wait_for_completion(run_id)

    quality_rules = result.metrics.get("quality_rules", [])
    for quality_rule in quality_rules:
        passed = quality_rule.get("passed", True)
        dataset_urn = quality_rule.get("dataset_urn", "")
        rule_name = quality_rule.get("name", "unknown")
        is_blocking = quality_rule.get("is_blocking", True)
        metadata.record_quality_check(dataset_urn, rule_name, passed, result.metrics)
        if not passed and is_blocking:
            raise QualityGateFailure(rule_name, result.metrics)

    return result


__all__ = ["execute_with_quality_gate"]
