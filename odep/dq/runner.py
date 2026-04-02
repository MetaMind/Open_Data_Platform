"""DQ Runner — runs a QualitySuite and persists results to the metadata catalog."""

from __future__ import annotations

from typing import Any, Optional

from odep.dq.engine import NativeQualityEngine
from odep.dq.models import QualitySuite, SuiteResult
from odep.exceptions import QualityGateFailure


def run_quality_suite(
    suite: QualitySuite,
    data: Any,
    metadata_adapter: Optional[Any] = None,
    engine: Optional[Any] = None,
    raise_on_blocking: bool = True,
) -> SuiteResult:
    """Evaluate a QualitySuite against data and optionally persist results.

    Args:
        suite: The QualitySuite to evaluate.
        data: pandas DataFrame, list of dicts, or DuckDB result.
        metadata_adapter: Optional MetadataService — if provided, each check
            result is persisted via record_quality_check().
        engine: Optional DataQualityEngine — defaults to NativeQualityEngine.
        raise_on_blocking: If True (default), raises QualityGateFailure when
            any blocking rule fails.

    Returns:
        SuiteResult with all check results and aggregate metrics.

    Raises:
        QualityGateFailure: if raise_on_blocking=True and a blocking rule fails.
    """
    dq_engine = engine or NativeQualityEngine()
    result = dq_engine.run_suite(suite, data)

    # Persist each check result to the metadata catalog
    if metadata_adapter is not None:
        for check in result.results:
            metadata_adapter.record_quality_check(
                urn=check.dataset_urn or suite.dataset_urn,
                check_name=check.rule_name,
                passed=check.passed,
                metrics={
                    **check.metrics,
                    "rule_type": check.rule_type.value,
                    "severity": check.severity.value,
                    "quality_score": result.quality_score,
                },
            )

    if raise_on_blocking and result.has_blocking_failures:
        failing = [r for r in result.results if not r.passed and r.is_blocking]
        first = failing[0]
        raise QualityGateFailure(
            first.rule_name,
            {
                "quality_score": result.quality_score,
                "blocking_failures": result.blocking_failures,
                "suite": suite.name,
                "dataset_urn": suite.dataset_urn,
                "metrics": first.metrics,
                "error": first.error_message,
            },
        )

    return result
