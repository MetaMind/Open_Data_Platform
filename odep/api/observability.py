"""OpenTelemetry tracing and Prometheus metrics for ODEP."""
from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Initialize OTel tracer
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(_provider)
tracer = trace.get_tracer("odep")

# Prometheus metrics
pipeline_runs_total = Counter(
    "odep_pipeline_runs_total",
    "Total number of pipeline runs",
    ["pipeline_name", "status"],
)

pipeline_runtime_ms = Histogram(
    "odep_pipeline_runtime_ms",
    "Pipeline run duration in milliseconds",
    ["pipeline_name"],
    buckets=[100, 500, 1000, 5000, 10000, 30000, 60000],
)

pipeline_rows_processed = Histogram(
    "odep_pipeline_rows_processed",
    "Number of rows processed per pipeline run",
    ["pipeline_name"],
    buckets=[100, 1000, 10000, 100000, 1000000],
)

pipeline_quality_score = Gauge(
    "odep_pipeline_quality_score",
    "Latest quality score for a pipeline",
    ["pipeline_name"],
)


def record_pipeline_metrics(
    pipeline_name: str,
    status: str,
    runtime_ms: int,
    rows_processed: int,
    quality_score: float,
) -> None:
    """Record metrics for a completed pipeline run."""
    pipeline_runs_total.labels(pipeline_name=pipeline_name, status=status).inc()
    pipeline_runtime_ms.labels(pipeline_name=pipeline_name).observe(runtime_ms)
    pipeline_rows_processed.labels(pipeline_name=pipeline_name).observe(rows_processed)
    pipeline_quality_score.labels(pipeline_name=pipeline_name).set(quality_score)


def trace_pipeline_operation(operation_name: str):
    """Context manager for tracing a pipeline operation."""
    return tracer.start_as_current_span(operation_name)


__all__ = [
    "tracer",
    "record_pipeline_metrics",
    "trace_pipeline_operation",
    "generate_latest",
    "CONTENT_TYPE_LATEST",
]
