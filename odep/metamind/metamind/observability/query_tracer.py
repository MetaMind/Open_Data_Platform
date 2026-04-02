"""
Query Tracer - Cross-Engine Query Tracing

File: metamind/observability/query_tracer.py
Role: Observability Engineer
Phase: 1
Dependencies: OpenTelemetry

Implements query-level tracing across all execution engines.
"""

from __future__ import annotations

import logging
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, Any, List, Optional, Generator

logger = logging.getLogger(__name__)


@dataclass
class QuerySpan:
    """A span in a query trace."""
    span_id: str
    parent_id: Optional[str]
    trace_id: str
    
    name: str
    engine: str  # oracle, trino, spark, metamind
    
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: int = 0
    
    # Span details
    operation: str  # parse, route, execute, transform
    sql: Optional[str] = None
    rows_processed: int = 0
    bytes_processed: int = 0
    
    # Status
    status: str = "running"  # running, success, error
    error_message: Optional[str] = None
    
    # Attributes
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None
        }


@dataclass
class QueryTrace:
    """Complete trace of a query execution."""
    trace_id: str
    query_id: str
    tenant_id: str
    user_id: str
    original_sql: str
    
    start_time: datetime
    end_time: Optional[datetime] = None
    total_duration_ms: int = 0
    
    spans: List[QuerySpan] = field(default_factory=list)
    
    # Final result
    target_engine: Optional[str] = None
    execution_strategy: Optional[str] = None
    row_count: int = 0
    
    # Status
    status: str = "running"
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "spans": [s.to_dict() for s in self.spans]
        }


class QueryTracer:
    """
    Query tracer for cross-engine tracing.
    
    Traces query execution across:
    - MetaMind router
    - Oracle
    - Trino
    - Spark
    """
    
    def __init__(self, enabled: bool = True):
        """
        Initialize query tracer.
        
        Args:
            enabled: Whether tracing is enabled
        """
        self.enabled = enabled
        self._active_traces: Dict[str, QueryTrace] = {}
        self._active_spans: Dict[str, QuerySpan] = {}
        logger.debug(f"QueryTracer initialized (enabled={enabled})")
    
    def start_trace(
        self,
        query_id: str,
        tenant_id: str,
        user_id: str,
        sql: str
    ) -> str:
        """
        Start a new query trace.
        
        Args:
            query_id: Query identifier
            tenant_id: Tenant identifier
            user_id: User identifier
            sql: Original SQL
            
        Returns:
            Trace ID
        """
        if not self.enabled:
            return ""
        
        trace_id = str(uuid.uuid4())
        
        trace = QueryTrace(
            trace_id=trace_id,
            query_id=query_id,
            tenant_id=tenant_id,
            user_id=user_id,
            original_sql=sql,
            start_time=datetime.now()
        )
        
        self._active_traces[trace_id] = trace
        
        # Start root span
        self.start_span(
            trace_id=trace_id,
            parent_id=None,
            name="query_execution",
            engine="metamind",
            operation="execute"
        )
        
        logger.debug(f"Started trace {trace_id} for query {query_id}")
        return trace_id
    
    def start_span(
        self,
        trace_id: str,
        parent_id: Optional[str],
        name: str,
        engine: str,
        operation: str,
        sql: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Start a new span.
        
        Args:
            trace_id: Trace ID
            parent_id: Parent span ID
            name: Span name
            engine: Engine name
            operation: Operation type
            sql: SQL being executed
            attributes: Additional attributes
            
        Returns:
            Span ID
        """
        if not self.enabled:
            return ""
        
        span_id = str(uuid.uuid4())
        
        span = QuerySpan(
            span_id=span_id,
            parent_id=parent_id,
            trace_id=trace_id,
            name=name,
            engine=engine,
            operation=operation,
            sql=sql[:1000] if sql else None,
            start_time=datetime.now(),
            attributes=attributes or {}
        )
        
        self._active_spans[span_id] = span
        
        # Add to trace
        if trace_id in self._active_traces:
            self._active_traces[trace_id].spans.append(span)
        
        return span_id
    
    def end_span(
        self,
        span_id: str,
        rows_processed: int = 0,
        bytes_processed: int = 0,
        status: str = "success",
        error_message: Optional[str] = None
    ) -> None:
        """
        End a span.
        
        Args:
            span_id: Span ID
            rows_processed: Rows processed
            bytes_processed: Bytes processed
            status: Span status
            error_message: Error message if failed
        """
        if not self.enabled or span_id not in self._active_spans:
            return
        
        span = self._active_spans[span_id]
        span.end_time = datetime.now()
        span.duration_ms = int(
            (span.end_time - span.start_time).total_seconds() * 1000
        )
        span.rows_processed = rows_processed
        span.bytes_processed = bytes_processed
        span.status = status
        span.error_message = error_message
        
        logger.debug(f"Ended span {span_id} ({span.name}) in {span.duration_ms}ms")
    
    def end_trace(
        self,
        trace_id: str,
        target_engine: Optional[str] = None,
        execution_strategy: Optional[str] = None,
        row_count: int = 0,
        status: str = "success",
        error_message: Optional[str] = None
    ) -> QueryTrace:
        """
        End a trace.
        
        Args:
            trace_id: Trace ID
            target_engine: Target engine used
            execution_strategy: Execution strategy
            row_count: Row count
            status: Trace status
            error_message: Error message if failed
            
        Returns:
            Completed trace
        """
        if not self.enabled or trace_id not in self._active_traces:
            return None
        
        trace = self._active_traces[trace_id]
        trace.end_time = datetime.now()
        trace.total_duration_ms = int(
            (trace.end_time - trace.start_time).total_seconds() * 1000
        )
        trace.target_engine = target_engine
        trace.execution_strategy = execution_strategy
        trace.row_count = row_count
        trace.status = status
        trace.error_message = error_message
        
        # End root span
        root_span = next(
            (s for s in trace.spans if s.parent_id is None),
            None
        )
        if root_span:
            self.end_span(root_span.span_id, row_count, status=status)
        
        # Clean up
        del self._active_traces[trace_id]
        
        logger.info(
            f"Trace {trace_id} completed: {trace.total_duration_ms}ms, "
            f"{len(trace.spans)} spans"
        )
        
        return trace
    
    def get_trace(self, trace_id: str) -> Optional[QueryTrace]:
        """Get a trace by ID."""
        return self._active_traces.get(trace_id)
    
    def get_span(self, span_id: str) -> Optional[QuerySpan]:
        """Get a span by ID."""
        return self._active_spans.get(span_id)
    
    @contextmanager
    def trace_span(
        self,
        trace_id: str,
        parent_id: Optional[str],
        name: str,
        engine: str,
        operation: str,
        sql: Optional[str] = None
    ) -> Generator[str, None, None]:
        """
        Context manager for tracing a span.
        
        Args:
            trace_id: Trace ID
            parent_id: Parent span ID
            name: Span name
            engine: Engine name
            operation: Operation type
            sql: SQL being executed
            
        Yields:
            Span ID
        """
        if not self.enabled:
            yield ""
            return
        
        span_id = self.start_span(
            trace_id, parent_id, name, engine, operation, sql
        )
        
        try:
            yield span_id
            self.end_span(span_id, status="success")
        except Exception as e:
            self.end_span(span_id, status="error", error_message=str(e))
            raise


class DistributedTracer:
    """
    Distributed tracer that integrates with OpenTelemetry.
    
    Exports traces to Jaeger, Zipkin, or other OTLP collectors.
    """
    
    def __init__(self, jaeger_endpoint: Optional[str] = None):
        """
        Initialize distributed tracer.
        
        Args:
            jaeger_endpoint: Jaeger collector endpoint
        """
        self.jaeger_endpoint = jaeger_endpoint
        self._tracer: Optional[Any] = None
        
        if jaeger_endpoint:
            try:
                from opentelemetry import trace
                from opentelemetry.exporter.jaeger.thrift import JaegerExporter
                from opentelemetry.sdk.trace import TracerProvider
                from opentelemetry.sdk.trace.export import BatchSpanProcessor
                from opentelemetry.sdk.resources import Resource
                
                resource = Resource.create({"service.name": "metamind"})
                provider = TracerProvider(resource=resource)
                
                jaeger_exporter = JaegerExporter(
                    agent_host_name=jaeger_endpoint.split(":")[0],
                    agent_port=int(jaeger_endpoint.split(":")[1]) if ":" in jaeger_endpoint else 6831
                )
                
                provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
                trace.set_tracer_provider(provider)
                self._tracer = trace.get_tracer("metamind")
                
                logger.info(f"DistributedTracer initialized with Jaeger: {jaeger_endpoint}")
            except ImportError:
                logger.warning("OpenTelemetry Jaeger exporter not installed")
    
    def start_span(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Optional[Any]:
        """Start an OpenTelemetry span."""
        if self._tracer:
            return self._tracer.start_span(name, attributes=attributes)
        return None
    
    def end_span(self, span: Any) -> None:
        """End an OpenTelemetry span."""
        if span:
            span.end()
