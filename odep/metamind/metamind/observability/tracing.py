"""
Tracing - OpenTelemetry Tracing

File: metamind/observability/tracing.py
Role: Observability Engineer
Phase: 1
Dependencies: opentelemetry

Distributed tracing for query execution.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any, Generator

logger = logging.getLogger(__name__)


class TracingManager:
    """
    OpenTelemetry tracing manager.
    
    Provides distributed tracing for query execution.
    """
    
    def __init__(self, enabled: bool = True, sampling_rate: float = 0.1):
        """
        Initialize tracing manager.
        
        Args:
            enabled: Whether tracing is enabled
            sampling_rate: Trace sampling rate (0-1)
        """
        self.enabled = enabled
        self.sampling_rate = sampling_rate
        self._tracer: Optional[Any] = None
        
        if enabled:
            try:
                from opentelemetry import trace
                from opentelemetry.sdk.trace import TracerProvider
                from opentelemetry.sdk.trace.export import BatchSpanProcessor
                from opentelemetry.sdk.resources import Resource
                
                resource = Resource.create({"service.name": "metamind"})
                provider = TracerProvider(resource=resource)
                
                # Add console exporter for development
                from opentelemetry.sdk.trace.export import ConsoleSpanExporter
                processor = BatchSpanProcessor(ConsoleSpanExporter())
                provider.add_span_processor(processor)
                
                trace.set_tracer_provider(provider)
                self._tracer = trace.get_tracer("metamind")
                
                logger.debug("TracingManager initialized")
            except ImportError:
                logger.warning("OpenTelemetry not installed, tracing disabled")
                self.enabled = False
    
    @contextmanager
    def start_span(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Generator[Optional[Any], None, None]:
        """
        Start a new span.
        
        Args:
            name: Span name
            attributes: Span attributes
            
        Yields:
            Span context or None
        """
        if not self.enabled or self._tracer is None:
            yield None
            return
        
        with self._tracer.start_as_current_span(name) as span:
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, value)
            yield span
    
    def add_event(
        self,
        span: Any,
        name: str,
        attributes: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Add event to span.
        
        Args:
            span: Span context
            name: Event name
            attributes: Event attributes
        """
        if span is not None:
            span.add_event(name, attributes)
    
    def set_attribute(
        self,
        span: Any,
        key: str,
        value: Any
    ) -> None:
        """
        Set span attribute.
        
        Args:
            span: Span context
            key: Attribute key
            value: Attribute value
        """
        if span is not None:
            span.set_attribute(key, value)
    
    def record_exception(
        self,
        span: Any,
        exception: Exception
    ) -> None:
        """
        Record exception in span.
        
        Args:
            span: Span context
            exception: Exception to record
        """
        if span is not None:
            span.record_exception(exception)


# Global tracing instance
_tracing: Optional[TracingManager] = None


def get_tracing(
    enabled: bool = True,
    sampling_rate: float = 0.1
) -> TracingManager:
    """Get global tracing manager."""
    global _tracing
    if _tracing is None:
        _tracing = TracingManager(enabled=enabled, sampling_rate=sampling_rate)
    return _tracing
