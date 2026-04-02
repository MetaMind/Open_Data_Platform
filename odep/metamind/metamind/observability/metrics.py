"""
Metrics - Prometheus Metrics Collection

File: metamind/observability/metrics.py
Role: Observability Engineer
Phase: 1
Dependencies: prometheus_client

Collects and exposes Prometheus metrics.
"""

from __future__ import annotations

import logging
from typing import Optional, Dict, Any

from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Prometheus metrics collector for MetaMind.
    
    Tracks:
    - Query counts and latencies
    - Routing decisions
    - Cache performance
    - CDC lag
    - Engine health
    """
    
    def __init__(self):
        """Initialize metrics collector."""
        
        # Query metrics
        self.queries_total = Counter(
            "metamind_queries_total",
            "Total queries processed",
            ["tenant_id", "source", "status", "strategy"]
        )
        
        self.query_duration = Histogram(
            "metamind_query_duration_seconds",
            "Query execution duration",
            ["tenant_id", "source"],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        )
        
        self.routing_decision_duration = Histogram(
            "metamind_routing_decision_seconds",
            "Routing decision latency",
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1]
        )
        
        # Cache metrics
        self.cache_hits = Counter(
            "metamind_cache_hits_total",
            "Cache hits",
            ["tier"]
        )
        
        self.cache_misses = Counter(
            "metamind_cache_misses_total",
            "Cache misses"
        )
        
        self.cache_size = Gauge(
            "metamind_cache_size",
            "Current cache size",
            ["tier"]
        )
        
        # CDC metrics
        self.cdc_lag = Gauge(
            "metamind_cdc_lag_seconds",
            "CDC replication lag",
            ["table_name"]
        )
        
        self.cdc_tables_healthy = Gauge(
            "metamind_cdc_tables_healthy",
            "Number of tables with healthy CDC"
        )
        
        self.cdc_tables_warning = Gauge(
            "metamind_cdc_tables_warning",
            "Number of tables with warning CDC"
        )
        
        self.cdc_tables_critical = Gauge(
            "metamind_cdc_tables_critical",
            "Number of tables with critical CDC"
        )
        
        # Engine metrics
        self.engine_connections = Gauge(
            "metamind_engine_connections",
            "Current connections to engine",
            ["engine"]
        )
        
        self.oracle_circuit_breaker = Gauge(
            "metamind_oracle_circuit_breaker",
            "Oracle circuit breaker state (0=closed, 1=open)"
        )
        
        # Cost model metrics
        self.cost_prediction_error = Histogram(
            "metamind_cost_prediction_error_seconds",
            "Cost prediction error (actual - predicted)",
            buckets=[-10, -5, -1, -0.5, 0, 0.5, 1, 5, 10]
        )
        
        # Info
        self.info = Info("metamind", "MetaMind platform information")
        self.info.info({"version": "4.0.0"})
        
        logger.debug("MetricsCollector initialized")
    
    def record_query(
        self,
        tenant_id: str,
        source: str,
        status: str,
        strategy: str,
        duration_seconds: float
    ) -> None:
        """
        Record query execution.
        
        Args:
            tenant_id: Tenant identifier
            source: Target source
            status: Query status (success, failed, cancelled)
            strategy: Execution strategy
            duration_seconds: Execution duration
        """
        self.queries_total.labels(
            tenant_id=tenant_id,
            source=source,
            status=status,
            strategy=strategy
        ).inc()
        
        self.query_duration.labels(
            tenant_id=tenant_id,
            source=source
        ).observe(duration_seconds)
    
    def record_routing_decision(self, duration_seconds: float) -> None:
        """
        Record routing decision latency.
        
        Args:
            duration_seconds: Decision latency
        """
        self.routing_decision_duration.observe(duration_seconds)
    
    def record_cache_hit(self, tier: str = "L1") -> None:
        """
        Record cache hit.
        
        Args:
            tier: Cache tier (L1, L2, L3)
        """
        self.cache_hits.labels(tier=tier).inc()
    
    def record_cache_miss(self) -> None:
        """Record cache miss."""
        self.cache_misses.inc()
    
    def update_cache_size(self, tier: str, size: int) -> None:
        """
        Update cache size gauge.
        
        Args:
            tier: Cache tier
            size: Current size
        """
        self.cache_size.labels(tier=tier).set(size)
    
    def update_cdc_lag(self, table_name: str, lag_seconds: int) -> None:
        """
        Update CDC lag gauge.
        
        Args:
            table_name: Table name
            lag_seconds: Lag in seconds
        """
        self.cdc_lag.labels(table_name=table_name).set(lag_seconds)
    
    def update_cdc_health(
        self,
        healthy: int,
        warning: int,
        critical: int
    ) -> None:
        """
        Update CDC health gauges.
        
        Args:
            healthy: Number of healthy tables
            warning: Number of warning tables
            critical: Number of critical tables
        """
        self.cdc_tables_healthy.set(healthy)
        self.cdc_tables_warning.set(warning)
        self.cdc_tables_critical.set(critical)
    
    def update_engine_connections(self, engine: str, count: int) -> None:
        """
        Update engine connection gauge.
        
        Args:
            engine: Engine name
            count: Connection count
        """
        self.engine_connections.labels(engine=engine).set(count)
    
    def update_oracle_circuit_breaker(self, is_open: bool) -> None:
        """
        Update Oracle circuit breaker state.
        
        Args:
            is_open: True if circuit is open
        """
        self.oracle_circuit_breaker.set(1 if is_open else 0)
    
    def record_cost_prediction_error(
        self,
        predicted_seconds: float,
        actual_seconds: float
    ) -> None:
        """
        Record cost prediction error.
        
        Args:
            predicted_seconds: Predicted duration
            actual_seconds: Actual duration
        """
        error = actual_seconds - predicted_seconds
        self.cost_prediction_error.observe(error)
    
    def get_metrics(self) -> bytes:
        """
        Get metrics in Prometheus format.
        
        Returns:
            Metrics as bytes
        """
        return generate_latest()
    
    def get_content_type(self) -> str:
        """Get Prometheus content type."""
        return CONTENT_TYPE_LATEST


# Global metrics instance
_metrics: Optional[MetricsCollector] = None


def get_metrics() -> MetricsCollector:
    """Get global metrics collector."""
    global _metrics
    if _metrics is None:
        _metrics = MetricsCollector()
    return _metrics


# ---------------------------------------------------------------------------
# Standalone Prometheus counters (importable directly without MetricsCollector)
# ---------------------------------------------------------------------------

from prometheus_client import Counter  # noqa: E402

DRIFT_ALERT_COUNTER = Counter(
    "metamind_drift_alerts_total",
    "Total number of model drift alerts fired",
    ["drift_type"],
)
