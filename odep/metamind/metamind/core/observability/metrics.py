"""Prometheus metrics."""
from __future__ import annotations
from prometheus_client import Counter, Histogram, Gauge
QUERIES_TOTAL = Counter("metamind_queries_total", "Total queries", ["tenant", "backend"])
QUERY_DURATION = Histogram("metamind_query_duration_ms", "Query duration ms", ["backend"])
ACTIVE_QUERIES = Gauge("metamind_active_queries", "Active queries")
