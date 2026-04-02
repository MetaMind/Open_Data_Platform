"""
Routing Decision — Data Contracts for Query Routing Outcomes

File: metamind/core/routing_decision.py
Role: Senior ML Engineer
Dependencies: dataclasses, enum

Contains all routing-related dataclasses and the RoutingDecisionBuilder
helper.  Extracted from metamind/core/router.py to keep that file ≤ 350
lines while providing a stable import surface for the rest of the codebase.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ExecutionStrategy(Enum):
    """Execution strategy types."""
    DIRECT = "direct"            # Single source
    CACHED = "cached"            # Cache hit
    HYBRID_UNION = "hybrid"      # Oracle + S3 union
    FEDERATED = "federated"      # Cross-cloud
    BATCH = "batch"              # Spark batch job
    GPU = "gpu"                  # GPU-accelerated execution


# ---------------------------------------------------------------------------
# Core DTO
# ---------------------------------------------------------------------------

@dataclass
class RoutingDecision:
    """Final routing decision with full execution plan."""
    target_source: str                    # Source ID (oracle_prod, s3_analytics, spark_batch)
    target_source_type: str               # oracle, trino, spark, gpu
    execution_strategy: ExecutionStrategy
    freshness_expected_seconds: int       # Guaranteed data freshness
    estimated_cost_ms: float              # ML-predicted execution time
    confidence: float                     # Model confidence (0–1)
    cache_key: Optional[str]              # Redis cache key
    rewritten_sql: str                    # Dialect-specific SQL
    fallback_source: Optional[str]        # If primary fails
    hybrid_config: Optional[Dict]         # For hybrid: time split, union SQL
    reason: str                           # Human-readable routing logic
    query_features: Dict[str, Any] = field(default_factory=dict)
    logical_plan: Optional[Any] = None
    execution_graph: Optional[Any] = None
    is_batch_job: bool = False
    uncertainty: float = 0.0             # Model uncertainty from MC Dropout


@dataclass
class RoutingContext:
    """Ephemeral context assembled during a single routing request."""
    sql: str
    tenant_id: str
    user_id: str = ""
    request_freshness: str = "standard"  # realtime | recent | standard | historical
    force_engine: Optional[str] = None   # admin override
    query_id: str = ""
    labels: Dict[str, str] = field(default_factory=dict)
    parsed_tables: List[str] = field(default_factory=list)
    features: Dict[str, Any] = field(default_factory=dict)
    cdc_lag_seconds: Optional[int] = None
    cache_hit: bool = False


# ---------------------------------------------------------------------------
# Builder helper
# ---------------------------------------------------------------------------

class RoutingDecisionBuilder:
    """
    Fluent builder for RoutingDecision objects.

    Usage::

        decision = (
            RoutingDecisionBuilder("s3_analytics", "trino")
            .strategy(ExecutionStrategy.DIRECT)
            .freshness(1800)
            .cost(142.5)
            .confidence(0.87)
            .sql("SELECT ...")
            .reason("CDC lag 120s < 300s threshold")
            .build()
        )
    """

    def __init__(self, target_source: str, target_source_type: str) -> None:
        self._source = target_source
        self._source_type = target_source_type
        self._strategy = ExecutionStrategy.DIRECT
        self._freshness = 1800
        self._cost = 100.0
        self._confidence = 0.5
        self._cache_key: Optional[str] = None
        self._sql = ""
        self._fallback: Optional[str] = None
        self._hybrid_cfg: Optional[Dict] = None
        self._reason = ""
        self._features: Dict[str, Any] = {}
        self._logical_plan: Optional[Any] = None
        self._uncertainty = 0.0

    def strategy(self, s: ExecutionStrategy) -> RoutingDecisionBuilder:
        self._strategy = s
        return self

    def freshness(self, seconds: int) -> RoutingDecisionBuilder:
        self._freshness = seconds
        return self

    def cost(self, ms: float) -> RoutingDecisionBuilder:
        self._cost = ms
        return self

    def confidence(self, c: float) -> RoutingDecisionBuilder:
        self._confidence = c
        return self

    def cache_key(self, key: str) -> RoutingDecisionBuilder:
        self._cache_key = key
        return self

    def sql(self, rewritten: str) -> RoutingDecisionBuilder:
        self._sql = rewritten
        return self

    def fallback(self, source: str) -> RoutingDecisionBuilder:
        self._fallback = source
        return self

    def hybrid_config(self, cfg: Dict) -> RoutingDecisionBuilder:
        self._hybrid_cfg = cfg
        return self

    def reason(self, r: str) -> RoutingDecisionBuilder:
        self._reason = r
        return self

    def features(self, f: Dict[str, Any]) -> RoutingDecisionBuilder:
        self._features = f
        return self

    def logical_plan(self, plan: Any) -> RoutingDecisionBuilder:
        self._logical_plan = plan
        return self

    def uncertainty(self, u: float) -> RoutingDecisionBuilder:
        self._uncertainty = u
        return self

    def build(self) -> RoutingDecision:
        return RoutingDecision(
            target_source=self._source,
            target_source_type=self._source_type,
            execution_strategy=self._strategy,
            freshness_expected_seconds=self._freshness,
            estimated_cost_ms=self._cost,
            confidence=self._confidence,
            cache_key=self._cache_key,
            rewritten_sql=self._sql,
            fallback_source=self._fallback,
            hybrid_config=self._hybrid_cfg,
            reason=self._reason,
            query_features=self._features,
            logical_plan=self._logical_plan,
            uncertainty=self._uncertainty,
        )


def compute_cache_key(sql: str, tenant_id: str) -> str:
    """Deterministic cache key from SQL + tenant."""
    raw = f"{tenant_id}:{sql.strip().upper()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# SQL Rewriting helpers (extracted from QueryRouter for file-size compliance)
# ---------------------------------------------------------------------------

import hashlib as _hashlib

def generate_hybrid_sql(parsed: Any, split_time: str, tables: List[str]) -> str:
    """Generate UNION ALL: Oracle (recent) + S3 (historical)."""
    import sqlglot as _sg
    time_col = find_time_column(parsed, tables)
    oracle_part = parsed.copy()
    oracle_where = _sg.exp.Where(
        this=_sg.exp.GTE(
            this=_sg.exp.Column(this=time_col),
            expression=_sg.exp.parse(split_time)[0],
        )
    )
    oracle_part.set("where", oracle_where)
    s3_part = parsed.copy()
    s3_where = _sg.exp.Where(
        this=_sg.exp.LT(
            this=_sg.exp.Column(this=time_col),
            expression=_sg.exp.parse(split_time)[0],
        )
    )
    s3_part.set("where", s3_where)
    return (
        f"({oracle_part.sql(dialect='oracle')})"
        f"\nUNION ALL\n"
        f"({s3_part.sql(dialect='trino')})"
    )


def rewrite_for_source(parsed: Any, source_type: str, tables: List[str]) -> str:
    """Rewrite SQL for target source dialect."""
    dialect = {"oracle": "oracle", "trino": "trino", "spark": "spark"}.get(source_type, "trino")
    return parsed.sql(dialect=dialect, pretty=True)


def find_time_column(parsed: Any, tables: List[str]) -> str:
    """Find the time column for hybrid queries."""
    import sqlglot as _sg
    time_patterns = ["created_at", "updated_at", "timestamp", "created_date", "event_time"]
    where_clause = parsed.find(_sg.exp.Where)
    if where_clause:
        for col in where_clause.find_all(_sg.exp.Column):
            col_name = col.name.lower()
            if any(p in col_name for p in time_patterns):
                return col_name
    return "created_at"
