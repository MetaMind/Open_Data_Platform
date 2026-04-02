"""
Workload Profiler — Query Workload Collection and Pattern Analysis

File: metamind/synthesis/workload_profiler.py
Role: Senior ML Engineer
Dependencies: sqlglot, sqlalchemy, asyncpg

Collects query execution records and derives aggregate workload statistics.
Uses sqlglot to parse SQL and extract structural features (join count,
aggregation count, subquery depth) so downstream rule generators can
identify optimisation opportunities without re-parsing raw SQL.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data-transfer objects
# ---------------------------------------------------------------------------

@dataclass
class QueryStructure:
    """Structural features extracted from a single SQL statement."""
    tables: List[str] = field(default_factory=list)
    join_count: int = 0
    agg_count: int = 0
    filter_count: int = 0
    subquery_depth: int = 0
    has_window: bool = False
    fingerprint: str = ""


@dataclass
class WorkloadStats:
    """Aggregate workload statistics over a time window."""
    total_queries: int = 0
    join_heavy_pct: float = 0.0        # % of queries with ≥ 2 joins
    agg_heavy_pct: float = 0.0         # % of queries with ≥ 1 aggregate
    filter_only_pct: float = 0.0       # % of queries that are pure filters
    avg_runtime_ms: float = 0.0
    p95_runtime_ms: float = 0.0
    top_tables: List[str] = field(default_factory=list)
    slow_query_fingerprints: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SQL structural feature extraction helpers
# ---------------------------------------------------------------------------

def _extract_structure(sql: str) -> QueryStructure:
    """Parse SQL with sqlglot and extract structural features."""
    struct = QueryStructure()
    try:
        parsed = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
        if parsed is None:
            return struct

        tables: List[str] = []
        for tbl in parsed.find_all(exp.Table):
            if tbl.name:
                tables.append(tbl.name.lower())
        struct.tables = list(dict.fromkeys(tables))  # deduplicate, preserve order

        struct.join_count = len(list(parsed.find_all(exp.Join)))
        struct.agg_count = len(list(parsed.find_all(exp.AggFunc)))
        struct.filter_count = len(list(parsed.find_all(exp.Where)))
        struct.has_window = bool(list(parsed.find_all(exp.Window)))

        # Subquery depth: count nested Subquery / Paren wrappers
        max_depth = 0

        def _depth(node: exp.Expression, d: int) -> None:
            nonlocal max_depth
            if d > max_depth:
                max_depth = d
            for child in node.args.values():
                if isinstance(child, exp.Expression):
                    inc = 1 if isinstance(child, (exp.Subquery, exp.Paren)) else 0
                    _depth(child, d + inc)

        _depth(parsed, 0)
        struct.subquery_depth = max_depth

        # Canonical fingerprint: normalise literals then hash
        normalised = sqlglot.transpile(sql, error_level=sqlglot.ErrorLevel.IGNORE)
        raw = normalised[0] if normalised else sql
        struct.fingerprint = hashlib.sha256(raw.encode()).hexdigest()[:16]

    except Exception as exc:  # noqa: BLE001
        logger.error("WorkloadProfiler._extract_structure failed: %s", exc)

    return struct


# ---------------------------------------------------------------------------
# WorkloadProfiler
# ---------------------------------------------------------------------------

class WorkloadProfiler:
    """
    Records individual query executions to mm_query_logs and provides
    aggregate workload statistics for synthesis cycles.

    All writes and reads use SQLAlchemy async Core with named parameters.
    """

    _SLOW_QUERY_THRESHOLD_MS: float = 5_000.0  # 5 s
    _TOP_TABLES_LIMIT: int = 10
    _SLOW_FINGERPRINTS_LIMIT: int = 20

    def __init__(self, db_engine: AsyncEngine) -> None:
        self._db = db_engine

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    async def record_execution(
        self,
        query_id: str,
        sql: str,
        plan_type: str,
        engine: str,
        runtime_ms: float,
        row_count: int,
        tenant_id: str,
    ) -> None:
        """
        Persist a single query execution record.

        Extracts structural features from *sql* before inserting so that
        aggregate queries in get_workload_stats() can use pre-computed
        columns rather than re-parsing on every stats call.
        """
        struct = _extract_structure(sql)
        try:
            async with self._db.begin() as conn:
                await conn.execute(
                    text(
                        """
                        INSERT INTO mm_query_logs (
                            query_id, tenant_id, user_id, original_sql, execution_strategy,
                            target_source, total_time_ms, row_count,
                            join_count, agg_count, filter_count,
                            subquery_depth, sql_fingerprint, status,
                            submitted_at, execution_start_at, execution_end_at, tables_accessed
                        ) VALUES (
                            :qid, :tid, :user_id, :sql, :plan, :engine,
                            :rt, :rows, :joins, :aggs, :filters,
                            :depth, :fp, 'success',
                            NOW() - (:rt / 1000.0) * INTERVAL '1 second',
                            NOW() - (:rt / 1000.0) * INTERVAL '1 second',
                            NOW(),
                            to_jsonb(CAST(:tables AS text[]))
                        )
                        ON CONFLICT (query_id) DO UPDATE SET
                            total_time_ms = EXCLUDED.total_time_ms,
                            row_count = EXCLUDED.row_count,
                            status = 'success',
                            execution_end_at = NOW()
                        """
                    ),
                    {
                        "qid": query_id,
                        "tid": tenant_id,
                        "user_id": "system",
                        "sql": sql[:4096],
                        "plan": plan_type,
                        "engine": engine,
                        "rt": runtime_ms,
                        "rows": row_count,
                        "joins": struct.join_count,
                        "aggs": struct.agg_count,
                        "filters": struct.filter_count,
                        "depth": struct.subquery_depth,
                        "fp": struct.fingerprint,
                        "tables": struct.tables,
                    },
                )
        except Exception as exc:
            logger.error(
                "WorkloadProfiler.record_execution failed tenant=%s query=%s: %s",
                tenant_id,
                query_id,
                exc,
            )

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    async def get_workload_stats(
        self,
        tenant_id: str,
        window_hours: int = 24,
    ) -> WorkloadStats:
        """Return aggregate workload statistics for the last *window_hours*."""
        stats = WorkloadStats()
        try:
            async with self._db.connect() as conn:
                stats = await self._aggregate_stats(conn, tenant_id, window_hours)
        except Exception as exc:
            logger.error(
                "WorkloadProfiler.get_workload_stats failed tenant=%s: %s",
                tenant_id,
                exc,
            )
        return stats

    async def _aggregate_stats(
        self,
        conn: Any,
        tenant_id: str,
        window_hours: int,
    ) -> WorkloadStats:
        """Inner method: run all stat queries inside a single connection."""
        cutoff_expr = "NOW() - :window * INTERVAL '1 hour'"

        # Basic counters + runtime percentiles
        row = (
            await conn.execute(
                text(
                    f"""
                    SELECT
                        COUNT(*) AS total,
                        AVG(total_time_ms) AS avg_rt,
                        PERCENTILE_CONT(0.95) WITHIN GROUP
                            (ORDER BY total_time_ms) AS p95_rt,
                        SUM(CASE WHEN join_count >= 2 THEN 1 ELSE 0 END) AS join_heavy,
                        SUM(CASE WHEN agg_count >= 1 THEN 1 ELSE 0 END) AS agg_heavy,
                        SUM(CASE WHEN join_count = 0 AND agg_count = 0 THEN 1 ELSE 0 END)
                            AS filter_only
                    FROM mm_query_logs
                    WHERE tenant_id = :tid
                      AND submitted_at >= {cutoff_expr}
                      AND status IN ('success', 'completed')
                    """
                ),
                {"tid": tenant_id, "window": window_hours},
            )
        ).fetchone()

        total = int(row.total or 0)
        if total == 0:
            return WorkloadStats()

        def _pct(n: Any) -> float:
            return round(float(n or 0) / total * 100, 2)

        stats = WorkloadStats(
            total_queries=total,
            join_heavy_pct=_pct(row.join_heavy),
            agg_heavy_pct=_pct(row.agg_heavy),
            filter_only_pct=_pct(row.filter_only),
            avg_runtime_ms=float(row.avg_rt or 0),
            p95_runtime_ms=float(row.p95_rt or 0),
        )

        # Top tables by query frequency
        table_rows = (
            await conn.execute(
                text(
                    f"""
                    SELECT sql_fingerprint,
                           unnest(string_to_array(tables_json, ',')) AS tbl,
                           COUNT(*) AS freq
                    FROM (
                        SELECT sql_fingerprint,
                               REPLACE(
                                 REPLACE(
                                   REPLACE(
                                     CAST(tables_accessed AS TEXT), '[',''), ']',''), '"','') AS tables_json
                        FROM mm_query_logs
                        WHERE tenant_id = :tid
                          AND submitted_at >= {cutoff_expr}
                          AND tables_accessed IS NOT NULL
                    ) sq
                    GROUP BY 1, 2
                    ORDER BY freq DESC
                    LIMIT :lim
                    """
                ),
                {
                    "tid": tenant_id,
                    "window": window_hours,
                    "lim": self._TOP_TABLES_LIMIT,
                },
            )
        ).fetchall()
        stats.top_tables = [r.tbl.strip() for r in table_rows if r.tbl]

        # Slow query fingerprints
        slow_rows = (
            await conn.execute(
                text(
                    f"""
                    SELECT DISTINCT sql_fingerprint
                    FROM mm_query_logs
                    WHERE tenant_id = :tid
                      AND submitted_at >= {cutoff_expr}
                      AND total_time_ms > :thresh
                      AND status IN ('success', 'completed')
                      AND sql_fingerprint IS NOT NULL
                    ORDER BY sql_fingerprint
                    LIMIT :lim
                    """
                ),
                {
                    "tid": tenant_id,
                    "window": window_hours,
                    "thresh": self._SLOW_QUERY_THRESHOLD_MS,
                    "lim": self._SLOW_FINGERPRINTS_LIMIT,
                },
            )
        ).fetchall()
        stats.slow_query_fingerprints = [r.sql_fingerprint for r in slow_rows]

        return stats
