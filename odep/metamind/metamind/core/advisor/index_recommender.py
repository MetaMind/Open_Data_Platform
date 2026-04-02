"""Automated Index Recommender.

Mines mm_query_logs slow queries and column lineage to surface the highest-
value missing indexes for a tenant.  Scoring: frequency * avg_duration_ms.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import sqlglot
import sqlglot.expressions as exp
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_SLOW_QUERY_THRESHOLD_MS = 1000
_TOP_N = 20


@dataclass
class IndexRecommendation:
    """A single index recommendation."""

    table: str
    columns: list[str]
    index_type: str  # BTREE | HASH
    estimated_speedup_pct: float
    query_count: int
    create_statement: str


class IndexRecommender:
    """Analyse slow query patterns to recommend missing indexes.

    Args:
        db_engine: SQLAlchemy Engine connected to the MetaMind metadata DB.
    """

    def __init__(self, db_engine: Engine) -> None:
        self._engine = db_engine

    async def analyze(
        self,
        tenant_id: str,
        lookback_days: int = 7,
    ) -> list[IndexRecommendation]:
        """Return up to TOP_N index recommendations for a tenant.

        Algorithm:
        1. Fetch slow queries from mm_query_logs.
        2. Parse each SQL; extract WHERE/JOIN/ORDER BY column refs.
        3. Score each (table, column) by frequency × avg_duration_ms.
        4. Cross-reference with existing indexes to remove duplicates.
        5. Return top-scored candidates.
        """
        slow_queries = self._fetch_slow_queries(tenant_id, lookback_days)
        if not slow_queries:
            return []

        # Accumulate column usage: (table, column) -> (total_ms, hit_count)
        usage: dict[tuple[str, str], list[float]] = {}
        for row in slow_queries:
            sql = row.get("sql", "")
            dur = float(row.get("duration_ms", 0))
            cols = self._extract_candidate_columns(sql)
            for tbl, col in cols:
                key = (tbl.lower(), col.lower())
                usage.setdefault(key, []).append(dur)

        if not usage:
            return []

        existing_indexes = self._fetch_existing_indexes()

        candidates: list[tuple[float, IndexRecommendation]] = []
        for (tbl, col), durations in usage.items():
            if (tbl, col) in existing_indexes:
                continue
            freq = len(durations)
            avg_dur = sum(durations) / freq
            score = freq * avg_dur
            idx_type = "HASH" if freq > 100 else "BTREE"
            speedup_pct = min(80.0, avg_dur / 100)  # heuristic estimate
            create_stmt = (
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                f"idx_{tbl}_{col} ON {tbl} USING {idx_type} ({col});"
            )
            candidates.append(
                (
                    score,
                    IndexRecommendation(
                        table=tbl,
                        columns=[col],
                        index_type=idx_type,
                        estimated_speedup_pct=round(speedup_pct, 1),
                        query_count=freq,
                        create_statement=create_stmt,
                    ),
                )
            )

        # Sort by descending score, deduplicate table+columns, take top N
        candidates.sort(key=lambda x: x[0], reverse=True)
        seen: set[tuple[str, ...]] = set()
        results: list[IndexRecommendation] = []
        for _, rec in candidates:
            key = (rec.table, *rec.columns)
            if key in seen:
                continue
            seen.add(key)
            results.append(rec)
            if len(results) >= _TOP_N:
                break

        logger.info(
            "IndexRecommender: %d recommendations for tenant=%s lookback=%dd",
            len(results),
            tenant_id,
            lookback_days,
        )
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_slow_queries(
        self, tenant_id: str, lookback_days: int
    ) -> list[dict]:
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT original_sql AS sql, total_time_ms AS duration_ms FROM mm_query_logs "
                        "WHERE tenant_id = :tid "
                        "  AND total_time_ms > :thresh "
                        "  AND submitted_at >= NOW() - (:days * INTERVAL '1 day') "
                        "ORDER BY duration_ms DESC LIMIT 5000"
                    ),
                    {
                        "tid": tenant_id,
                        "thresh": _SLOW_QUERY_THRESHOLD_MS,
                        "days": lookback_days,
                    },
                ).fetchall()
            return [dict(r._mapping) for r in rows]
        except Exception as exc:
            logger.error("IndexRecommender._fetch_slow_queries failed: %s", exc)
            return []

    def _extract_candidate_columns(
        self, sql: str
    ) -> list[tuple[str, str]]:
        """Parse SQL and return (table, column) pairs from WHERE/JOIN/ORDER BY."""
        results: list[tuple[str, str]] = []
        try:
            stmts = sqlglot.parse(sql)
            if not stmts or stmts[0] is None:
                return results
            stmt = stmts[0]

            # Collect table aliases
            aliases: dict[str, str] = {}
            for table in stmt.find_all(exp.Table):
                name = (table.name or "").lower()
                alias = table.alias_or_name.lower() if table.alias_or_name else name
                aliases[alias] = name

            # Extract columns from WHERE, JOIN conditions, ORDER BY
            for node in stmt.walk():
                if isinstance(node, exp.Column):
                    tbl = (node.table or "").lower()
                    col = (node.name or "").lower()
                    if not col or col == "*":
                        continue
                    resolved_tbl = aliases.get(tbl, tbl)
                    if resolved_tbl:
                        results.append((resolved_tbl, col))
        except Exception as exc:
            logger.debug("IndexRecommender SQL parse error: %s", exc)
        return results

    def _fetch_existing_indexes(self) -> set[tuple[str, str]]:
        """Return (table, column) pairs that already have an index."""
        existing: set[tuple[str, str]] = set()
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT t.relname AS table_name, a.attname AS column_name "
                        "FROM pg_index i "
                        "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                        "  AND a.attnum = ANY(i.indkey) "
                        "JOIN pg_class t ON t.oid = i.indrelid "
                        "WHERE i.indisprimary = FALSE"
                    )
                ).fetchall()
            for r in rows:
                existing.add((r.table_name.lower(), r.column_name.lower()))
        except Exception as exc:
            logger.warning("IndexRecommender._fetch_existing_indexes failed: %s", exc)
        return existing
