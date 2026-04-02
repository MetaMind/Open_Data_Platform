"""Query rewrite suggestions with anti-pattern detection and improvement estimation.

Analyzes SQL queries for common anti-patterns and suggests optimized rewrites
with estimated performance improvement percentages.

Feature: F29_query_rewrite
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from metamind.core.catalog.metadata import MetadataCatalog
from metamind.core.types import LogicalNode

logger = logging.getLogger(__name__)


@dataclass
class RewriteSuggestion:
    """A suggested query rewrite with estimated improvement."""

    rule_name: str
    description: str
    original_sql: str
    rewritten_sql: str
    estimated_improvement_pct: float  # e.g. 45.0 = ~45% faster
    confidence: str  # "high", "medium", "low"
    explanation: str


class AntiPatternDetector:
    """Detects common SQL anti-patterns and suggests rewrites."""

    def __init__(self, catalog: Optional[MetadataCatalog] = None) -> None:
        self.catalog = catalog

    def detect_select_star(
        self, sql: str, tenant_id: str = ""
    ) -> Optional[RewriteSuggestion]:
        """SELECT * → SELECT only needed columns."""
        pattern = re.compile(r"\bSELECT\s+\*\s+FROM\s+(\w+)", re.IGNORECASE)
        match = pattern.search(sql)
        if not match:
            return None

        table_name = match.group(1)
        col_count = 10
        if self.catalog and tenant_id:
            table = self.catalog.get_table(tenant_id, table_name)
            if table:
                col_count = len(table.columns)

        if col_count <= 3:
            return None

        sample_cols = "id, name, created_at"
        if self.catalog and tenant_id:
            table = self.catalog.get_table(tenant_id, table_name)
            if table and table.columns:
                cols = [c.name for c in table.columns[:5]]
                sample_cols = ", ".join(cols)

        rewritten = sql.replace("SELECT *", f"SELECT {sample_cols}", 1)
        improvement = min(80.0, 10.0 + col_count * 5.0)

        return RewriteSuggestion(
            rule_name="select_star",
            description="Replace SELECT * with explicit column list",
            original_sql=sql,
            rewritten_sql=rewritten,
            estimated_improvement_pct=round(improvement, 1),
            confidence="high",
            explanation=(
                f"SELECT * fetches all {col_count} columns including potentially "
                f"large text/blob columns. Selecting only needed columns reduces "
                f"I/O and network transfer by up to {improvement:.0f}%."
            ),
        )

    def detect_implicit_cross_join(self, sql: str, tenant_id: str = "") -> Optional[RewriteSuggestion]:
        """FROM a, b without proper join → explicit JOIN with ON clause."""
        pattern = re.compile(
            r"\bFROM\s+(\w+)\s*,\s*(\w+)\b(?!.*\bJOIN\b)", re.IGNORECASE | re.DOTALL
        )
        match = pattern.search(sql)
        if not match:
            return None

        t1, t2 = match.group(1), match.group(2)
        where_match = re.search(
            rf"\bWHERE\b.*{t1}\.(\w+)\s*=\s*{t2}\.(\w+)", sql, re.IGNORECASE | re.DOTALL
        )

        if where_match:
            col1, col2 = where_match.group(1), where_match.group(2)
            rewritten = re.sub(
                rf"\bFROM\s+{t1}\s*,\s*{t2}\b",
                f"FROM {t1} INNER JOIN {t2} ON {t1}.{col1} = {t2}.{col2}",
                sql, count=1, flags=re.IGNORECASE,
            )
            cond = f"{t1}.{col1} = {t2}.{col2}"
            rewritten = re.sub(
                rf"\s+AND\s+{re.escape(cond)}", "", rewritten, flags=re.IGNORECASE
            )
            rewritten = re.sub(
                rf"{re.escape(cond)}\s+AND\s+", "", rewritten, flags=re.IGNORECASE
            )
        else:
            rewritten = re.sub(
                rf"\bFROM\s+{t1}\s*,\s*{t2}\b",
                f"FROM {t1} INNER JOIN {t2} ON {t1}.id = {t2}.{t1}_id",
                sql, count=1, flags=re.IGNORECASE,
            )

        return RewriteSuggestion(
            rule_name="implicit_cross_join",
            description="Replace implicit cross join with explicit JOIN",
            original_sql=sql,
            rewritten_sql=rewritten,
            estimated_improvement_pct=60.0,
            confidence="medium",
            explanation=(
                "Implicit cross joins (FROM a, b) can accidentally produce "
                "cartesian products. Explicit JOINs make intent clear and "
                "allow the optimizer to choose efficient join strategies."
            ),
        )

    def detect_or_in_filter(self, sql: str, tenant_id: str = "") -> Optional[RewriteSuggestion]:
        """WHERE col = 'x' OR col = 'y' → WHERE col IN ('x', 'y')."""
        pattern = re.compile(
            r"(\w+)\s*=\s*'([^']+)'\s+OR\s+\1\s*=\s*'([^']+)'",
            re.IGNORECASE,
        )
        match = pattern.search(sql)
        if not match:
            return None

        col = match.group(1)
        val1 = match.group(2)
        val2 = match.group(3)

        full_pattern = re.compile(
            rf"{col}\s*=\s*'[^']+'\s*(?:OR\s+{col}\s*=\s*'[^']+'\s*)+",
            re.IGNORECASE,
        )
        full_match = full_pattern.search(sql)
        if full_match:
            vals = re.findall(rf"{col}\s*=\s*'([^']+)'", full_match.group(0), re.IGNORECASE)
            in_clause = ", ".join(f"'{v}'" for v in vals)
            rewritten = sql[:full_match.start()] + f"{col} IN ({in_clause})" + sql[full_match.end():]
        else:
            in_clause = f"'{val1}', '{val2}'"
            rewritten = sql[:match.start()] + f"{col} IN ({in_clause})" + sql[match.end():]

        return RewriteSuggestion(
            rule_name="or_to_in",
            description="Replace OR equality chain with IN clause",
            original_sql=sql,
            rewritten_sql=rewritten,
            estimated_improvement_pct=30.0,
            confidence="high",
            explanation=(
                "Multiple OR conditions on the same column prevent index usage. "
                "IN clause enables the optimizer to use index range scans."
            ),
        )

    def detect_function_on_indexed_column(
        self, sql: str, tenant_id: str = ""
    ) -> Optional[RewriteSuggestion]:
        """WHERE UPPER(col) = 'X' → WHERE col = LOWER('X')."""
        pattern = re.compile(
            r"\b(UPPER|LOWER|TRIM|CAST)\s*\(\s*(\w+)\s*\)\s*=\s*'([^']+)'",
            re.IGNORECASE,
        )
        match = pattern.search(sql)
        if not match:
            return None

        func = match.group(1).upper()
        col = match.group(2)
        val = match.group(3)

        if func == "UPPER":
            rewritten_condition = f"{col} = LOWER('{val}')"
        elif func == "LOWER":
            rewritten_condition = f"{col} = UPPER('{val}')"
        elif func == "TRIM":
            rewritten_condition = f"{col} = '{val.strip()}'"
        else:
            rewritten_condition = f"{col} = '{val}'"

        rewritten = sql[:match.start()] + rewritten_condition + sql[match.end():]

        return RewriteSuggestion(
            rule_name="function_on_indexed_column",
            description="Remove function wrapper on indexed column to enable index scan",
            original_sql=sql,
            rewritten_sql=rewritten,
            estimated_improvement_pct=40.0,
            confidence="medium",
            explanation=(
                f"Wrapping a column in {func}() prevents index usage. "
                "Moving the transformation to the literal value side "
                "enables the optimizer to use existing indexes."
            ),
        )

    def detect_correlated_subquery(
        self, sql: str, tenant_id: str = ""
    ) -> Optional[RewriteSuggestion]:
        """Correlated subquery → LEFT JOIN."""
        pattern = re.compile(
            r"\bWHERE\b.*\bIN\s*\(\s*SELECT\s+(\w+)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)",
            re.IGNORECASE | re.DOTALL,
        )
        match = pattern.search(sql)
        if not match:
            exists_pattern = re.compile(
                r"\bWHERE\b.*\bEXISTS\s*\(\s*SELECT\b",
                re.IGNORECASE | re.DOTALL,
            )
            if exists_pattern.search(sql):
                return RewriteSuggestion(
                    rule_name="correlated_subquery",
                    description="Consider replacing correlated subquery with JOIN",
                    original_sql=sql,
                    rewritten_sql=sql,
                    estimated_improvement_pct=50.0,
                    confidence="low",
                    explanation=(
                        "Correlated subqueries execute once per outer row, resulting in "
                        "O(n²) performance. JOINs allow hash/merge join strategies "
                        "which are 10-100x faster for large tables."
                    ),
                )
            return None

        sub_col = match.group(1)
        sub_table = match.group(2)
        ref_table_a = match.group(3)
        ref_col_a = match.group(4)
        ref_table_b = match.group(5)
        ref_col_b = match.group(6)

        from_match = re.search(r"\bFROM\s+(\w+)", sql, re.IGNORECASE)
        main_table = from_match.group(1) if from_match else ref_table_a

        select_part = re.match(r"(SELECT\s+.+?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
        select_clause = select_part.group(1) if select_part else "SELECT *"

        rewritten = (
            f"{select_clause} FROM {main_table} "
            f"INNER JOIN {sub_table} ON {ref_table_a}.{ref_col_a} = {ref_table_b}.{ref_col_b}"
        )

        where_after = re.search(
            r"\bWHERE\b\s*(.+?)(?:$|\bORDER\b|\bGROUP\b|\bLIMIT\b|\bHAVING\b)",
            sql, re.IGNORECASE | re.DOTALL,
        )
        remaining_conditions: list[str] = []
        if where_after:
            full_where = where_after.group(1).strip()
            in_sub = re.sub(
                r"\w+\s+IN\s*\(.*?\)", "", full_where, flags=re.IGNORECASE | re.DOTALL
            ).strip()
            in_sub = re.sub(r"^\s*AND\s+", "", in_sub, flags=re.IGNORECASE).strip()
            in_sub = re.sub(r"\s+AND\s*$", "", in_sub, flags=re.IGNORECASE).strip()
            if in_sub:
                remaining_conditions.append(in_sub)

        if remaining_conditions:
            rewritten += " WHERE " + " AND ".join(remaining_conditions)

        return RewriteSuggestion(
            rule_name="correlated_subquery",
            description="Replace correlated subquery with JOIN",
            original_sql=sql,
            rewritten_sql=rewritten,
            estimated_improvement_pct=70.0,
            confidence="medium",
            explanation=(
                "Correlated subqueries execute once per outer row (O(n²)). "
                "Rewriting as a JOIN allows hash/merge join strategies that "
                "are typically 10-100x faster."
            ),
        )

    def detect_missing_limit(self, sql: str, tenant_id: str = "") -> Optional[RewriteSuggestion]:
        """Large table query without LIMIT → suggest LIMIT 1000."""
        if re.search(r"\bLIMIT\s+\d+", sql, re.IGNORECASE):
            return None
        if re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", sql, re.IGNORECASE):
            return None
        if re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE):
            return None
        if re.search(r"\bINSERT\b|\bUPDATE\b|\bDELETE\b", sql, re.IGNORECASE):
            return None

        rewritten = sql.rstrip().rstrip(";") + " LIMIT 1000"

        return RewriteSuggestion(
            rule_name="missing_limit",
            description="Add LIMIT clause to prevent unbounded result sets",
            original_sql=sql,
            rewritten_sql=rewritten,
            estimated_improvement_pct=20.0,
            confidence="low",
            explanation=(
                "Queries without LIMIT can return millions of rows, consuming "
                "excessive memory and network bandwidth. Add LIMIT for exploration queries."
            ),
        )

    def detect_count_distinct_on_hll(
        self, sql: str, tenant_id: str = ""
    ) -> Optional[RewriteSuggestion]:
        """COUNT(DISTINCT col) on large tables → HyperLogLog approximation."""
        pattern = re.compile(r"\bCOUNT\s*\(\s*DISTINCT\s+(\w+)\s*\)", re.IGNORECASE)
        match = pattern.search(sql)
        if not match:
            return None

        col = match.group(1)
        rewritten = sql[:match.start()] + f"approx_count_distinct({col})" + sql[match.end():]

        return RewriteSuggestion(
            rule_name="count_distinct_hll",
            description="Replace COUNT(DISTINCT) with HyperLogLog approximation",
            original_sql=sql,
            rewritten_sql=rewritten,
            estimated_improvement_pct=60.0,
            confidence="medium",
            explanation=(
                "COUNT(DISTINCT) requires sorting or hash-building all values. "
                "HyperLogLog provides ~1% error with O(1) memory and dramatically "
                "faster execution on large datasets."
            ),
        )

    def detect_non_sargable_predicate(
        self, sql: str, tenant_id: str = ""
    ) -> Optional[RewriteSuggestion]:
        """WHERE col LIKE '%suffix' → suggest full-text search."""
        pattern = re.compile(r"(\w+)\s+LIKE\s+'%([^']+)'", re.IGNORECASE)
        match = pattern.search(sql)
        if not match:
            return None

        col = match.group(1)
        val = match.group(2)

        rewritten = sql[:match.start()] + f"to_tsvector({col}) @@ to_tsquery('{val}')" + sql[match.end():]

        return RewriteSuggestion(
            rule_name="non_sargable_like",
            description="Replace leading-wildcard LIKE with full-text search",
            original_sql=sql,
            rewritten_sql=rewritten,
            estimated_improvement_pct=50.0,
            confidence="medium",
            explanation=(
                "LIKE '%pattern' requires a full table scan because B-tree "
                "indexes cannot be used with leading wildcards. Full-text "
                "search indexes (GIN/GiST) provide efficient suffix matching."
            ),
        )


class RewriteAnalyzer:
    """Analyzes SQL for anti-patterns and returns sorted suggestions."""

    def __init__(self, catalog: Optional[MetadataCatalog] = None) -> None:
        self.catalog = catalog
        self.detector = AntiPatternDetector(catalog=catalog)

    def analyze(
        self,
        root: Optional[LogicalNode],
        sql: str,
        tenant_id: str,
    ) -> list[RewriteSuggestion]:
        """Run all anti-pattern detectors and return sorted suggestions."""
        detectors = [
            self.detector.detect_select_star,
            self.detector.detect_implicit_cross_join,
            self.detector.detect_or_in_filter,
            self.detector.detect_function_on_indexed_column,
            self.detector.detect_correlated_subquery,
            self.detector.detect_missing_limit,
            self.detector.detect_count_distinct_on_hll,
            self.detector.detect_non_sargable_predicate,
        ]

        suggestions: list[RewriteSuggestion] = []
        for detect_fn in detectors:
            try:
                result = detect_fn(sql, tenant_id)
                if result is not None:
                    suggestions.append(result)
            except Exception as exc:
                logger.warning("Detector %s failed: %s", detect_fn.__name__, exc)

        suggestions.sort(key=lambda s: s.estimated_improvement_pct, reverse=True)
        return suggestions


class RewriteSuggester:
    """High-level suggestion interface with persistence."""

    def __init__(self, analyzer: RewriteAnalyzer) -> None:
        self.analyzer = analyzer

    def suggest(
        self,
        root: Optional[LogicalNode],
        sql: str,
        tenant_id: str,
    ) -> list[RewriteSuggestion]:
        """Get rewrite suggestions for a SQL query."""
        return self.analyzer.analyze(root, sql, tenant_id)

    def apply_rewrite(self, suggestion: RewriteSuggestion, sql: str) -> str:
        """Apply a rewrite suggestion, returning the rewritten SQL."""
        return suggestion.rewritten_sql

    def persist_suggestion(
        self, suggestion: RewriteSuggestion, tenant_id: str, engine: Any
    ) -> None:
        """Store a suggestion in the workload patterns table."""
        if engine is None:
            logger.debug("No engine; skipping suggestion persistence")
            return

        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                conn.execute(
                    text(
                        "INSERT INTO mm_workload_patterns "
                        "(tenant_id, rule_name, original_sql, rewritten_sql, "
                        "improvement_pct, confidence, created_at) "
                        "VALUES (:tid, :rule, :orig, :rewr, :imp, :conf, :ts)"
                    ),
                    {
                        "tid": tenant_id,
                        "rule": suggestion.rule_name,
                        "orig": suggestion.original_sql,
                        "rewr": suggestion.rewritten_sql,
                        "imp": suggestion.estimated_improvement_pct,
                        "conf": suggestion.confidence,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    },
                )
                conn.commit()
        except Exception as exc:
            logger.warning("Failed to persist suggestion: %s", exc)
