"""F29 Rewrite suggester — batch analysis, ranking, and auto-apply of SQL rewrites.

Bridges the RewriteAnalyzer (anti-pattern detection) to the query engine,
providing ranked suggestions, batch workload analysis, and auto-apply logic.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from metamind.core.logical.nodes import LogicalNode
from metamind.core.rewrite.analyzer import RewriteAnalyzer, RewriteSuggestion

logger = logging.getLogger(__name__)


@dataclass
class SuggestionBatch:
    """Results from analyzing a batch of queries."""

    total_queries: int = 0
    queries_with_suggestions: int = 0
    total_suggestions: int = 0
    estimated_total_improvement_pct: float = 0.0
    suggestions_by_rule: dict[str, int] = field(default_factory=dict)
    top_suggestions: list[RewriteSuggestion] = field(default_factory=list)


class RewriteSuggester:
    """Production-ready rewrite suggestion engine.

    Wraps the RewriteAnalyzer with:
    - Batch workload analysis (analyze many queries at once)
    - Ranking by estimated impact
    - Deduplication across similar queries
    - Auto-apply with safety checks
    - Persistence of suggestion history
    """

    def __init__(
        self,
        catalog: Any = None,
        auto_apply_threshold: float = 0.9,
        max_suggestions_per_query: int = 5,
    ) -> None:
        """Initialize with optional catalog and configuration.

        Args:
            catalog: MetadataCatalog for schema-aware analysis.
            auto_apply_threshold: Confidence threshold for automatic rewrites.
            max_suggestions_per_query: Max suggestions to return per query.
        """
        self._analyzer = RewriteAnalyzer(catalog)
        self._auto_apply_threshold = auto_apply_threshold
        self._max_per_query = max_suggestions_per_query
        self._history: list[RewriteSuggestion] = []

    def suggest(
        self,
        root: Optional[LogicalNode],
        original_sql: str,
        tenant_id: str = "",
    ) -> list[RewriteSuggestion]:
        """Get ranked rewrite suggestions for a single query.

        Args:
            root: Optional logical plan root (enables plan-aware analysis).
            original_sql: The SQL query to analyze.
            tenant_id: Tenant identifier for schema context.

        Returns:
            List of RewriteSuggestion sorted by estimated improvement.
        """
        suggestions = self._analyzer.analyze(root, original_sql, tenant_id)

        # Deduplicate by rule name (same anti-pattern may trigger multiple times)
        seen_rules: set[str] = set()
        unique: list[RewriteSuggestion] = []
        for s in suggestions:
            if s.rule_name not in seen_rules:
                seen_rules.add(s.rule_name)
                unique.append(s)

        # Sort by impact (highest improvement first)
        unique.sort(key=lambda s: s.estimated_improvement_pct, reverse=True)

        result = unique[: self._max_per_query]
        self._history.extend(result)
        return result

    def suggest_batch(
        self,
        queries: list[tuple[Optional[LogicalNode], str]],
        tenant_id: str = "",
    ) -> SuggestionBatch:
        """Analyze a batch of queries and return aggregated suggestions.

        Useful for workload-level analysis. Deduplicates across queries
        and ranks by total estimated impact across the workload.

        Args:
            queries: List of (logical_plan_root, sql_string) tuples.
            tenant_id: Tenant identifier.

        Returns:
            SuggestionBatch with aggregated results.
        """
        batch = SuggestionBatch(total_queries=len(queries))
        all_suggestions: list[RewriteSuggestion] = []
        rule_counts: dict[str, int] = {}

        for root, sql in queries:
            suggestions = self._analyzer.analyze(root, sql, tenant_id)
            if suggestions:
                batch.queries_with_suggestions += 1
                batch.total_suggestions += len(suggestions)
                all_suggestions.extend(suggestions)

                for s in suggestions:
                    rule_counts[s.rule_name] = rule_counts.get(s.rule_name, 0) + 1

        batch.suggestions_by_rule = rule_counts

        if all_suggestions:
            batch.estimated_total_improvement_pct = sum(
                s.estimated_improvement_pct for s in all_suggestions
            ) / max(1, batch.total_queries)

        # Rank: prioritize suggestions that appear across many queries
        all_suggestions.sort(
            key=lambda s: (
                rule_counts.get(s.rule_name, 0),
                s.estimated_improvement_pct,
            ),
            reverse=True,
        )

        # Deduplicate for top suggestions
        seen: set[str] = set()
        for s in all_suggestions:
            key = f"{s.rule_name}:{s.original_sql[:80]}"
            if key not in seen:
                seen.add(key)
                batch.top_suggestions.append(s)
            if len(batch.top_suggestions) >= 20:
                break

        return batch

    def apply_rewrite(
        self, suggestion: RewriteSuggestion, original_sql: str
    ) -> tuple[str, bool]:
        """Apply a rewrite suggestion with safety checks.

        Args:
            suggestion: The suggestion to apply.
            original_sql: Original SQL for verification.

        Returns:
            Tuple of (rewritten_sql, was_auto_applied).
        """
        if not suggestion.rewritten_sql:
            logger.debug("Suggestion %s has no rewritten SQL", suggestion.rule_name)
            return original_sql, False

        rewritten = suggestion.rewritten_sql
        auto_applied = suggestion.confidence >= self._auto_apply_threshold

        if auto_applied:
            logger.info(
                "Auto-applied rewrite %s (confidence=%.2f): %s",
                suggestion.rule_name,
                suggestion.confidence,
                rewritten[:100],
            )

        return rewritten, auto_applied

    def apply_all_safe(
        self,
        suggestions: list[RewriteSuggestion],
        original_sql: str,
    ) -> str:
        """Apply all high-confidence suggestions sequentially.

        Only applies suggestions with confidence >= auto_apply_threshold.
        Returns the final rewritten SQL (or original if no safe rewrites).
        """
        current_sql = original_sql

        for s in sorted(suggestions, key=lambda x: x.confidence, reverse=True):
            if s.confidence >= self._auto_apply_threshold and s.rewritten_sql:
                current_sql = s.rewritten_sql
                logger.info("Applied safe rewrite: %s", s.rule_name)

        return current_sql

    def get_history(self, limit: int = 50) -> list[RewriteSuggestion]:
        """Return recent suggestion history."""
        return self._history[-limit:]

    def persist_suggestions(
        self,
        suggestions: list[RewriteSuggestion],
        tenant_id: str,
        engine: Any,
    ) -> int:
        """Persist multiple suggestions to the database.

        Args:
            suggestions: Suggestions to store.
            tenant_id: Tenant identifier.
            engine: SQLAlchemy engine.

        Returns:
            Number of suggestions persisted.
        """
        from metamind.core.rewrite.analyzer import RewriteSuggester as AnalyzerSuggester

        persister = AnalyzerSuggester(self._analyzer)
        count = 0
        for s in suggestions:
            try:
                persister.persist_suggestion(s, tenant_id, engine)
                count += 1
            except Exception as exc:
                logger.debug("Could not persist suggestion: %s", exc)

        logger.info("Persisted %d/%d suggestions for tenant %s", count, len(suggestions), tenant_id)
        return count

    def get_rule_stats(self) -> dict[str, dict[str, Any]]:
        """Return statistics about which rules fired most frequently."""
        stats: dict[str, dict[str, Any]] = {}
        for s in self._history:
            if s.rule_name not in stats:
                stats[s.rule_name] = {
                    "count": 0,
                    "total_improvement": 0.0,
                    "avg_confidence": 0.0,
                }
            stats[s.rule_name]["count"] += 1
            stats[s.rule_name]["total_improvement"] += s.estimated_improvement_pct

        for rule, data in stats.items():
            data["avg_improvement"] = data["total_improvement"] / max(1, data["count"])

        return stats
