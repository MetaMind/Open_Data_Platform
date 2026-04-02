"""AI-Powered Query Auto-Tuner — rewrites slow SQL using LLM assistance.

Triggers when execution_stats['duration_ms'] exceeds latency_threshold_ms.
Uses Anthropic claude-sonnet to suggest optimized rewrites, validated by sqlglot.
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

import sqlglot

logger = logging.getLogger(__name__)


@dataclass
class TuneResult:
    """Result of an AI tuning attempt."""

    original_sql: str
    rewritten_sql: str
    explanation: str
    was_changed: bool
    model_used: str = "claude-sonnet-4-20250514"
    original_sql_hash: str = ""
    rewritten_sql_hash: str = ""
    latency_before_ms: float = 0.0

    def __post_init__(self) -> None:
        self.original_sql_hash = hashlib.sha256(
            self.original_sql.encode()
        ).hexdigest()[:16]
        self.rewritten_sql_hash = hashlib.sha256(
            self.rewritten_sql.encode()
        ).hexdigest()[:16]


class AIQueryTuner:
    """LLM-backed SQL rewriter for slow queries.

    Only activates for queries exceeding latency_threshold_ms. Validates
    all LLM output with sqlglot before returning; falls back to original
    on any parse failure or API error.
    """

    _MODEL = "claude-sonnet-4-20250514"
    _MAX_TOKENS = 2048

    def __init__(
        self,
        llm_client: Any,
        latency_threshold_ms: int = 5000,
    ) -> None:
        self._llm = llm_client
        self._threshold_ms = latency_threshold_ms

    async def tune(
        self,
        sql: str,
        execution_stats: dict[str, Any],
        tenant_id: str,
    ) -> TuneResult:
        """Attempt to rewrite SQL if it exceeds the latency threshold.

        Args:
            sql: Original SQL string.
            execution_stats: Dict containing at minimum 'duration_ms'.
            tenant_id: Tenant scope for logging.

        Returns:
            TuneResult with rewritten_sql == sql when no change is made.
        """
        duration_ms: float = float(execution_stats.get("duration_ms", 0))

        # Queries strictly below the threshold are skipped.
        # Queries at or above the threshold are candidates for rewriting (fixes W-18).
        if duration_ms < self._threshold_ms:
            return TuneResult(
                original_sql=sql,
                rewritten_sql=sql,
                explanation="Query is within latency threshold; no rewrite attempted.",
                was_changed=False,
                latency_before_ms=duration_ms,
            )

        try:
            return await self._rewrite(sql, execution_stats, tenant_id, duration_ms)
        except Exception as exc:
            logger.error(
                "AIQueryTuner LLM call failed for tenant=%s: %s",
                tenant_id,
                exc,
                exc_info=True,
            )
            return TuneResult(
                original_sql=sql,
                rewritten_sql=sql,
                explanation=f"Tuner error (returned original): {exc}",
                was_changed=False,
                latency_before_ms=duration_ms,
            )

    async def _rewrite(
        self,
        sql: str,
        stats: dict[str, Any],
        tenant_id: str,
        duration_ms: float,
    ) -> TuneResult:
        """Internal: call LLM and parse response."""
        explain_excerpt = stats.get("explain_plan", "")[:800]
        slow_predicates = stats.get("slow_predicates", [])

        prompt = self._build_prompt(sql, explain_excerpt, slow_predicates, duration_ms)

        response = await self._llm.messages.create(
            model=self._MODEL,
            max_tokens=self._MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_text: str = response.content[0].text if response.content else ""
        rewritten, explanation = self._parse_response(raw_text, sql)

        if rewritten == sql:
            logger.info(
                "AIQueryTuner: no rewrite for tenant=%s hash=%s",
                tenant_id,
                hashlib.sha256(sql.encode()).hexdigest()[:16],
            )
            return TuneResult(
                original_sql=sql,
                rewritten_sql=sql,
                explanation=explanation or "LLM returned no improvement.",
                was_changed=False,
                latency_before_ms=duration_ms,
            )

        logger.info(
            "AIQueryTuner: rewrite accepted tenant=%s latency_before=%.0fms",
            tenant_id,
            duration_ms,
        )
        return TuneResult(
            original_sql=sql,
            rewritten_sql=rewritten,
            explanation=explanation,
            was_changed=True,
            model_used=self._MODEL,
            latency_before_ms=duration_ms,
        )

    def _build_prompt(
        self,
        sql: str,
        explain_excerpt: str,
        slow_predicates: list[str],
        duration_ms: float,
    ) -> str:
        predicates_str = (
            "\n".join(f"  - {p}" for p in slow_predicates)
            if slow_predicates
            else "  (none identified)"
        )
        return (
            "You are a senior database engineer specializing in SQL performance.\n"
            f"The following query ran in {duration_ms:.0f}ms and is too slow.\n\n"
            "## Original SQL\n"
            f"```sql\n{sql}\n```\n\n"
            "## EXPLAIN Plan Excerpt\n"
            f"```\n{explain_excerpt}\n```\n\n"
            "## Identified Slow Predicates\n"
            f"{predicates_str}\n\n"
            "Rewrite the query to improve performance. "
            "Preserve exact semantics. "
            "Return your answer in this format:\n\n"
            "<rewritten_sql>\nYOUR_OPTIMIZED_SQL_HERE\n</rewritten_sql>\n\n"
            "<explanation>\nBrief explanation of changes.\n</explanation>"
        )

    def _parse_response(self, text: str, original_sql: str) -> tuple[str, str]:
        """Extract rewritten SQL and explanation from LLM response."""
        sql_match = re.search(
            r"<rewritten_sql>\s*(.*?)\s*</rewritten_sql>",
            text,
            re.DOTALL | re.IGNORECASE,
        )
        explanation_match = re.search(
            r"<explanation>\s*(.*?)\s*</explanation>",
            text,
            re.DOTALL | re.IGNORECASE,
        )

        explanation = (
            explanation_match.group(1).strip() if explanation_match else ""
        )

        if not sql_match:
            return original_sql, explanation or "No <rewritten_sql> tag in response."

        candidate = sql_match.group(1).strip()

        if not self._validate_sql(candidate):
            logger.error(
                "AIQueryTuner: LLM produced invalid SQL; reverting to original"
            )
            return original_sql, "Rewritten SQL failed sqlglot validation."

        return candidate, explanation

    def _validate_sql(self, sql: str) -> bool:
        """Return True if sqlglot can parse the SQL without errors."""
        try:
            parsed = sqlglot.parse(sql)
            return bool(parsed) and parsed[0] is not None
        except Exception as exc:
            logger.error("sqlglot validation error: %s", exc)
            return False
