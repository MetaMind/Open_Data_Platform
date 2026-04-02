"""F03 — Skew Compensation strategies for join execution."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SkewInfo:
    """Skew analysis result for a column."""

    table_name: str
    column_name: str
    skew_ratio: float
    is_skewed: bool
    top_values: list[dict[str, object]]
    compensation: str          # none, broadcast, salt
    estimated_skew_factor: float = 1.0


class SkewCompensator:
    """Applies skew compensation strategies to query plans (F03).

    Supports:
    - Broadcast join: replicate small side to all nodes
    - Salt join: randomly bucket skewed keys to distribute load
    - Partial broadcast: broadcast only skewed keys, shuffle the rest
    """

    def get_salt_factor(self, skew_info: SkewInfo) -> int:
        """Compute optimal salt factor based on skew ratio."""
        if skew_info.skew_ratio <= 0:
            return 1
        return max(2, min(64, math.ceil(skew_info.estimated_skew_factor / 2)))

    def build_salt_sql(
        self, original_sql: str, salt_column: str, salt_factor: int
    ) -> str:
        """Generate salted SQL for skew compensation.

        Adds a random bucket to join key, distributing skewed keys across partitions.
        """
        return f"""
WITH salted AS (
    SELECT *, (HASHTEXT(CAST({salt_column} AS TEXT)) % {salt_factor}) AS __salt_bucket
    FROM ({original_sql}) __base
)
SELECT * FROM salted
""".strip()

    def build_broadcast_hint(self, table_name: str, dialect: str = "spark") -> str:
        """Generate broadcast hint for small-table join optimization."""
        hints = {
            "spark": f"/*+ BROADCAST({table_name}) */",
            "postgres": f"/* metamind:broadcast:{table_name} */",
            "duckdb": f"/* metamind:broadcast:{table_name} */",
        }
        return hints.get(dialect, f"/* broadcast:{table_name} */")

    def apply_compensation(
        self,
        sql: str,
        skew_info: SkewInfo,
        dialect: str = "postgres",
    ) -> tuple[str, dict[str, object]]:
        """Apply recommended compensation to SQL query.

        Returns:
            (modified_sql, compensation_metadata)
        """
        comp = skew_info.compensation
        metadata: dict[str, object] = {
            "compensation_applied": comp,
            "original_column": skew_info.column_name,
            "skew_ratio": skew_info.skew_ratio,
        }

        if comp == "broadcast":
            hint = self.build_broadcast_hint(skew_info.table_name, dialect)
            modified = f"SELECT {hint} * FROM ({sql}) __broadcast_wrapped"
            metadata["broadcast_hint"] = hint
            logger.info(
                "Applied broadcast compensation for %s.%s",
                skew_info.table_name, skew_info.column_name
            )
            return modified, metadata

        if comp == "salt":
            factor = self.get_salt_factor(skew_info)
            modified = self.build_salt_sql(sql, skew_info.column_name, factor)
            metadata["salt_factor"] = factor
            logger.info(
                "Applied salt compensation (factor=%d) for %s.%s",
                factor, skew_info.table_name, skew_info.column_name
            )
            return modified, metadata

        # No compensation
        return sql, metadata

    def estimate_improvement(self, skew_info: SkewInfo) -> float:
        """Estimate query time improvement factor from applying compensation."""
        if not skew_info.is_skewed or skew_info.compensation == "none":
            return 1.0

        factor = skew_info.estimated_skew_factor
        if skew_info.compensation == "broadcast":
            # Broadcast eliminates shuffle: ~2-5x improvement
            return min(5.0, 1.0 + factor * 0.5)
        elif skew_info.compensation == "salt":
            salt_f = self.get_salt_factor(skew_info)
            # Salt reduces max partition size by salt_factor
            return min(factor, float(salt_f))

        return 1.0
