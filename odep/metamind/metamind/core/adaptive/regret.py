"""F20 — Regret Minimization: tracks optimization decisions and learns from mistakes."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class OptimizationDecision:
    """A recorded optimization decision with outcome tracking."""

    query_id: str
    tenant_id: str
    decision_type: str          # join_order, index_choice, engine_routing, mv_selection
    chosen_option: str          # Description of choice made
    alternatives: list[str]     # Other options considered
    predicted_cost: float
    actual_cost: Optional[float] = None
    regret: float = 0.0
    decided_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class RuleWeight:
    """Weight for an optimization rule, updated via regret minimization."""

    rule_name: str
    tenant_id: str
    weight: float = 1.0
    cumulative_regret: float = 0.0
    update_count: int = 0

    def update(self, observed_regret: float, learning_rate: float = 0.1) -> None:
        """Update weight using multiplicative weights algorithm."""
        self.cumulative_regret += observed_regret
        self.update_count += 1
        # Multiplicative weight update: penalize rules that incur regret
        penalty = math.exp(-learning_rate * observed_regret)
        self.weight = max(0.01, self.weight * penalty)

    @property
    def average_regret(self) -> float:
        """Average regret per decision."""
        if self.update_count == 0:
            return 0.0
        return self.cumulative_regret / self.update_count


class RegretTracker:
    """Tracks optimization decisions and computes regret from actual outcomes (F20).

    Implements online multiplicative weights regret minimization.
    """

    def __init__(self, engine: Engine, tenant_id: str) -> None:
        """Initialize with database engine and tenant context."""
        self._engine = engine
        self._tenant_id = tenant_id
        self._rule_weights: dict[str, RuleWeight] = {}

    def record_decision(self, decision: OptimizationDecision) -> None:
        """Persist an optimization decision to the tracking table."""
        import json
        stmt = sa.text(
            """INSERT INTO mm_optimization_decisions
               (tenant_id, query_id, decision_type, chosen_option, alternatives,
                predicted_cost, actual_cost, regret, decided_at)
               VALUES (:tid, :qid, :dtype, :chosen, :alts::jsonb,
                       :pred_cost, :actual_cost, :regret, :decided_at)"""
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, {
                "tid": self._tenant_id,
                "qid": decision.query_id,
                "dtype": decision.decision_type,
                "chosen": decision.chosen_option,
                "alts": json.dumps(decision.alternatives),
                "pred_cost": decision.predicted_cost,
                "actual_cost": decision.actual_cost,
                "regret": decision.regret,
                "decided_at": decision.decided_at,
            })

    def record_outcome(
        self, query_id: str, actual_duration_ms: float
    ) -> Optional[float]:
        """Record actual query duration and compute regret for this decision.

        Returns computed regret value, or None if no decision found.
        """
        stmt = sa.text(
            """SELECT id, decision_type, chosen_option, predicted_cost
               FROM mm_optimization_decisions
               WHERE tenant_id = :tid AND query_id = :qid
               ORDER BY decided_at DESC LIMIT 1"""
        )
        with self._engine.connect() as conn:
            row = conn.execute(stmt, {"tid": self._tenant_id, "qid": query_id}).fetchone()

        if row is None:
            return None

        decision_id, decision_type, chosen, predicted_cost = row
        regret = max(0.0, actual_duration_ms - predicted_cost)

        # Update the record
        update_stmt = sa.text(
            """UPDATE mm_optimization_decisions
               SET actual_cost = :actual, regret = :regret
               WHERE id = :id"""
        )
        with self._engine.begin() as conn:
            conn.execute(update_stmt, {
                "actual": actual_duration_ms,
                "regret": regret,
                "id": decision_id,
            })

        # Update rule weights
        self._update_rule_weight(str(decision_type), regret)
        logger.debug(
            "Recorded regret %.2f for decision %s (query %s)",
            regret, decision_type, query_id
        )
        return regret

    def get_rule_weight(self, rule_name: str) -> float:
        """Get current weight for an optimization rule."""
        if rule_name in self._rule_weights:
            return self._rule_weights[rule_name].weight

        # Load from DB
        stmt = sa.text(
            """SELECT weight FROM mm_regret_scores
               WHERE tenant_id = :tid AND rule_name = :rule"""
        )
        with self._engine.connect() as conn:
            row = conn.execute(stmt, {"tid": self._tenant_id, "rule": rule_name}).fetchone()

        if row:
            w = float(row[0])
            self._rule_weights[rule_name] = RuleWeight(
                rule_name=rule_name, tenant_id=self._tenant_id, weight=w
            )
            return w

        return 1.0  # Default weight

    def get_top_regret_rules(self, limit: int = 10) -> list[dict[str, object]]:
        """Return rules with highest cumulative regret — targets for improvement."""
        stmt = sa.text(
            """SELECT rule_name, cumulative_regret, weight, update_count
               FROM mm_regret_scores
               WHERE tenant_id = :tid
               ORDER BY cumulative_regret DESC
               LIMIT :limit"""
        )
        with self._engine.connect() as conn:
            rows = conn.execute(stmt, {"tid": self._tenant_id, "limit": limit}).fetchall()

        return [
            {
                "rule_name": r[0],
                "cumulative_regret": float(r[1]),
                "weight": float(r[2]),
                "update_count": int(r[3]),
                "average_regret": float(r[1]) / max(1, int(r[3])),
            }
            for r in rows
        ]

    def _update_rule_weight(self, rule_name: str, regret: float) -> None:
        """Update in-memory weight and persist to database."""
        if rule_name not in self._rule_weights:
            self._rule_weights[rule_name] = RuleWeight(
                rule_name=rule_name, tenant_id=self._tenant_id
            )

        rule = self._rule_weights[rule_name]
        rule.update(regret)

        # Persist
        stmt = sa.text(
            """INSERT INTO mm_regret_scores (tenant_id, rule_name, cumulative_regret, weight, update_count, updated_at)
               VALUES (:tid, :rule, :cr, :w, 1, NOW())
               ON CONFLICT (tenant_id, rule_name) DO UPDATE SET
               cumulative_regret = mm_regret_scores.cumulative_regret + EXCLUDED.cumulative_regret,
               weight = EXCLUDED.weight,
               update_count = mm_regret_scores.update_count + 1,
               updated_at = NOW()"""
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, {
                "tid": self._tenant_id,
                "rule": rule_name,
                "cr": regret,
                "w": rule.weight,
            })
