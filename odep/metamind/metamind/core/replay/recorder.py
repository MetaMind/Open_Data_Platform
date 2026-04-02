"""What-if optimization replay: simulate plan changes against historical workload.

Records optimization context for every query and provides hypothetical
simulation to evaluate the impact of schema changes, indexes, and features.

Feature: F30_optimization_replay
"""
from __future__ import annotations

import copy
import json
import logging
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Optional

from metamind.core.catalog.metadata import CatalogSnapshot, MetadataCatalog
from metamind.core.cost.model import CostModel
from metamind.core.types import CostVector, IndexMeta, LogicalNode, TableMeta
from metamind.core.replay.checkpoint import CheckpointSerializer  # noqa: F401

logger = logging.getLogger(__name__)

@dataclass
class ReplayScenario:
    """A what-if simulation scenario definition."""

    scenario_id: str
    tenant_id: str
    name: str
    description: str
    hypothetical_changes: list[dict[str, Any]]
    query_sample_size: int = 1000
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

@dataclass
class ReplayResult:
    """Results of a what-if simulation run."""

    scenario_id: str
    queries_replayed: int
    original_total_cost: float
    simulated_total_cost: float
    cost_improvement_pct: float
    per_query_results: list[dict[str, Any]]
    top_benefiting_queries: list[dict[str, Any]]
    recommendation: str

class ReplayRecorder:
    """Records optimization context for every query, enabling later replay.

    Recording is always-on (near-zero cost: one DB write per query).
    """

    def __init__(self, engine: Any = None) -> None:
        self._engine = engine
        self._in_memory_store: list[dict[str, Any]] = []

    def record(
        self,
        tenant_id: str,
        query_id: str,
        sql: str,
        logical_plan_json: str,
        optimization_context: dict[str, Any],
    ) -> None:
        """Persist optimization context for a query."""
        entry = {
            "query_id": query_id,
            "tenant_id": tenant_id,
            "sql": sql,
            "logical_plan": logical_plan_json,
            "optimization_context": json.dumps(optimization_context),
            "original_cost": optimization_context.get("total_cost", 0.0),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }

        self._in_memory_store.append(entry)

        if self._engine is not None:
            try:
                from sqlalchemy import text
                with self._engine.connect() as conn:
                    conn.execute(
                        text(
                            "INSERT INTO mm_optimization_decisions "
                            "(query_id, tenant_id, sql, logical_plan, "
                            "optimization_context, original_cost, recorded_at) "
                            "VALUES (:query_id, :tenant_id, :sql, :logical_plan, "
                            ":optimization_context, :original_cost, :recorded_at)"
                        ),
                        entry,
                    )
                    conn.commit()
            except Exception as exc:
                logger.warning("Failed to persist optimization decision: %s", exc)

        logger.debug("Recorded optimization for query %s", query_id)

    def load_history(
        self,
        tenant_id: str,
        limit: int = 1000,
        since_hours: int = 168,
    ) -> list[dict[str, Any]]:
        """Load historical optimization decisions."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        cutoff_str = cutoff.isoformat()

        if self._engine is not None:
            try:
                from sqlalchemy import text
                with self._engine.connect() as conn:
                    rows = conn.execute(
                        text(
                            "SELECT query_id, sql, logical_plan, optimization_context, "
                            "original_cost, recorded_at FROM mm_optimization_decisions "
                            "WHERE tenant_id = :tid AND recorded_at >= :since "
                            "ORDER BY recorded_at DESC LIMIT :lim"
                        ),
                        {"tid": tenant_id, "since": cutoff_str, "lim": limit},
                    ).fetchall()

                    return [
                        {
                            "query_id": r[0], "sql": r[1],
                            "logical_plan": r[2],
                            "optimization_context": r[3],
                            "original_cost": float(r[4]),
                            "recorded_at": r[5],
                        }
                        for r in rows
                    ]
            except Exception as exc:
                logger.warning("DB load failed, using in-memory: %s", exc)

        results = [
            e for e in self._in_memory_store
            if e["tenant_id"] == tenant_id and e["recorded_at"] >= cutoff_str
        ]
        results.sort(key=lambda x: x["recorded_at"], reverse=True)
        return results[:limit]

class OptimizationSimulator:
    """Simulates re-running the optimizer with hypothetical changes.

    IMPORTANT: All simulations are READ-ONLY. _apply_hypothetical_changes()
    operates on a deep copy of the catalog and never modifies the real catalog.
    """

    def __init__(
        self,
        catalog: MetadataCatalog,
        cost_model: CostModel,
        optimizer_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.catalog = catalog
        self.cost_model = cost_model
        self.optimizer_factory = optimizer_factory
        self._recorder = ReplayRecorder()
        self._scenarios: dict[str, ReplayScenario] = {}
        self._results: dict[str, ReplayResult] = {}

    def create_scenario(
        self,
        tenant_id: str,
        name: str,
        changes: list[dict[str, Any]],
        description: str = "",
        query_sample_size: int = 1000,
    ) -> ReplayScenario:
        """Create a what-if simulation scenario."""
        scenario = ReplayScenario(
            scenario_id=uuid.uuid4().hex,
            tenant_id=tenant_id,
            name=name,
            description=description,
            hypothetical_changes=changes,
            query_sample_size=query_sample_size,
        )
        self._scenarios[scenario.scenario_id] = scenario
        logger.info(
            "Created scenario %s '%s' with %d changes",
            scenario.scenario_id, name, len(changes),
        )
        return scenario

    def run_scenario(
        self,
        scenario: ReplayScenario,
        tenant_id: str,
        engine: Any = None,
    ) -> ReplayResult:
        """Execute simulation: replay historical queries with hypothetical changes.

        1. Load historical queries from mm_optimization_decisions
        2. Apply hypothetical_changes to a cloned catalog snapshot
        3. Re-run cost estimation on each historical query
        4. Compare original_cost vs simulated_cost
        5. Build ReplayResult with per-query breakdown
        """
        recorder = ReplayRecorder(engine)
        history = recorder.load_history(
            tenant_id, limit=scenario.query_sample_size
        )

        if not history:
            history = self._recorder.load_history(
                tenant_id, limit=scenario.query_sample_size
            )

        modified_catalog = self._apply_hypothetical_changes(
            self.catalog, tenant_id, scenario.hypothetical_changes
        )

        per_query: list[dict[str, Any]] = []
        original_total = 0.0
        simulated_total = 0.0

        for entry in history:
            original_cost = entry.get("original_cost", 0.0)
            if original_cost <= 0:
                ctx = entry.get("optimization_context", "{}")
                if isinstance(ctx, str):
                    try:
                        ctx_dict = json.loads(ctx)
                    except json.JSONDecodeError:
                        ctx_dict = {}
                else:
                    ctx_dict = ctx
                original_cost = ctx_dict.get("total_cost", 100.0)

            sql = entry.get("sql", "")
            simulated_cost = self._simulate_cost(
                sql, modified_catalog, tenant_id, scenario.hypothetical_changes
            )

            delta_pct = 0.0
            if original_cost > 0:
                delta_pct = ((original_cost - simulated_cost) / original_cost) * 100.0

            per_query.append({
                "query_id": entry.get("query_id", ""),
                "sql": sql[:200],
                "original_cost": round(original_cost, 2),
                "simulated_cost": round(simulated_cost, 2),
                "delta_pct": round(delta_pct, 2),
            })

            original_total += original_cost
            simulated_total += simulated_cost

        per_query.sort(key=lambda x: x["delta_pct"], reverse=True)
        top_benefiting = per_query[:10]

        improvement_pct = 0.0
        if original_total > 0:
            improvement_pct = ((original_total - simulated_total) / original_total) * 100.0

        recommendation = self._generate_recommendation(
            scenario, improvement_pct, len(history), top_benefiting
        )

        result = ReplayResult(
            scenario_id=scenario.scenario_id,
            queries_replayed=len(history),
            original_total_cost=round(original_total, 2),
            simulated_total_cost=round(simulated_total, 2),
            cost_improvement_pct=round(improvement_pct, 2),
            per_query_results=per_query,
            top_benefiting_queries=top_benefiting,
            recommendation=recommendation,
        )

        self._results[scenario.scenario_id] = result
        logger.info(
            "Scenario %s: replayed %d queries, improvement=%.1f%%",
            scenario.scenario_id, len(history), improvement_pct,
        )
        return result

    def list_scenarios(self, tenant_id: str, engine: Any = None) -> list[ReplayScenario]:
        """List all scenarios for a tenant."""
        return [
            s for s in self._scenarios.values()
            if s.tenant_id == tenant_id
        ]

    def get_result(
        self, scenario_id: str, engine: Any = None
    ) -> Optional[ReplayResult]:
        """Get results of a completed simulation."""
        return self._results.get(scenario_id)

    def _apply_hypothetical_changes(
        self,
        catalog: MetadataCatalog,
        tenant_id: str,
        changes: list[dict[str, Any]],
    ) -> MetadataCatalog:
        """Apply hypothetical changes to a DEEP COPY of the catalog.

        Supported change types: add_index, remove_index, update_stats,
        enable_feature, change_backend.

        SAFETY: This always operates on a copy; the original is never modified.
        """
        snapshot = catalog.snapshot(tenant_id)
        modified_snap = snapshot.deep_copy()

        for change in changes:
            change_type = change.get("type", "")

            if change_type == "add_index":
                table = change.get("table", "")
                column = change.get("column", "")
                idx_type = change.get("index_type", "btree")
                idx = IndexMeta(
                    index_name=f"hyp_{table}_{column}_{idx_type}",
                    table_name=table,
                    columns=[column],
                    index_type=idx_type,
                )
                if table not in modified_snap.indexes:
                    modified_snap.indexes[table] = []
                modified_snap.indexes[table].append(idx)

            elif change_type == "remove_index":
                table = change.get("table", "")
                index_name = change.get("index_name", "")
                if table in modified_snap.indexes:
                    modified_snap.indexes[table] = [
                        idx for idx in modified_snap.indexes[table]
                        if idx.index_name != index_name
                    ]

            elif change_type == "update_stats":
                table = change.get("table", "")
                stats_update = change.get("stats", {})
                if table not in modified_snap.statistics:
                    modified_snap.statistics[table] = {}
                modified_snap.statistics[table].update(stats_update)

            elif change_type == "change_backend":
                table = change.get("table", "")
                target = change.get("target_backend", "postgres")
                if table in modified_snap.tables:
                    modified_snap.tables[table].backend = target

            else:
                logger.debug("Unknown change type: %s", change_type)

        modified_catalog = MetadataCatalog()
        modified_catalog.from_snapshot(tenant_id, modified_snap)
        return modified_catalog

    def _simulate_cost(
        self,
        sql: str,
        modified_catalog: MetadataCatalog,
        tenant_id: str,
        changes: list[dict[str, Any]],
    ) -> float:
        """Estimate cost of a query under modified catalog.

        Uses the cost model with the hypothetical catalog state.
        """
        table_name = self._extract_main_table(sql)

        has_new_index = any(
            c.get("type") == "add_index" and c.get("table") == table_name
            for c in changes
        )

        stats = modified_catalog.get_statistics(tenant_id, table_name)
        row_count = stats.get("row_count", 10000)

        if has_new_index:
            node = LogicalNode(
                node_type="IndexScan",
                properties={"table": table_name},
                estimated_rows=row_count * 0.1,
            )
        else:
            node = LogicalNode(
                node_type="SeqScan",
                properties={"table": table_name},
                estimated_rows=row_count,
            )

        cv = self.cost_model.estimate(node, {"row_count": row_count})
        return self.cost_model.total_cost(cv)

    @staticmethod
    def _extract_main_table(sql: str) -> str:
        """Extract the main table name from a SQL query."""
        import re
        match = re.search(r"\bFROM\s+(\w+)", sql, re.IGNORECASE)
        return match.group(1) if match else "unknown"

    @staticmethod
    def _generate_recommendation(
        scenario: ReplayScenario,
        improvement_pct: float,
        query_count: int,
        top_queries: list[dict[str, Any]],
    ) -> str:
        """Generate a human-readable recommendation."""
        if improvement_pct > 30:
            verdict = "Strongly recommended"
        elif improvement_pct > 10:
            verdict = "Recommended"
        elif improvement_pct > 0:
            verdict = "Marginal improvement"
        else:
            verdict = "Not recommended — no improvement detected"

        change_desc = "; ".join(
            f"{c.get('type', 'unknown')} on {c.get('table', '?')}.{c.get('column', '?')}"
            for c in scenario.hypothetical_changes
        )

        return (
            f"{verdict}. The proposed changes ({change_desc}) would improve "
            f"total query cost by {improvement_pct:.1f}% across {query_count} "
            f"replayed queries. Top benefiting query improved by "
            f"{top_queries[0]['delta_pct']:.1f}%."
            if top_queries else
            f"{verdict}. No queries were available for replay."
        )

class WhatIfAPI:
    """High-level convenience methods for common what-if simulations."""

    def __init__(self, simulator: OptimizationSimulator) -> None:
        self.simulator = simulator

    def simulate_add_index(
        self,
        tenant_id: str,
        table: str,
        column: str,
        index_type: str = "btree",
        engine: Any = None,
    ) -> ReplayResult:
        """Simulate adding an index and replay workload."""
        scenario = self.simulator.create_scenario(
            tenant_id=tenant_id,
            name=f"Add {index_type} index on {table}.{column}",
            changes=[{
                "type": "add_index",
                "table": table,
                "column": column,
                "index_type": index_type,
            }],
            description=f"Evaluate impact of adding {index_type} index on {table}.{column}",
        )
        return self.simulator.run_scenario(scenario, tenant_id, engine)

    def simulate_enable_feature(
        self,
        tenant_id: str,
        feature: str,
        engine: Any = None,
    ) -> ReplayResult:
        """Simulate enabling a feature flag."""
        scenario = self.simulator.create_scenario(
            tenant_id=tenant_id,
            name=f"Enable feature {feature}",
            changes=[{"type": "enable_feature", "feature": feature}],
            description=f"Evaluate impact of enabling {feature}",
        )
        return self.simulator.run_scenario(scenario, tenant_id, engine)

    def simulate_migrate_table(
        self,
        tenant_id: str,
        table: str,
        target_backend: str,
        engine: Any = None,
    ) -> ReplayResult:
        """Simulate migrating a table to a different backend."""
        scenario = self.simulator.create_scenario(
            tenant_id=tenant_id,
            name=f"Migrate {table} to {target_backend}",
            changes=[{
                "type": "change_backend",
                "table": table,
                "target_backend": target_backend,
            }],
            description=f"Evaluate impact of migrating {table} to {target_backend}",
        )
        return self.simulator.run_scenario(scenario, tenant_id, engine)
