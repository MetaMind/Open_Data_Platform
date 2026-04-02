"""
Rule Generator — Synthesize Optimizer Transformation Rules from Workload Patterns

File: metamind/synthesis/rule_generator.py
Role: Query Optimizer Engineer
Dependencies: sqlalchemy, metamind.synthesis.workload_profiler

Analyses aggregate workload statistics to produce SynthesizedRule objects
representing reusable optimisation patterns.  Rules are upserted into
mm_synthesized_rules and optionally mirrored as routing policies in
mm_routing_policies so that QueryRouter can act on them immediately.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from metamind.synthesis.workload_profiler import WorkloadStats

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------

@dataclass
class SynthesizedRule:
    """An optimizer transformation rule derived from workload analysis."""
    rule_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    rule_type: str = ""                   # join_order | pushdown | engine_affinity | agg_pushdown
    condition: Dict[str, Any] = field(default_factory=dict)
    transformation: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    support_count: int = 0
    tenant_id: str = ""
    is_active: bool = True


# ---------------------------------------------------------------------------
# Rule generation helpers
# ---------------------------------------------------------------------------

def _join_order_rules(
    stats: WorkloadStats,
    tenant_id: str,
    query_patterns: List[Dict[str, Any]],
) -> List[SynthesizedRule]:
    """
    Emit a JoinOrderRule when a consistent table pair (A ⋈ B) is observed
    to always execute faster as A→B than B→A.
    """
    rules: List[SynthesizedRule] = []
    seen: Dict[str, Dict[str, float]] = {}  # pair_key → {order: avg_ms}

    for p in query_patterns:
        tables = p.get("tables", [])
        runtime = float(p.get("runtime_ms", 0))
        if len(tables) < 2:
            continue
        pair = tuple(sorted(tables[:2]))
        key = f"{pair[0]}__{pair[1]}"
        order_key = f"{tables[0]}__{tables[1]}"
        if key not in seen:
            seen[key] = {}
        prev = seen[key].get(order_key)
        seen[key][order_key] = (prev + runtime) / 2 if prev else runtime

    for pair_key, orders in seen.items():
        if len(orders) < 2:
            continue
        sorted_orders = sorted(orders.items(), key=lambda x: x[1])
        best_order, best_ms = sorted_orders[0]
        worst_order, worst_ms = sorted_orders[-1]
        if worst_ms == 0:
            continue
        improvement = (worst_ms - best_ms) / worst_ms
        if improvement < 0.20:  # require ≥ 20% improvement
            continue
        tables = best_order.split("__")
        rules.append(
            SynthesizedRule(
                name=f"join_order__{pair_key}",
                description=(
                    f"Prefer join order {tables[0]}→{tables[1]} "
                    f"({improvement*100:.0f}% faster over {len(orders)} observations)"
                ),
                rule_type="join_order",
                condition={"table_pair": pair_key, "min_support": len(orders)},
                transformation={"preferred_order": tables, "estimated_speedup_pct": round(improvement * 100, 1)},
                confidence=min(0.99, 0.5 + improvement),
                support_count=len(orders),
                tenant_id=tenant_id,
            )
        )
    return rules


def _pushdown_rules(
    stats: WorkloadStats,
    tenant_id: str,
    query_patterns: List[Dict[str, Any]],
) -> List[SynthesizedRule]:
    """
    Emit a PushdownRule when queries with filter-before-join show >30%
    runtime improvement over filter-after-join for the same table.
    """
    rules: List[SynthesizedRule] = []
    table_runtimes: Dict[str, Dict[str, List[float]]] = {}  # tbl → {with|without → [ms]}

    for p in query_patterns:
        for tbl in p.get("tables", []):
            if tbl not in table_runtimes:
                table_runtimes[tbl] = {"with_filter": [], "without_filter": []}
            key = "with_filter" if p.get("filter_count", 0) > 0 else "without_filter"
            table_runtimes[tbl][key].append(float(p.get("runtime_ms", 0)))

    for tbl, buckets in table_runtimes.items():
        wf = buckets["with_filter"]
        wo = buckets["without_filter"]
        if not wf or not wo:
            continue
        avg_wf = sum(wf) / len(wf)
        avg_wo = sum(wo) / len(wo)
        if avg_wo == 0:
            continue
        improvement = (avg_wo - avg_wf) / avg_wo
        if improvement < 0.30:
            continue
        rules.append(
            SynthesizedRule(
                name=f"pushdown_filter__{tbl}",
                description=(
                    f"Push down filter on '{tbl}' before join "
                    f"({improvement*100:.0f}% faster, {len(wf)} observations)"
                ),
                rule_type="pushdown",
                condition={"table": tbl, "has_filter": True, "min_improvement_pct": 30},
                transformation={"action": "push_filter_before_join", "target_table": tbl,
                                 "estimated_speedup_pct": round(improvement * 100, 1)},
                confidence=min(0.95, 0.4 + improvement),
                support_count=len(wf),
                tenant_id=tenant_id,
            )
        )
    return rules


def _engine_affinity_rules(
    stats: WorkloadStats,
    tenant_id: str,
    query_patterns: List[Dict[str, Any]],
) -> List[SynthesizedRule]:
    """
    Emit an EngineAffinityRule when table T consistently performs faster
    on engine E than any other engine.
    """
    rules: List[SynthesizedRule] = []
    tbl_engine_ms: Dict[str, Dict[str, List[float]]] = {}

    for p in query_patterns:
        eng = p.get("engine", "")
        for tbl in p.get("tables", []):
            if tbl not in tbl_engine_ms:
                tbl_engine_ms[tbl] = {}
            if eng not in tbl_engine_ms[tbl]:
                tbl_engine_ms[tbl][eng] = []
            tbl_engine_ms[tbl][eng].append(float(p.get("runtime_ms", 0)))

    for tbl, eng_map in tbl_engine_ms.items():
        if len(eng_map) < 2:
            continue
        averages = {eng: sum(times) / len(times) for eng, times in eng_map.items()}
        best_eng = min(averages, key=lambda e: averages[e])
        worst_avg = max(averages.values())
        best_avg = averages[best_eng]
        if worst_avg == 0:
            continue
        improvement = (worst_avg - best_avg) / worst_avg
        if improvement < 0.25:
            continue
        support = len(eng_map.get(best_eng, []))
        rules.append(
            SynthesizedRule(
                name=f"engine_affinity__{tbl}__{best_eng}",
                description=(
                    f"Route queries on '{tbl}' to {best_eng} "
                    f"({improvement*100:.0f}% faster, support={support})"
                ),
                rule_type="engine_affinity",
                condition={"table": tbl, "engines_observed": list(eng_map.keys())},
                transformation={"preferred_engine": best_eng,
                                 "estimated_speedup_pct": round(improvement * 100, 1)},
                confidence=min(0.95, 0.45 + improvement),
                support_count=support,
                tenant_id=tenant_id,
            )
        )
    return rules


def _agg_pushdown_rules(
    stats: WorkloadStats,
    tenant_id: str,
    query_patterns: List[Dict[str, Any]],
) -> List[SynthesizedRule]:
    """
    Emit an AggregationPushdownRule when pushing GROUP BY through JOIN is
    beneficial (agg_count > 0 and join_count > 0 and improvement measurable).
    """
    rules: List[SynthesizedRule] = []
    if stats.agg_heavy_pct < 20:
        return rules

    # Identify table pairs where agg queries are consistently slow
    for p in query_patterns:
        if p.get("agg_count", 0) == 0 or p.get("join_count", 0) == 0:
            continue
        tables = p.get("tables", [])
        if not tables:
            continue
        primary = tables[0]
        rules.append(
            SynthesizedRule(
                name=f"agg_pushdown__{primary}",
                description=(
                    f"Push GROUP BY through JOIN on '{primary}' "
                    f"to reduce intermediate rows"
                ),
                rule_type="agg_pushdown",
                condition={"table": primary, "has_agg": True, "has_join": True},
                transformation={"action": "push_agg_below_join", "target_table": primary},
                confidence=0.72,
                support_count=1,
                tenant_id=tenant_id,
            )
        )
    # Deduplicate by name
    seen_names: Dict[str, SynthesizedRule] = {}
    for r in rules:
        if r.name not in seen_names:
            seen_names[r.name] = r
        else:
            seen_names[r.name].support_count += 1
    return list(seen_names.values())


# ---------------------------------------------------------------------------
# RuleGenerator
# ---------------------------------------------------------------------------

class RuleGenerator:
    """
    Generates SynthesizedRule objects from workload statistics and registers
    them in ``mm_synthesized_rules`` + mirrors high-confidence rules to
    ``mm_routing_policies`` for immediate use by QueryRouter.
    """

    _MIN_CONFIDENCE_FOR_POLICY = 0.75

    def __init__(self, db_engine: AsyncEngine) -> None:
        self._db = db_engine

    async def generate_rules(
        self,
        tenant_id: str,
        workload_stats: WorkloadStats,
    ) -> List[SynthesizedRule]:
        """Produce rules from workload stats + recent query patterns."""
        patterns = await self._fetch_recent_patterns(tenant_id)
        rules: List[SynthesizedRule] = []
        rules.extend(_join_order_rules(workload_stats, tenant_id, patterns))
        rules.extend(_pushdown_rules(workload_stats, tenant_id, patterns))
        rules.extend(_engine_affinity_rules(workload_stats, tenant_id, patterns))
        rules.extend(_agg_pushdown_rules(workload_stats, tenant_id, patterns))
        logger.info(
            "RuleGenerator.generate_rules tenant=%s generated=%d rules",
            tenant_id,
            len(rules),
        )
        return rules

    async def register_rules(self, rules: List[SynthesizedRule]) -> None:
        """Upsert rules into mm_synthesized_rules and mirror to routing policies."""
        if not rules:
            return
        async with self._db.begin() as conn:
            for rule in rules:
                await conn.execute(
                    text(
                        """
                        INSERT INTO mm_synthesized_rules (
                            rule_id, tenant_id, name, description, rule_type,
                            condition, transformation, confidence, support_count,
                            is_active, created_at
                        ) VALUES (
                            :rid, :tid, :name, :desc, :rtype,
                            :cond::jsonb, :trans::jsonb, :conf, :supp,
                            TRUE, NOW()
                        )
                        ON CONFLICT (tenant_id, name) DO UPDATE SET
                            description = EXCLUDED.description,
                            confidence = EXCLUDED.confidence,
                            support_count = mm_synthesized_rules.support_count + EXCLUDED.support_count,
                            transformation = EXCLUDED.transformation,
                            is_active = TRUE
                        """
                    ),
                    {
                        "rid": rule.rule_id,
                        "tid": rule.tenant_id,
                        "name": rule.name,
                        "desc": rule.description,
                        "rtype": rule.rule_type,
                        "cond": json.dumps(rule.condition),
                        "trans": json.dumps(rule.transformation),
                        "conf": rule.confidence,
                        "supp": rule.support_count,
                    },
                )
                # Mirror high-confidence engine affinity rules to routing policies
                if (
                    rule.rule_type == "engine_affinity"
                    and rule.confidence >= self._MIN_CONFIDENCE_FOR_POLICY
                ):
                    await self._mirror_to_routing_policy(conn, rule)

    async def retire_stale_rules(
        self,
        tenant_id: str,
        max_age_days: int = 30,
    ) -> int:
        """Mark rules that haven't fired in *max_age_days* as inactive."""
        try:
            async with self._db.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                        UPDATE mm_synthesized_rules
                        SET is_active = FALSE,
                            retired_at = NOW()
                        WHERE tenant_id = :tid
                          AND is_active = TRUE
                          AND (
                              last_fired_at IS NULL
                              OR last_fired_at < NOW() - :days * INTERVAL '1 day'
                          )
                        """
                    ),
                    {"tid": tenant_id, "days": max_age_days},
                )
                retired = result.rowcount
                logger.info(
                    "RuleGenerator.retire_stale_rules tenant=%s retired=%d",
                    tenant_id,
                    retired,
                )
                return retired
        except Exception as exc:
            logger.error(
                "RuleGenerator.retire_stale_rules failed tenant=%s: %s", tenant_id, exc
            )
            return 0

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _fetch_recent_patterns(
        self, tenant_id: str, limit: int = 2000
    ) -> List[Dict[str, Any]]:
        """Return lightweight query pattern dicts from mm_query_logs."""
        try:
            async with self._db.connect() as conn:
                rows = (
                    await conn.execute(
                        text(
                            """
                            SELECT
                                target_source AS engine,
                                total_time_ms AS runtime_ms,
                                join_count, agg_count, filter_count,
                                tables_accessed AS tables_json
                            FROM mm_query_logs
                            WHERE tenant_id = :tid
                              AND status IN ('success', 'completed')
                              AND submitted_at >= NOW() - INTERVAL '7 days'
                            ORDER BY submitted_at DESC
                            LIMIT :lim
                            """
                        ),
                        {"tid": tenant_id, "lim": limit},
                    )
                ).fetchall()
            patterns = []
            for r in rows:
                tables: List[str] = []
                if r.tables_json:
                    raw = r.tables_json
                    if isinstance(raw, list):
                        tables = [str(t) for t in raw]
                    elif isinstance(raw, str):
                        try:
                            tables = json.loads(raw)
                        except Exception:
                            tables = []
                patterns.append(
                    {
                        "engine": r.engine or "",
                        "runtime_ms": float(r.runtime_ms or 0),
                        "join_count": int(r.join_count or 0),
                        "agg_count": int(r.agg_count or 0),
                        "filter_count": int(r.filter_count or 0),
                        "tables": tables,
                    }
                )
            return patterns
        except Exception as exc:
            logger.error(
                "RuleGenerator._fetch_recent_patterns failed tenant=%s: %s",
                tenant_id,
                exc,
            )
            return []

    async def _mirror_to_routing_policy(
        self,
        conn: Any,
        rule: SynthesizedRule,
    ) -> None:
        """Insert/update a row in mm_routing_policies for engine-affinity rules."""
        try:
            target_engine = rule.transformation.get("preferred_engine", "trino")
            await conn.execute(
                text(
                    """
                    INSERT INTO mm_routing_policies (
                        tenant_id, policy_name, description, priority,
                        conditions, target_engine, is_active, created_at
                    ) VALUES (
                        :tid, :name, :desc, 50,
                        :cond::jsonb, :engine, TRUE, NOW()
                    )
                    ON CONFLICT (tenant_id, policy_name) DO UPDATE SET
                        description = EXCLUDED.description,
                        conditions = EXCLUDED.conditions,
                        target_engine = EXCLUDED.target_engine,
                        is_active = TRUE
                    """
                ),
                {
                    "tid": rule.tenant_id,
                    "name": f"synth_{rule.name}",
                    "desc": rule.description,
                    "cond": json.dumps(rule.condition),
                    "engine": target_engine,
                },
            )
        except Exception as exc:
            logger.error(
                "RuleGenerator._mirror_to_routing_policy failed rule=%s: %s",
                rule.name,
                exc,
            )
