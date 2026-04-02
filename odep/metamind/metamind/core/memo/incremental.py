"""F10 — Incremental Memo Optimization for repeated query templates."""
from __future__ import annotations

import hashlib
import logging
import re
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from metamind.core.logical.nodes import (
    FilterNode,
    JoinNode,
    LogicalNode,
    Predicate,
    ScanNode,
)
from metamind.core.costing.cost_model import CostModel
from metamind.core.metadata.catalog import MetadataCatalog

logger = logging.getLogger(__name__)

# Pattern for stripping literals (strings, numbers) from SQL
_LITERAL_PATTERN = re.compile(
    r"'[^']*'|\"[^\"]*\"|\b\d+(?:\.\d+)?\b", re.IGNORECASE
)


@dataclass
class MemoSnapshot:
    """Serializable snapshot of a memo for a given SQL template."""

    template: str
    tenant_id: str
    memo_groups: dict[str, object]      # group_id → best expression
    best_plan_cost: float
    created_at: datetime
    predicates_hash: str                # hash of the WHERE clause parameters
    hit_count: int = 0
    last_used_at: float = field(default_factory=time.time)


class IncrementalOptimizer:
    """F10: Reuses memo structure from previous optimization of same SQL template.

    Use case: Dashboard running "SELECT region, SUM(sales) FROM orders
               WHERE date >= '2024-01-01' GROUP BY region" every 30 seconds —
    only the date parameter changes. The join order / aggregation plan is identical.

    Algorithm:
    1. Compute template fingerprint (strip literal values)
    2. Check memo_cache for existing snapshot of this template
    3. If found: apply new predicate selectivities to existing best plan
       (update cardinality estimates only, skip full re-enumeration)
    4. If not found: run full Cascades optimization, cache result
    """

    def __init__(
        self,
        memo_cache: Optional[dict[str, MemoSnapshot]] = None,
        max_cache_entries: int = 500,
    ) -> None:
        """Initialize with optional shared cache and LRU eviction limit."""
        # OrderedDict gives us LRU ordering cheaply
        self._cache: OrderedDict[str, MemoSnapshot] = OrderedDict()
        if memo_cache:
            for k, v in memo_cache.items():
                self._cache[k] = v
        self._max_entries = max_cache_entries
        self._hits = 0
        self._misses = 0

    # ── Public API ────────────────────────────────────────────

    def optimize_incremental(
        self,
        sql: str,
        tenant_id: str,
        new_predicates: list[Predicate],
        catalog: MetadataCatalog,
        cost_model: CostModel,
        full_optimizer: Optional[object] = None,
    ) -> Optional[LogicalNode]:
        """Return optimized plan. Reuses cached memo if template matches.

        Falls back to full optimization on cache miss (returns None to signal
        the caller should run full Cascades).
        """
        template = self._extract_template(sql)
        cache_key = f"{tenant_id}:{self._hash_template(template)}"

        if cache_key in self._cache:
            snapshot = self._cache[cache_key]
            if self._templates_match(sql, snapshot.template):
                # Move to end for LRU ordering
                self._cache.move_to_end(cache_key)
                snapshot.hit_count += 1
                snapshot.last_used_at = time.time()
                self._hits += 1
                logger.debug(
                    "Incremental memo hit for template (tenant=%s, key=%s)",
                    tenant_id,
                    cache_key[:16],
                )
                # Apply new selectivities to cached plan
                if snapshot.memo_groups.get("best_plan") is not None:
                    updated = self._update_cardinalities(
                        snapshot.memo_groups["best_plan"],  # type: ignore[arg-type]
                        new_predicates,
                        catalog,
                    )
                    return updated
        self._misses += 1
        logger.debug(
            "Incremental memo miss for template (tenant=%s, key=%s)",
            tenant_id,
            cache_key[:16],
        )
        return None  # Caller runs full optimization

    def store_snapshot(
        self,
        sql: str,
        tenant_id: str,
        best_plan: LogicalNode,
        best_cost: float,
        predicates: list[Predicate],
    ) -> None:
        """Cache the result of a full optimization for future incremental reuse."""
        template = self._extract_template(sql)
        cache_key = f"{tenant_id}:{self._hash_template(template)}"
        predicates_hash = self._hash_predicates(predicates)

        snapshot = MemoSnapshot(
            template=template,
            tenant_id=tenant_id,
            memo_groups={"best_plan": best_plan},
            best_plan_cost=best_cost,
            created_at=datetime.utcnow(),
            predicates_hash=predicates_hash,
        )
        self._cache[cache_key] = snapshot
        self._cache.move_to_end(cache_key)

        if len(self._cache) > self._max_entries:
            self._evict_oldest()

    # ── Template Matching ─────────────────────────────────────

    def _templates_match(self, sql: str, cached_template: str) -> bool:
        """True if SQL differs only in literal values."""
        return self._extract_template(sql) == cached_template

    def _extract_template(self, sql: str) -> str:
        """Strip literal values from SQL to produce a stable template."""
        normalized = sql.strip().upper()
        normalized = re.sub(r"\s+", " ", normalized)
        normalized = _LITERAL_PATTERN.sub("?", normalized)
        return normalized

    def _hash_template(self, template: str) -> str:
        """Short hash of template for cache key."""
        return hashlib.sha256(template.encode()).hexdigest()[:16]

    def _hash_predicates(self, predicates: list[Predicate]) -> str:
        """Hash predicate values for change detection."""
        parts = sorted(
            f"{p.column}{p.operator}{p.value}" for p in predicates
        )
        return hashlib.md5("|".join(parts).encode()).hexdigest()[:12]

    # ── Cardinality Update ────────────────────────────────────

    def _update_cardinalities(
        self,
        cached_plan: LogicalNode,
        new_predicates: list[Predicate],
        catalog: MetadataCatalog,
    ) -> LogicalNode:
        """Walk plan tree, update ScanNode.estimated_rows based on new predicate
        selectivities. Propagate updated estimates upward through JoinNodes."""
        import copy
        updated_plan = copy.deepcopy(cached_plan)
        pred_map: dict[str, list[Predicate]] = {}
        for pred in new_predicates:
            pred_map.setdefault(pred.table_alias or pred.column, []).append(pred)
        self._walk_and_update(updated_plan, pred_map)
        return updated_plan

    def _walk_and_update(
        self,
        node: LogicalNode,
        pred_map: dict[str, list[Predicate]],
    ) -> float:
        """Recursively update cardinalities. Returns updated estimated_rows."""
        # Recurse into children first
        child_rows: list[float] = []
        for child in node.children:
            child_rows.append(self._walk_and_update(child, pred_map))

        if isinstance(node, ScanNode):
            base_rows = node.estimated_rows
            # Apply selectivity for predicates that reference this table
            key = node.alias or node.table_name
            preds = pred_map.get(key, [])
            selectivity = 1.0
            for pred in preds:
                sel = self._predicate_selectivity(pred)
                selectivity *= sel
            new_rows = max(1.0, base_rows * selectivity)
            node.estimated_rows = new_rows
            logger.debug(
                "Updated scan %s: %f → %f (sel=%.3f)",
                node.table_name, base_rows, new_rows, selectivity,
            )
            return new_rows

        if isinstance(node, FilterNode) and child_rows:
            # Propagate child estimate
            node.estimated_rows = child_rows[0]
            return child_rows[0]

        if isinstance(node, JoinNode) and len(child_rows) == 2:
            # Simple join cardinality: product * 0.1 (join selectivity)
            join_rows = max(1.0, child_rows[0] * child_rows[1] * 0.1)
            node.estimated_rows = join_rows
            return join_rows

        # Default: use first child's estimate or keep existing
        if child_rows:
            node.estimated_rows = child_rows[0]
            return child_rows[0]
        return node.estimated_rows

    def _predicate_selectivity(self, pred: Predicate) -> float:
        """Estimate selectivity of a predicate (heuristic)."""
        op = pred.operator.upper()
        if op in ("=", "IS"):
            return 0.05
        if op in ("<", "<=", ">", ">="):
            return 0.33
        if op == "LIKE":
            return 0.1
        if op == "IN":
            if isinstance(pred.value, (list, tuple)):
                n = len(pred.value)
                return min(1.0, 0.05 * n)
            return 0.1
        if op in ("IS NULL", "IS NOT NULL"):
            return 0.05
        if op == "BETWEEN":
            return 0.25
        return 0.5

    # ── Eviction ──────────────────────────────────────────────

    def _evict_oldest(self) -> None:
        """LRU eviction when cache exceeds max_cache_entries."""
        while len(self._cache) > self._max_entries:
            evicted_key, _ = self._cache.popitem(last=False)
            logger.debug("Evicted incremental memo snapshot: %s", evicted_key[:16])

    # ── Stats ─────────────────────────────────────────────────

    def cache_stats(self) -> dict[str, object]:
        """Return {entries, hits, misses, hit_rate}."""
        total = self._hits + self._misses
        return {
            "entries": len(self._cache),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": (self._hits / total) if total > 0 else 0.0,
        }
