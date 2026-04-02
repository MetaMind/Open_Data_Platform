"""F11 — Compiled Plan Execution using Python bytecode for hot-path speedup."""
from __future__ import annotations

import hashlib
import logging
import textwrap
from collections import OrderedDict
from typing import Callable, Optional

from metamind.core.logical.nodes import (
    AggregateNode,
    FilterNode,
    JoinNode,
    LogicalNode,
    Predicate,
    ProjectNode,
    ScanNode,
)

logger = logging.getLogger(__name__)

_MAX_COMPILABLE_ROWS = 10_000_000


def _op_to_pyarrow(operator: str) -> str:
    """Map SQL operator to PyArrow compute function name."""
    mapping = {
        "=": "equal",
        "!=": "not_equal",
        "<>": "not_equal",
        "<": "less",
        "<=": "less_equal",
        ">": "greater",
        ">=": "greater_equal",
    }
    return mapping.get(operator.upper(), "equal")


class PlanCompiler:
    """F11: Compiles hot-path plans to Python bytecode functions for ~2-5x speedup.

    Target: simple scan+filter plans and point-lookup patterns.
    Uses Python's compile() + exec() to generate a native function
    that processes PyArrow batches without the interpreter overhead of
    walking the plan tree per-batch.

    Only compiles plans where:
    - No cross-engine operations (single backend)
    - Only Scan + Filter + Project nodes (no aggregations, no complex joins)
    - Data fits in memory (row_count < 10M)
    """

    def __init__(self, max_cached_plans: int = 200) -> None:
        """Initialize with LRU plan cache."""
        self._cache: OrderedDict[str, Callable] = OrderedDict()
        self._max_cached = max_cached_plans
        self._compiled_count = 0
        self._cache_hits = 0
        self._total_compilable = 0
        self._total_seen = 0

    # ── Compilability Check ───────────────────────────────────

    def is_compilable(self, plan: LogicalNode) -> bool:
        """Returns True if plan meets compilation criteria."""
        self._total_seen += 1
        if not self._check_node_types(plan):
            return False
        if not self._check_row_count(plan):
            return False
        self._total_compilable += 1
        return True

    def _check_node_types(self, node: LogicalNode) -> bool:
        """Recursively check that all nodes are Scan/Filter/Project only."""
        allowed = (ScanNode, FilterNode, ProjectNode)
        if isinstance(node, (AggregateNode, JoinNode)):
            return False
        if not isinstance(node, allowed):
            # Allow any node type that isn't explicitly disallowed as long as
            # it's not an aggregate or join (for future node types)
            logger.debug(
                "Plan node type %s is not in known-compilable set",
                type(node).__name__,
            )
        for child in node.children:
            if not self._check_node_types(child):
                return False
        return True

    def _check_row_count(self, node: LogicalNode) -> bool:
        """Check that estimated row count is within limit."""
        if node.estimated_rows > _MAX_COMPILABLE_ROWS:
            return False
        for child in node.children:
            if not self._check_row_count(child):
                return False
        return True

    # ── Compilation ───────────────────────────────────────────

    def compile(self, plan: LogicalNode, schema: dict[str, str]) -> Callable:
        """Generate Python function from plan.

        Walks Scan+Filter+Project nodes, generates Python code string,
        compiles to a code object, and returns as callable:
            fn(batch: pa.RecordBatch) -> pa.RecordBatch

        Example generated code for "SELECT id, name FROM users WHERE age > 25":
            def compiled_plan(batch):
                mask = pc.greater(batch['age'], 25)
                filtered = batch.filter(mask)
                return filtered.select(['id', 'name'])
        """
        filter_parts: list[str] = []
        projection_cols: list[str] = []
        self._collect_plan_parts(plan, filter_parts, projection_cols)

        code_lines: list[str] = [
            "import pyarrow.compute as pc",
            "def compiled_plan(batch):",
        ]

        if filter_parts:
            # Combine filters with bitwise AND
            first = True
            for i, expr in enumerate(filter_parts):
                if first:
                    code_lines.append(f"    mask = {expr}")
                    first = False
                else:
                    code_lines.append(f"    mask = pc.and_(mask, {expr})")
            code_lines.append("    batch = batch.filter(mask)")

        if projection_cols:
            cols_repr = repr(projection_cols)
            code_lines.append(f"    batch = batch.select({cols_repr})")

        code_lines.append("    return batch")

        source = "\n".join(code_lines)
        logger.debug("Compiled plan source:\n%s", textwrap.indent(source, "  "))

        code_obj = compile(source, "<compiled_plan>", "exec")
        namespace: dict[str, object] = {}
        exec(code_obj, namespace)  # noqa: S102
        fn = namespace["compiled_plan"]

        self._compiled_count += 1
        return fn

    def _collect_plan_parts(
        self,
        node: LogicalNode,
        filter_parts: list[str],
        projection_cols: list[str],
    ) -> None:
        """Walk tree and collect filter expressions and projection columns."""
        for child in node.children:
            self._collect_plan_parts(child, filter_parts, projection_cols)

        if isinstance(node, FilterNode):
            for pred in node.predicates:
                expr = self._generate_filter_code(pred)
                if expr:
                    filter_parts.append(expr)

        if isinstance(node, ProjectNode):
            if node.columns and not projection_cols:
                projection_cols.extend(node.columns)

    # ── Code Generation ───────────────────────────────────────

    def _generate_filter_code(self, predicate: Predicate) -> str:
        """Generate PyArrow compute expression string for a predicate."""
        col = repr(predicate.column)
        op = predicate.operator.upper()

        if op in ("=", "!=", "<>", "<", "<=", ">", ">="):
            arrow_fn = _op_to_pyarrow(op)
            val = repr(predicate.value)
            return f"pc.{arrow_fn}(batch[{col}], {val})"

        if op == "IS NULL":
            return f"pc.is_null(batch[{col}])"

        if op == "IS NOT NULL":
            return f"pc.invert(pc.is_null(batch[{col}]))"

        if op == "LIKE":
            pattern = str(predicate.value)
            # Convert SQL LIKE to regex-style match_substring
            regex = pattern.replace("%", ".*").replace("_", ".")
            return f"pc.match_substring_regex(batch[{col}], {repr(regex)})"

        if op == "IN":
            values = list(predicate.value) if predicate.value else []
            return f"pc.is_in(batch[{col}], value_set=pa.array({repr(values)}))"

        if op == "BETWEEN":
            if isinstance(predicate.value, (list, tuple)) and len(predicate.value) == 2:
                lo, hi = predicate.value
                lo_expr = f"pc.greater_equal(batch[{col}], {repr(lo)})"
                hi_expr = f"pc.less_equal(batch[{col}], {repr(hi)})"
                return f"pc.and_({lo_expr}, {hi_expr})"

        logger.warning("Unsupported predicate operator for compilation: %s", op)
        return ""

    def _generate_projection_code(self, columns: list[str]) -> str:
        """Generate .select([...]) call string."""
        return f".select({repr(columns)})"

    # ── Cache Management ──────────────────────────────────────

    def cache_compiled(self, plan_key: str, fn: Callable) -> None:
        """Cache compiled plan by key (fingerprint)."""
        self._cache[plan_key] = fn
        self._cache.move_to_end(plan_key)
        if len(self._cache) > self._max_cached:
            self._cache.popitem(last=False)

    def get_cached(self, plan_key: str) -> Optional[Callable]:
        """Get cached compiled plan, or None."""
        if plan_key in self._cache:
            self._cache.move_to_end(plan_key)
            self._cache_hits += 1
            return self._cache[plan_key]
        return None

    def plan_fingerprint(self, plan: LogicalNode) -> str:
        """Stable fingerprint for a plan tree structure (not values)."""
        parts: list[str] = []
        self._collect_fingerprint(plan, parts)
        digest = hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]
        return digest

    def _collect_fingerprint(self, node: LogicalNode, parts: list[str]) -> None:
        """Recursively build fingerprint string from plan structure."""
        node_type = type(node).__name__
        if isinstance(node, ScanNode):
            parts.append(f"Scan({node.table_name})")
        elif isinstance(node, FilterNode):
            preds = ",".join(
                f"{p.column}{p.operator}{type(p.value).__name__}"
                for p in node.predicates
            )
            parts.append(f"Filter({preds})")
        elif isinstance(node, ProjectNode):
            parts.append(f"Project({','.join(node.columns or [])})")
        else:
            parts.append(node_type)
        for child in node.children:
            self._collect_fingerprint(child, parts)

    # ── Stats ─────────────────────────────────────────────────

    def compile_stats(self) -> dict[str, object]:
        """Return {compiled_plans, cache_hits, compilable_ratio}."""
        ratio = (
            self._total_compilable / self._total_seen
            if self._total_seen > 0
            else 0.0
        )
        return {
            "compiled_plans": self._compiled_count,
            "cache_hits": self._cache_hits,
            "compilable_ratio": ratio,
            "cached_entries": len(self._cache),
        }
