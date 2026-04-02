"""F05 — Predicate Inference: transitivity, constant propagation, range tightening."""
from __future__ import annotations

import logging
from typing import Optional

from metamind.core.logical.nodes import LogicalNode, Predicate, ScanNode, FilterNode, JoinNode

logger = logging.getLogger(__name__)


class PredicateInference:
    """Derives new predicates from existing ones via logical rules.

    Implements:
    - Transitivity: A=B, B=C → A=C
    - Constant propagation: A=5, B=A → B=5
    - Range tightening: A>5, A>3 → A>5
    - Join implication: ON a.x=b.x AND a.x=5 → b.x=5
    """

    def infer(self, root: LogicalNode) -> LogicalNode:
        """Apply predicate inference to entire plan tree. Returns modified tree."""
        self._traverse(root)
        return root

    def _traverse(self, node: LogicalNode) -> list[Predicate]:
        """Traverse plan tree bottom-up, collecting and propagating predicates."""
        inherited: list[Predicate] = []
        for child in node.children:
            child_preds = self._traverse(child)
            inherited.extend(child_preds)

        if isinstance(node, FilterNode):
            new_preds = self._derive_predicates(node.predicates, inherited)
            added = [p for p in new_preds if p not in node.predicates]
            if added:
                logger.debug("Inferred %d new predicates", len(added))
                node.predicates.extend(added)
            return node.predicates + inherited

        if isinstance(node, JoinNode):
            new_preds = self._derive_join_predicates(node.conditions, inherited)
            node.conditions.extend(new_preds)
            return node.conditions + inherited

        if isinstance(node, ScanNode):
            new_preds = self._derive_predicates(node.predicates, inherited)
            added = [p for p in new_preds if p not in node.predicates]
            node.predicates.extend(added)
            return node.predicates

        return inherited

    def _derive_predicates(
        self, preds: list[Predicate], context: list[Predicate]
    ) -> list[Predicate]:
        """Derive new predicates from existing set plus context."""
        all_preds = preds + context
        new_preds: list[Predicate] = []

        # Build equality graph for transitivity
        eq_graph = self._build_equality_graph(all_preds)

        # Constant propagation: if col is aliased to a constant, propagate
        constants = self._extract_constants(all_preds)
        for col, val in constants.items():
            # Find all columns equal to this column via transitivity
            equiv = self._find_equivalents(col, eq_graph)
            for equiv_col in equiv:
                if equiv_col != col:
                    new_p = Predicate(column=equiv_col, operator="=", value=val)
                    if new_p not in all_preds and new_p not in new_preds:
                        new_preds.append(new_p)
                        logger.debug("Constant propagation: %s = %s", equiv_col, val)

        # Range tightening: keep tightest bound
        new_preds.extend(self._tighten_ranges(all_preds))

        return new_preds

    def _derive_join_predicates(
        self, conditions: list[Predicate], context: list[Predicate]
    ) -> list[Predicate]:
        """Derive additional join predicates from constant context."""
        all_preds = conditions + context
        constants = self._extract_constants(all_preds)
        new_preds: list[Predicate] = []

        for cond in conditions:
            if cond.operator == "=":
                # If left side of join has a constant, push to right side
                left_col = cond.column
                right_col = str(cond.value)
                if left_col in constants:
                    new_p = Predicate(column=right_col, operator="=", value=constants[left_col])
                    if new_p not in all_preds:
                        new_preds.append(new_p)
                elif right_col in constants:
                    new_p = Predicate(column=left_col, operator="=", value=constants[right_col])
                    if new_p not in all_preds:
                        new_preds.append(new_p)

        return new_preds

    def _build_equality_graph(
        self, preds: list[Predicate]
    ) -> dict[str, set[str]]:
        """Build graph of column equality relationships."""
        graph: dict[str, set[str]] = {}
        for p in preds:
            if p.operator == "=" and isinstance(p.value, str) and "." in p.value:
                # Column = column (join equality)
                col_a, col_b = p.column, str(p.value)
                graph.setdefault(col_a, set()).add(col_b)
                graph.setdefault(col_b, set()).add(col_a)
        return graph

    def _find_equivalents(
        self, col: str, graph: dict[str, set[str]]
    ) -> set[str]:
        """Find all columns transitively equal to col via BFS."""
        visited: set[str] = {col}
        queue = [col]
        while queue:
            curr = queue.pop()
            for neighbor in graph.get(curr, set()):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
        return visited

    def _extract_constants(self, preds: list[Predicate]) -> dict[str, object]:
        """Extract column-to-constant mappings from equality predicates."""
        constants: dict[str, object] = {}
        for p in preds:
            if p.operator == "=" and isinstance(p.value, (int, float, str)):
                # Check value is not another column reference
                val_str = str(p.value)
                if "." not in val_str or not val_str.replace(".", "").isalpha():
                    constants[p.column] = p.value
        return constants

    def _tighten_ranges(self, preds: list[Predicate]) -> list[Predicate]:
        """Find and return tighter range predicates, replacing looser ones."""
        # Group range predicates by column and direction
        lower: dict[str, object] = {}
        upper: dict[str, object] = {}
        new_preds: list[Predicate] = []

        for p in preds:
            if p.operator in (">", ">="):
                col = p.column
                current = lower.get(col)
                if current is None or self._compare_values(p.value, current) > 0:
                    lower[col] = p.value
            elif p.operator in ("<", "<="):
                col = p.column
                current = upper.get(col)
                if current is None or self._compare_values(p.value, current) < 0:
                    upper[col] = p.value

        # Any tighter bounds found become new predicates (duplicates filtered upstream)
        for col, val in lower.items():
            new_preds.append(Predicate(column=col, operator=">=", value=val))
        for col, val in upper.items():
            new_preds.append(Predicate(column=col, operator="<=", value=val))

        return new_preds

    def _compare_values(self, a: object, b: object) -> int:
        """Compare two values numerically or lexicographically. Returns -1, 0, 1."""
        try:
            fa, fb = float(str(a)), float(str(b))
            return -1 if fa < fb else (1 if fa > fb else 0)
        except (ValueError, TypeError):
            sa, sb = str(a), str(b)
            return -1 if sa < sb else (1 if sa > sb else 0)
