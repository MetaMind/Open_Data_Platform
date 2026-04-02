"""F04 — DPccp: Dynamic Programming on Connected Complementary Partitions.

Optimal join order enumeration for 2–15 tables in O(3^n) time.
Based on Moerkotte & Neumann, 'Analysis of Two Existing and One New DP Algorithm.'
"""
from __future__ import annotations

import itertools
import logging
import math
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# Maximum tables for full DPccp (above this, use greedy)
DPCCP_MAX_TABLES = 15


@dataclass
class JoinRelation:
    """A single table or sub-plan in the join graph."""

    relation_id: int
    table_name: str
    row_count: float
    size_bytes: int = 0
    backend: str = "postgres"

    def bit(self) -> int:
        """Bit position for this relation in a bitmask."""
        return 1 << self.relation_id


@dataclass
class JoinEdge:
    """An edge in the join graph between two relations."""

    left_id: int
    right_id: int
    selectivity: float = 1.0
    predicate: Optional[str] = None


@dataclass
class DPEntry:
    """Best join plan for a subset of relations."""

    subset: int            # bitmask of relation IDs
    cost: float
    left: Optional[int] = None   # bitmask of left sub-plan
    right: Optional[int] = None  # bitmask of right sub-plan
    join_type: str = "hash"
    estimated_rows: float = 0.0


class DPccp:
    """DPccp optimal join enumeration algorithm.

    Builds the optimal join order for up to DPCCP_MAX_TABLES tables.
    Falls back to left-deep greedy for larger queries.
    """

    def __init__(self, relations: list[JoinRelation], edges: list[JoinEdge]) -> None:
        """Initialize with list of relations and join edges."""
        if len(relations) > DPCCP_MAX_TABLES:
            raise ValueError(
                f"DPccp supports at most {DPCCP_MAX_TABLES} tables, got {len(relations)}"
            )
        self._relations = relations
        self._edges = edges
        self._n = len(relations)
        self._adj: dict[int, list[int]] = self._build_adj()
        self._best: dict[int, DPEntry] = {}

    def enumerate(self) -> Optional[DPEntry]:
        """Run DPccp and return the optimal join plan."""
        if self._n == 0:
            return None
        if self._n == 1:
            rel = self._relations[0]
            entry = DPEntry(
                subset=rel.bit(),
                cost=rel.row_count * 0.001,
                estimated_rows=rel.row_count,
            )
            self._best[rel.bit()] = entry
            return entry

        # Initialize base cases: single tables
        for rel in self._relations:
            mask = rel.bit()
            self._best[mask] = DPEntry(
                subset=mask,
                cost=rel.row_count * 0.001,
                estimated_rows=rel.row_count,
            )

        # Enumerate all non-empty subsets in ascending size
        full_mask = (1 << self._n) - 1
        for size in range(2, self._n + 1):
            for subset in self._subsets_of_size(full_mask, size):
                self._dp_step(subset)

        full = (1 << self._n) - 1
        return self._best.get(full)

    # ── Core DP Logic ─────────────────────────────────────────

    def _dp_step(self, subset: int) -> None:
        """Find the best join for a given subset of relations."""
        best_cost = math.inf
        best_entry: Optional[DPEntry] = None

        # Enumerate all connected complementary partitions (S1, S2) of subset
        for s1, s2 in self._enumerate_ccp(subset):
            if s1 not in self._best or s2 not in self._best:
                continue
            p1 = self._best[s1]
            p2 = self._best[s2]

            # Check connectivity (must share an edge)
            if not self._are_connected(s1, s2):
                continue

            sel = self._edge_selectivity(s1, s2)
            output_rows = p1.estimated_rows * p2.estimated_rows * sel
            join_type, join_cost = self._best_join_strategy(
                p1.estimated_rows, p2.estimated_rows
            )
            total_cost = p1.cost + p2.cost + join_cost

            if total_cost < best_cost:
                best_cost = total_cost
                best_entry = DPEntry(
                    subset=subset,
                    cost=total_cost,
                    left=s1,
                    right=s2,
                    join_type=join_type,
                    estimated_rows=max(1.0, output_rows),
                )

        if best_entry is not None:
            self._best[subset] = best_entry

    def _enumerate_ccp(self, subset: int) -> list[tuple[int, int]]:
        """Enumerate all valid complementary partition pairs (S1, S2)."""
        pairs: list[tuple[int, int]] = []
        # Iterate over all non-empty proper subsets
        s = subset
        sub = (s - 1) & s
        while sub > 0:
            complement = subset ^ sub
            if complement > 0 and sub < complement:  # avoid duplicates
                pairs.append((sub, complement))
            sub = (sub - 1) & s
        return pairs

    # ── Helpers ───────────────────────────────────────────────

    def _build_adj(self) -> dict[int, list[int]]:
        """Build adjacency list of relation IDs that share join edges."""
        adj: dict[int, list[int]] = {r.relation_id: [] for r in self._relations}
        for edge in self._edges:
            adj[edge.left_id].append(edge.right_id)
            adj[edge.right_id].append(edge.left_id)
        return adj

    def _are_connected(self, s1: int, s2: int) -> bool:
        """Check if any relation in s1 has a join edge to any in s2."""
        for rel in self._relations:
            if rel.bit() & s1:
                for neighbor in self._adj[rel.relation_id]:
                    neighbor_bit = 1 << neighbor
                    if neighbor_bit & s2:
                        return True
        return False

    def _edge_selectivity(self, s1: int, s2: int) -> float:
        """Get selectivity of edges between two subsets."""
        sel = 1.0
        for edge in self._edges:
            left_bit = 1 << edge.left_id
            right_bit = 1 << edge.right_id
            if (left_bit & s1 and right_bit & s2) or (right_bit & s1 and left_bit & s2):
                sel *= edge.selectivity
        return max(0.0001, sel)

    def _subsets_of_size(self, universe: int, size: int) -> list[int]:
        """Generate all subsets of given bit universe with exactly `size` bits set."""
        bits = [i for i in range(self._n) if (1 << i) & universe]
        result = []
        for combo in itertools.combinations(bits, size):
            mask = 0
            for b in combo:
                mask |= (1 << b)
            result.append(mask)
        return result

    def _best_join_strategy(
        self, left_rows: float, right_rows: float
    ) -> tuple[str, float]:
        """Select best join strategy based on row counts. Returns (type, cost)."""
        # Hash join: O(M + N)
        hash_cost = (left_rows + right_rows) * 0.001 + min(left_rows, right_rows) * 0.002
        # Merge join: O((M+N) log(M+N))
        merge_cost = (left_rows + right_rows) * math.log2(max(2, left_rows + right_rows)) * 0.0005
        # Nested loop: O(M*N) — only for small relations
        nl_cost = (left_rows * right_rows * 0.00001
                   if min(left_rows, right_rows) < 1000 else math.inf)

        costs = {"hash": hash_cost, "merge": merge_cost, "nested_loop": nl_cost}
        best = min(costs, key=lambda k: costs[k])
        return best, costs[best]


def greedy_join_order(
    relations: list[JoinRelation], edges: list[JoinEdge]
) -> list[int]:
    """Greedy left-deep join order for large queries (> DPCCP_MAX_TABLES tables).

    Uses selectivity-weighted row count heuristic (smallest first).
    """
    if not relations:
        return []

    remaining = list(range(len(relations)))
    order = [min(remaining, key=lambda i: relations[i].row_count)]
    remaining.remove(order[0])

    edge_sel: dict[tuple[int, int], float] = {
        (e.left_id, e.right_id): e.selectivity for e in edges
    }
    edge_sel.update({(e.right_id, e.left_id): e.selectivity for e in edges})

    while remaining:
        # Pick next relation with highest join selectivity to current set
        current_set = set(order)
        best_next = min(
            remaining,
            key=lambda i: relations[i].row_count
            * min((edge_sel.get((j, i), 1.0) for j in current_set), default=1.0),
        )
        order.append(best_next)
        remaining.remove(best_next)

    return order
