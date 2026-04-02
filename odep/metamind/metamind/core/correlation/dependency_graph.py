"""Column dependency graph from correlation data."""
from __future__ import annotations
class DependencyGraph:
    def __init__(self) -> None:
        self._edges: dict[tuple[str,str], float] = {}
    def add_edge(self, col_a: str, col_b: str, correlation: float) -> None:
        key = (min(col_a, col_b), max(col_a, col_b))
        self._edges[key] = correlation
    def get_correlation(self, col_a: str, col_b: str) -> float:
        key = (min(col_a, col_b), max(col_a, col_b))
        return self._edges.get(key, 0.0)
    def neighbors(self, col: str) -> list[str]:
        return [b if a == col else a for (a, b) in self._edges if col in (a, b)]
