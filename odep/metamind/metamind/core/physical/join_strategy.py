"""Join strategy selector."""
from __future__ import annotations
def select_join_strategy(left_rows: float, right_rows: float, is_distributed: bool) -> str:
    small = min(left_rows, right_rows)
    if is_distributed and small < 100_000: return "broadcast"
    if left_rows > 1_000_000 or right_rows > 1_000_000: return "merge"
    return "hash"
