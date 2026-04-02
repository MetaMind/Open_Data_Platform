"""Count-Min Sketch for multi-column dependency estimation."""
from __future__ import annotations
import hashlib, numpy as np
class CountMinSketch:
    def __init__(self, width: int = 1024, depth: int = 5) -> None:
        self._width = width; self._depth = depth
        self._table = np.zeros((depth, width), dtype=np.int64)
    def add(self, item: str) -> None:
        for i in range(self._depth):
            h = int(hashlib.md5(f"{i}:{item}".encode()).hexdigest(), 16) % self._width
            self._table[i, h] += 1
    def estimate(self, item: str) -> int:
        vals = []
        for i in range(self._depth):
            h = int(hashlib.md5(f"{i}:{item}".encode()).hexdigest(), 16) % self._width
            vals.append(self._table[i, h])
        return int(min(vals))
