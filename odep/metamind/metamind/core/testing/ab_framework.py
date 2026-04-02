"""Query A/B Testing Framework.

Compares two SQL variants (A and B) for correctness and performance,
enabling safe rollout of query rewrites and engine changes.
"""
from __future__ import annotations

import hashlib
import json
import logging
import statistics
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional, TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from metamind.core.query_engine import QueryEngine

logger = logging.getLogger(__name__)

_DEFAULT_RUNS = 5


@dataclass
class VariantStats:
    """Performance statistics for one A/B variant."""

    sql: str
    runs: int
    latencies_ms: list[float]
    row_counts: list[int]
    result_hashes: list[str]

    @property
    def mean_ms(self) -> float:
        return statistics.mean(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def p50_ms(self) -> float:
        return statistics.median(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def p95_ms(self) -> float:
        if len(self.latencies_ms) < 2:
            return self.mean_ms
        s = sorted(self.latencies_ms)
        idx = int(0.95 * len(s))
        return s[min(idx, len(s) - 1)]

    @property
    def p99_ms(self) -> float:
        if len(self.latencies_ms) < 2:
            return self.mean_ms
        s = sorted(self.latencies_ms)
        idx = int(0.99 * len(s))
        return s[min(idx, len(s) - 1)]

    def to_dict(self) -> dict:
        return {
            "runs": self.runs,
            "mean_ms": round(self.mean_ms, 2),
            "p50_ms": round(self.p50_ms, 2),
            "p95_ms": round(self.p95_ms, 2),
            "p99_ms": round(self.p99_ms, 2),
        }


@dataclass
class ABResult:
    """Full result of an A/B experiment run."""

    experiment_id: str
    variant_a_stats: VariantStats
    variant_b_stats: VariantStats
    match: bool            # True if result sets are semantically identical
    winner: Optional[str]  # 'a', 'b', or None if tied / diverged
    speedup_ratio: float   # variant_a.mean / variant_b.mean

    def to_dict(self) -> dict:
        return {
            "experiment_id": self.experiment_id,
            "variant_a": self.variant_a_stats.to_dict(),
            "variant_b": self.variant_b_stats.to_dict(),
            "match": self.match,
            "winner": self.winner,
            "speedup_ratio": round(self.speedup_ratio, 4),
        }


class ABTest:
    """Query A/B experiment manager.

    Args:
        db_engine: SQLAlchemy Engine for persisting experiments.
        query_engine: MetaMind QueryEngine used to run SQL variants.
    """

    def __init__(self, db_engine: Engine, query_engine: Any) -> None:
        self._engine = db_engine
        self._qe = query_engine

    # ------------------------------------------------------------------
    # Experiment lifecycle
    # ------------------------------------------------------------------

    async def create_experiment(
        self,
        name: str,
        sql_a: str,
        sql_b: str,
        tenant_id: str,
        sample_pct: float = 0.1,
    ) -> str:
        """Create a new experiment and return its experiment_id."""
        experiment_id = str(uuid.uuid4())
        try:
            ins = text(
                "INSERT INTO mm_ab_experiments "
                "(experiment_id, name, sql_a, sql_b, tenant_id, "
                " sample_pct, status, created_at) "
                "VALUES (:eid, :name, :sa, :sb, :tid, :pct, 'pending', NOW())"
            )
            with self._engine.begin() as conn:
                conn.execute(
                    ins,
                    {
                        "eid": experiment_id,
                        "name": name,
                        "sa": sql_a,
                        "sb": sql_b,
                        "tid": tenant_id,
                        "pct": sample_pct,
                    },
                )
            logger.info("Created A/B experiment %s name=%s", experiment_id, name)
        except Exception as exc:
            logger.error("ABTest.create_experiment failed: %s", exc)
        return experiment_id

    async def run(
        self,
        experiment_id: str,
        n_runs: int = _DEFAULT_RUNS,
    ) -> ABResult:
        """Execute both SQL variants n_runs times each and compare results."""
        exp = self._load_experiment(experiment_id)
        if exp is None:
            raise ValueError(f"Experiment {experiment_id} not found")

        sql_a: str = exp["sql_a"]
        sql_b: str = exp["sql_b"]
        tenant_id: str = exp["tenant_id"]

        stats_a = await self._benchmark(sql_a, tenant_id, n_runs)
        stats_b = await self._benchmark(sql_b, tenant_id, n_runs)

        match = self._results_match(stats_a.result_hashes, stats_b.result_hashes)
        winner = self._pick_winner(stats_a, stats_b, match)
        speedup = (stats_a.mean_ms / stats_b.mean_ms) if stats_b.mean_ms > 0 else 1.0

        result = ABResult(
            experiment_id=experiment_id,
            variant_a_stats=stats_a,
            variant_b_stats=stats_b,
            match=match,
            winner=winner,
            speedup_ratio=speedup,
        )
        self._persist_result(experiment_id, result)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _benchmark(
        self, sql: str, tenant_id: str, n_runs: int
    ) -> VariantStats:
        """Run sql n_runs times and collect latency + result hashes."""
        latencies: list[float] = []
        row_counts: list[int] = []
        result_hashes: list[str] = []

        for _ in range(n_runs):
            t0 = time.monotonic()
            try:
                with self._engine.connect() as conn:
                    rows = conn.execute(text(sql)).fetchall()
                dur = (time.monotonic() - t0) * 1000
                row_hash = hashlib.sha256(str(rows).encode()).hexdigest()[:16]
                latencies.append(dur)
                row_counts.append(len(rows))
                result_hashes.append(row_hash)
            except Exception as exc:
                logger.error("ABTest benchmark run failed: %s", exc)
                latencies.append(float("inf"))
                row_counts.append(0)
                result_hashes.append("ERROR")

        return VariantStats(
            sql=sql,
            runs=n_runs,
            latencies_ms=latencies,
            row_counts=row_counts,
            result_hashes=result_hashes,
        )

    def _results_match(self, hashes_a: list[str], hashes_b: list[str]) -> bool:
        """True if all majority result hashes align between A and B."""
        if not hashes_a or not hashes_b:
            return False
        # Take the modal hash for each variant
        mode_a = max(set(hashes_a), key=hashes_a.count)
        mode_b = max(set(hashes_b), key=hashes_b.count)
        return mode_a == mode_b and mode_a not in ("ERROR",)

    def _pick_winner(
        self, a: VariantStats, b: VariantStats, match: bool
    ) -> Optional[str]:
        if not match:
            return None   # Semantic divergence — no winner
        if a.mean_ms < b.mean_ms * 0.95:
            return "a"
        if b.mean_ms < a.mean_ms * 0.95:
            return "b"
        return None  # Tie within 5%

    def _load_experiment(self, experiment_id: str) -> Optional[dict]:
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT experiment_id, name, sql_a, sql_b, tenant_id, "
                        "sample_pct FROM mm_ab_experiments "
                        "WHERE experiment_id = :eid"
                    ),
                    {"eid": experiment_id},
                ).fetchone()
            return dict(row._mapping) if row else None
        except Exception as exc:
            logger.error("ABTest._load_experiment failed: %s", exc)
            return None

    def _persist_result(self, experiment_id: str, result: ABResult) -> None:
        try:
            upd = text(
                "UPDATE mm_ab_experiments "
                "SET status = 'complete', result_json = :rj, completed_at = NOW() "
                "WHERE experiment_id = :eid"
            )
            with self._engine.begin() as conn:
                conn.execute(
                    upd,
                    {"rj": json.dumps(result.to_dict()), "eid": experiment_id},
                )
        except Exception as exc:
            logger.error("ABTest._persist_result failed: %s", exc)
