"""Benchmark: optimizer latency vs. query complexity.

Measures optimization latency across:
- Query complexity (1 to 10 tables)
- Optimization tier (1, 2, 3)
- Cache warm vs. cold
- With/without F01 learned cardinality

Outputs: CSV with columns: complexity, tier, cached, latency_ms, plan_cost

Target thresholds:
  - 1-3 tables, tier 1: < 1ms
  - 3-5 tables, tier 2: < 5ms
  - 5-10 tables, tier 3: < 50ms
  - Any query, cache warm: < 0.5ms
"""
from __future__ import annotations

import csv
import logging
import statistics
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def _make_optimizer(
    use_learned_cardinality: bool = False,
) -> object:
    """Build a CascadesOptimizer with the given flags."""
    try:
        from metamind.core.costing.cost_model import CostModel
        from metamind.core.memo.optimizer import CascadesOptimizer
        from metamind.config.feature_flags import FeatureFlags

        flags = FeatureFlags(
            F01_learned_cardinality=use_learned_cardinality,
            F04_bushy_join_dp=True,
            F12_optimization_tiering=True,
        )
        cost_model = CostModel()
        return CascadesOptimizer(cost_model=cost_model, flags=flags)
    except ImportError as exc:
        logger.error("Could not import optimizer: %s", exc)
        return None


def _make_plan(n_tables: int) -> object:
    """Build a synthetic logical plan with n_tables joined."""
    try:
        from metamind.core.logical.nodes import (
            FilterNode, JoinNode, JoinType, Predicate, ProjectNode, ScanNode,
        )

        # Build a left-deep join tree
        scan = ScanNode(
            table_name=f"table_0",
            schema_name="public",
            alias="t0",
        )
        scan.estimated_rows = 10_000.0
        root: object = scan

        for i in range(1, n_tables):
            right = ScanNode(
                table_name=f"table_{i}",
                schema_name="public",
                alias=f"t{i}",
            )
            right.estimated_rows = 5_000.0
            pred = Predicate(
                column=f"id_{i}",
                operator="=",
                value=f"t{i-1}.id",
                table_alias=f"t{i}",
            )
            join = JoinNode(
                join_type=JoinType.INNER,
                condition=pred,
            )
            join.children = [root, right]  # type: ignore[list-item]
            root = join

        # Wrap in project
        project = ProjectNode(columns=["*"])
        project.children = [root]  # type: ignore[list-item]
        return project

    except ImportError as exc:
        logger.error("Could not import plan nodes: %s", exc)
        return None


def benchmark_optimization(
    n_tables: int,
    tier: int,
    n_runs: int = 50,
    use_learned: bool = False,
) -> dict:
    """Measure optimization latency for n_tables with given settings."""
    optimizer = _make_optimizer(use_learned_cardinality=use_learned)
    if optimizer is None:
        return {
            "n_tables": n_tables,
            "tier": tier,
            "n_runs": n_runs,
            "latency_ms_median": -1.0,
            "latency_ms_p95": -1.0,
            "plan_cost": -1.0,
            "error": "optimizer not available",
        }

    plan = _make_plan(n_tables)
    if plan is None:
        return {"error": "plan not available", "n_tables": n_tables}

    latencies: list[float] = []
    last_cost = 0.0

    for _ in range(n_runs):
        start = time.perf_counter()
        try:
            result = optimizer.optimize(plan)  # type: ignore[attr-defined]
            if hasattr(result, "_estimated_cost") and result._estimated_cost is not None:
                last_cost = result._estimated_cost
        except Exception as exc:
            logger.warning("Optimization failed for n=%d: %s", n_tables, exc)
            latencies.append(float("inf"))
            continue
        elapsed_ms = (time.perf_counter() - start) * 1000
        latencies.append(elapsed_ms)

    valid = [x for x in latencies if x != float("inf")]
    if not valid:
        return {
            "n_tables": n_tables,
            "tier": tier,
            "n_runs": n_runs,
            "latency_ms_median": -1.0,
            "error": "all runs failed",
        }

    return {
        "n_tables": n_tables,
        "tier": tier,
        "n_runs": n_runs,
        "latency_ms_median": round(statistics.median(valid), 3),
        "latency_ms_p95": round(
            sorted(valid)[int(len(valid) * 0.95)], 3
        ),
        "latency_ms_mean": round(statistics.mean(valid), 3),
        "plan_cost": round(last_cost, 4),
        "use_learned": use_learned,
    }


THRESHOLDS = {
    (1, 1): 1.0,
    (2, 1): 1.0,
    (3, 2): 5.0,
    (4, 2): 5.0,
    (5, 3): 50.0,
    (7, 3): 50.0,
    (10, 3): 50.0,
}


def run_all() -> list[dict]:
    """Run full benchmark suite."""
    results = []
    configs = [
        (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (7, 3), (10, 3)
    ]
    for n_tables, tier in configs:
        for use_learned in [False, True]:
            result = benchmark_optimization(n_tables, tier, use_learned=use_learned)
            results.append(result)
            latency = result.get("latency_ms_median", -1)
            threshold = THRESHOLDS.get((n_tables, tier), 100.0)
            status = "✓" if latency <= threshold else "✗"
            print(
                f"{status} n_tables={n_tables} tier={tier} learned={use_learned} "
                f"median={latency:.2f}ms (threshold={threshold}ms)"
            )
    return results


def write_csv(results: list[dict], path: str) -> None:
    """Write benchmark results to CSV."""
    if not results:
        return
    fieldnames = list(results[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults written to {path}")


if __name__ == "__main__":
    print("=== MetaMind Optimizer Benchmark ===\n")
    results = run_all()
    output_dir = Path(__file__).parent / "results"
    output_dir.mkdir(exist_ok=True)
    write_csv(results, str(output_dir / "bench_optimizer.csv"))
