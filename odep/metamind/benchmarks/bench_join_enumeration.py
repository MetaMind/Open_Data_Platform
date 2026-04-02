"""Benchmark: DPccp vs greedy join ordering.

For N = 2..12 tables: measure:
  (a) plan cost difference (DPccp finds 10-30% cheaper plans for N>=5)
  (b) enumeration time

Expected:
  - DPccp finds 10-30% cheaper plans for N >= 5
  - Greedy is 2-10x faster but finds suboptimal plans
"""
from __future__ import annotations

import logging
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def _build_join_edges(n: int) -> list[object]:
    """Build synthetic join edges for n tables."""
    try:
        from metamind.core.memo.dpccp import JoinEdge, JoinRelation
        relations = [
            JoinRelation(
                name=f"t{i}",
                estimated_rows=random.randint(100, 100_000),
                scan_cost=float(random.randint(10, 10_000)),
            )
            for i in range(n)
        ]
        edges = []
        # Chain joins + some cross edges
        for i in range(n - 1):
            edges.append(JoinEdge(left=relations[i], right=relations[i + 1], selectivity=0.01))
        # Add a few random cross edges for density
        for _ in range(min(3, n // 2)):
            a, b = random.sample(range(n), 2)
            if a != b:
                edges.append(JoinEdge(
                    left=relations[a], right=relations[b], selectivity=0.05
                ))
        return relations, edges
    except Exception as exc:
        logger.error("Could not build join edges: %s", exc)
        return None, None


def benchmark_join_enumeration(n_tables: int, n_runs: int = 20) -> dict:
    """Compare DPccp vs greedy for n_tables."""
    relations, edges = _build_join_edges(n_tables)
    if relations is None:
        return {"n_tables": n_tables, "error": "join edge construction failed"}

    try:
        from metamind.core.memo.dpccp import DPccp, greedy_join_order
    except ImportError as exc:
        return {"n_tables": n_tables, "error": f"DPccp import failed: {exc}"}

    dpccp_times: list[float] = []
    greedy_times: list[float] = []
    dpccp_costs: list[float] = []
    greedy_costs: list[float] = []

    for _ in range(n_runs):
        # DPccp
        try:
            t0 = time.perf_counter()
            result_dp = DPccp(relations, edges).enumerate()  # type: ignore[call-arg]
            dpccp_times.append((time.perf_counter() - t0) * 1000)
            cost_dp = getattr(result_dp, "estimated_cost", None) or getattr(result_dp, "_estimated_cost", 0.0)
            dpccp_costs.append(float(cost_dp))
        except Exception as exc:
            logger.debug("DPccp failed for n=%d: %s", n_tables, exc)
            dpccp_times.append(float("inf"))
            dpccp_costs.append(float("inf"))

        # Greedy
        try:
            t1 = time.perf_counter()
            result_g = greedy_join_order(relations, edges)  # type: ignore[call-arg]
            greedy_times.append((time.perf_counter() - t1) * 1000)
            cost_g = getattr(result_g, "estimated_cost", None) or getattr(result_g, "_estimated_cost", 0.0)
            greedy_costs.append(float(cost_g))
        except Exception as exc:
            logger.debug("Greedy failed for n=%d: %s", n_tables, exc)
            greedy_times.append(float("inf"))
            greedy_costs.append(float("inf"))

    def _safe_median(lst: list[float]) -> float:
        valid = [x for x in lst if x != float("inf")]
        if not valid:
            return -1.0
        import statistics
        return statistics.median(valid)

    dp_time = _safe_median(dpccp_times)
    g_time = _safe_median(greedy_times)
    dp_cost = _safe_median(dpccp_costs)
    g_cost = _safe_median(greedy_costs)

    # Cost improvement: (greedy - dpccp) / greedy
    cost_improvement_pct = (
        (g_cost - dp_cost) / g_cost * 100
        if g_cost > 0 and dp_cost > 0
        else 0.0
    )
    speedup_ratio = g_time / dp_time if dp_time > 0 and g_time > 0 else 1.0

    return {
        "n_tables": n_tables,
        "n_runs": n_runs,
        "dpccp_median_ms": round(dp_time, 3),
        "greedy_median_ms": round(g_time, 3),
        "greedy_speedup_ratio": round(1 / speedup_ratio, 2) if speedup_ratio > 0 else 0.0,
        "dpccp_cost": round(dp_cost, 2),
        "greedy_cost": round(g_cost, 2),
        "cost_improvement_pct": round(cost_improvement_pct, 1),
    }


def run_all() -> list[dict]:
    """Run for N = 2..12 tables."""
    results = []
    print(f"{'N':>3} | {'DPccp ms':>10} | {'Greedy ms':>10} | {'Cost Imp%':>10}")
    print("-" * 45)

    for n in range(2, 13):
        result = benchmark_join_enumeration(n_tables=n, n_runs=15)
        results.append(result)
        if "error" not in result:
            print(
                f"{n:>3} | {result['dpccp_median_ms']:>10.3f} | "
                f"{result['greedy_median_ms']:>10.3f} | "
                f"{result['cost_improvement_pct']:>10.1f}%"
            )
        else:
            print(f"{n:>3} | ERROR: {result.get('error', '?')}")

    return results


if __name__ == "__main__":
    print("=== MetaMind Join Enumeration Benchmark ===\n")
    results = run_all()

    import csv
    output_dir = Path(__file__).parent / "results"
    output_dir.mkdir(exist_ok=True)
    out_path = output_dir / "bench_join_enumeration.csv"
    valid = [r for r in results if "error" not in r]
    if valid:
        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(valid[0].keys()))
            writer.writeheader()
            writer.writerows(valid)
        print(f"\nResults written to {out_path}")
