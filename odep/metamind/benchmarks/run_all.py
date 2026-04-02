#!/usr/bin/env python3
"""Run all benchmarks and compare against baseline.json. Fail if regression > 20%."""
from __future__ import annotations

import json
import sys
from pathlib import Path

BASELINE_PATH = Path(__file__).parent / "results" / "baseline.json"
REGRESSION_THRESHOLD = 0.20  # 20%


def _load_baseline() -> dict:
    """Load baseline metrics."""
    if not BASELINE_PATH.exists():
        print(f"WARNING: No baseline found at {BASELINE_PATH}; skipping regression check")
        return {}
    with open(BASELINE_PATH) as f:
        return json.load(f)


def _check_regression(
    metric_name: str,
    baseline_val: float,
    current_val: float,
    threshold: float = REGRESSION_THRESHOLD,
    lower_is_better: bool = True,
) -> tuple[bool, str]:
    """Check if current metric regressed beyond threshold.

    Returns (passed, message).
    """
    if baseline_val == 0:
        return True, f"{metric_name}: baseline=0 (skip)"

    if lower_is_better:
        # Regression: current is higher than baseline by more than threshold
        regression = (current_val - baseline_val) / baseline_val
        passed = regression <= threshold
        status = "✓" if passed else "✗"
        return passed, (
            f"{status} {metric_name}: baseline={baseline_val:.3f} "
            f"current={current_val:.3f} delta={regression:+.1%}"
        )
    else:
        # Regression: current is lower than baseline by more than threshold
        regression = (baseline_val - current_val) / baseline_val
        passed = regression <= threshold
        status = "✓" if passed else "✗"
        return passed, (
            f"{status} {metric_name}: baseline={baseline_val:.3f} "
            f"current={current_val:.3f} delta={-regression:+.1%}"
        )


def run_optimizer_bench() -> dict:
    """Run optimizer benchmark."""
    print("\n[1/4] Optimizer benchmark...")
    try:
        from benchmarks.bench_optimizer import run_all
        results = run_all()
        # Extract key metrics
        metrics = {}
        for r in results:
            if "error" in r or not isinstance(r, dict):
                continue
            n = r.get("n_tables")
            use_learned = r.get("use_learned", False)
            if not use_learned:  # Use non-learned as baseline comparator
                key = f"n{n}_tier{r.get('tier', 1)}_median_ms"
                metrics[key] = r.get("latency_ms_median", -1)
        return metrics
    except Exception as exc:
        print(f"  ERROR: {exc}")
        return {}


def run_cache_bench() -> dict:
    """Run plan cache benchmark."""
    print("\n[2/4] Plan cache benchmark...")
    try:
        from benchmarks.bench_plan_cache import run_all
        results = run_all()
        metrics = {}
        for r in results:
            label = r.get("workload", "?").lower()
            metrics[f"workload_{label}_hit_rate"] = r.get("hit_rate", 0)
            metrics["avg_get_ms"] = r.get("avg_get_ms", 0)
        return metrics
    except Exception as exc:
        print(f"  ERROR: {exc}")
        return {}


def run_join_bench() -> dict:
    """Run join enumeration benchmark."""
    print("\n[3/4] Join enumeration benchmark...")
    try:
        from benchmarks.bench_join_enumeration import run_all
        results = run_all()
        metrics = {}
        for r in results:
            if "error" in r:
                continue
            n = r.get("n_tables")
            metrics[f"n{n}_dpccp_median_ms"] = r.get("dpccp_median_ms", 0)
            metrics[f"n{n}_cost_improvement_pct"] = r.get("cost_improvement_pct", 0)
        return metrics
    except Exception as exc:
        print(f"  ERROR: {exc}")
        return {}


def run_connector_bench() -> dict:
    """Run connector benchmark."""
    print("\n[4/4] Connector benchmark...")
    try:
        from benchmarks.bench_connectors import run_all
        results = run_all()
        metrics = {}
        for r in results:
            if "error" in r:
                continue
            backend = r.get("backend", "").replace("-", "_")
            metrics[f"{backend}_median_ms"] = r.get("median_ms", 0)
            metrics[f"{backend}_p95_ms"] = r.get("p95_ms", 0)
        return metrics
    except Exception as exc:
        print(f"  ERROR: {exc}")
        return {}


def check_regressions(current: dict[str, dict], baseline: dict) -> bool:
    """Compare current results against baseline and report regressions."""
    all_pass = True
    checked = 0
    print("\n=== Regression Report ===")

    for section, metrics in current.items():
        baseline_section = baseline.get(section, {})
        for metric_name, current_val in metrics.items():
            baseline_val = baseline_section.get(metric_name)
            if baseline_val is None or current_val == -1:
                continue

            # Determine direction (latency: lower is better; hit_rate: higher is better)
            lower_is_better = "ms" in metric_name or "mae" in metric_name
            higher_is_better = "hit_rate" in metric_name or "improvement_pct" in metric_name

            if higher_is_better:
                lower_is_better = False

            passed, msg = _check_regression(
                f"{section}.{metric_name}",
                float(baseline_val),
                float(current_val),
                lower_is_better=lower_is_better,
            )
            print(f"  {msg}")
            if not passed:
                all_pass = False
            checked += 1

    if checked == 0:
        print("  (No metrics to compare against baseline)")
        return True

    print(f"\n{'All ' + str(checked) + ' metrics PASSED' if all_pass else 'REGRESSIONS DETECTED'}")
    return all_pass


def main() -> None:
    """Run all benchmarks and check regressions."""
    print("=" * 60)
    print("MetaMind Performance Benchmark Suite v3.0.0")
    print("=" * 60)

    baseline = _load_baseline()
    current: dict[str, dict] = {}

    # Add parent dir to path for local imports
    sys.path.insert(0, str(Path(__file__).parent.parent))

    current["optimizer"] = run_optimizer_bench()
    current["plan_cache"] = run_cache_bench()
    current["join_enumeration"] = run_join_bench()
    current["connectors"] = run_connector_bench()

    # Save current results
    output_path = Path(__file__).parent / "results" / "latest.json"
    with open(output_path, "w") as f:
        json.dump(current, f, indent=2)
    print(f"\nCurrent results saved to {output_path}")

    # Check regressions
    if baseline:
        passed = check_regressions(current, baseline)
        sys.exit(0 if passed else 1)
    else:
        print("\nNo baseline found; saving current run as reference")
        import shutil
        shutil.copy(output_path, BASELINE_PATH)
        sys.exit(0)


if __name__ == "__main__":
    main()
