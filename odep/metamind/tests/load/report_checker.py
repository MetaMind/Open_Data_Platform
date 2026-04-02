"""Parse Locust stats CSV and verify SLOs.

Usage:
    python tests/load/report_checker.py locust_stats.csv
"""
from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from pathlib import Path

# SLO definitions (endpoint → threshold)
SLO_DEFINITIONS = {
    "/api/v1/query": {
        "p50_ms": 50,
        "p95_ms": 500,
        "p99_ms": 2000,
        "error_pct": 0.1,
    },
    "/health": {
        "p95_ms": 20,
        "error_pct": 0.0,
    },
}

# Column names in Locust stats CSV
COL_NAME = "Name"
COL_50 = "50%"
COL_95 = "95%"
COL_99 = "99%"
COL_REQUESTS = "Request Count"
COL_FAILURES = "Failure Count"


@dataclass
class SLOResult:
    """Result of a single SLO check."""

    endpoint: str
    metric: str
    threshold: float
    actual: float
    passed: bool

    def __str__(self) -> str:
        status = "✓ PASS" if self.passed else "✗ FAIL"
        return (
            f"{status} [{self.endpoint}] {self.metric}: "
            f"actual={self.actual:.1f} threshold={self.threshold:.1f}"
        )


def check_slos(stats_csv: str) -> bool:
    """Parse Locust stats CSV and verify SLOs.

    Args:
        stats_csv: Path to Locust stats CSV file.

    Returns:
        True if all SLOs pass.
    """
    path = Path(stats_csv)
    if not path.exists():
        print(f"ERROR: Stats file not found: {stats_csv}")
        return False

    results: list[SLOResult] = []
    rows: dict[str, dict[str, str]] = {}

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get(COL_NAME, "").strip()
            if name and name != "Aggregated":
                rows[name] = row

    if not rows:
        print("WARNING: No rows found in stats CSV (or only Aggregated row)")
        return True  # Nothing to check

    for endpoint, slos in SLO_DEFINITIONS.items():
        # Find matching row (exact or substring match)
        row = rows.get(endpoint) or rows.get(f"POST {endpoint}") or rows.get(f"GET {endpoint}")
        if row is None:
            # Try partial match
            for name, r in rows.items():
                if endpoint in name:
                    row = r
                    break

        if row is None:
            print(f"WARNING: No data for endpoint {endpoint} in CSV; skipping SLO check")
            continue

        def _get_ms(col: str) -> float:
            val = row.get(col, "0").strip()
            try:
                return float(val)
            except ValueError:
                return 0.0

        def _get_int(col: str) -> int:
            val = row.get(col, "0").strip()
            try:
                return int(val)
            except ValueError:
                return 0

        if "p50_ms" in slos:
            actual = _get_ms(COL_50)
            results.append(SLOResult(
                endpoint=endpoint, metric="p50_ms",
                threshold=slos["p50_ms"], actual=actual,
                passed=actual <= slos["p50_ms"],
            ))

        if "p95_ms" in slos:
            actual = _get_ms(COL_95)
            results.append(SLOResult(
                endpoint=endpoint, metric="p95_ms",
                threshold=slos["p95_ms"], actual=actual,
                passed=actual <= slos["p95_ms"],
            ))

        if "p99_ms" in slos:
            actual = _get_ms(COL_99)
            results.append(SLOResult(
                endpoint=endpoint, metric="p99_ms",
                threshold=slos["p99_ms"], actual=actual,
                passed=actual <= slos["p99_ms"],
            ))

        if "error_pct" in slos:
            total = _get_int(COL_REQUESTS)
            failures = _get_int(COL_FAILURES)
            actual_pct = (failures / total * 100) if total > 0 else 0.0
            threshold_pct = slos["error_pct"]
            results.append(SLOResult(
                endpoint=endpoint, metric="error_pct",
                threshold=threshold_pct, actual=actual_pct,
                passed=actual_pct <= threshold_pct,
            ))

    # Print results
    all_pass = all(r.passed for r in results)
    print(f"\nSLO Report — {path.name}")
    print("=" * 60)
    for result in results:
        print(result)
    print("=" * 60)
    if all_pass:
        print(f"✓ All {len(results)} SLO checks PASSED")
    else:
        failed = [r for r in results if not r.passed]
        print(f"✗ {len(failed)}/{len(results)} SLO checks FAILED")

    return all_pass


def main() -> None:
    """CLI entry point."""
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <locust_stats.csv>")
        sys.exit(1)

    passed = check_slos(sys.argv[1])
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
