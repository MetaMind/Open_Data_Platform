"""Benchmark: connector overhead per engine."""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def benchmark_duckdb_connector(n_queries: int = 100) -> dict:
    """Benchmark DuckDB connector overhead."""
    try:
        from metamind.core.backends.duckdb_connector import DuckDBConnector
        conn = DuckDBConnector(database=":memory:")
        conn.connect()

        # Warm up
        for _ in range(5):
            try:
                conn.execute_raw("SELECT 1")  # type: ignore[attr-defined]
            except Exception:
                break

        timings: list[float] = []
        for _ in range(n_queries):
            t0 = time.perf_counter()
            try:
                conn.execute_raw("SELECT 42 as val")  # type: ignore[attr-defined]
            except AttributeError:
                try:
                    conn._conn.execute("SELECT 42 as val").fetchall()  # type: ignore[attr-defined]
                except Exception as exc:
                    logger.debug("DuckDB raw execute error: %s", exc)
            timings.append((time.perf_counter() - t0) * 1000)

        conn.disconnect()

        import statistics
        return {
            "backend": "duckdb",
            "n_queries": n_queries,
            "median_ms": round(statistics.median(timings), 3),
            "p95_ms": round(sorted(timings)[int(len(timings) * 0.95)], 3),
            "overhead_note": "DuckDB in-process; minimal network overhead",
        }
    except ImportError as exc:
        return {"backend": "duckdb", "error": f"Import failed: {exc}"}
    except Exception as exc:
        return {"backend": "duckdb", "error": str(exc)}


def benchmark_connection_pool_overhead(n_acquires: int = 200) -> dict:
    """Measure connection pool acquire/release overhead."""
    try:
        import sqlalchemy as sa
        engine = sa.create_engine(
            "sqlite:///:memory:", pool_size=5, max_overflow=10
        )
        timings: list[float] = []
        for _ in range(n_acquires):
            t0 = time.perf_counter()
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            timings.append((time.perf_counter() - t0) * 1000)
        engine.dispose()

        import statistics
        return {
            "backend": "sqlite_pool",
            "n_acquires": n_acquires,
            "median_ms": round(statistics.median(timings), 3),
            "p95_ms": round(sorted(timings)[int(len(timings) * 0.95)], 3),
        }
    except Exception as exc:
        return {"backend": "sqlite_pool", "error": str(exc)}


def run_all() -> list[dict]:
    """Run connector benchmarks."""
    results = []
    print("DuckDB connector...")
    r = benchmark_duckdb_connector(n_queries=100)
    results.append(r)
    if "error" not in r:
        print(f"  DuckDB: median={r['median_ms']}ms p95={r['p95_ms']}ms")
    else:
        print(f"  DuckDB: {r['error']}")

    print("SQLAlchemy pool overhead...")
    r2 = benchmark_connection_pool_overhead(n_acquires=200)
    results.append(r2)
    if "error" not in r2:
        print(f"  SQLite pool: median={r2['median_ms']}ms p95={r2['p95_ms']}ms")
    else:
        print(f"  SQLite pool: {r2['error']}")

    return results


if __name__ == "__main__":
    print("=== MetaMind Connector Benchmark ===\n")
    results = run_all()

    import csv
    output_dir = Path(__file__).parent / "results"
    output_dir.mkdir(exist_ok=True)
    out_path = output_dir / "bench_connectors.csv"
    valid = [r for r in results if "error" not in r]
    if valid:
        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(valid[0].keys()))
            writer.writeheader()
            writer.writerows(valid)
        print(f"\nResults written to {out_path}")
