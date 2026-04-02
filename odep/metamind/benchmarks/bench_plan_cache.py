"""Benchmark: plan cache hit rate vs workload diversity.

Simulates workloads with varying diversity:
  Workload A: 10 distinct query templates, 1000 queries → expected hit rate ~90%
  Workload B: 100 distinct templates, 1000 queries → expected ~50%
  Workload C: 1000 unique queries (no repeats) → expected ~0%
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


def _make_plan_cache() -> object:
    """Build a PlanCache for benchmarking."""
    try:
        from metamind.core.cache.plan_cache import PlanCache
        return PlanCache(redis_client=None, ttl_seconds=3600)
    except ImportError as exc:
        logger.error("Cannot import PlanCache: %s", exc)
        return None


def _generate_queries(n_templates: int, n_total: int) -> list[str]:
    """Generate n_total queries using n_templates distinct patterns."""
    templates = [
        f"SELECT * FROM orders WHERE customer_id = {{}} AND region = '{{}}' LIMIT {i + 1}"
        for i in range(n_templates)
    ]
    queries = []
    for _ in range(n_total):
        template = random.choice(templates)
        cid = random.randint(1, 1000)
        region = random.choice(["US", "EU", "APAC", "LATAM"])
        queries.append(template.format(cid, region))
    return queries


def benchmark_cache(
    n_templates: int,
    n_queries: int,
    tenant_id: str = "bench-tenant",
    workload_label: str = "A",
) -> dict:
    """Measure cache hit rate for a workload."""
    cache = _make_plan_cache()
    if cache is None:
        return {"error": "PlanCache not available", "workload": workload_label}

    queries = _generate_queries(n_templates, n_queries)
    hits = 0
    misses = 0
    total_get_ms = 0.0
    total_put_ms = 0.0

    for sql in queries:
        try:
            from metamind.core.cache.plan_cache import QueryFingerprint
            fp_computer = QueryFingerprint()
            fingerprint = fp_computer.compute(sql, tenant_id)
        except Exception:
            import hashlib
            fingerprint = hashlib.md5(f"{tenant_id}:{sql}".encode()).hexdigest()

        # Try cache get
        t0 = time.perf_counter()
        cached = cache.get(fingerprint, tenant_id)
        total_get_ms += (time.perf_counter() - t0) * 1000

        if cached is not None:
            hits += 1
        else:
            misses += 1
            # Simulate putting a plan into cache
            dummy_plan = {"sql": sql, "backend": "duckdb", "cost": 1.0}
            t1 = time.perf_counter()
            try:
                cache.put(fingerprint, tenant_id, dummy_plan, backend="duckdb", cost=1.0)
            except Exception:
                pass
            total_put_ms += (time.perf_counter() - t1) * 1000

    total = hits + misses
    hit_rate = hits / total if total > 0 else 0.0

    result = {
        "workload": workload_label,
        "n_templates": n_templates,
        "n_queries": n_queries,
        "hits": hits,
        "misses": misses,
        "hit_rate": round(hit_rate, 4),
        "avg_get_ms": round(total_get_ms / total, 4) if total > 0 else 0.0,
        "avg_put_ms": round(total_put_ms / misses, 4) if misses > 0 else 0.0,
    }
    return result


def run_all() -> list[dict]:
    """Run all cache benchmark workloads."""
    workloads = [
        (10, 1000, "A", "~90% hit rate"),
        (100, 1000, "B", "~50% hit rate"),
        (1000, 1000, "C", "~0% hit rate"),
    ]
    results = []
    for n_templates, n_queries, label, expected in workloads:
        result = benchmark_cache(n_templates, n_queries, workload_label=label)
        results.append(result)
        hit_rate = result.get("hit_rate", 0)
        print(
            f"Workload {label}: n_templates={n_templates} n_queries={n_queries} "
            f"hit_rate={hit_rate:.1%} (expected {expected})"
        )
    return results


if __name__ == "__main__":
    print("=== MetaMind Plan Cache Benchmark ===\n")
    results = run_all()

    import csv
    output_dir = Path(__file__).parent / "results"
    output_dir.mkdir(exist_ok=True)
    out_path = output_dir / "bench_plan_cache.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults written to {out_path}")
