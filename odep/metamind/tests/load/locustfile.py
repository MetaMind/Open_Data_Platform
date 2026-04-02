"""MetaMind load tests using Locust.

Run: locust -f tests/load/locustfile.py --host http://localhost:8000
"""
from __future__ import annotations

import random

try:
    from locust import HttpUser, between, constant_throughput, task
except ImportError:  # allow file to compile without locust installed
    import logging
    logging.warning("locust not installed; load tests cannot run")

    class HttpUser:  # type: ignore[no-redef]
        wait_time = None

    def task(weight: int = 1):  # type: ignore[misc]
        def decorator(fn):
            return fn
        return decorator

    def between(low, high):  # noqa: D103
        return None

    def constant_throughput(rps):  # noqa: D103
        return None


class MetaMindUser(HttpUser):
    """Simulates a typical MetaMind tenant making mixed query workloads."""

    wait_time = between(0.1, 2.0)

    def on_start(self) -> None:
        """Set up tenant headers."""
        # 10-tenant simulation
        tenant_num = getattr(self, "user_id", random.randint(0, 9)) % 10
        self.tenant_id = f"load-tenant-{tenant_num}"
        self.headers = {"X-Tenant-ID": self.tenant_id}

    @task(5)
    def point_lookup(self) -> None:
        """High-frequency point lookup (simulates OLTP workload)."""
        row_id = random.randint(1, 100_000)
        self.client.post(
            "/api/v1/query",
            json={"sql": f"SELECT * FROM orders WHERE id = {row_id}"},
            headers=self.headers,
            name="/api/v1/query [point_lookup]",
        )

    @task(3)
    def dashboard_aggregate(self) -> None:
        """Dashboard aggregation query."""
        self.client.post(
            "/api/v1/query",
            json={
                "sql": (
                    "SELECT region, COUNT(*), SUM(total) "
                    "FROM orders GROUP BY region"
                )
            },
            headers=self.headers,
            name="/api/v1/query [dashboard_agg]",
        )

    @task(2)
    def multi_join(self) -> None:
        """3-table join query (moderate complexity)."""
        self.client.post(
            "/api/v1/query",
            json={
                "sql": (
                    "SELECT o.id, c.name, p.name as product "
                    "FROM orders o "
                    "JOIN customers c ON o.customer_id = c.id "
                    "JOIN products p ON o.product_id = p.id "
                    "LIMIT 100"
                )
            },
            headers=self.headers,
            name="/api/v1/query [multi_join]",
        )

    @task(1)
    def health_check(self) -> None:
        """Background health polling."""
        self.client.get("/health", name="/health")

    @task(1)
    def table_list(self) -> None:
        """List registered tables."""
        self.client.get(
            "/api/v1/tables",
            headers=self.headers,
            name="/api/v1/tables",
        )


class HighThroughputUser(HttpUser):
    """Simulates 10 RPS per user for cache performance testing."""

    wait_time = constant_throughput(10)  # type: ignore[assignment]

    def on_start(self) -> None:
        """Set up dedicated perf tenant."""
        self.headers = {"X-Tenant-ID": "perf-tenant-1"}

    @task
    def cached_query(self) -> None:
        """Same query repeated — should hit plan cache."""
        self.client.post(
            "/api/v1/query",
            json={"sql": "SELECT COUNT(*) FROM orders WHERE status = 'pending'"},
            headers=self.headers,
            name="/api/v1/query [cached]",
        )
