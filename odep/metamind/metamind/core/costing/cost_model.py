"""MetaMind cost model — multi-objective optimization (F06, F27)."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# Default hardware cost factors (calibrated via F06)
DEFAULT_CPU_COST_PER_ROW = 0.000001       # per row processed
DEFAULT_IO_COST_PER_PAGE = 0.005          # per 8KB page scan
DEFAULT_NETWORK_COST_PER_MB = 0.01        # cross-engine data transfer
DEFAULT_ROWS_PER_PAGE = 100


@dataclass
class CostVector:
    """Multi-dimensional cost vector (F27) — all cost objectives."""

    latency_ms: float = 0.0
    cloud_cost_usd: float = 0.0
    throughput_rows_sec: float = 0.0
    freshness_seconds: float = 0.0
    memory_mb: float = 0.0
    cpu_units: float = 0.0
    io_pages: float = 0.0
    network_mb: float = 0.0

    def weighted_total(self, weights: "CostWeights") -> float:
        """Compute weighted scalar cost from vector."""
        return (
            self.latency_ms * weights.latency
            + self.cloud_cost_usd * weights.cloud_cost * 1000
            + (1.0 / max(1.0, self.throughput_rows_sec)) * weights.throughput * 1e6
            + self.freshness_seconds * weights.freshness
        )

    def __add__(self, other: "CostVector") -> "CostVector":
        """Add two cost vectors."""
        return CostVector(
            latency_ms=self.latency_ms + other.latency_ms,
            cloud_cost_usd=self.cloud_cost_usd + other.cloud_cost_usd,
            throughput_rows_sec=min(self.throughput_rows_sec, other.throughput_rows_sec)
            if other.throughput_rows_sec > 0
            else self.throughput_rows_sec,
            freshness_seconds=max(self.freshness_seconds, other.freshness_seconds),
            memory_mb=self.memory_mb + other.memory_mb,
            cpu_units=self.cpu_units + other.cpu_units,
            io_pages=self.io_pages + other.io_pages,
            network_mb=self.network_mb + other.network_mb,
        )


@dataclass
class CostWeights:
    """Optimization objective weights — configurable per tenant (F27)."""

    latency: float = 1.0
    cloud_cost: float = 0.0
    throughput: float = 0.0
    freshness: float = 0.0

    def normalize(self) -> "CostWeights":
        """Return copy with weights normalized to sum to 1."""
        total = self.latency + self.cloud_cost + self.throughput + self.freshness
        if total == 0:
            return CostWeights(latency=1.0)
        return CostWeights(
            latency=self.latency / total,
            cloud_cost=self.cloud_cost / total,
            throughput=self.throughput / total,
            freshness=self.freshness / total,
        )


@dataclass
class HardwareCosts:
    """Per-backend hardware cost calibration (F06)."""

    backend: str
    cpu_cost_per_row: float = DEFAULT_CPU_COST_PER_ROW
    io_cost_per_page: float = DEFAULT_IO_COST_PER_PAGE
    network_cost_per_mb: float = DEFAULT_NETWORK_COST_PER_MB
    rows_per_page: int = DEFAULT_ROWS_PER_PAGE
    parallel_factor: int = 1
    cloud_cost_per_tb_scan: float = 5.0    # e.g., BigQuery $5/TB
    is_serverless: bool = False

    @classmethod
    def defaults(cls, backend: str) -> "HardwareCosts":
        """Create default costs for known backends."""
        presets = {
            "bigquery": cls(backend="bigquery", cloud_cost_per_tb_scan=5.0,
                           is_serverless=True, io_cost_per_page=0.001),
            "snowflake": cls(backend="snowflake", cloud_cost_per_tb_scan=3.0,
                            is_serverless=False),
            "redshift": cls(backend="redshift", cloud_cost_per_tb_scan=1.0,
                           is_serverless=False, parallel_factor=8),
            "spark": cls(backend="spark", parallel_factor=64,
                        cpu_cost_per_row=0.0000005),
            "duckdb": cls(backend="duckdb", io_cost_per_page=0.002,
                         cpu_cost_per_row=0.0000003, parallel_factor=4),
            "postgres": cls(backend="postgres"),
        }
        return presets.get(backend, cls(backend=backend))


class CostModel:
    """Central cost model with calibrated hardware costs per backend (F06).

    Computes multi-dimensional CostVector for each plan operator.
    """

    def __init__(self) -> None:
        """Initialize with default hardware cost profiles."""
        self._hw_costs: dict[str, HardwareCosts] = {}
        self._weights: CostWeights = CostWeights()

    def calibrate(self, backend: str, hw_costs: HardwareCosts) -> None:
        """Set calibrated hardware costs for a backend (F06)."""
        self._hw_costs[backend] = hw_costs
        logger.info("Calibrated costs for backend %s", backend)

    def set_weights(self, weights: CostWeights) -> None:
        """Set multi-objective cost weights (F27)."""
        self._weights = weights.normalize()

    def get_hw(self, backend: str) -> HardwareCosts:
        """Get hardware costs for backend, using defaults if not calibrated."""
        if backend not in self._hw_costs:
            self._hw_costs[backend] = HardwareCosts.defaults(backend)
        return self._hw_costs[backend]

    def scan_cost(
        self,
        table_rows: int,
        table_size_bytes: int,
        selectivity: float,
        backend: str = "postgres",
    ) -> CostVector:
        """Compute cost of a table scan with predicate filtering."""
        hw = self.get_hw(backend)
        pages = max(1, table_size_bytes // (8 * 1024))
        output_rows = max(1.0, table_rows * selectivity)

        io_cost = pages * hw.io_cost_per_page
        cpu_cost = table_rows * hw.cpu_cost_per_row
        latency = (io_cost + cpu_cost) / hw.parallel_factor * 1000  # ms

        cloud_cost = 0.0
        if hw.is_serverless:
            tb_scanned = table_size_bytes / (1024**4)
            cloud_cost = tb_scanned * hw.cloud_cost_per_tb_scan

        return CostVector(
            latency_ms=latency,
            cloud_cost_usd=cloud_cost,
            throughput_rows_sec=output_rows / max(0.001, latency / 1000),
            io_pages=float(pages),
            cpu_units=cpu_cost,
        )

    def join_cost(
        self,
        left_rows: float,
        right_rows: float,
        join_type: str,
        backend: str = "postgres",
    ) -> CostVector:
        """Compute cost of a join operation."""
        hw = self.get_hw(backend)

        if join_type == "hash":
            # Hash join: build + probe
            build_cost = right_rows * hw.cpu_cost_per_row * 2
            probe_cost = left_rows * hw.cpu_cost_per_row
            latency = (build_cost + probe_cost) / hw.parallel_factor * 1000
            memory_mb = (right_rows * 100) / (1024 * 1024)  # rough estimate
        elif join_type == "nested_loop":
            # Nested loop: O(M*N)
            latency = (left_rows * right_rows * hw.cpu_cost_per_row
                       / hw.parallel_factor * 1000)
            memory_mb = 1.0
        else:  # merge join
            sort_cost = (left_rows + right_rows) * math.log2(
                max(2, left_rows + right_rows)
            ) * hw.cpu_cost_per_row
            latency = sort_cost / hw.parallel_factor * 1000
            memory_mb = (left_rows + right_rows) * 100 / (1024 * 1024)

        output_rows = left_rows  # inner join estimate
        return CostVector(
            latency_ms=latency,
            throughput_rows_sec=output_rows / max(0.001, latency / 1000),
            memory_mb=memory_mb,
            cpu_units=latency / 1000,
        )

    def agg_cost(self, input_rows: float, group_count: int, backend: str = "postgres") -> CostVector:
        """Compute cost of aggregation."""
        hw = self.get_hw(backend)
        sort_factor = math.log2(max(2, input_rows))
        latency = (input_rows * sort_factor * hw.cpu_cost_per_row
                   / hw.parallel_factor * 1000)
        return CostVector(
            latency_ms=latency,
            throughput_rows_sec=group_count / max(0.001, latency / 1000),
            memory_mb=float(group_count) * 200 / (1024 * 1024),
            cpu_units=latency / 1000,
        )

    def network_transfer_cost(
        self, size_bytes: int, source: str, target: str
    ) -> CostVector:
        """Compute cost of cross-engine data transfer."""
        hw_src = self.get_hw(source)
        mb = size_bytes / (1024 * 1024)
        cost = mb * hw_src.network_cost_per_mb
        latency = mb * 10  # rough 100MB/s transfer rate
        return CostVector(
            latency_ms=latency,
            cloud_cost_usd=cost,
            network_mb=mb,
        )

    def scalar_cost(self, cv: CostVector) -> float:
        """Convert CostVector to scalar using current weights."""
        return cv.weighted_total(self._weights)
