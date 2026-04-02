"""F06 Cost calibration via per-operator micro-benchmarks and regression modeling.

Runs targeted micro-benchmarks against each backend to measure real hardware
performance, then adjusts the cost model parameters to match observed reality.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from metamind.core.costing.cost_model import CostModel, HardwareCosts

logger = logging.getLogger(__name__)


@dataclass
class CalibrationResult:
    """Results from a calibration run against a single backend."""

    backend_id: str
    base_latency_ms: float = 0.0
    cpu_cost_per_row: float = 0.0
    io_cost_per_page: float = 0.0
    scan_throughput_rows_sec: float = 0.0
    hash_build_cost_per_row: float = 0.0
    sort_cost_per_row_log: float = 0.0
    network_throughput_mb_sec: float = 100.0
    parallel_speedup_factor: float = 1.0
    sample_count: int = 0
    calibrated_at: float = 0.0
    raw_measurements: dict[str, float] = field(default_factory=dict)


@dataclass
class BenchmarkQuery:
    """A micro-benchmark query with expected behavior."""

    name: str
    sql: str
    measures: str  # what this benchmark measures: scan, filter, join, sort, agg
    expected_rows: int = 0


# Standard micro-benchmark queries for calibration
CALIBRATION_BENCHMARKS: list[BenchmarkQuery] = [
    BenchmarkQuery(
        name="ping",
        sql="SELECT 1 AS alive",
        measures="base_latency",
        expected_rows=1,
    ),
    BenchmarkQuery(
        name="sequential_scan",
        sql="SELECT COUNT(*) FROM generate_series(1, 100000) AS t(x)",
        measures="scan",
        expected_rows=1,
    ),
    BenchmarkQuery(
        name="filter_scan",
        sql="SELECT COUNT(*) FROM generate_series(1, 100000) AS t(x) WHERE x > 50000",
        measures="filter",
        expected_rows=1,
    ),
    BenchmarkQuery(
        name="sort_small",
        sql="SELECT x FROM generate_series(1, 10000) AS t(x) ORDER BY x DESC",
        measures="sort",
        expected_rows=10000,
    ),
    BenchmarkQuery(
        name="aggregation",
        sql="SELECT x %% 100 AS grp, COUNT(*), SUM(x) FROM generate_series(1, 100000) AS t(x) GROUP BY grp",
        measures="aggregation",
        expected_rows=100,
    ),
    BenchmarkQuery(
        name="hash_build",
        sql=(
            "SELECT a.x, b.x FROM generate_series(1, 10000) AS a(x) "
            "INNER JOIN generate_series(1, 10000) AS b(x) ON a.x = b.x"
        ),
        measures="hash_join",
        expected_rows=10000,
    ),
]

# Spark-compatible benchmarks (no generate_series)
SPARK_BENCHMARKS: list[BenchmarkQuery] = [
    BenchmarkQuery(name="ping", sql="SELECT 1 AS alive", measures="base_latency"),
    BenchmarkQuery(
        name="sequential_scan",
        sql="SELECT COUNT(*) FROM range(100000)",
        measures="scan",
    ),
    BenchmarkQuery(
        name="sort_small",
        sql="SELECT id FROM range(10000) ORDER BY id DESC",
        measures="sort",
    ),
]


def _get_benchmarks_for_backend(backend_type: str) -> list[BenchmarkQuery]:
    """Select appropriate benchmark suite for the backend type."""
    if backend_type in ("spark", "databricks", "flink"):
        return SPARK_BENCHMARKS
    if backend_type in ("bigquery", "snowflake"):
        # These don't support generate_series; use simpler benchmarks
        return [
            BenchmarkQuery(name="ping", sql="SELECT 1", measures="base_latency"),
            BenchmarkQuery(
                name="math_compute",
                sql="SELECT SUM(x) FROM UNNEST(GENERATE_ARRAY(1, 100000)) AS x",
                measures="scan",
            ),
        ]
    return CALIBRATION_BENCHMARKS


class CostCalibrator:
    """Calibrates cost model parameters by running micro-benchmarks.

    For each backend, runs a suite of targeted queries that isolate specific
    cost components (scan, filter, sort, join, aggregation). Measures wall-clock
    latency and derives per-operator cost coefficients.

    Usage:
        calibrator = CostCalibrator(engine)
        result = calibrator.calibrate_backend("prod-postgres-1", connector)
        calibrator.apply_to_cost_model(result, cost_model)
    """

    def __init__(self, engine: Optional[Engine] = None, warmup_rounds: int = 2,
                 measure_rounds: int = 3) -> None:
        """Initialize calibrator.

        Args:
            engine: SQLAlchemy engine for persisting calibration history.
            warmup_rounds: Number of warmup executions before measuring.
            measure_rounds: Number of timed executions to average.
        """
        self._engine = engine
        self._warmup = warmup_rounds
        self._measure = measure_rounds

    def calibrate_backend(self, backend_id: str, connector: Any) -> CalibrationResult:
        """Run full calibration suite against a backend connector.

        Args:
            backend_id: Unique backend identifier.
            connector: BackendConnector instance (must have .execute() method).

        Returns:
            CalibrationResult with derived cost coefficients.
        """
        backend_type = backend_id.split("-")[0] if "-" in backend_id else backend_id
        benchmarks = _get_benchmarks_for_backend(backend_type)
        measurements: dict[str, float] = {}

        for bench in benchmarks:
            latency = self._run_benchmark(connector, bench)
            if latency is not None:
                measurements[bench.name] = latency
                logger.info(
                    "Calibration %s/%s: %.2fms",
                    backend_id, bench.name, latency,
                )

        result = self._derive_coefficients(backend_id, measurements)

        if self._engine is not None:
            self._persist_result(result)

        return result

    def _run_benchmark(self, connector: Any, bench: BenchmarkQuery) -> Optional[float]:
        """Execute a single benchmark query with warmup and averaging.

        Returns:
            Average latency in milliseconds, or None if benchmark failed.
        """
        # Warmup rounds (discard results)
        for _ in range(self._warmup):
            try:
                connector.execute(bench.sql, timeout_seconds=30)
            except Exception:
                logger.error("Unhandled exception in calibrator.py: %s", exc)

        # Measurement rounds
        latencies: list[float] = []
        for _ in range(self._measure):
            try:
                start = time.monotonic()
                connector.execute(bench.sql, timeout_seconds=30)
                elapsed_ms = (time.monotonic() - start) * 1000
                latencies.append(elapsed_ms)
            except Exception as exc:
                logger.debug("Benchmark %s failed: %s", bench.name, exc)

        if not latencies:
            return None

        # Use median to reduce outlier impact
        latencies.sort()
        median_idx = len(latencies) // 2
        return latencies[median_idx]

    def _derive_coefficients(
        self, backend_id: str, measurements: dict[str, float]
    ) -> CalibrationResult:
        """Derive cost model coefficients from benchmark measurements.

        Uses the relationship between benchmarks to isolate individual costs:
        - base_latency = ping time
        - cpu_per_row = (scan_time - base_latency) / rows_scanned
        - filter_cost = (filter_time - scan_time) / rows_filtered (predicate eval)
        - sort_cost = (sort_time - base_latency) / (N * log2(N))
        - hash_cost = (hash_time - base_latency - 2*scan_time_per_10k) / (2*N)
        """
        base_latency = measurements.get("ping", 1.0)
        scan_time = measurements.get("sequential_scan", base_latency + 10.0)
        filter_time = measurements.get("filter_scan", scan_time)
        sort_time = measurements.get("sort_small", base_latency + 5.0)
        agg_time = measurements.get("aggregation", scan_time)
        hash_time = measurements.get("hash_build", scan_time * 2)

        # Derive per-row CPU cost from sequential scan (100K rows)
        scan_rows = 100000
        effective_scan = max(0.1, scan_time - base_latency)
        cpu_per_row = effective_scan / (scan_rows * 1000)  # convert ms to seconds

        # Derive IO cost (assume 8KB pages, ~100 rows/page)
        pages = scan_rows / 100
        io_per_page = effective_scan / (pages * 1000) if pages > 0 else 0.005

        # Derive sort cost coefficient: time / (N * log2(N))
        sort_rows = 10000
        effective_sort = max(0.1, sort_time - base_latency)
        sort_factor = sort_rows * math.log2(max(2, sort_rows))
        sort_cost_per_row_log = effective_sort / (sort_factor * 1000)

        # Derive hash join build cost
        hash_rows = 10000
        effective_hash = max(0.1, hash_time - base_latency - (cpu_per_row * hash_rows * 2 * 1000))
        hash_build_per_row = max(cpu_per_row, effective_hash / (hash_rows * 2 * 1000))

        # Scan throughput
        throughput = scan_rows / max(0.001, effective_scan / 1000)

        result = CalibrationResult(
            backend_id=backend_id,
            base_latency_ms=base_latency,
            cpu_cost_per_row=max(1e-8, cpu_per_row),
            io_cost_per_page=max(1e-6, io_per_page),
            scan_throughput_rows_sec=throughput,
            hash_build_cost_per_row=max(1e-8, hash_build_per_row),
            sort_cost_per_row_log=max(1e-8, sort_cost_per_row_log),
            sample_count=len(measurements),
            calibrated_at=time.time(),
            raw_measurements=measurements,
        )

        logger.info(
            "Calibration complete for %s: cpu=%.2e io=%.2e sort=%.2e hash=%.2e throughput=%.0f rows/s",
            backend_id, result.cpu_cost_per_row, result.io_cost_per_page,
            result.sort_cost_per_row_log, result.hash_build_cost_per_row,
            result.scan_throughput_rows_sec,
        )
        return result

    def apply_to_cost_model(self, result: CalibrationResult, cost_model: CostModel) -> None:
        """Apply calibration results to the cost model.

        Args:
            result: CalibrationResult from calibrate_backend().
            cost_model: CostModel instance to update.
        """
        backend_type = result.backend_id.split("-")[0]
        hw = cost_model.get_hw(backend_type)

        hw.cpu_cost_per_row = result.cpu_cost_per_row
        hw.io_cost_per_page = result.io_cost_per_page

        cost_model.calibrate(backend_type, hw)
        logger.info("Applied calibration for %s to cost model", result.backend_id)

    def needs_recalibration(self, backend_id: str, max_age_hours: float = 24.0) -> bool:
        """Check if a backend needs recalibration based on last calibration time.

        Args:
            backend_id: Backend to check.
            max_age_hours: Maximum age of calibration data in hours.

        Returns:
            True if recalibration is needed.
        """
        if self._engine is None:
            return True

        try:
            stmt = sa.text(
                "SELECT calibrated_at FROM mm_calibration_history "
                "WHERE backend_id = :bid ORDER BY calibrated_at DESC LIMIT 1"
            )
            with self._engine.connect() as conn:
                row = conn.execute(stmt, {"bid": backend_id}).fetchone()

            if row is None:
                return True

            age_hours = (time.time() - float(row[0])) / 3600
            return age_hours > max_age_hours
        except Exception:
            return True

    def _persist_result(self, result: CalibrationResult) -> None:
        """Save calibration result to database for historical tracking."""
        if self._engine is None:
            return

        try:
            import json
            stmt = sa.text(
                "INSERT INTO mm_calibration_history "
                "(backend_id, base_latency_ms, cpu_cost_per_row, io_cost_per_page, "
                "scan_throughput, hash_build_cost, sort_cost, measurements, calibrated_at) "
                "VALUES (:bid, :lat, :cpu, :io, :thr, :hash, :sort, :meas::jsonb, :ts)"
            )
            with self._engine.begin() as conn:
                conn.execute(stmt, {
                    "bid": result.backend_id,
                    "lat": result.base_latency_ms,
                    "cpu": result.cpu_cost_per_row,
                    "io": result.io_cost_per_page,
                    "thr": result.scan_throughput_rows_sec,
                    "hash": result.hash_build_cost_per_row,
                    "sort": result.sort_cost_per_row_log,
                    "meas": json.dumps(result.raw_measurements),
                    "ts": result.calibrated_at,
                })
        except Exception as exc:
            logger.warning("Failed to persist calibration result: %s", exc)
