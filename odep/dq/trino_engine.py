"""TrinoQualityEngine — runs DQ rules as SQL queries on a Trino cluster.

All rules are translated to Trino SQL and executed as server-side aggregations.
No data is transferred to the Python process — only scalar results come back.
This makes it suitable for very large tables (billions of rows).

Install: pip install trino

Usage:
    from odep.dq.trino_engine import TrinoQualityEngine
    from odep.dq.models import QualitySuite, QualityRule

    suite = QualitySuite(name="orders", dataset_urn="urn:...")
    suite.add_rule(QualityRule.not_null("order_id"))
    suite.add_rule(QualityRule.min_value("amount", 0.0))

    engine = TrinoQualityEngine(
        table="tpch.tiny.orders",
        host="localhost", port=8082
    )
    result = engine.run_suite(suite)   # no data argument needed
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional

from odep.dq.models import (
    CheckResult,
    QualityRule,
    QualitySuite,
    RuleType,
    Severity,
    SuiteResult,
)
from odep.exceptions import QualityGateFailure


class TrinoQualityEngine:
    """DQ engine that evaluates rules as server-side SQL on Trino.

    Rules are translated to Trino SQL aggregations — no data leaves the cluster.
    Pass `table` as the fully-qualified table name (catalog.schema.table) or
    pass a subquery as `table` (e.g. "(SELECT * FROM orders WHERE dt='2024-01-01') t").
    """

    def __init__(
        self,
        table: str,
        host: str = "localhost",
        port: int = 8082,
        user: str = "odep",
        catalog: str = "tpch",
        schema: str = "tiny",
    ) -> None:
        self.table = table
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema

    def _connect(self):
        try:
            import trino
            return trino.dbapi.connect(
                host=self.host, port=self.port, user=self.user,
                catalog=self.catalog, schema=self.schema,
            )
        except ImportError:
            raise RuntimeError("trino not installed. Run: pip install trino")

    def _execute(self, sql: str) -> Any:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchone()

    def run_suite(self, suite: QualitySuite, data: Any = None) -> SuiteResult:
        """Evaluate all rules as Trino SQL. `data` argument is ignored."""
        results: List[CheckResult] = []
        for rule in suite.rules:
            result = self._evaluate_rule(rule)
            results.append(result)

        passed = sum(1 for r in results if r.passed)
        failed = len(results) - passed
        warnings = sum(1 for r in results if not r.passed and r.severity == Severity.WARNING)
        blocking = sum(1 for r in results if not r.passed and r.is_blocking)

        return SuiteResult(
            suite_name=suite.name,
            dataset_urn=suite.dataset_urn,
            total_rules=len(results),
            passed=passed,
            failed=failed,
            warnings=warnings,
            blocking_failures=blocking,
            results=results,
        )

    def run_rule(self, rule: QualityRule, data: Any = None) -> CheckResult:
        return self._evaluate_rule(rule)

    def assert_suite(self, suite: QualitySuite, data: Any = None) -> SuiteResult:
        result = self.run_suite(suite)
        if result.has_blocking_failures:
            failing = [r for r in result.results if not r.passed and r.is_blocking]
            first = failing[0]
            raise QualityGateFailure(first.rule_name, {
                "quality_score": result.quality_score,
                "blocking_failures": result.blocking_failures,
            })
        return result

    # ------------------------------------------------------------------
    # Rule → SQL translation
    # ------------------------------------------------------------------

    def _evaluate_rule(self, rule: QualityRule) -> CheckResult:
        try:
            if rule.rule_type == RuleType.NOT_NULL:
                return self._trino_not_null(rule)
            elif rule.rule_type == RuleType.UNIQUE:
                return self._trino_unique(rule)
            elif rule.rule_type == RuleType.MIN:
                return self._trino_min(rule)
            elif rule.rule_type == RuleType.MAX:
                return self._trino_max(rule)
            elif rule.rule_type == RuleType.MIN_LENGTH:
                return self._trino_min_length(rule)
            elif rule.rule_type == RuleType.MAX_LENGTH:
                return self._trino_max_length(rule)
            elif rule.rule_type == RuleType.REGEX:
                return self._trino_regex(rule)
            elif rule.rule_type == RuleType.ACCEPTED_VALUES:
                return self._trino_accepted_values(rule)
            elif rule.rule_type == RuleType.ROW_COUNT_MIN:
                return self._trino_row_count_min(rule)
            elif rule.rule_type == RuleType.ROW_COUNT_MAX:
                return self._trino_row_count_max(rule)
            elif rule.rule_type == RuleType.FRESHNESS:
                return self._trino_freshness(rule)
            elif rule.rule_type == RuleType.COMPLETENESS:
                return self._trino_completeness(rule)
            elif rule.rule_type == RuleType.CUSTOM_SQL:
                return self._trino_custom_sql(rule)
            else:
                return CheckResult(rule_name=rule.name, rule_type=rule.rule_type,
                                   column=rule.column, passed=False, severity=rule.severity,
                                   dataset_urn=rule.dataset_urn,
                                   error_message=f"Unknown rule type: {rule.rule_type}")
        except Exception as exc:
            return CheckResult(rule_name=rule.name, rule_type=rule.rule_type,
                               column=rule.column, passed=False, severity=rule.severity,
                               dataset_urn=rule.dataset_urn, error_message=str(exc))

    def _trino_not_null(self, rule):
        row = self._execute(
            f"SELECT COUNT(*) FROM {self.table} WHERE {rule.column} IS NULL"
        )
        null_count = row[0] if row else 0
        passed = null_count == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"null_count": null_count},
                           error_message=None if passed else f"{null_count} nulls in '{rule.column}'")

    def _trino_unique(self, rule):
        row = self._execute(
            f"SELECT COUNT(*) - COUNT(DISTINCT {rule.column}) FROM {self.table}"
        )
        duplicates = row[0] if row else 0
        passed = duplicates == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"duplicate_count": duplicates},
                           error_message=None if passed else f"{duplicates} duplicates in '{rule.column}'")

    def _trino_min(self, rule):
        min_val = rule.params["min"]
        row = self._execute(f"SELECT MIN({rule.column}) FROM {self.table}")
        actual = float(row[0]) if row and row[0] is not None else float("inf")
        passed = actual >= min_val
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"actual_min": actual, "expected_min": min_val},
                           error_message=None if passed else f"Min {actual} < {min_val}")

    def _trino_max(self, rule):
        max_val = rule.params["max"]
        row = self._execute(f"SELECT MAX({rule.column}) FROM {self.table}")
        actual = float(row[0]) if row and row[0] is not None else float("-inf")
        passed = actual <= max_val
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"actual_max": actual, "expected_max": max_val},
                           error_message=None if passed else f"Max {actual} > {max_val}")

    def _trino_min_length(self, rule):
        min_len = rule.params["min_length"]
        row = self._execute(
            f"SELECT COUNT(*) FROM {self.table} WHERE LENGTH(CAST({rule.column} AS VARCHAR)) < {min_len}"
        )
        violations = row[0] if row else 0
        passed = violations == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"violations": violations},
                           error_message=None if passed else f"{violations} values shorter than {min_len}")

    def _trino_max_length(self, rule):
        max_len = rule.params["max_length"]
        row = self._execute(
            f"SELECT COUNT(*) FROM {self.table} WHERE LENGTH(CAST({rule.column} AS VARCHAR)) > {max_len}"
        )
        violations = row[0] if row else 0
        passed = violations == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"violations": violations},
                           error_message=None if passed else f"{violations} values longer than {max_len}")

    def _trino_regex(self, rule):
        pattern = rule.params["pattern"].replace("'", "''")
        row = self._execute(
            f"SELECT COUNT(*) FROM {self.table} WHERE NOT REGEXP_LIKE(CAST({rule.column} AS VARCHAR), '{pattern}')"
        )
        violations = row[0] if row else 0
        passed = violations == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"violations": violations},
                           error_message=None if passed else f"{violations} values don't match pattern")

    def _trino_accepted_values(self, rule):
        values = rule.params["values"]
        in_list = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
        row = self._execute(
            f"SELECT COUNT(*) FROM {self.table} WHERE {rule.column} NOT IN ({in_list})"
        )
        violations = row[0] if row else 0
        passed = violations == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"violations": violations},
                           error_message=None if passed else f"{violations} values not in accepted list")

    def _trino_row_count_min(self, rule):
        min_rows = rule.params["min_rows"]
        row = self._execute(f"SELECT COUNT(*) FROM {self.table}")
        actual = row[0] if row else 0
        passed = actual >= min_rows
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=None,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"actual_rows": actual, "min_rows": min_rows},
                           error_message=None if passed else f"Row count {actual} < {min_rows}")

    def _trino_row_count_max(self, rule):
        max_rows = rule.params["max_rows"]
        row = self._execute(f"SELECT COUNT(*) FROM {self.table}")
        actual = row[0] if row else 0
        passed = actual <= max_rows
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=None,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"actual_rows": actual, "max_rows": max_rows},
                           error_message=None if passed else f"Row count {actual} > {max_rows}")

    def _trino_freshness(self, rule):
        max_age_hours = rule.params["max_age_hours"]
        row = self._execute(f"SELECT MAX({rule.column}) FROM {self.table}")
        latest = row[0] if row else None
        if latest is None:
            return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                               passed=False, severity=rule.severity, dataset_urn=rule.dataset_urn,
                               error_message=f"No timestamps in '{rule.column}'")
        now = datetime.now(timezone.utc)
        if hasattr(latest, "tzinfo") and latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age_hours = (now - latest).total_seconds() / 3600
        passed = age_hours <= max_age_hours
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"age_hours": round(age_hours, 2), "max_age_hours": max_age_hours},
                           error_message=None if passed else f"Data is {age_hours:.1f}h old")

    def _trino_completeness(self, rule):
        min_pct = rule.params.get("min_pct", 95.0)
        row = self._execute(
            f"SELECT COUNT(*), COUNT({rule.column}) FROM {self.table}"
        )
        total, non_null = (row[0], row[1]) if row else (0, 0)
        actual_pct = (non_null / total * 100) if total > 0 else 0.0
        passed = actual_pct >= min_pct
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"completeness_pct": round(actual_pct, 2), "min_pct": min_pct},
                           error_message=None if passed else f"Completeness {actual_pct:.1f}% < {min_pct}%")

    def _trino_custom_sql(self, rule):
        sql = rule.params["sql"].replace("data", self.table)
        row = self._execute(sql)
        passed = bool(row[0]) if row else False
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=None,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"sql_result": row[0] if row else None},
                           error_message=None if passed else f"Custom SQL returned: {row}")
