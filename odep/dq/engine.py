"""DataQualityEngine Protocol and NativeQualityEngine implementation.

The NativeQualityEngine evaluates rules directly against pandas DataFrames
or DuckDB query results — no external service required.

Supported rule types:
  not_null, unique, min, max, min_length, max_length, regex,
  accepted_values, row_count_min, row_count_max, freshness,
  completeness, custom_sql
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from odep.dq.models import (
    CheckResult,
    QualityRule,
    QualitySuite,
    RuleType,
    Severity,
    SuiteResult,
)
from odep.exceptions import QualityGateFailure


@runtime_checkable
class DataQualityEngine(Protocol):
    """Protocol for data quality engines."""

    def run_suite(self, suite: QualitySuite, data: Any) -> SuiteResult:
        """Evaluate all rules in the suite against `data`. Returns SuiteResult."""
        ...

    def run_rule(self, rule: QualityRule, data: Any) -> CheckResult:
        """Evaluate a single rule against `data`. Returns CheckResult."""
        ...

    def assert_suite(self, suite: QualitySuite, data: Any) -> SuiteResult:
        """Run suite and raise QualityGateFailure if any blocking rule fails."""
        ...


class NativeQualityEngine:
    """In-process DQ engine. Evaluates rules against pandas DataFrames or lists of dicts.

    `data` can be:
      - pandas DataFrame
      - list of dicts (e.g. from DuckDB .fetchall() with column names)
      - dict with keys "columns" (list) and "rows" (list of tuples)
    """

    def run_suite(self, suite: QualitySuite, data: Any) -> SuiteResult:
        """Evaluate all rules in the suite and return a SuiteResult."""
        df = self._to_dataframe(data)
        results: List[CheckResult] = []

        for rule in suite.rules:
            result = self._evaluate_rule(rule, df)
            results.append(result)

        passed = sum(1 for r in results if r.passed)
        failed = sum(1 for r in results if not r.passed)
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

    def run_rule(self, rule: QualityRule, data: Any) -> CheckResult:
        """Evaluate a single rule and return a CheckResult."""
        df = self._to_dataframe(data)
        return self._evaluate_rule(rule, df)

    def assert_suite(self, suite: QualitySuite, data: Any) -> SuiteResult:
        """Run suite and raise QualityGateFailure if any blocking rule fails."""
        result = self.run_suite(suite, data)
        if result.has_blocking_failures:
            failing = [r for r in result.results if not r.passed and r.is_blocking]
            first = failing[0]
            raise QualityGateFailure(
                first.rule_name,
                {
                    "quality_score": result.quality_score,
                    "blocking_failures": result.blocking_failures,
                    "failed_rule": first.rule_name,
                    "metrics": first.metrics,
                },
            )
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _to_dataframe(self, data: Any) -> "Any":
        """Normalise input to a pandas DataFrame."""
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("pandas is required for NativeQualityEngine. Run: pip install pandas")

        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, list):
            if not data:
                return pd.DataFrame()
            if isinstance(data[0], dict):
                return pd.DataFrame(data)
            # list of tuples — no column names
            return pd.DataFrame(data)
        if isinstance(data, dict) and "columns" in data and "rows" in data:
            return pd.DataFrame(data["rows"], columns=data["columns"])
        raise TypeError(f"Unsupported data type for DQ engine: {type(data)}")

    def _evaluate_rule(self, rule: QualityRule, df: "Any") -> CheckResult:
        """Dispatch to the appropriate rule evaluator."""
        import pandas as pd

        try:
            if rule.rule_type == RuleType.NOT_NULL:
                return self._check_not_null(rule, df)
            elif rule.rule_type == RuleType.UNIQUE:
                return self._check_unique(rule, df)
            elif rule.rule_type == RuleType.MIN:
                return self._check_min(rule, df)
            elif rule.rule_type == RuleType.MAX:
                return self._check_max(rule, df)
            elif rule.rule_type == RuleType.MIN_LENGTH:
                return self._check_min_length(rule, df)
            elif rule.rule_type == RuleType.MAX_LENGTH:
                return self._check_max_length(rule, df)
            elif rule.rule_type == RuleType.REGEX:
                return self._check_regex(rule, df)
            elif rule.rule_type == RuleType.ACCEPTED_VALUES:
                return self._check_accepted_values(rule, df)
            elif rule.rule_type == RuleType.ROW_COUNT_MIN:
                return self._check_row_count_min(rule, df)
            elif rule.rule_type == RuleType.ROW_COUNT_MAX:
                return self._check_row_count_max(rule, df)
            elif rule.rule_type == RuleType.FRESHNESS:
                return self._check_freshness(rule, df)
            elif rule.rule_type == RuleType.COMPLETENESS:
                return self._check_completeness(rule, df)
            elif rule.rule_type == RuleType.CUSTOM_SQL:
                return self._check_custom_sql(rule, df)
            else:
                return CheckResult(
                    rule_name=rule.name, rule_type=rule.rule_type,
                    column=rule.column, passed=False, severity=rule.severity,
                    dataset_urn=rule.dataset_urn,
                    error_message=f"Unknown rule type: {rule.rule_type}",
                )
        except Exception as exc:
            return CheckResult(
                rule_name=rule.name, rule_type=rule.rule_type,
                column=rule.column, passed=False, severity=rule.severity,
                dataset_urn=rule.dataset_urn,
                error_message=str(exc),
            )

    def _check_not_null(self, rule: QualityRule, df: "Any") -> CheckResult:
        col = df[rule.column]
        null_count = int(col.isna().sum())
        passed = null_count == 0
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"null_count": null_count, "total_rows": len(df)},
            error_message=None if passed else f"{null_count} null values found in '{rule.column}'",
        )

    def _check_unique(self, rule: QualityRule, df: "Any") -> CheckResult:
        col = df[rule.column]
        duplicate_count = int(col.duplicated().sum())
        passed = duplicate_count == 0
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"duplicate_count": duplicate_count, "total_rows": len(df)},
            error_message=None if passed else f"{duplicate_count} duplicate values in '{rule.column}'",
        )

    def _check_min(self, rule: QualityRule, df: "Any") -> CheckResult:
        min_val = rule.params["min"]
        actual_min = float(df[rule.column].min())
        passed = actual_min >= min_val
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"actual_min": actual_min, "expected_min": min_val},
            error_message=None if passed else f"Min {actual_min} < expected {min_val} in '{rule.column}'",
        )

    def _check_max(self, rule: QualityRule, df: "Any") -> CheckResult:
        max_val = rule.params["max"]
        actual_max = float(df[rule.column].max())
        passed = actual_max <= max_val
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"actual_max": actual_max, "expected_max": max_val},
            error_message=None if passed else f"Max {actual_max} > expected {max_val} in '{rule.column}'",
        )

    def _check_min_length(self, rule: QualityRule, df: "Any") -> CheckResult:
        min_len = rule.params["min_length"]
        lengths = df[rule.column].astype(str).str.len()
        violations = int((lengths < min_len).sum())
        passed = violations == 0
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"violations": violations, "min_length": min_len},
            error_message=None if passed else f"{violations} values shorter than {min_len} in '{rule.column}'",
        )

    def _check_max_length(self, rule: QualityRule, df: "Any") -> CheckResult:
        max_len = rule.params["max_length"]
        lengths = df[rule.column].astype(str).str.len()
        violations = int((lengths > max_len).sum())
        passed = violations == 0
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"violations": violations, "max_length": max_len},
            error_message=None if passed else f"{violations} values longer than {max_len} in '{rule.column}'",
        )

    def _check_regex(self, rule: QualityRule, df: "Any") -> CheckResult:
        pattern = rule.params["pattern"]
        col = df[rule.column].astype(str)
        violations = int((~col.str.match(pattern, na=False)).sum())
        passed = violations == 0
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"violations": violations, "pattern": pattern},
            error_message=None if passed else f"{violations} values don't match pattern '{pattern}' in '{rule.column}'",
        )

    def _check_accepted_values(self, rule: QualityRule, df: "Any") -> CheckResult:
        values = rule.params["values"]
        violations = int((~df[rule.column].isin(values)).sum())
        passed = violations == 0
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"violations": violations, "accepted_values": values},
            error_message=None if passed else f"{violations} values not in accepted list in '{rule.column}'",
        )

    def _check_row_count_min(self, rule: QualityRule, df: "Any") -> CheckResult:
        min_rows = rule.params["min_rows"]
        actual = len(df)
        passed = actual >= min_rows
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=None,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"actual_rows": actual, "min_rows": min_rows},
            error_message=None if passed else f"Row count {actual} < minimum {min_rows}",
        )

    def _check_row_count_max(self, rule: QualityRule, df: "Any") -> CheckResult:
        max_rows = rule.params["max_rows"]
        actual = len(df)
        passed = actual <= max_rows
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=None,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"actual_rows": actual, "max_rows": max_rows},
            error_message=None if passed else f"Row count {actual} > maximum {max_rows}",
        )

    def _check_freshness(self, rule: QualityRule, df: "Any") -> CheckResult:
        import pandas as pd
        max_age_hours = rule.params["max_age_hours"]
        col = pd.to_datetime(df[rule.column], errors="coerce")
        latest = col.max()
        if pd.isna(latest):
            return CheckResult(
                rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                passed=False, severity=rule.severity, dataset_urn=rule.dataset_urn,
                metrics={}, error_message=f"No valid timestamps in '{rule.column}'",
            )
        now = datetime.now(timezone.utc)
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age_hours = (now - latest).total_seconds() / 3600
        passed = age_hours <= max_age_hours
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"age_hours": round(age_hours, 2), "max_age_hours": max_age_hours},
            error_message=None if passed else f"Data is {age_hours:.1f}h old, max allowed {max_age_hours}h",
        )

    def _check_completeness(self, rule: QualityRule, df: "Any") -> CheckResult:
        min_pct = rule.params.get("min_pct", 95.0)
        total = len(df)
        non_null = int(df[rule.column].notna().sum())
        actual_pct = (non_null / total * 100) if total > 0 else 0.0
        passed = actual_pct >= min_pct
        return CheckResult(
            rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
            passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
            metrics={"completeness_pct": round(actual_pct, 2), "min_pct": min_pct},
            error_message=None if passed else f"Completeness {actual_pct:.1f}% < required {min_pct}% in '{rule.column}'",
        )

    def _check_custom_sql(self, rule: QualityRule, df: "Any") -> CheckResult:
        """Evaluate a custom SQL expression using DuckDB against the DataFrame."""
        try:
            import duckdb
            conn = duckdb.connect()
            conn.register("data", df)
            result = conn.execute(rule.params["sql"]).fetchone()
            passed = bool(result[0]) if result else False
            return CheckResult(
                rule_name=rule.name, rule_type=rule.rule_type, column=None,
                passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                metrics={"sql_result": result[0] if result else None},
                error_message=None if passed else f"Custom SQL returned falsy: {result}",
            )
        except ImportError:
            raise RuntimeError("duckdb is required for custom_sql rules. Run: pip install duckdb")
