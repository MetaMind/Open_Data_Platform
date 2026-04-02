"""SparkQualityEngine — runs DQ rules natively on a Spark DataFrame.

Rules are translated to Spark SQL expressions and executed as distributed
aggregations — no data is collected to the driver for most rule types.
Only `custom_sql` and `freshness` require a small collect().

Install: pip install pyspark

Usage:
    from odep.dq.spark_engine import SparkQualityEngine
    from odep.dq.models import QualitySuite, QualityRule

    suite = QualitySuite(name="orders", dataset_urn="urn:...")
    suite.add_rule(QualityRule.not_null("order_id"))
    suite.add_rule(QualityRule.min_value("amount", 0.0))

    engine = SparkQualityEngine()
    result = engine.run_suite(suite, spark_df)
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


class SparkQualityEngine:
    """DQ engine that evaluates rules on PySpark DataFrames using Spark SQL aggregations.

    Most rules run as a single distributed aggregation pass — no driver-side collect
    except for freshness (needs max timestamp) and custom_sql.
    """

    def __init__(self, spark=None) -> None:
        """
        Args:
            spark: SparkSession. If None, uses SparkSession.builder.getOrCreate().
        """
        self._spark = spark

    def _get_spark(self):
        if self._spark is not None:
            return self._spark
        try:
            from pyspark.sql import SparkSession
            return SparkSession.builder.getOrCreate()
        except ImportError:
            raise RuntimeError("pyspark not installed. Run: pip install pyspark")

    def run_suite(self, suite: QualitySuite, data: Any) -> SuiteResult:
        """Evaluate all rules against a Spark DataFrame."""
        spark_df = self._ensure_spark_df(data)
        results: List[CheckResult] = []

        for rule in suite.rules:
            result = self._evaluate_rule(rule, spark_df)
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

    def run_rule(self, rule: QualityRule, data: Any) -> CheckResult:
        spark_df = self._ensure_spark_df(data)
        return self._evaluate_rule(rule, spark_df)

    def assert_suite(self, suite: QualitySuite, data: Any) -> SuiteResult:
        result = self.run_suite(suite, data)
        if result.has_blocking_failures:
            failing = [r for r in result.results if not r.passed and r.is_blocking]
            first = failing[0]
            raise QualityGateFailure(first.rule_name, {
                "quality_score": result.quality_score,
                "blocking_failures": result.blocking_failures,
            })
        return result

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _ensure_spark_df(self, data: Any) -> Any:
        """Convert input to a Spark DataFrame if needed."""
        try:
            from pyspark.sql import DataFrame as SparkDF
            if isinstance(data, SparkDF):
                return data
        except ImportError:
            pass

        # Convert pandas DataFrame to Spark
        try:
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                spark = self._get_spark()
                return spark.createDataFrame(data)
        except ImportError:
            pass

        raise TypeError(f"SparkQualityEngine requires a Spark or pandas DataFrame, got {type(data)}")

    def _evaluate_rule(self, rule: QualityRule, df: Any) -> CheckResult:
        try:
            from pyspark.sql import functions as F

            if rule.rule_type == RuleType.NOT_NULL:
                return self._spark_not_null(rule, df, F)
            elif rule.rule_type == RuleType.UNIQUE:
                return self._spark_unique(rule, df, F)
            elif rule.rule_type == RuleType.MIN:
                return self._spark_min(rule, df, F)
            elif rule.rule_type == RuleType.MAX:
                return self._spark_max(rule, df, F)
            elif rule.rule_type == RuleType.MIN_LENGTH:
                return self._spark_min_length(rule, df, F)
            elif rule.rule_type == RuleType.MAX_LENGTH:
                return self._spark_max_length(rule, df, F)
            elif rule.rule_type == RuleType.REGEX:
                return self._spark_regex(rule, df, F)
            elif rule.rule_type == RuleType.ACCEPTED_VALUES:
                return self._spark_accepted_values(rule, df, F)
            elif rule.rule_type == RuleType.ROW_COUNT_MIN:
                return self._spark_row_count_min(rule, df)
            elif rule.rule_type == RuleType.ROW_COUNT_MAX:
                return self._spark_row_count_max(rule, df)
            elif rule.rule_type == RuleType.FRESHNESS:
                return self._spark_freshness(rule, df, F)
            elif rule.rule_type == RuleType.COMPLETENESS:
                return self._spark_completeness(rule, df, F)
            elif rule.rule_type == RuleType.CUSTOM_SQL:
                return self._spark_custom_sql(rule, df)
            else:
                return CheckResult(rule_name=rule.name, rule_type=rule.rule_type,
                                   column=rule.column, passed=False, severity=rule.severity,
                                   dataset_urn=rule.dataset_urn,
                                   error_message=f"Unknown rule type: {rule.rule_type}")
        except Exception as exc:
            return CheckResult(rule_name=rule.name, rule_type=rule.rule_type,
                               column=rule.column, passed=False, severity=rule.severity,
                               dataset_urn=rule.dataset_urn, error_message=str(exc))

    def _spark_not_null(self, rule, df, F):
        null_count = df.filter(F.col(rule.column).isNull()).count()
        passed = null_count == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"null_count": null_count, "total_rows": df.count()},
                           error_message=None if passed else f"{null_count} nulls in '{rule.column}'")

    def _spark_unique(self, rule, df, F):
        total = df.count()
        distinct = df.select(rule.column).distinct().count()
        duplicates = total - distinct
        passed = duplicates == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"duplicate_count": duplicates, "total_rows": total},
                           error_message=None if passed else f"{duplicates} duplicates in '{rule.column}'")

    def _spark_min(self, rule, df, F):
        min_val = rule.params["min"]
        actual = df.agg(F.min(rule.column)).collect()[0][0]
        actual = float(actual) if actual is not None else float("inf")
        passed = actual >= min_val
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"actual_min": actual, "expected_min": min_val},
                           error_message=None if passed else f"Min {actual} < {min_val}")

    def _spark_max(self, rule, df, F):
        max_val = rule.params["max"]
        actual = df.agg(F.max(rule.column)).collect()[0][0]
        actual = float(actual) if actual is not None else float("-inf")
        passed = actual <= max_val
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"actual_max": actual, "expected_max": max_val},
                           error_message=None if passed else f"Max {actual} > {max_val}")

    def _spark_min_length(self, rule, df, F):
        min_len = rule.params["min_length"]
        violations = df.filter(F.length(F.col(rule.column).cast("string")) < min_len).count()
        passed = violations == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"violations": violations},
                           error_message=None if passed else f"{violations} values shorter than {min_len}")

    def _spark_max_length(self, rule, df, F):
        max_len = rule.params["max_length"]
        violations = df.filter(F.length(F.col(rule.column).cast("string")) > max_len).count()
        passed = violations == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"violations": violations},
                           error_message=None if passed else f"{violations} values longer than {max_len}")

    def _spark_regex(self, rule, df, F):
        pattern = rule.params["pattern"]
        violations = df.filter(~F.col(rule.column).cast("string").rlike(pattern)).count()
        passed = violations == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"violations": violations, "pattern": pattern},
                           error_message=None if passed else f"{violations} values don't match '{pattern}'")

    def _spark_accepted_values(self, rule, df, F):
        values = rule.params["values"]
        violations = df.filter(~F.col(rule.column).isin(values)).count()
        passed = violations == 0
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"violations": violations},
                           error_message=None if passed else f"{violations} values not in accepted list")

    def _spark_row_count_min(self, rule, df):
        min_rows = rule.params["min_rows"]
        actual = df.count()
        passed = actual >= min_rows
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=None,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"actual_rows": actual, "min_rows": min_rows},
                           error_message=None if passed else f"Row count {actual} < {min_rows}")

    def _spark_row_count_max(self, rule, df):
        max_rows = rule.params["max_rows"]
        actual = df.count()
        passed = actual <= max_rows
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=None,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"actual_rows": actual, "max_rows": max_rows},
                           error_message=None if passed else f"Row count {actual} > {max_rows}")

    def _spark_freshness(self, rule, df, F):
        max_age_hours = rule.params["max_age_hours"]
        latest_row = df.agg(F.max(rule.column)).collect()[0][0]
        if latest_row is None:
            return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                               passed=False, severity=rule.severity, dataset_urn=rule.dataset_urn,
                               error_message=f"No timestamps in '{rule.column}'")
        now = datetime.now(timezone.utc)
        if hasattr(latest_row, "tzinfo") and latest_row.tzinfo is None:
            from datetime import timezone as tz
            latest_row = latest_row.replace(tzinfo=tz.utc)
        age_hours = (now - latest_row).total_seconds() / 3600
        passed = age_hours <= max_age_hours
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"age_hours": round(age_hours, 2), "max_age_hours": max_age_hours},
                           error_message=None if passed else f"Data is {age_hours:.1f}h old")

    def _spark_completeness(self, rule, df, F):
        min_pct = rule.params.get("min_pct", 95.0)
        total = df.count()
        non_null = df.filter(F.col(rule.column).isNotNull()).count()
        actual_pct = (non_null / total * 100) if total > 0 else 0.0
        passed = actual_pct >= min_pct
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"completeness_pct": round(actual_pct, 2), "min_pct": min_pct},
                           error_message=None if passed else f"Completeness {actual_pct:.1f}% < {min_pct}%")

    def _spark_custom_sql(self, rule, df):
        """Register DataFrame as temp view and run custom SQL."""
        spark = self._get_spark()
        view_name = f"_dq_temp_{rule.name.replace('-', '_')}"
        df.createOrReplaceTempView(view_name)
        sql = rule.params["sql"].replace("data", view_name)
        result = spark.sql(sql).collect()
        passed = bool(result[0][0]) if result else False
        return CheckResult(rule_name=rule.name, rule_type=rule.rule_type, column=None,
                           passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                           metrics={"sql_result": result[0][0] if result else None},
                           error_message=None if passed else f"Custom SQL returned: {result}")
