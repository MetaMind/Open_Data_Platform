"""GreatExpectationsAdapter — DataQualityEngine backed by Great Expectations.

Wraps GE's Validator API so ODEP QualitySuites can be run through GE
and results are mapped back to ODEP CheckResult / SuiteResult objects.

Install: pip install great-expectations

Usage:
    from odep.dq.ge_adapter import GreatExpectationsAdapter
    from odep.dq.models import QualitySuite, QualityRule

    suite = QualitySuite(name="orders_suite", dataset_urn="urn:li:dataset:(...)")
    suite.add_rule(QualityRule.not_null("order_id"))
    suite.add_rule(QualityRule.min_value("amount", 0.0))

    engine = GreatExpectationsAdapter()
    result = engine.run_suite(suite, df)
"""

from __future__ import annotations

from typing import Any

from odep.dq.models import (
    CheckResult,
    QualityRule,
    QualitySuite,
    RuleType,
    Severity,
    SuiteResult,
)
from odep.exceptions import QualityGateFailure

_GE_AVAILABLE = False
try:
    import great_expectations as gx  # noqa: F401
    _GE_AVAILABLE = True
except ImportError:
    pass


class GreatExpectationsAdapter:
    """DataQualityEngine implementation backed by Great Expectations.

    Translates ODEP QualityRules into GE Expectations and maps results back.
    """

    def run_suite(self, suite: QualitySuite, data: Any) -> SuiteResult:
        if not _GE_AVAILABLE:
            raise RuntimeError("great-expectations is not installed. Run: pip install great-expectations")

        import pandas as pd
        import great_expectations as gx
        from great_expectations.dataset import PandasDataset

        if not isinstance(data, pd.DataFrame):
            import pandas as pd
            data = pd.DataFrame(data)

        ge_dataset = PandasDataset(data)
        results = []

        for rule in suite.rules:
            result = self._run_ge_rule(rule, ge_dataset)
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
        if not _GE_AVAILABLE:
            raise RuntimeError("great-expectations is not installed.")
        import pandas as pd
        from great_expectations.dataset import PandasDataset
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)
        ge_dataset = PandasDataset(data)
        return self._run_ge_rule(rule, ge_dataset)

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

    def _run_ge_rule(self, rule: QualityRule, ge_dataset: Any) -> CheckResult:
        """Map a QualityRule to a GE expectation and evaluate it."""
        try:
            if rule.rule_type == RuleType.NOT_NULL:
                ge_result = ge_dataset.expect_column_values_to_not_be_null(rule.column)
            elif rule.rule_type == RuleType.UNIQUE:
                ge_result = ge_dataset.expect_column_values_to_be_unique(rule.column)
            elif rule.rule_type == RuleType.MIN:
                ge_result = ge_dataset.expect_column_min_to_be_between(
                    rule.column, min_value=rule.params["min"])
            elif rule.rule_type == RuleType.MAX:
                ge_result = ge_dataset.expect_column_max_to_be_between(
                    rule.column, max_value=rule.params["max"])
            elif rule.rule_type == RuleType.REGEX:
                ge_result = ge_dataset.expect_column_values_to_match_regex(
                    rule.column, rule.params["pattern"])
            elif rule.rule_type == RuleType.ACCEPTED_VALUES:
                ge_result = ge_dataset.expect_column_values_to_be_in_set(
                    rule.column, rule.params["values"])
            elif rule.rule_type == RuleType.ROW_COUNT_MIN:
                ge_result = ge_dataset.expect_table_row_count_to_be_between(
                    min_value=rule.params["min_rows"])
            elif rule.rule_type == RuleType.COMPLETENESS:
                min_pct = rule.params.get("min_pct", 95.0) / 100.0
                ge_result = ge_dataset.expect_column_values_to_not_be_null(
                    rule.column, mostly=min_pct)
            else:
                # Fall back to NativeQualityEngine for unsupported types
                from odep.dq.engine import NativeQualityEngine
                return NativeQualityEngine().run_rule(rule, ge_dataset._dataframe)

            passed = ge_result.success
            metrics = ge_result.result if hasattr(ge_result, "result") else {}
            return CheckResult(
                rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                passed=passed, severity=rule.severity, dataset_urn=rule.dataset_urn,
                metrics=metrics if isinstance(metrics, dict) else {},
                error_message=None if passed else str(ge_result.exception_info),
            )
        except Exception as exc:
            return CheckResult(
                rule_name=rule.name, rule_type=rule.rule_type, column=rule.column,
                passed=False, severity=rule.severity, dataset_urn=rule.dataset_urn,
                error_message=str(exc),
            )
