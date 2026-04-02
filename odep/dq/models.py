"""Data Quality models — rule definitions, check results, and suite configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class RuleType(str, Enum):
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    MIN = "min"
    MAX = "max"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    REGEX = "regex"
    ACCEPTED_VALUES = "accepted_values"
    ROW_COUNT_MIN = "row_count_min"
    ROW_COUNT_MAX = "row_count_max"
    FRESHNESS = "freshness"
    COMPLETENESS = "completeness"       # % non-null >= threshold
    CUSTOM_SQL = "custom_sql"           # arbitrary SQL expression that must return True


class Severity(str, Enum):
    BLOCKING = "blocking"       # raises QualityGateFailure on failure
    WARNING = "warning"         # records failure but does not block


@dataclass
class QualityRule:
    """A single data quality assertion."""

    name: str
    rule_type: RuleType
    column: Optional[str] = None        # None for table-level rules
    severity: Severity = Severity.BLOCKING
    # Rule-specific parameters
    params: Dict[str, Any] = field(default_factory=dict)
    dataset_urn: str = ""

    # Convenience constructors
    @classmethod
    def not_null(cls, column: str, severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=f"not_null_{column}", rule_type=RuleType.NOT_NULL,
                   column=column, severity=severity, dataset_urn=urn)

    @classmethod
    def unique(cls, column: str, severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=f"unique_{column}", rule_type=RuleType.UNIQUE,
                   column=column, severity=severity, dataset_urn=urn)

    @classmethod
    def min_value(cls, column: str, min_val: float, severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=f"min_{column}", rule_type=RuleType.MIN,
                   column=column, severity=severity, params={"min": min_val}, dataset_urn=urn)

    @classmethod
    def max_value(cls, column: str, max_val: float, severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=f"max_{column}", rule_type=RuleType.MAX,
                   column=column, severity=severity, params={"max": max_val}, dataset_urn=urn)

    @classmethod
    def regex(cls, column: str, pattern: str, severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=f"regex_{column}", rule_type=RuleType.REGEX,
                   column=column, severity=severity, params={"pattern": pattern}, dataset_urn=urn)

    @classmethod
    def accepted_values(cls, column: str, values: List[Any], severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=f"accepted_values_{column}", rule_type=RuleType.ACCEPTED_VALUES,
                   column=column, severity=severity, params={"values": values}, dataset_urn=urn)

    @classmethod
    def row_count_min(cls, min_rows: int, severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=f"row_count_min_{min_rows}", rule_type=RuleType.ROW_COUNT_MIN,
                   severity=severity, params={"min_rows": min_rows}, dataset_urn=urn)

    @classmethod
    def freshness(cls, column: str, max_age_hours: float, severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=f"freshness_{column}", rule_type=RuleType.FRESHNESS,
                   column=column, severity=severity, params={"max_age_hours": max_age_hours}, dataset_urn=urn)

    @classmethod
    def completeness(cls, column: str, min_pct: float = 95.0, severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=f"completeness_{column}", rule_type=RuleType.COMPLETENESS,
                   column=column, severity=severity, params={"min_pct": min_pct}, dataset_urn=urn)

    @classmethod
    def custom_sql(cls, name: str, sql: str, severity: Severity = Severity.BLOCKING, urn: str = "") -> "QualityRule":
        return cls(name=name, rule_type=RuleType.CUSTOM_SQL,
                   severity=severity, params={"sql": sql}, dataset_urn=urn)


@dataclass
class CheckResult:
    """Result of evaluating a single QualityRule."""

    rule_name: str
    rule_type: RuleType
    column: Optional[str]
    passed: bool
    severity: Severity
    dataset_urn: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    evaluated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def is_blocking(self) -> bool:
        return self.severity == Severity.BLOCKING


@dataclass
class SuiteResult:
    """Aggregated result of running a QualitySuite."""

    suite_name: str
    dataset_urn: str
    total_rules: int
    passed: int
    failed: int
    warnings: int
    blocking_failures: int
    results: List[CheckResult] = field(default_factory=list)
    evaluated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def quality_score(self) -> float:
        if self.total_rules == 0:
            return 0.0
        return (self.passed / self.total_rules) * 100.0

    @property
    def has_blocking_failures(self) -> bool:
        return self.blocking_failures > 0


@dataclass
class QualitySuite:
    """A named collection of QualityRules for a dataset."""

    name: str
    dataset_urn: str
    rules: List[QualityRule] = field(default_factory=list)
    description: str = ""

    def add_rule(self, rule: QualityRule) -> "QualitySuite":
        rule.dataset_urn = self.dataset_urn
        self.rules.append(rule)
        return self
