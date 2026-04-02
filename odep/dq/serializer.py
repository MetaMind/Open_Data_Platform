"""Rule serialization — export QualitySuites to YAML/JSON and load them back.

File format (YAML):
    suite:
      name: orders_suite
      dataset_urn: "urn:li:dataset:(duckdb,orders,prod)"
      description: "Quality rules for the orders table"
      rules:
        - name: not_null_order_id
          rule_type: not_null
          column: order_id
          severity: blocking
          params: {}
        - name: min_amount
          rule_type: min
          column: amount
          severity: blocking
          params:
            min: 0.0
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Union

from odep.dq.models import QualityRule, QualitySuite, RuleType, Severity


def suite_to_dict(suite: QualitySuite) -> Dict[str, Any]:
    """Convert a QualitySuite to a plain dict (serialisable to YAML/JSON)."""
    return {
        "suite": {
            "name": suite.name,
            "dataset_urn": suite.dataset_urn,
            "description": suite.description,
            "rules": [_rule_to_dict(r) for r in suite.rules],
        }
    }


def suite_from_dict(data: Dict[str, Any]) -> QualitySuite:
    """Load a QualitySuite from a plain dict."""
    s = data.get("suite", data)  # support both wrapped and flat formats
    rules = [_rule_from_dict(r, s.get("dataset_urn", "")) for r in s.get("rules", [])]
    return QualitySuite(
        name=s["name"],
        dataset_urn=s.get("dataset_urn", ""),
        description=s.get("description", ""),
        rules=rules,
    )


def save_suite(suite: QualitySuite, path: Union[str, Path]) -> None:
    """Save a QualitySuite to a YAML or JSON file."""
    path = Path(path)
    data = suite_to_dict(suite)

    if path.suffix in (".yaml", ".yml"):
        import yaml
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    elif path.suffix == ".json":
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}. Use .yaml, .yml, or .json")


def load_suite(path: Union[str, Path]) -> QualitySuite:
    """Load a QualitySuite from a YAML or JSON file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Suite file not found: {path}")

    if path.suffix in (".yaml", ".yml"):
        import yaml
        with open(path) as f:
            data = yaml.safe_load(f)
    elif path.suffix == ".json":
        with open(path) as f:
            data = json.load(f)
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}. Use .yaml, .yml, or .json")

    return suite_from_dict(data)


def load_suites(path: Union[str, Path]) -> List[QualitySuite]:
    """Load multiple suites from a directory or a single file."""
    path = Path(path)
    if path.is_dir():
        suites = []
        for f in sorted(path.glob("*.yaml")) + sorted(path.glob("*.yml")) + sorted(path.glob("*.json")):
            suites.append(load_suite(f))
        return suites
    return [load_suite(path)]


def suite_to_yaml_str(suite: QualitySuite) -> str:
    """Return the YAML string representation of a suite (for display/preview)."""
    import yaml
    return yaml.dump(suite_to_dict(suite), default_flow_style=False, sort_keys=False, allow_unicode=True)


def suite_to_json_str(suite: QualitySuite) -> str:
    """Return the JSON string representation of a suite."""
    return json.dumps(suite_to_dict(suite), indent=2)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _rule_to_dict(rule: QualityRule) -> Dict[str, Any]:
    return {
        "name": rule.name,
        "rule_type": rule.rule_type.value,
        "column": rule.column,
        "severity": rule.severity.value,
        "params": rule.params,
    }


def _rule_from_dict(d: Dict[str, Any], dataset_urn: str) -> QualityRule:
    return QualityRule(
        name=d["name"],
        rule_type=RuleType(d["rule_type"]),
        column=d.get("column"),
        severity=Severity(d.get("severity", "blocking")),
        params=d.get("params") or {},
        dataset_urn=dataset_urn,
    )
