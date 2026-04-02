"""LLM-powered DQ Advisor — generates and evaluates quality rules using an LLM.

Supports OpenAI (gpt-4o, gpt-4-turbo), Anthropic (claude-3-5-sonnet),
and any OpenAI-compatible endpoint (Ollama, Azure OpenAI, etc.).

Install:
    pip install openai          # for OpenAI / Azure / Ollama
    pip install anthropic       # for Claude

Config (.odep.env):
    ODEP_LLM__PROVIDER=openai          # openai | anthropic | ollama
    ODEP_LLM__MODEL=gpt-4o
    ODEP_LLM__API_KEY=sk-...
    ODEP_LLM__BASE_URL=                # optional: custom endpoint (Ollama, Azure)
    ODEP_LLM__MAX_TOKENS=4096
    ODEP_LLM__TEMPERATURE=0.2
"""

from __future__ import annotations

import json
import textwrap
from typing import Any, Dict, List, Optional

from odep.dq.models import QualityRule, QualitySuite, RuleType, Severity

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_SCHEMA_ANALYSIS_PROMPT = textwrap.dedent("""
You are a senior data quality engineer. Analyse the dataset schema and sample data
provided below, then generate a comprehensive set of data quality rules.

## Dataset Information
- Dataset URN: {urn}
- Table name: {table_name}
- Description: {description}

## Schema (column name → data type)
{schema}

## Sample Data (first {sample_rows} rows)
{sample_data}

## Column Statistics
{stats}

## Instructions
Generate data quality rules that cover:
1. Null checks for columns that should never be null (primary keys, required fields)
2. Uniqueness checks for identifier columns
3. Value range checks (min/max) for numeric columns based on observed data
4. Regex pattern checks for structured strings (emails, phone numbers, IDs, codes)
5. Accepted values checks for low-cardinality categorical columns
6. Completeness thresholds for optional columns with high fill rates
7. Freshness checks for timestamp columns
8. Custom SQL checks for complex business rules you infer from the data

For each rule, assign severity:
- "blocking": critical rules that must pass (PKs, required fields, business invariants)
- "warning": advisory rules (optional fields, soft constraints)

Return ONLY a valid JSON array. No markdown, no explanation, just the JSON.

## Output Format
[
  {{
    "name": "not_null_order_id",
    "rule_type": "not_null",
    "column": "order_id",
    "severity": "blocking",
    "params": {{}},
    "rationale": "order_id is the primary key and must never be null"
  }},
  {{
    "name": "min_amount",
    "rule_type": "min",
    "column": "amount",
    "severity": "blocking",
    "params": {{"min": 0.0}},
    "rationale": "Order amounts cannot be negative"
  }},
  {{
    "name": "valid_email_format",
    "rule_type": "regex",
    "column": "email",
    "severity": "warning",
    "params": {{"pattern": "^[a-zA-Z0-9._%+\\\\-]+@[a-zA-Z0-9.\\\\-]+\\\\.[a-zA-Z]{{2,}}$"}},
    "rationale": "Email addresses should follow standard format"
  }}
]

Valid rule_type values: not_null, unique, min, max, min_length, max_length, regex,
accepted_values, row_count_min, row_count_max, freshness, completeness, custom_sql
""").strip()

_EVALUATION_PROMPT = textwrap.dedent("""
You are a senior data quality engineer reviewing the results of automated data quality checks.

## Dataset
- URN: {urn}
- Suite: {suite_name}

## Check Results
{results_table}

## Quality Score: {score:.1f}%
- Total rules: {total}
- Passed: {passed}
- Failed: {failed} ({blocking} blocking, {warnings} warnings)

## Instructions
Provide a concise data quality assessment with:
1. **Executive Summary** (2-3 sentences): overall health, key concerns
2. **Critical Issues** (blocking failures only): what failed, why it matters, recommended fix
3. **Warnings**: non-blocking issues worth monitoring
4. **Root Cause Hypotheses**: likely causes for the failures based on the data patterns
5. **Recommended Actions**: prioritised list of fixes (most critical first)
6. **Trend Indicator**: HEALTHY / DEGRADED / CRITICAL based on the score and failure types

Be specific and actionable. Reference column names and actual metric values.
""").strip()

_ANOMALY_DETECTION_PROMPT = textwrap.dedent("""
You are a data quality analyst specialising in anomaly detection.

## Dataset: {table_name}
## Column Statistics
{stats}

## Recent Sample (last {sample_rows} rows)
{sample_data}

## Historical Baseline (if available)
{baseline}

## Instructions
Identify data anomalies and quality issues that standard rule-based checks might miss:

1. **Statistical Anomalies**: unusual distributions, outliers, unexpected spikes/drops
2. **Pattern Breaks**: changes in string formats, encoding issues, truncated values
3. **Referential Issues**: orphaned foreign keys, broken relationships
4. **Temporal Anomalies**: gaps in time series, future dates, implausible timestamps
5. **Business Logic Violations**: combinations of values that violate domain rules
6. **Data Drift**: columns whose distributions have shifted from the baseline

For each anomaly found, suggest a specific QualityRule (in the same JSON format as
the rule generation prompt) that would catch it in future runs.

Return a JSON object:
{{
  "anomalies": [
    {{
      "column": "amount",
      "type": "statistical_outlier",
      "description": "3 values exceed 3 standard deviations from mean",
      "severity": "warning",
      "suggested_rule": {{
        "name": "max_amount_outlier",
        "rule_type": "max",
        "column": "amount",
        "severity": "warning",
        "params": {{"max": 99999.0}},
        "rationale": "Cap at 3-sigma upper bound based on historical data"
      }}
    }}
  ],
  "summary": "Brief summary of findings"
}}
""").strip()


# ---------------------------------------------------------------------------
# LLM Client
# ---------------------------------------------------------------------------

class LLMAdvisor:
    """LLM-powered data quality advisor.

    Generates rules from schema/data, evaluates check results, and detects anomalies.
    """

    def __init__(
        self,
        provider: str = "openai",
        model: str = "gpt-4o",
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        max_tokens: int = 4096,
        temperature: float = 0.2,
    ) -> None:
        self.provider = provider
        self.model = model
        self.api_key = api_key
        self.base_url = base_url
        self.max_tokens = max_tokens
        self.temperature = temperature

    @classmethod
    def from_config(cls) -> "LLMAdvisor":
        """Create from environment variables / .odep.env."""
        import os
        return cls(
            provider=os.getenv("ODEP_LLM__PROVIDER", "openai"),
            model=os.getenv("ODEP_LLM__MODEL", "gpt-4o"),
            api_key=os.getenv("ODEP_LLM__API_KEY") or os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("ODEP_LLM__BASE_URL") or None,
            max_tokens=int(os.getenv("ODEP_LLM__MAX_TOKENS", "4096")),
            temperature=float(os.getenv("ODEP_LLM__TEMPERATURE", "0.2")),
        )

    def _call(self, prompt: str) -> str:
        """Send a prompt to the configured LLM and return the response text."""
        if self.provider in ("openai", "ollama", "azure"):
            return self._call_openai(prompt)
        elif self.provider == "anthropic":
            return self._call_anthropic(prompt)
        else:
            raise ValueError(f"Unknown LLM provider: {self.provider!r}. Use openai, anthropic, or ollama.")

    def _call_openai(self, prompt: str) -> str:
        try:
            from openai import OpenAI
        except ImportError:
            raise RuntimeError("openai package not installed. Run: pip install openai")

        kwargs: Dict[str, Any] = {"api_key": self.api_key}
        if self.base_url:
            kwargs["base_url"] = self.base_url

        client = OpenAI(**kwargs)
        response = client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=self.max_tokens,
            temperature=self.temperature,
        )
        return response.choices[0].message.content.strip()

    def _call_anthropic(self, prompt: str) -> str:
        try:
            import anthropic
        except ImportError:
            raise RuntimeError("anthropic package not installed. Run: pip install anthropic")

        client = anthropic.Anthropic(api_key=self.api_key)
        message = client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_rules(
        self,
        data: Any,
        urn: str = "",
        table_name: str = "dataset",
        description: str = "",
        sample_rows: int = 10,
    ) -> QualitySuite:
        """Analyse schema and sample data, return a QualitySuite with LLM-generated rules.

        Args:
            data: pandas DataFrame
            urn: dataset URN
            table_name: human-readable table name
            description: optional dataset description
            sample_rows: number of sample rows to include in the prompt

        Returns:
            QualitySuite populated with generated QualityRule objects
        """
        import pandas as pd

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        schema = self._describe_schema(data)
        sample = data.head(sample_rows).to_string(index=False)
        stats = self._compute_stats(data)

        prompt = _SCHEMA_ANALYSIS_PROMPT.format(
            urn=urn,
            table_name=table_name,
            description=description or "No description provided",
            schema=schema,
            sample_data=sample,
            sample_rows=min(sample_rows, len(data)),
            stats=stats,
        )

        raw = self._call(prompt)
        rules = self._parse_rules_json(raw, urn)

        suite = QualitySuite(
            name=f"{table_name}_llm_suite",
            dataset_urn=urn,
            description=f"LLM-generated rules for {table_name}",
            rules=rules,
        )
        return suite

    def evaluate_results(self, suite_result: Any) -> str:
        """Generate a natural-language evaluation of a SuiteResult.

        Returns a formatted assessment string.
        """
        results_table = self._format_results_table(suite_result)
        prompt = _EVALUATION_PROMPT.format(
            urn=suite_result.dataset_urn,
            suite_name=suite_result.suite_name,
            results_table=results_table,
            score=suite_result.quality_score,
            total=suite_result.total_rules,
            passed=suite_result.passed,
            failed=suite_result.failed,
            blocking=suite_result.blocking_failures,
            warnings=suite_result.warnings,
        )
        return self._call(prompt)

    def detect_anomalies(
        self,
        data: Any,
        table_name: str = "dataset",
        baseline: Optional[Dict[str, Any]] = None,
        sample_rows: int = 20,
    ) -> Dict[str, Any]:
        """Detect anomalies in data using LLM reasoning.

        Returns a dict with 'anomalies' list and 'summary' string.
        Also returns suggested QualityRule objects for each anomaly.
        """
        import pandas as pd

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        stats = self._compute_stats(data)
        sample = data.tail(sample_rows).to_string(index=False)
        baseline_str = json.dumps(baseline, indent=2) if baseline else "No baseline available"

        prompt = _ANOMALY_DETECTION_PROMPT.format(
            table_name=table_name,
            stats=stats,
            sample_data=sample,
            sample_rows=min(sample_rows, len(data)),
            baseline=baseline_str,
        )

        raw = self._call(prompt)

        try:
            # Strip markdown code fences if present
            clean = raw.strip()
            if clean.startswith("```"):
                clean = "\n".join(clean.split("\n")[1:])
            if clean.endswith("```"):
                clean = "\n".join(clean.split("\n")[:-1])
            result = json.loads(clean)
        except json.JSONDecodeError:
            result = {"anomalies": [], "summary": raw, "parse_error": True}

        # Convert suggested_rule dicts to QualityRule objects
        for anomaly in result.get("anomalies", []):
            if "suggested_rule" in anomaly:
                try:
                    anomaly["suggested_rule_obj"] = self._dict_to_rule(
                        anomaly["suggested_rule"], dataset_urn=""
                    )
                except Exception:
                    pass

        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _describe_schema(self, df: "Any") -> str:
        lines = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            null_pct = df[col].isna().mean() * 100
            lines.append(f"  {col}: {dtype} (null: {null_pct:.1f}%)")
        return "\n".join(lines)

    def _compute_stats(self, df: "Any") -> str:
        import pandas as pd
        lines = []
        for col in df.columns:
            s = df[col]
            null_count = int(s.isna().sum())
            unique_count = int(s.nunique())
            line = f"  {col}: nulls={null_count}, unique={unique_count}"
            if pd.api.types.is_numeric_dtype(s):
                line += f", min={s.min():.4g}, max={s.max():.4g}, mean={s.mean():.4g}, std={s.std():.4g}"
            elif pd.api.types.is_string_dtype(s) or pd.api.types.is_object_dtype(s):
                top = s.value_counts().head(5).to_dict()
                line += f", top_values={top}"
            lines.append(line)
        return "\n".join(lines)

    def _format_results_table(self, suite_result: Any) -> str:
        lines = ["Rule Name | Type | Column | Status | Severity | Details"]
        lines.append("-" * 80)
        for r in suite_result.results:
            status = "PASS" if r.passed else ("WARN" if r.severity == Severity.WARNING else "FAIL")
            detail = r.error_message or str(r.metrics)[:60]
            lines.append(f"{r.rule_name} | {r.rule_type.value} | {r.column or '-'} | {status} | {r.severity.value} | {detail}")
        return "\n".join(lines)

    def _parse_rules_json(self, raw: str, dataset_urn: str) -> List[QualityRule]:
        """Parse LLM JSON output into QualityRule objects."""
        # Strip markdown code fences
        clean = raw.strip()
        if clean.startswith("```"):
            lines = clean.split("\n")
            clean = "\n".join(lines[1:] if lines[0].startswith("```") else lines)
        if clean.endswith("```"):
            clean = "\n".join(clean.split("\n")[:-1])

        try:
            rule_dicts = json.loads(clean)
        except json.JSONDecodeError as e:
            raise ValueError(f"LLM returned invalid JSON: {e}\nRaw output:\n{raw[:500]}")

        rules = []
        for d in rule_dicts:
            try:
                rules.append(self._dict_to_rule(d, dataset_urn))
            except Exception as e:
                # Skip malformed rules but don't fail entirely
                pass
        return rules

    def _dict_to_rule(self, d: Dict[str, Any], dataset_urn: str) -> QualityRule:
        """Convert a dict (from LLM JSON) to a QualityRule."""
        return QualityRule(
            name=d["name"],
            rule_type=RuleType(d["rule_type"]),
            column=d.get("column"),
            severity=Severity(d.get("severity", "blocking")),
            params=d.get("params", {}),
            dataset_urn=dataset_urn,
        )


# ---------------------------------------------------------------------------
# Additional prompts for new capabilities
# ---------------------------------------------------------------------------

_DOMAIN_AWARE_PROMPT = textwrap.dedent("""
You are a senior data quality engineer with deep expertise in {domain}.

## Domain Context
{domain_context}

## Dataset Information
- Dataset URN: {urn}
- Table name: {table_name}
- Description: {description}

## Schema (column name → data type)
{schema}

## Sample Data (first {sample_rows} rows)
{sample_data}

## Column Statistics
{stats}

## Domain-Specific Instructions
Given that this is a {domain} dataset, apply domain knowledge to generate rules that go
beyond generic checks. Consider:

{domain_rules_guidance}

For each rule, assign severity:
- "blocking": critical rules that must pass for data to be usable
- "warning": advisory rules for monitoring

Return ONLY a valid JSON array with the same format as before. No markdown, no explanation.

[
  {{
    "name": "rule_name",
    "rule_type": "not_null|unique|min|max|min_length|max_length|regex|accepted_values|row_count_min|row_count_max|freshness|completeness|custom_sql",
    "column": "column_name_or_null",
    "severity": "blocking|warning",
    "params": {{}},
    "rationale": "Why this rule matters for {domain} data"
  }}
]
""").strip()

_RULE_EXPLANATION_PROMPT = textwrap.dedent("""
You are a senior data quality engineer. Explain the following data quality rules
in plain language that a business analyst or data consumer can understand.

## Dataset: {table_name}
## Dataset URN: {urn}

## Rules to Explain
{rules_list}

For each rule, provide:
1. **What it checks**: plain English description of what the rule validates
2. **Why it matters**: business impact if this rule fails
3. **What failure looks like**: concrete example of data that would fail this rule
4. **Severity justification**: why it is blocking or warning

Return a JSON array:
[
  {{
    "rule_name": "not_null_order_id",
    "what_it_checks": "Every row must have a non-null order_id value",
    "why_it_matters": "Without an order_id, the record cannot be linked to any order in the system, making it orphaned and unusable for reporting",
    "failure_example": "A row where order_id is NULL or missing",
    "severity_justification": "Blocking because downstream joins on order_id will silently drop these rows"
  }}
]
""").strip()

_INCREMENTAL_SUGGESTION_PROMPT = textwrap.dedent("""
You are a senior data quality engineer reviewing an existing set of data quality rules.
Your job is to identify gaps and suggest ADDITIONAL rules that are missing.

## Dataset: {table_name}
## Dataset URN: {urn}

## Schema (column name → data type)
{schema}

## Column Statistics
{stats}

## Sample Data
{sample_data}

## Existing Rules (already covered — do NOT suggest these again)
{existing_rules}

## Instructions
Analyse the schema, statistics, and sample data. Identify quality dimensions that the
existing rules do NOT cover, then suggest new rules to fill those gaps.

Focus on:
1. Columns with no rules at all
2. Missing cross-column consistency checks (e.g. end_date > start_date)
3. Missing business logic rules inferred from the data patterns
4. Missing statistical bounds for numeric columns not yet covered
5. Missing format checks for string columns not yet covered
6. Missing referential integrity hints

Return ONLY a JSON array of new rules (same format as before). If no gaps exist, return [].

[
  {{
    "name": "rule_name",
    "rule_type": "...",
    "column": "column_or_null",
    "severity": "blocking|warning",
    "params": {{}},
    "rationale": "Gap this fills: ..."
  }}
]
""").strip()

# Domain guidance library — injected into _DOMAIN_AWARE_PROMPT
_DOMAIN_GUIDANCE: dict = {
    "financial_transactions": textwrap.dedent("""
    - Amount fields must be non-negative (or explicitly allow negatives for refunds with a type flag)
    - Transaction IDs must be unique and non-null
    - Currency codes must follow ISO 4217 (3-letter uppercase: USD, EUR, GBP)
    - Timestamps must be in the past (no future-dated transactions)
    - Account numbers should match expected format patterns
    - Status fields should only contain known values (pending, completed, failed, reversed)
    - Large amounts (> 3 standard deviations) should be flagged as warnings
    - Merchant category codes (MCC) should be 4-digit numeric strings
    """),
    "healthcare": textwrap.dedent("""
    - Patient IDs must be unique and non-null
    - Date of birth must be in the past and result in a plausible age (0-130 years)
    - ICD codes must match known format patterns (e.g. A00-Z99 for ICD-10)
    - Vital signs must be within physiologically plausible ranges
    - Medication dosages must be positive numbers
    - Gender/sex fields should use standardised values
    - Timestamps for procedures must be after patient admission date
    """),
    "ecommerce": textwrap.dedent("""
    - Order IDs must be unique and non-null
    - Product SKUs must follow the expected format
    - Prices must be positive
    - Quantities must be positive integers
    - Email addresses must be valid format
    - Shipping addresses must have required fields (street, city, country)
    - Order status must be in known values (pending, processing, shipped, delivered, cancelled)
    - Discount percentages must be between 0 and 100
    """),
    "iot_telemetry": textwrap.dedent("""
    - Device IDs must be non-null and unique per reading
    - Timestamps must be recent (freshness check)
    - Sensor readings must be within physically plausible ranges for the sensor type
    - No duplicate readings for the same device at the same timestamp
    - Signal strength / battery level must be 0-100
    - Latitude must be -90 to 90, longitude -180 to 180
    """),
    "user_events": textwrap.dedent("""
    - User IDs must be non-null
    - Event types must be in the known event taxonomy
    - Session IDs must be non-null for session-scoped events
    - Timestamps must be in the past
    - Page URLs should be valid URL format
    - Duration values must be non-negative
    - Device type should be in known values (mobile, desktop, tablet)
    """),
    "generic": textwrap.dedent("""
    - Apply standard data quality checks appropriate for the column names and data types observed
    - Use statistical bounds (mean ± 3 std) for numeric columns as warning-level rules
    - Check string format consistency for columns that appear to have structured values
    - Ensure primary key candidates are unique and non-null
    """),
}


class LLMRuleAdvisor(LLMAdvisor):
    """Extended LLM advisor with interactive refinement, domain awareness,
    rule explanation, and incremental suggestion capabilities.

    Inherits all existing LLMAdvisor functionality unchanged.
    New methods are purely additive.
    """

    # ------------------------------------------------------------------
    # 1. Domain-context-aware rule generation
    # ------------------------------------------------------------------

    def generate_rules_with_domain(
        self,
        data: "Any",
        domain: str,
        urn: str = "",
        table_name: str = "dataset",
        description: str = "",
        sample_rows: int = 10,
        custom_domain_context: str = "",
    ) -> "QualitySuite":
        """Generate DQ rules with domain-specific business knowledge injected.

        Args:
            data: pandas DataFrame
            domain: one of 'financial_transactions', 'healthcare', 'ecommerce',
                    'iot_telemetry', 'user_events', 'generic' — or any free-text domain
            urn: dataset URN
            table_name: human-readable table name
            description: optional dataset description
            sample_rows: number of sample rows to include in the prompt
            custom_domain_context: optional free-text domain context that overrides
                                   the built-in domain guidance

        Returns:
            QualitySuite with domain-aware rules
        """
        import pandas as pd

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        schema = self._describe_schema(data)
        sample = data.head(sample_rows).to_string(index=False)
        stats = self._compute_stats(data)

        domain_guidance = custom_domain_context or _DOMAIN_GUIDANCE.get(
            domain.lower().replace(" ", "_"), _DOMAIN_GUIDANCE["generic"]
        )
        domain_context = f"This is a {domain} dataset. {description}" if description else f"This is a {domain} dataset."

        prompt = _DOMAIN_AWARE_PROMPT.format(
            domain=domain,
            domain_context=domain_context,
            urn=urn,
            table_name=table_name,
            description=description or "No description provided",
            schema=schema,
            sample_data=sample,
            sample_rows=min(sample_rows, len(data)),
            stats=stats,
            domain_rules_guidance=domain_guidance,
        )

        raw = self._call(prompt)
        rules = self._parse_rules_json(raw, urn)

        return QualitySuite(
            name=f"{table_name}_{domain}_suite",
            dataset_urn=urn,
            description=f"Domain-aware ({domain}) LLM-generated rules for {table_name}",
            rules=rules,
        )

    # ------------------------------------------------------------------
    # 2. Rule explanation
    # ------------------------------------------------------------------

    def explain_rules(
        self,
        suite: "QualitySuite",
        table_name: str = "",
    ) -> List[Dict[str, Any]]:
        """Ask the LLM to explain each rule in a suite in plain language.

        Args:
            suite: QualitySuite whose rules to explain
            table_name: optional human-readable table name

        Returns:
            List of dicts, one per rule, with keys:
              rule_name, what_it_checks, why_it_matters,
              failure_example, severity_justification
        """
        rules_list = "\n".join(
            f"  {i+1}. name={r.name}, type={r.rule_type.value}, "
            f"column={r.column or 'table-level'}, severity={r.severity.value}, params={r.params}"
            for i, r in enumerate(suite.rules)
        )

        prompt = _RULE_EXPLANATION_PROMPT.format(
            table_name=table_name or suite.name,
            urn=suite.dataset_urn,
            rules_list=rules_list,
        )

        raw = self._call(prompt)

        # Parse JSON response
        clean = raw.strip()
        if clean.startswith("```"):
            clean = "\n".join(clean.split("\n")[1:])
        if clean.endswith("```"):
            clean = "\n".join(clean.split("\n")[:-1])

        try:
            return json.loads(clean)
        except json.JSONDecodeError:
            # Return raw text wrapped in a single explanation if JSON fails
            return [{"rule_name": "all_rules", "what_it_checks": raw,
                     "why_it_matters": "", "failure_example": "", "severity_justification": ""}]

    # ------------------------------------------------------------------
    # 3. Incremental rule suggestion
    # ------------------------------------------------------------------

    def suggest_additional_rules(
        self,
        data: "Any",
        existing_suite: "QualitySuite",
        table_name: str = "",
        sample_rows: int = 10,
    ) -> "QualitySuite":
        """Given an existing suite, suggest additional rules that are missing.

        Args:
            data: pandas DataFrame
            existing_suite: the suite already in place
            table_name: optional human-readable table name
            sample_rows: sample rows for the prompt

        Returns:
            A new QualitySuite containing ONLY the suggested additional rules
            (does not modify the existing suite)
        """
        import pandas as pd

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        schema = self._describe_schema(data)
        sample = data.head(sample_rows).to_string(index=False)
        stats = self._compute_stats(data)

        existing_rules_str = "\n".join(
            f"  - {r.name} ({r.rule_type.value} on {r.column or 'table'})"
            for r in existing_suite.rules
        )

        prompt = _INCREMENTAL_SUGGESTION_PROMPT.format(
            table_name=table_name or existing_suite.name,
            urn=existing_suite.dataset_urn,
            schema=schema,
            stats=stats,
            sample_data=sample,
            existing_rules=existing_rules_str or "  (none)",
        )

        raw = self._call(prompt)
        new_rules = self._parse_rules_json(raw, existing_suite.dataset_urn)

        return QualitySuite(
            name=f"{existing_suite.name}_suggestions",
            dataset_urn=existing_suite.dataset_urn,
            description=f"Incremental suggestions for {existing_suite.name}",
            rules=new_rules,
        )

    # ------------------------------------------------------------------
    # 4. Interactive rule refinement (returns structured data for CLI)
    # ------------------------------------------------------------------

    def prepare_interactive_review(
        self,
        suite: "QualitySuite",
    ) -> List[Dict[str, Any]]:
        """Prepare rules for interactive review.

        Returns a list of dicts, each containing the rule and its rationale,
        ready to be presented to the user for accept/reject/edit decisions.

        Args:
            suite: QualitySuite to review

        Returns:
            List of review items:
              {rule: QualityRule, rationale: str, index: int}
        """
        return [
            {
                "index": i,
                "rule": rule,
                "rationale": getattr(rule, "_rationale", ""),
                "display": (
                    f"[{i+1}] {rule.name}\n"
                    f"    type={rule.rule_type.value}  column={rule.column or 'table-level'}"
                    f"  severity={rule.severity.value}"
                    + (f"  params={rule.params}" if rule.params else "")
                ),
            }
            for i, rule in enumerate(suite.rules)
        ]

    def generate_rules_with_rationale(
        self,
        data: "Any",
        urn: str = "",
        table_name: str = "dataset",
        description: str = "",
        sample_rows: int = 10,
    ) -> List[Dict[str, Any]]:
        """Like generate_rules() but returns raw dicts that include the LLM rationale.

        Used by the interactive review CLI so the user can see the LLM's reasoning
        before deciding to accept/reject each rule.

        Returns:
            List of dicts with keys: name, rule_type, column, severity, params, rationale
        """
        import pandas as pd

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        schema = self._describe_schema(data)
        sample = data.head(sample_rows).to_string(index=False)
        stats = self._compute_stats(data)

        prompt = _SCHEMA_ANALYSIS_PROMPT.format(
            urn=urn,
            table_name=table_name,
            description=description or "No description provided",
            schema=schema,
            sample_data=sample,
            sample_rows=min(sample_rows, len(data)),
            stats=stats,
        )

        raw = self._call(prompt)

        # Parse and return raw dicts (preserving rationale field)
        clean = raw.strip()
        if clean.startswith("```"):
            clean = "\n".join(clean.split("\n")[1:])
        if clean.endswith("```"):
            clean = "\n".join(clean.split("\n")[:-1])

        try:
            return json.loads(clean)
        except json.JSONDecodeError as e:
            raise ValueError(f"LLM returned invalid JSON: {e}\nRaw:\n{raw[:500]}")
