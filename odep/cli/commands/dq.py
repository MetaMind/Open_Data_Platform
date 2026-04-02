"""DQ command group — data quality engine CLI commands.

Commands:
  odep dq generate   — LLM generates rules from a CSV/Parquet file or SQL query
  odep dq run        — run a suite file against data
  odep dq evaluate   — LLM evaluates check results and gives recommendations
  odep dq anomalies  — LLM detects anomalies in data
  odep dq export     — export rules from a suite file to YAML/JSON
  odep dq show       — print a suite file in human-readable form
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click


@click.group()
def dq():
    """Data Quality engine — generate, run, and evaluate quality rules."""
    pass


# ---------------------------------------------------------------------------
# odep dq generate
# ---------------------------------------------------------------------------

@dq.command()
@click.argument("source")
@click.option("--urn", default="", help="Dataset URN (e.g. urn:li:dataset:(duckdb,orders,prod))")
@click.option("--name", default="", help="Table/dataset name for the prompt")
@click.option("--description", default="", help="Optional dataset description")
@click.option("--output", "-o", default="", help="Output file path (.yaml or .json). Prints to stdout if omitted.")
@click.option("--sample-rows", default=10, type=int, show_default=True, help="Sample rows to include in LLM prompt")
@click.option("--provider", default="", help="LLM provider override (openai|anthropic|ollama)")
@click.option("--model", default="", help="LLM model override (e.g. gpt-4o, claude-3-5-sonnet-20241022)")
def generate(source: str, urn: str, name: str, description: str, output: str,
             sample_rows: int, provider: str, model: str) -> None:
    """Generate DQ rules from a data file or SQL query using an LLM.

    SOURCE can be:
      - A CSV file path:        orders.csv
      - A Parquet file path:    orders.parquet
      - A SQL query:            "SELECT * FROM orders LIMIT 100"
      - A DuckDB table name:    orders

    Examples:
      odep dq generate orders.csv --urn urn:li:dataset:(duckdb,orders,prod) -o rules/orders.yaml
      odep dq generate "SELECT * FROM tpch.tiny.orders" --name orders -o rules/orders.yaml
    """
    try:
        import pandas as pd
        from odep.dq.llm_advisor import LLMAdvisor
        from odep.dq.serializer import save_suite, suite_to_yaml_str

        df = _load_data(source)
        table_name = name or (Path(source).stem if Path(source).exists() else (name or "dataset"))

        click.echo(f"🔍 Analysing {len(df)} rows × {len(df.columns)} columns...")
        click.echo(f"🤖 Generating DQ rules with LLM...")

        advisor = LLMAdvisor.from_config()
        if provider:
            advisor.provider = provider
        if model:
            advisor.model = model

        suite = advisor.generate_rules(
            data=df,
            urn=urn,
            table_name=table_name,
            description=description,
            sample_rows=sample_rows,
        )

        click.echo(f"✅ Generated {len(suite.rules)} rules for '{suite.name}'")

        if output:
            save_suite(suite, output)
            click.echo(f"💾 Saved to {output}")
        else:
            click.echo("\n" + suite_to_yaml_str(suite))

    except Exception as e:
        click.echo(f"❌ Generate failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# odep dq run
# ---------------------------------------------------------------------------

@dq.command("run")
@click.argument("suite_file")
@click.argument("source")
@click.option("--persist", is_flag=True, default=False, help="Persist results to metadata catalog")
@click.option("--no-fail", is_flag=True, default=False, help="Exit 0 even if blocking rules fail")
@click.option("--output", "-o", default="", help="Save results to a JSON file")
@click.option("--llm-evaluate", is_flag=True, default=False, help="Use LLM to evaluate results after running")
@click.option("--engine", default="native",
              type=click.Choice(["native", "spark", "trino"]), show_default=True,
              help="Execution engine for rule evaluation")
@click.option("--trino-table", default="", help="Fully-qualified Trino table (catalog.schema.table) for trino engine")
@click.option("--trino-host", default="localhost", show_default=True)
@click.option("--trino-port", default=8082, type=int, show_default=True)
def run_suite(suite_file: str, source: str, persist: bool, no_fail: bool,
              output: str, llm_evaluate: bool, engine: str,
              trino_table: str, trino_host: str, trino_port: int) -> None:
    """Run a DQ suite file against a data source.

    SUITE_FILE: path to a .yaml or .json suite file
    SOURCE: CSV, Parquet, ORC, Avro, JSON, JSONL, Delta file — or a SQL query

    Supported engines:
      native  — pandas + DuckDB (default, no cluster needed)
      spark   — PySpark distributed evaluation (requires pyspark)
      trino   — server-side SQL on Trino (no data transfer, requires --trino-table)

    Examples:
      odep dq run rules/orders.yaml orders.parquet
      odep dq run rules/orders.yaml orders.avro --engine spark
      odep dq run rules/orders.yaml "" --engine trino --trino-table tpch.tiny.orders
      odep dq run rules/orders.yaml "SELECT * FROM orders" --persist --llm-evaluate
    """
    try:
        from odep.dq.runner import run_quality_suite
        from odep.dq.serializer import load_suite
        from odep.exceptions import QualityGateFailure

        suite = load_suite(suite_file)

        # Select engine
        if engine == "trino":
            from odep.dq.trino_engine import TrinoQualityEngine
            table = trino_table or source
            dq_engine = TrinoQualityEngine(table=table, host=trino_host, port=trino_port)
            click.echo(f"🧪 Running suite '{suite.name}' ({len(suite.rules)} rules) on Trino table '{table}'...")
            df = None
        elif engine == "spark":
            from odep.dq.spark_engine import SparkQualityEngine
            from odep.dq.reader import read_data_spark
            click.echo(f"🧪 Running suite '{suite.name}' ({len(suite.rules)} rules) on Spark...")
            df = read_data_spark(source)
            dq_engine = SparkQualityEngine()
        else:
            from odep.dq.engine import NativeQualityEngine
            df = _load_data(source)
            click.echo(f"🧪 Running suite '{suite.name}' ({len(suite.rules)} rules) against {len(df)} rows...")
            dq_engine = NativeQualityEngine()

        metadata_adapter = None
        if persist:
            from odep.adapters.openmeta.adapter import OpenMetaAdapter
            from odep.config import MetadataConfig
            metadata_adapter = OpenMetaAdapter(MetadataConfig())

        result = None
        try:
            result = run_quality_suite(
                suite, df,
                metadata_adapter=metadata_adapter,
                engine=dq_engine,
                raise_on_blocking=not no_fail,
            )
        except QualityGateFailure as e:
            _print_results(result)
            click.echo(f"\n❌ Quality gate failed: {e}")
            if output and result:
                _save_results(result, output)
            sys.exit(1)

        _print_results(result)

        if output:
            _save_results(result, output)
            click.echo(f"\n💾 Results saved to {output}")

        if llm_evaluate:
            click.echo("\n🤖 LLM evaluation:")
            from odep.dq.llm_advisor import LLMAdvisor
            click.echo(LLMAdvisor.from_config().evaluate_results(result))

        if result.has_blocking_failures and not no_fail:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Run failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# odep dq evaluate
# ---------------------------------------------------------------------------

@dq.command()
@click.argument("results_file")
def evaluate(results_file: str) -> None:
    """Use LLM to evaluate a saved results JSON file and give recommendations.

    RESULTS_FILE: path to a results JSON file saved by 'odep dq run --output'

    Example:
      odep dq run rules/orders.yaml orders.csv -o results.json
      odep dq evaluate results.json
    """
    try:
        import json as json_mod
        from odep.dq.llm_advisor import LLMAdvisor
        from odep.dq.models import CheckResult, RuleType, Severity, SuiteResult

        with open(results_file) as f:
            data = json_mod.load(f)

        # Reconstruct SuiteResult from JSON
        results = [
            CheckResult(
                rule_name=r["rule_name"],
                rule_type=RuleType(r["rule_type"]),
                column=r.get("column"),
                passed=r["passed"],
                severity=Severity(r["severity"]),
                dataset_urn=r.get("dataset_urn", ""),
                metrics=r.get("metrics", {}),
                error_message=r.get("error_message"),
            )
            for r in data.get("results", [])
        ]
        suite_result = SuiteResult(
            suite_name=data["suite_name"],
            dataset_urn=data["dataset_urn"],
            total_rules=data["total_rules"],
            passed=data["passed"],
            failed=data["failed"],
            warnings=data["warnings"],
            blocking_failures=data["blocking_failures"],
            results=results,
        )

        click.echo(f"🤖 LLM evaluation of '{suite_result.suite_name}'...")
        advisor = LLMAdvisor.from_config()
        evaluation = advisor.evaluate_results(suite_result)
        click.echo(evaluation)

    except Exception as e:
        click.echo(f"❌ Evaluate failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# odep dq anomalies
# ---------------------------------------------------------------------------

@dq.command()
@click.argument("source")
@click.option("--name", default="dataset", help="Table/dataset name")
@click.option("--baseline", default="", help="Path to a JSON baseline stats file")
@click.option("--sample-rows", default=20, type=int, show_default=True)
@click.option("--output", "-o", default="", help="Save anomaly report to JSON file")
@click.option("--export-rules", default="", help="Export suggested rules to a YAML file")
def anomalies(source: str, name: str, baseline: str, sample_rows: int,
              output: str, export_rules: str) -> None:
    """Use LLM to detect anomalies in data.

    SOURCE: CSV file, Parquet file, or SQL query

    Examples:
      odep dq anomalies orders.csv --name orders
      odep dq anomalies orders.csv --export-rules rules/anomaly_rules.yaml
    """
    try:
        import json as json_mod
        from odep.dq.llm_advisor import LLMAdvisor
        from odep.dq.models import QualitySuite
        from odep.dq.serializer import save_suite

        df = _load_data(source)
        baseline_data = None
        if baseline:
            with open(baseline) as f:
                baseline_data = json_mod.load(f)

        click.echo(f"🔍 Detecting anomalies in {len(df)} rows × {len(df.columns)} columns...")
        advisor = LLMAdvisor.from_config()
        result = advisor.detect_anomalies(df, table_name=name, baseline=baseline_data, sample_rows=sample_rows)

        anomaly_list = result.get("anomalies", [])
        summary = result.get("summary", "")

        click.echo(f"\n📊 Summary: {summary}")
        click.echo(f"\n🚨 Found {len(anomaly_list)} anomalies:\n")

        for i, a in enumerate(anomaly_list, 1):
            severity_icon = "🔴" if a.get("severity") == "blocking" else "🟡"
            click.echo(f"{severity_icon} [{i}] {a.get('type', 'anomaly')} — column: {a.get('column', '-')}")
            click.echo(f"    {a.get('description', '')}")
            if "suggested_rule" in a:
                r = a["suggested_rule"]
                click.echo(f"    → Suggested rule: {r.get('name')} ({r.get('rule_type')}) — {r.get('rationale', '')}")
            click.echo()

        if output:
            with open(output, "w") as f:
                json_mod.dump(result, f, indent=2, default=str)
            click.echo(f"💾 Anomaly report saved to {output}")

        if export_rules:
            suggested_rules = []
            for a in anomaly_list:
                if "suggested_rule_obj" in a:
                    suggested_rules.append(a["suggested_rule_obj"])
            if suggested_rules:
                suite = QualitySuite(
                    name=f"{name}_anomaly_rules",
                    dataset_urn="",
                    description=f"Anomaly-detected rules for {name}",
                    rules=suggested_rules,
                )
                save_suite(suite, export_rules)
                click.echo(f"💾 {len(suggested_rules)} suggested rules exported to {export_rules}")

    except Exception as e:
        click.echo(f"❌ Anomaly detection failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# odep dq export
# ---------------------------------------------------------------------------

@dq.command()
@click.argument("suite_file")
@click.option("--format", "fmt", default="yaml", type=click.Choice(["yaml", "json"]), show_default=True)
@click.option("--output", "-o", default="", help="Output file path. Prints to stdout if omitted.")
def export(suite_file: str, fmt: str, output: str) -> None:
    """Export a suite file to YAML or JSON format.

    Example:
      odep dq export rules/orders.yaml --format json -o rules/orders.json
      odep dq export rules/orders.yaml          # prints YAML to stdout
    """
    try:
        from odep.dq.serializer import load_suite, save_suite, suite_to_json_str, suite_to_yaml_str

        suite = load_suite(suite_file)

        if output:
            save_suite(suite, output)
            click.echo(f"✅ Exported {len(suite.rules)} rules to {output}")
        else:
            if fmt == "json":
                click.echo(suite_to_json_str(suite))
            else:
                click.echo(suite_to_yaml_str(suite))

    except Exception as e:
        click.echo(f"❌ Export failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# odep dq show
# ---------------------------------------------------------------------------

@dq.command()
@click.argument("suite_file")
def show(suite_file: str) -> None:
    """Print a suite file in human-readable table format.

    Example:
      odep dq show rules/orders.yaml
    """
    try:
        from odep.dq.serializer import load_suite

        suite = load_suite(suite_file)
        click.echo(f"\nSuite: {suite.name}")
        click.echo(f"URN:   {suite.dataset_urn}")
        click.echo(f"Rules: {len(suite.rules)}\n")
        click.echo(f"{'#':<4} {'Name':<35} {'Type':<20} {'Column':<20} {'Severity':<10} {'Params'}")
        click.echo("-" * 110)
        for i, r in enumerate(suite.rules, 1):
            params_str = ", ".join(f"{k}={v}" for k, v in r.params.items()) if r.params else ""
            click.echo(f"{i:<4} {r.name:<35} {r.rule_type.value:<20} {(r.column or '-'):<20} {r.severity.value:<10} {params_str}")

    except Exception as e:
        click.echo(f"❌ Show failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_data(source: str, engine: str = "native") -> "Any":
    """Load data from any supported source into a pandas DataFrame.

    Supports: CSV, Parquet, ORC, Avro, JSON, JSONL, TSV, Excel, Feather, Delta,
              SQL queries (via DuckDB), S3/GCS/Azure remote paths.
    """
    from odep.dq.reader import read_data
    return read_data(source)


def _print_results(result: "Any") -> None:
    """Print a SuiteResult to the terminal."""
    if result is None:
        return
    from odep.dq.models import Severity

    score_icon = "✅" if result.quality_score >= 90 else ("⚠️" if result.quality_score >= 70 else "❌")
    click.echo(f"\n{score_icon} Quality Score: {result.quality_score:.1f}%  "
               f"({result.passed}/{result.total_rules} passed, "
               f"{result.blocking_failures} blocking failures, "
               f"{result.warnings} warnings)\n")

    for r in result.results:
        if r.passed:
            icon = "✅"
        elif r.severity == Severity.WARNING:
            icon = "⚠️ "
        else:
            icon = "❌"
        col = f"[{r.column}]" if r.column else ""
        detail = r.error_message or ""
        click.echo(f"  {icon} {r.rule_name} {col}  {detail}")


def _save_results(result: "Any", path: str) -> None:
    """Save a SuiteResult to a JSON file."""
    import json as json_mod

    data = {
        "suite_name": result.suite_name,
        "dataset_urn": result.dataset_urn,
        "total_rules": result.total_rules,
        "passed": result.passed,
        "failed": result.failed,
        "warnings": result.warnings,
        "blocking_failures": result.blocking_failures,
        "quality_score": result.quality_score,
        "evaluated_at": result.evaluated_at.isoformat(),
        "results": [
            {
                "rule_name": r.rule_name,
                "rule_type": r.rule_type.value,
                "column": r.column,
                "passed": r.passed,
                "severity": r.severity.value,
                "dataset_urn": r.dataset_urn,
                "metrics": r.metrics,
                "error_message": r.error_message,
                "evaluated_at": r.evaluated_at.isoformat(),
            }
            for r in result.results
        ],
    }
    with open(path, "w") as f:
        json_mod.dump(data, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# odep dq review  — interactive rule refinement
# ---------------------------------------------------------------------------

@dq.command()
@click.argument("source")
@click.option("--urn", default="", help="Dataset URN")
@click.option("--name", default="", help="Table name")
@click.option("--description", default="", help="Dataset description")
@click.option("--output", "-o", required=True, help="Output .yaml or .json file for accepted rules")
@click.option("--sample-rows", default=10, type=int, show_default=True)
@click.option("--provider", default="", help="LLM provider override")
@click.option("--model", default="", help="LLM model override")
def review(source: str, urn: str, name: str, description: str, output: str,
           sample_rows: int, provider: str, model: str) -> None:
    """Generate rules with LLM, then interactively accept/reject/edit each one.

    For each generated rule you can:
      [a] Accept as-is
      [r] Reject (skip)
      [e] Edit severity (blocking/warning)
      [s] Skip remaining and save what's accepted so far

    Example:
      odep dq review orders.csv --urn urn:li:dataset:(duckdb,orders,prod) -o rules/orders.yaml
    """
    try:
        from odep.dq.llm_advisor import LLMRuleAdvisor
        from odep.dq.models import QualityRule, QualitySuite, RuleType, Severity
        from odep.dq.serializer import save_suite

        df = _load_data(source)
        table_name = name or (Path(source).stem if Path(source).exists() else "dataset")

        click.echo(f"🔍 Analysing {len(df)} rows × {len(df.columns)} columns...")
        click.echo("🤖 Generating rules with LLM (this may take a moment)...\n")

        advisor = LLMRuleAdvisor.from_config()
        if provider:
            advisor.provider = provider
        if model:
            advisor.model = model

        rule_dicts = advisor.generate_rules_with_rationale(
            data=df, urn=urn, table_name=table_name,
            description=description, sample_rows=sample_rows,
        )

        click.echo(f"✅ LLM generated {len(rule_dicts)} candidate rules.\n")
        click.echo("─" * 60)
        click.echo("Review each rule: [a]ccept  [r]eject  [e]dit severity  [s]kip rest")
        click.echo("─" * 60 + "\n")

        accepted_rules = []

        for i, rd in enumerate(rule_dicts):
            # Display rule
            click.echo(f"[{i+1}/{len(rule_dicts)}] {rd.get('name', 'unnamed')}")
            click.echo(f"  Type:     {rd.get('rule_type', '?')}")
            click.echo(f"  Column:   {rd.get('column') or 'table-level'}")
            click.echo(f"  Severity: {rd.get('severity', 'blocking')}")
            if rd.get("params"):
                click.echo(f"  Params:   {rd['params']}")
            if rd.get("rationale"):
                click.echo(f"  Rationale: {rd['rationale']}")
            click.echo()

            while True:
                choice = click.prompt("  Action", default="a",
                                      type=click.Choice(["a", "r", "e", "s"], case_sensitive=False))
                if choice == "a":
                    try:
                        rule = QualityRule(
                            name=rd["name"],
                            rule_type=RuleType(rd["rule_type"]),
                            column=rd.get("column"),
                            severity=Severity(rd.get("severity", "blocking")),
                            params=rd.get("params") or {},
                            dataset_urn=urn,
                        )
                        accepted_rules.append(rule)
                        click.echo("  ✅ Accepted\n")
                    except Exception as ex:
                        click.echo(f"  ⚠️  Could not parse rule: {ex} — skipping\n")
                    break
                elif choice == "r":
                    click.echo("  ❌ Rejected\n")
                    break
                elif choice == "e":
                    new_sev = click.prompt("  New severity",
                                           type=click.Choice(["blocking", "warning"]))
                    rd["severity"] = new_sev
                    click.echo(f"  ✏️  Severity changed to {new_sev} — accepting\n")
                    try:
                        rule = QualityRule(
                            name=rd["name"],
                            rule_type=RuleType(rd["rule_type"]),
                            column=rd.get("column"),
                            severity=Severity(new_sev),
                            params=rd.get("params") or {},
                            dataset_urn=urn,
                        )
                        accepted_rules.append(rule)
                    except Exception as ex:
                        click.echo(f"  ⚠️  Could not parse rule: {ex} — skipping\n")
                    break
                elif choice == "s":
                    click.echo("  ⏭️  Skipping remaining rules\n")
                    break
            else:
                continue
            if choice == "s":
                break

        if not accepted_rules:
            click.echo("⚠️  No rules accepted. Nothing saved.")
            sys.exit(0)

        suite = QualitySuite(
            name=f"{table_name}_reviewed_suite",
            dataset_urn=urn,
            description=f"Interactively reviewed rules for {table_name}",
            rules=accepted_rules,
        )
        save_suite(suite, output)
        click.echo(f"\n💾 Saved {len(accepted_rules)} accepted rules to {output}")

    except Exception as e:
        click.echo(f"❌ Review failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# odep dq domain-generate  — domain-context-aware rule generation
# ---------------------------------------------------------------------------

@dq.command("domain-generate")
@click.argument("source")
@click.argument("domain")
@click.option("--urn", default="", help="Dataset URN")
@click.option("--name", default="", help="Table name")
@click.option("--description", default="", help="Dataset description")
@click.option("--output", "-o", default="", help="Output .yaml or .json file")
@click.option("--sample-rows", default=10, type=int, show_default=True)
@click.option("--context", default="", help="Custom domain context (overrides built-in guidance)")
@click.option("--provider", default="", help="LLM provider override")
@click.option("--model", default="", help="LLM model override")
def domain_generate(source: str, domain: str, urn: str, name: str, description: str,
                    output: str, sample_rows: int, context: str,
                    provider: str, model: str) -> None:
    """Generate DQ rules with domain-specific business knowledge.

    DOMAIN: financial_transactions | healthcare | ecommerce | iot_telemetry |
            user_events | generic | (any free-text domain)

    Built-in domains inject specialised guidance (e.g. ISO 4217 currency codes
    for financial data, physiological ranges for healthcare).

    Examples:
      odep dq domain-generate orders.csv financial_transactions -o rules/orders.yaml
      odep dq domain-generate patients.parquet healthcare --urn urn:... -o rules/patients.yaml
      odep dq domain-generate events.json user_events -o rules/events.yaml
      odep dq domain-generate data.csv "supply chain logistics" --context "Focus on lead times and inventory levels" -o rules/supply.yaml
    """
    try:
        from odep.dq.llm_advisor import LLMRuleAdvisor, _DOMAIN_GUIDANCE
        from odep.dq.serializer import save_suite, suite_to_yaml_str

        df = _load_data(source)
        table_name = name or (Path(source).stem if Path(source).exists() else "dataset")

        # Show available built-in domains if domain not recognised
        known = list(_DOMAIN_GUIDANCE.keys())
        if domain.lower().replace(" ", "_") not in known:
            click.echo(f"ℹ️  '{domain}' is not a built-in domain. Using free-text domain context.")
            click.echo(f"   Built-in domains: {', '.join(k for k in known if k != 'generic')}")

        click.echo(f"🔍 Analysing {len(df)} rows × {len(df.columns)} columns...")
        click.echo(f"🤖 Generating domain-aware rules for '{domain}'...")

        advisor = LLMRuleAdvisor.from_config()
        if provider:
            advisor.provider = provider
        if model:
            advisor.model = model

        suite = advisor.generate_rules_with_domain(
            data=df,
            domain=domain,
            urn=urn,
            table_name=table_name,
            description=description,
            sample_rows=sample_rows,
            custom_domain_context=context,
        )

        click.echo(f"✅ Generated {len(suite.rules)} domain-aware rules for '{suite.name}'")

        if output:
            save_suite(suite, output)
            click.echo(f"💾 Saved to {output}")
        else:
            click.echo("\n" + suite_to_yaml_str(suite))

    except Exception as e:
        click.echo(f"❌ Domain generate failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# odep dq explain  — explain why each rule in a suite matters
# ---------------------------------------------------------------------------

@dq.command()
@click.argument("suite_file")
@click.option("--name", default="", help="Table name for context")
@click.option("--output", "-o", default="", help="Save explanations to JSON file")
@click.option("--provider", default="", help="LLM provider override")
@click.option("--model", default="", help="LLM model override")
def explain(suite_file: str, name: str, output: str, provider: str, model: str) -> None:
    """Ask the LLM to explain why each rule in a suite matters.

    Produces plain-language explanations covering:
      - What the rule checks
      - Why it matters (business impact)
      - What a failure looks like (concrete example)
      - Why the severity is blocking or warning

    Examples:
      odep dq explain rules/orders.yaml
      odep dq explain rules/orders.yaml --name orders -o explanations.json
    """
    try:
        import json as json_mod
        from odep.dq.llm_advisor import LLMRuleAdvisor
        from odep.dq.serializer import load_suite

        suite = load_suite(suite_file)
        table_name = name or suite.name

        click.echo(f"🤖 Asking LLM to explain {len(suite.rules)} rules in '{suite.name}'...\n")

        advisor = LLMRuleAdvisor.from_config()
        if provider:
            advisor.provider = provider
        if model:
            advisor.model = model

        explanations = advisor.explain_rules(suite, table_name=table_name)

        # Print formatted output
        click.echo(f"Suite: {suite.name}  |  URN: {suite.dataset_urn}\n")
        click.echo("─" * 70)

        for exp in explanations:
            rule_name = exp.get("rule_name", "?")
            click.echo(f"\n📋 {rule_name}")
            click.echo(f"  What it checks:  {exp.get('what_it_checks', '')}")
            click.echo(f"  Why it matters:  {exp.get('why_it_matters', '')}")
            click.echo(f"  Failure example: {exp.get('failure_example', '')}")
            click.echo(f"  Severity reason: {exp.get('severity_justification', '')}")

        if output:
            with open(output, "w") as f:
                json_mod.dump(explanations, f, indent=2)
            click.echo(f"\n💾 Explanations saved to {output}")

    except Exception as e:
        click.echo(f"❌ Explain failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# odep dq suggest  — incremental rule suggestion for an existing suite
# ---------------------------------------------------------------------------

@dq.command()
@click.argument("suite_file")
@click.argument("source")
@click.option("--name", default="", help="Table name for context")
@click.option("--output", "-o", default="", help="Save suggested rules to .yaml or .json")
@click.option("--merge", is_flag=True, default=False,
              help="Merge suggestions into the existing suite file")
@click.option("--sample-rows", default=10, type=int, show_default=True)
@click.option("--provider", default="", help="LLM provider override")
@click.option("--model", default="", help="LLM model override")
def suggest(suite_file: str, source: str, name: str, output: str, merge: bool,
            sample_rows: int, provider: str, model: str) -> None:
    """Suggest additional rules for an existing suite by analysing data gaps.

    The LLM looks at your existing rules and the data, then suggests rules
    for columns or quality dimensions not yet covered.

    Examples:
      odep dq suggest rules/orders.yaml orders.csv
      odep dq suggest rules/orders.yaml orders.csv -o rules/orders_extra.yaml
      odep dq suggest rules/orders.yaml orders.csv --merge   # adds to existing file
    """
    try:
        from odep.dq.llm_advisor import LLMRuleAdvisor
        from odep.dq.serializer import load_suite, save_suite, suite_to_yaml_str

        existing_suite = load_suite(suite_file)
        df = _load_data(source)
        table_name = name or existing_suite.name

        click.echo(f"🔍 Existing suite has {len(existing_suite.rules)} rules.")
        click.echo(f"🤖 Asking LLM to suggest additional rules for '{table_name}'...\n")

        advisor = LLMRuleAdvisor.from_config()
        if provider:
            advisor.provider = provider
        if model:
            advisor.model = model

        suggestions = advisor.suggest_additional_rules(
            data=df,
            existing_suite=existing_suite,
            table_name=table_name,
            sample_rows=sample_rows,
        )

        if not suggestions.rules:
            click.echo("✅ No gaps found — your existing suite looks comprehensive!")
            sys.exit(0)

        click.echo(f"💡 LLM suggests {len(suggestions.rules)} additional rules:\n")
        for r in suggestions.rules:
            click.echo(f"  + {r.name}  ({r.rule_type.value} on {r.column or 'table'})  [{r.severity.value}]")
            if r.params:
                click.echo(f"    params: {r.params}")

        if merge:
            # Add suggestions to the existing suite and overwrite the file
            for r in suggestions.rules:
                existing_suite.rules.append(r)
            save_suite(existing_suite, suite_file)
            click.echo(f"\n✅ Merged {len(suggestions.rules)} rules into {suite_file}")
        elif output:
            save_suite(suggestions, output)
            click.echo(f"\n💾 Saved {len(suggestions.rules)} suggested rules to {output}")
        else:
            click.echo("\n" + suite_to_yaml_str(suggestions))

    except Exception as e:
        click.echo(f"❌ Suggest failed: {e}")
        sys.exit(1)
