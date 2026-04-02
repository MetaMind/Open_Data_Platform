"""Prompt construction utilities — extracted from generator.py.

File: metamind/core/nl_interface/prompt_builder.py
Feature: F28_nl_interface
"""
from __future__ import annotations

from typing import Any


def build_system_prompt() -> str:
    """Return the base system prompt for NL-to-SQL generation."""
    return (
        "You are a SQL query generator. Given a database schema and a natural "
        "language question, generate a valid SQL query. Return ONLY the SQL query "
        "without any explanation or markdown formatting. Use standard SQL syntax."
    )


def format_schema_context(tables: list[str], catalog: Any, tenant_id: str) -> str:
    """Build schema description string for LLM context.

    Args:
        tables: List of table names to include.
        catalog: MetadataCatalog instance.
        tenant_id: Tenant scoping for catalog look-ups.

    Returns:
        Multi-line schema description suitable for an LLM system message.
    """
    if not catalog:
        return "No schema information available."

    parts: list[str] = []
    for table_name in tables:
        table = catalog.get_table(tenant_id, table_name)
        if table is None:
            continue
        cols = ", ".join(
            f"{c.name} ({c.dtype}{'?' if c.nullable else ''})"
            for c in table.columns
        )
        parts.append(f"Table: {table_name} ({table.row_count} rows)\n  Columns: {cols}")

    return "\n\n".join(parts) if parts else "No tables found."


def assemble_few_shot_examples(examples: list[dict[str, str]], max_examples: int = 5) -> str:
    """Format verified NL→SQL pairs as a few-shot context block.

    Args:
        examples: List of dicts with 'nl' and 'sql' keys.
        max_examples: Maximum number of examples to include.

    Returns:
        Formatted string block for inclusion in an LLM message.
    """
    if not examples:
        return ""
    return "\n".join(
        f"Question: {ex['nl']}\nSQL: {ex['sql']}"
        for ex in examples[:max_examples]
    )


def build_prompt_messages(
    nl_text: str,
    schema_ctx: str,
    conversation: list[dict[str, str]],
    examples: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Assemble full LLM message list for NL-to-SQL generation.

    Args:
        nl_text: The user's natural language question.
        schema_ctx: Pre-formatted schema context string.
        conversation: Recent conversation turns as role/content dicts.
        examples: Verified NL→SQL examples for few-shot prompting.

    Returns:
        List of message dicts ready for an LLM chat API.
    """
    messages: list[dict[str, str]] = [{"role": "system", "content": build_system_prompt()}]

    few_shot = assemble_few_shot_examples(examples)
    if few_shot:
        messages.append({"role": "system", "content": f"Here are verified example queries:\n{few_shot}"})

    messages.append({"role": "system", "content": f"Available schema:\n{schema_ctx}"})

    for turn in conversation:
        messages.append({"role": turn["role"], "content": turn["content"]})

    messages.append({"role": "user", "content": nl_text})
    return messages
