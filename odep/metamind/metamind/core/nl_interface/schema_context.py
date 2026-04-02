"""F28 Schema context builder for NL-to-SQL translation.

Builds rich schema context that helps the NL parser and generator
understand available tables, columns, relationships, and naming conventions.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from metamind.core.metadata.catalog import MetadataCatalog

logger = logging.getLogger(__name__)

# Re-export for backward compatibility
__all__ = ["SchemaContextBuilder"]


@dataclass
class TableContext:
    """Rich context about a single table for NL understanding."""

    table_name: str
    schema_name: str
    description: str = ""
    column_names: list[str] = field(default_factory=list)
    column_types: dict[str, str] = field(default_factory=dict)
    primary_key: Optional[str] = None
    foreign_keys: list[dict[str, str]] = field(default_factory=list)
    row_count: int = 0
    natural_name: str = ""  # human-friendly name derived from table name
    synonyms: list[str] = field(default_factory=list)


@dataclass
class SchemaContext:
    """Complete schema context for NL understanding."""

    tenant_id: str
    tables: list[TableContext] = field(default_factory=list)
    relationships: list[dict[str, str]] = field(default_factory=list)
    name_index: dict[str, list[str]] = field(default_factory=dict)
    column_index: dict[str, list[str]] = field(default_factory=dict)


def _table_to_natural_name(table_name: str) -> str:
    """Convert snake_case table name to natural language."""
    name = table_name.replace("_", " ").replace("-", " ")
    name = re.sub(r"(tbl|tb|t)", "", name, flags=re.IGNORECASE)
    name = re.sub(r"(dim|fact|stg|staging|raw)", "", name, flags=re.IGNORECASE)
    return name.strip().title()


def _generate_synonyms(table_name: str) -> list[str]:
    """Generate common synonyms for a table name."""
    synonyms: list[str] = []
    natural = _table_to_natural_name(table_name).lower()

    synonyms.append(natural)
    synonyms.append(table_name.replace("_", ""))

    # Singular/plural variants
    if natural.endswith("s"):
        synonyms.append(natural[:-1])
    else:
        synonyms.append(natural + "s")

    # Common abbreviations
    abbrevs = {
        "customer": ["cust", "clients", "buyers"],
        "order": ["orders", "purchase", "transactions"],
        "product": ["products", "items", "skus"],
        "employee": ["employees", "staff", "workers"],
        "invoice": ["invoices", "bills", "billing"],
        "payment": ["payments", "transactions"],
        "user": ["users", "accounts", "members"],
    }

    for key, aliases in abbrevs.items():
        if key in natural:
            synonyms.extend(aliases)

    return list(set(synonyms))


class SchemaContextBuilder:
    """Builds schema context from the metadata catalog.

    Creates a searchable, NL-friendly representation of the database schema
    that helps with entity resolution and SQL generation.
    """

    def __init__(self, catalog: MetadataCatalog) -> None:
        """Initialize with metadata catalog.

        Args:
            catalog: MetadataCatalog for accessing schema information.
        """
        self._catalog = catalog
        self._cache: dict[str, SchemaContext] = {}

    def build(self, tenant_id: str, refresh: bool = False) -> SchemaContext:
        """Build or retrieve cached schema context for a tenant.

        Args:
            tenant_id: Tenant identifier.
            refresh: Force rebuild even if cached.

        Returns:
            SchemaContext with all tables, columns, and relationships.
        """
        if not refresh and tenant_id in self._cache:
            return self._cache[tenant_id]

        context = SchemaContext(tenant_id=tenant_id)

        # Load all tables for tenant
        tables = self._load_tables(tenant_id)
        context.tables = tables

        # Build name index (maps natural names and synonyms to table names)
        for tc in tables:
            all_names = [tc.table_name.lower(), tc.natural_name.lower()] + [
                s.lower() for s in tc.synonyms
            ]
            for name in all_names:
                if name not in context.name_index:
                    context.name_index[name] = []
                context.name_index[name].append(tc.table_name)

            # Build column index
            for col in tc.column_names:
                col_lower = col.lower()
                if col_lower not in context.column_index:
                    context.column_index[col_lower] = []
                context.column_index[col_lower].append(f"{tc.table_name}.{col}")

        # Detect relationships from foreign keys
        context.relationships = self._detect_relationships(tables)

        self._cache[tenant_id] = context
        logger.info(
            "Built schema context for tenant %s: %d tables, %d name entries",
            tenant_id, len(tables), len(context.name_index),
        )
        return context

    def resolve_name(
        self, name: str, tenant_id: str
    ) -> Optional[dict[str, Any]]:
        """Resolve a natural language name to a schema object.

        Args:
            name: Natural language name to resolve.
            tenant_id: Tenant identifier.

        Returns:
            Dict with type, name, confidence, and optional ambiguity info.
        """
        context = self.build(tenant_id)
        lower = name.lower().replace(" ", "_")

        # Exact table match
        for tc in context.tables:
            if tc.table_name.lower() == lower:
                return {"type": "table", "name": tc.table_name, "confidence": 1.0}

        # Synonym match
        if lower in context.name_index:
            matches = context.name_index[lower]
            if len(matches) == 1:
                return {"type": "table", "name": matches[0], "confidence": 0.9}
            return {
                "type": "table", "name": matches[0], "confidence": 0.6,
                "ambiguous": True, "candidates": matches,
            }

        # Column match
        if lower in context.column_index:
            cols = context.column_index[lower]
            if len(cols) == 1:
                return {"type": "column", "name": cols[0], "confidence": 0.9}
            return {
                "type": "column", "name": cols[0], "confidence": 0.5,
                "ambiguous": True, "candidates": cols,
            }

        # Fuzzy match
        best = self._fuzzy_match(lower, context)
        if best:
            return best

        return None

    def get_table_context(
        self, tenant_id: str, table_name: str
    ) -> Optional[TableContext]:
        """Get rich context for a specific table."""
        context = self.build(tenant_id)
        for tc in context.tables:
            if tc.table_name == table_name:
                return tc
        return None

    def format_for_prompt(self, tenant_id: str, relevant_tables: Optional[list[str]] = None) -> str:
        """Format schema context as a string for LLM prompts.

        Args:
            tenant_id: Tenant identifier.
            relevant_tables: Optional filter for specific tables.

        Returns:
            Formatted schema description string.
        """
        context = self.build(tenant_id)
        parts: list[str] = ["Available tables:"]

        for tc in context.tables:
            if relevant_tables and tc.table_name not in relevant_tables:
                continue

            cols = ", ".join(
                f"{c} ({tc.column_types.get(c, 'unknown')})" for c in tc.column_names[:15]
            )
            row_info = f", ~{tc.row_count:,} rows" if tc.row_count > 0 else ""
            parts.append(f"- {tc.schema_name}.{tc.table_name} ({cols}{row_info})")

        if context.relationships:
            parts.append("\nRelationships:")
            for rel in context.relationships[:10]:
                parts.append(
                    f"- {rel['from_table']}.{rel['from_column']} -> "
                    f"{rel['to_table']}.{rel['to_column']}"
                )

        return "\n".join(parts)

    def _load_tables(self, tenant_id: str) -> list[TableContext]:
        """Load all tables from catalog and build contexts."""
        tables: list[TableContext] = []

        try:
            all_tables = self._catalog.list_tables(tenant_id)
        except Exception:
            all_tables = []

        for table_meta in all_tables:
            tc = TableContext(
                table_name=table_meta.table_name,
                schema_name=getattr(table_meta, "schema_name", "public"),
                column_names=[c.column_name for c in table_meta.columns],
                column_types={
                    c.column_name: c.data_type.value if hasattr(c.data_type, "value") else str(c.data_type)
                    for c in table_meta.columns
                },
                row_count=getattr(table_meta, "row_count", 0),
                natural_name=_table_to_natural_name(table_meta.table_name),
                synonyms=_generate_synonyms(table_meta.table_name),
            )

            # Detect primary key
            for c in table_meta.columns:
                if c.column_name.lower() in ("id", f"{table_meta.table_name}_id"):
                    tc.primary_key = c.column_name
                    break

            tables.append(tc)

        return tables

    def _detect_relationships(self, tables: list[TableContext]) -> list[dict[str, str]]:
        """Detect FK relationships from column naming conventions."""
        relationships: list[dict[str, str]] = []
        table_names = {tc.table_name.lower(): tc for tc in tables}

        for tc in tables:
            for col in tc.column_names:
                if col.lower().endswith("_id") and col.lower() != "id":
                    ref_table = col.lower().replace("_id", "")
                    if ref_table in table_names or ref_table + "s" in table_names:
                        target = ref_table if ref_table in table_names else ref_table + "s"
                        relationships.append({
                            "from_table": tc.table_name,
                            "from_column": col,
                            "to_table": table_names[target].table_name,
                            "to_column": "id",
                        })

        return relationships

    def _fuzzy_match(
        self, name: str, context: SchemaContext
    ) -> Optional[dict[str, Any]]:
        """Fuzzy match a name against the schema."""
        best_score = 0.0
        best_match: Optional[dict[str, Any]] = None

        for key, tables in context.name_index.items():
            score = self._similarity(name, key)
            if score > best_score and score > 0.6:
                best_score = score
                best_match = {
                    "type": "table",
                    "name": tables[0],
                    "confidence": score * 0.8,
                }

        for key, cols in context.column_index.items():
            score = self._similarity(name, key)
            if score > best_score and score > 0.6:
                best_score = score
                best_match = {
                    "type": "column",
                    "name": cols[0],
                    "confidence": score * 0.7,
                }

        return best_match

    @staticmethod
    def _similarity(a: str, b: str) -> float:
        """Simple character-level similarity (Jaccard on character bigrams)."""
        if a == b:
            return 1.0
        if not a or not b:
            return 0.0

        bigrams_a = {a[i:i+2] for i in range(len(a) - 1)}
        bigrams_b = {b[i:i+2] for i in range(len(b) - 1)}

        if not bigrams_a or not bigrams_b:
            return 0.0

        intersection = len(bigrams_a & bigrams_b)
        union = len(bigrams_a | bigrams_b)
        return intersection / union if union > 0 else 0.0
