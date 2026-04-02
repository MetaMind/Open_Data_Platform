"""F28 Natural language query parser with intent classification and entity extraction.

Parses user natural language queries into structured intents that the
NLQueryGenerator can use to produce SQL. Handles:
- Intent classification (select, aggregate, filter, join, describe)
- Entity extraction (table names, column names, values)
- Temporal expression parsing
- Aggregation/grouping detection
- Disambiguation for ambiguous references
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class QueryIntent(str, Enum):
    """Classified intent of a natural language query."""

    SELECT = "select"           # Simple data retrieval
    AGGREGATE = "aggregate"     # COUNT, SUM, AVG, etc.
    FILTER = "filter"           # Data with conditions
    JOIN = "join"               # Multi-table query
    DESCRIBE = "describe"       # Schema/metadata question
    COMPARE = "compare"         # Comparison between groups
    TREND = "trend"             # Time-series / temporal analysis
    TOP_N = "top_n"             # Ranking query
    UNKNOWN = "unknown"


@dataclass
class TemporalExpression:
    """A parsed temporal reference."""

    raw_text: str
    temporal_type: str  # absolute, relative, range
    column_hint: str = ""
    sql_fragment: str = ""


@dataclass
class ParsedEntity:
    """An extracted entity from natural language."""

    text: str
    entity_type: str  # table, column, value, number, temporal
    confidence: float = 0.0
    resolved_name: Optional[str] = None  # actual DB name if matched


@dataclass
class ParsedQuery:
    """Fully parsed natural language query."""

    original_text: str
    intent: QueryIntent
    entities: list[ParsedEntity] = field(default_factory=list)
    tables: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    aggregations: list[str] = field(default_factory=list)
    filters: list[dict[str, str]] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    order_by: Optional[str] = None
    order_direction: str = "DESC"
    limit: Optional[int] = None
    temporal: Optional[TemporalExpression] = None
    confidence: float = 0.0
    ambiguities: list[str] = field(default_factory=list)


# Pattern constants
_AGG_WORDS = {"count", "total", "sum", "average", "avg", "mean", "minimum", "min",
              "maximum", "max", "median"}
_FILTER_WORDS = {"where", "when", "since", "after", "before", "between", "during",
                 "greater", "less", "more", "fewer", "above", "below", "equal",
                 "exceeding", "at least", "at most", "only"}
_JOIN_WORDS = {"and", "with", "across", "between", "combining", "joined", "related"}
_TEMPORAL_WORDS = {"today", "yesterday", "last", "this", "past", "recent",
                   "week", "month", "year", "quarter", "day", "hour"}
_TOP_WORDS = {"top", "best", "worst", "highest", "lowest", "most", "least",
              "largest", "smallest", "biggest"}
_COMPARE_WORDS = {"compare", "versus", "vs", "difference", "compared", "relative"}
_DESCRIBE_WORDS = {"describe", "schema", "structure", "columns", "tables", "what is",
                   "show me the", "list all"}

_TEMPORAL_PATTERNS = [
    (r"last (\d+) (day|week|month|year|hour|minute)s?", "relative"),
    (r"(today|yesterday|this week|this month|this year)", "relative"),
    (r"since (\d{4}-\d{2}-\d{2})", "absolute"),
    (r"between (\d{4}-\d{2}-\d{2}) and (\d{4}-\d{2}-\d{2})", "range"),
    (r"in (\d{4})", "absolute"),
    (r"(Q[1-4]) (\d{4})", "absolute"),
    (r"(january|february|march|april|may|june|july|august|september|october|november|december)\s*(\d{4})?", "absolute"),
]

_NUMBER_PATTERN = re.compile(r"(\d+(?:\.\d+)?)")
_TOP_N_PATTERN = re.compile(r"top\s+(\d+)", re.IGNORECASE)
_LIMIT_PATTERN = re.compile(r"(?:show|give|display|list)\s+(?:me\s+)?(\d+)", re.IGNORECASE)


class NLParser:
    """Parses natural language queries into structured ParsedQuery objects.

    Uses rule-based intent classification and pattern-matching entity extraction.
    Designed to work with the SchemaContextBuilder for entity resolution.
    """

    def __init__(self, schema_context: Optional[Any] = None) -> None:
        """Initialize with optional schema context for entity resolution.

        Args:
            schema_context: SchemaContextBuilder for resolving table/column names.
        """
        self._schema = schema_context

    def parse(self, text: str, tenant_id: str = "") -> ParsedQuery:
        """Parse a natural language query into structured form.

        Args:
            text: Raw natural language text.
            tenant_id: Tenant for schema resolution.

        Returns:
            ParsedQuery with classified intent and extracted entities.
        """
        normalized = text.strip()
        lower = normalized.lower()
        tokens = self._tokenize(lower)

        # Step 1: Classify intent
        intent, intent_confidence = self._classify_intent(tokens, lower)

        # Step 2: Extract entities
        entities = self._extract_entities(normalized, tokens)

        # Step 3: Parse temporal expressions
        temporal = self._parse_temporal(lower)

        # Step 4: Extract aggregations
        aggregations = self._extract_aggregations(tokens)

        # Step 5: Detect grouping
        group_by = self._extract_group_by(lower, tokens)

        # Step 6: Detect ordering and limits
        order_by, order_dir = self._extract_ordering(lower, tokens)
        limit = self._extract_limit(lower)

        # Step 7: Resolve entities against schema if available
        tables, columns, ambiguities = self._resolve_entities(
            entities, tenant_id
        )

        # Step 8: Extract filter conditions
        filters = self._extract_filters(lower, tokens, columns)

        return ParsedQuery(
            original_text=normalized,
            intent=intent,
            entities=entities,
            tables=tables,
            columns=columns,
            aggregations=aggregations,
            filters=filters,
            group_by=group_by,
            order_by=order_by,
            order_direction=order_dir,
            limit=limit,
            temporal=temporal,
            confidence=intent_confidence,
            ambiguities=ambiguities,
        )

    def parse_intent(self, text: str) -> dict[str, object]:
        """Legacy interface for backward compatibility."""
        parsed = self.parse(text)
        return {
            "intent": parsed.intent.value,
            "tables": parsed.tables,
            "columns": parsed.columns,
            "aggregation": len(parsed.aggregations) > 0,
            "filter": len(parsed.filters) > 0,
            "temporal": parsed.temporal is not None,
            "confidence": parsed.confidence,
        }

    def _tokenize(self, text: str) -> list[str]:
        """Tokenize and normalize text."""
        text = re.sub(r"[^a-z0-9_\s]", " ", text)
        return [t for t in text.split() if len(t) > 0]

    def _classify_intent(
        self, tokens: list[str], lower: str
    ) -> tuple[QueryIntent, float]:
        """Classify the query intent using keyword scoring."""
        scores: dict[QueryIntent, float] = {i: 0.0 for i in QueryIntent}
        token_set = set(tokens)

        # Score each intent based on keyword overlap
        agg_overlap = len(token_set & _AGG_WORDS)
        filter_overlap = len(token_set & _FILTER_WORDS)
        top_overlap = len(token_set & _TOP_WORDS)
        compare_overlap = len(token_set & _COMPARE_WORDS)
        temporal_overlap = len(token_set & _TEMPORAL_WORDS)

        scores[QueryIntent.AGGREGATE] = agg_overlap * 2.0
        scores[QueryIntent.FILTER] = filter_overlap * 1.5
        scores[QueryIntent.TOP_N] = top_overlap * 2.5
        scores[QueryIntent.COMPARE] = compare_overlap * 2.0
        scores[QueryIntent.TREND] = temporal_overlap * 1.8 + (1.0 if agg_overlap > 0 else 0.0)

        # Join detection: multiple table references or join keywords
        join_overlap = len(token_set & _JOIN_WORDS)
        if "join" in lower or join_overlap > 1:
            scores[QueryIntent.JOIN] = 3.0

        # Describe detection
        for phrase in _DESCRIBE_WORDS:
            if phrase in lower:
                scores[QueryIntent.DESCRIBE] = 4.0
                break

        # Default: SELECT
        scores[QueryIntent.SELECT] = 0.5

        # Pick highest
        best_intent = max(scores, key=scores.get)
        best_score = scores[best_intent]
        total = sum(scores.values())
        confidence = best_score / max(0.1, total)

        return best_intent, min(1.0, confidence)

    def _extract_entities(
        self, text: str, tokens: list[str]
    ) -> list[ParsedEntity]:
        """Extract potential entities (tables, columns, values)."""
        entities: list[ParsedEntity] = []

        # Extract numbers
        for match in _NUMBER_PATTERN.finditer(text):
            entities.append(ParsedEntity(
                text=match.group(1),
                entity_type="number",
                confidence=0.9,
            ))

        # Extract quoted strings as values
        for match in re.finditer(r'"([^"]+)"', text):
            entities.append(ParsedEntity(
                text=match.group(1),
                entity_type="value",
                confidence=0.95,
            ))
        for match in re.finditer(r"'([^']+)'", text):
            entities.append(ParsedEntity(
                text=match.group(1),
                entity_type="value",
                confidence=0.95,
            ))

        # Extract potential identifiers (CamelCase or snake_case words)
        for token in tokens:
            if "_" in token and len(token) > 2:
                entities.append(ParsedEntity(
                    text=token, entity_type="identifier", confidence=0.6,
                ))

        return entities

    def _parse_temporal(self, lower: str) -> Optional[TemporalExpression]:
        """Parse temporal expressions from the query."""
        for pattern, ttype in _TEMPORAL_PATTERNS:
            match = re.search(pattern, lower)
            if match:
                raw = match.group(0)
                sql = self._temporal_to_sql(raw, ttype)
                return TemporalExpression(
                    raw_text=raw,
                    temporal_type=ttype,
                    sql_fragment=sql,
                )
        return None

    def _temporal_to_sql(self, raw: str, ttype: str) -> str:
        """Convert temporal expression to SQL WHERE clause fragment."""
        if "today" in raw:
            return ">= CURRENT_DATE"
        if "yesterday" in raw:
            return ">= CURRENT_DATE - INTERVAL '1 day' AND < CURRENT_DATE"
        if "this week" in raw:
            return ">= DATE_TRUNC('week', CURRENT_DATE)"
        if "this month" in raw:
            return ">= DATE_TRUNC('month', CURRENT_DATE)"
        if "this year" in raw:
            return ">= DATE_TRUNC('year', CURRENT_DATE)"

        last_match = re.match(r"last (\d+) (\w+)", raw)
        if last_match:
            n, unit = last_match.group(1), last_match.group(2).rstrip("s")
            return f">= CURRENT_DATE - INTERVAL '{n} {unit}'"

        return ""

    def _extract_aggregations(self, tokens: list[str]) -> list[str]:
        """Extract aggregation functions from tokens."""
        agg_map = {
            "count": "COUNT", "total": "SUM", "sum": "SUM",
            "average": "AVG", "avg": "AVG", "mean": "AVG",
            "minimum": "MIN", "min": "MIN",
            "maximum": "MAX", "max": "MAX",
        }
        found: list[str] = []
        for t in tokens:
            if t in agg_map and agg_map[t] not in found:
                found.append(agg_map[t])
        return found

    def _extract_group_by(self, lower: str, tokens: list[str]) -> list[str]:
        """Detect grouping keywords."""
        groups: list[str] = []
        patterns = [
            (r"(?:by|per|for each|grouped by|group by)\s+(\w+)", 1),
        ]
        for pattern, group_idx in patterns:
            match = re.search(pattern, lower)
            if match:
                groups.append(match.group(group_idx))
        return groups

    def _extract_ordering(
        self, lower: str, tokens: list[str]
    ) -> tuple[Optional[str], str]:
        """Detect ORDER BY intent."""
        direction = "DESC"  # default for top-N style queries

        if any(w in tokens for w in ("lowest", "least", "smallest", "ascending", "oldest")):
            direction = "ASC"

        top_match = _TOP_N_PATTERN.search(lower)
        if top_match:
            return None, direction

        order_match = re.search(r"(?:order|sort|rank)\s+by\s+(\w+)", lower)
        if order_match:
            return order_match.group(1), direction

        return None, direction

    def _extract_limit(self, lower: str) -> Optional[int]:
        """Extract LIMIT value from the query."""
        top_match = _TOP_N_PATTERN.search(lower)
        if top_match:
            return int(top_match.group(1))

        limit_match = _LIMIT_PATTERN.search(lower)
        if limit_match:
            return int(limit_match.group(1))

        return None

    def _extract_filters(
        self, lower: str, tokens: list[str], columns: list[str]
    ) -> list[dict[str, str]]:
        """Extract filter conditions from the text."""
        filters: list[dict[str, str]] = []

        # Pattern: "where/with X = Y" or "X greater than Y"
        comparison_patterns = [
            (r"(\w+)\s+(?:greater than|more than|above|over|exceeding|>)\s+(\d+[\.\d]*)", ">"),
            (r"(\w+)\s+(?:less than|fewer than|below|under|<)\s+(\d+[\.\d]*)", "<"),
            (r"(\w+)\s+(?:equal to|equals|=)\s+(\w+)", "="),
            (r"(\w+)\s+(?:at least|>=)\s+(\d+[\.\d]*)", ">="),
            (r"(\w+)\s+(?:at most|<=)\s+(\d+[\.\d]*)", "<="),
        ]

        for pattern, op in comparison_patterns:
            match = re.search(pattern, lower)
            if match:
                filters.append({
                    "column": match.group(1),
                    "operator": op,
                    "value": match.group(2),
                })

        return filters

    def _resolve_entities(
        self,
        entities: list[ParsedEntity],
        tenant_id: str,
    ) -> tuple[list[str], list[str], list[str]]:
        """Resolve extracted entities against the schema."""
        tables: list[str] = []
        columns: list[str] = []
        ambiguities: list[str] = []

        if self._schema is None:
            return tables, columns, ambiguities

        for entity in entities:
            if entity.entity_type == "identifier":
                resolved = self._schema.resolve_name(entity.text, tenant_id)
                if resolved:
                    if resolved["type"] == "table":
                        tables.append(resolved["name"])
                        entity.resolved_name = resolved["name"]
                        entity.confidence = resolved.get("confidence", 0.8)
                    elif resolved["type"] == "column":
                        columns.append(resolved["name"])
                        entity.resolved_name = resolved["name"]
                    if resolved.get("ambiguous"):
                        ambiguities.append(
                            f"'{entity.text}' could match: {resolved.get('candidates', [])}"
                        )

        return tables, columns, ambiguities
