"""Production NL-to-SQL interface with conversation management and feedback loop.

Provides multi-turn natural language query generation with schema auto-discovery,
verified example few-shot prompting, and multi-provider LLM support.

Feature: F28_nl_interface
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Optional

from metamind.core.catalog.metadata import MetadataCatalog
from metamind.core.types import NLQueryResult
from metamind.core.nl_interface.conversation_manager import (
    ConversationManager,
    NLConversationTurn,
)
from metamind.core.nl_interface.prompt_builder import (
    build_prompt_messages,
    format_schema_context,
    assemble_few_shot_examples,
)

logger = logging.getLogger(__name__)

STOPWORDS: set[str] = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "need", "dare", "ought",
    "used", "to", "of", "in", "for", "on", "with", "at", "by", "from",
    "as", "into", "through", "during", "before", "after", "above", "below",
    "between", "out", "off", "over", "under", "again", "further", "then",
    "once", "here", "there", "when", "where", "why", "how", "all", "both",
    "each", "few", "more", "most", "other", "some", "such", "no", "nor",
    "not", "only", "own", "same", "so", "than", "too", "very", "just",
    "and", "but", "or", "if", "while", "that", "this", "what", "which",
    "who", "whom", "me", "my", "i", "you", "your", "he", "she", "it",
    "we", "they", "them", "show", "get", "give", "find", "list", "tell",
}


# ──────────────────────── Schema Auto-Discovery ────────────────────────────


class SchemaAutoDiscovery:
    """Identifies relevant tables for an NL query via TF-IDF-style matching."""

    def __init__(self, catalog: MetadataCatalog) -> None:
        self.catalog = catalog

    def discover_tables(self, nl_text: str, tenant_id: str, max_tables: int = 5) -> list[str]:
        """Find the most relevant tables for a natural language query."""
        query_terms = self._tokenize_query(nl_text)
        if not query_terms:
            return [t.table_name for t in self.catalog.list_tables(tenant_id)][:max_tables]

        tables = self.catalog.list_tables(tenant_id)
        if not tables:
            return []

        scored: list[tuple[str, float]] = [
            (table.table_name, self._score_table(query_terms, table))
            for table in tables
        ]
        scored.sort(key=lambda x: x[1], reverse=True)

        if scored and scored[0][1] < 0.05:
            return [t.table_name for t in tables][:max_tables]
        return [name for name, score in scored if score > 0][:max_tables]

    def _tokenize_query(self, text: str) -> list[str]:
        words = re.findall(r"[a-zA-Z_]+", text.lower())
        return [w for w in words if w not in STOPWORDS and len(w) > 1]

    def _score_table(self, query_terms: list[str], table_meta: Any) -> float:
        table_tokens: list[str] = []
        table_name = table_meta.table_name.lower()
        table_tokens.extend(table_name.replace("_", " ").split())
        table_tokens.append(table_name)
        for col in table_meta.columns:
            col_name = col.name.lower()
            table_tokens.extend(col_name.replace("_", " ").split())
            table_tokens.append(col_name)

        table_counter = Counter(table_tokens)
        score = 0.0
        for term in query_terms:
            for token, count in table_counter.items():
                if term in token or token in term:
                    overlap = len(set(term) & set(token)) / max(len(term), len(token))
                    score += overlap * (1.0 + math.log(count + 1))
                elif self._edit_distance_ratio(term, token) > 0.75:
                    score += 0.3
        return score

    @staticmethod
    def _edit_distance_ratio(a: str, b: str) -> float:
        if a == b:
            return 1.0
        max_len = max(len(a), len(b))
        if max_len == 0:
            return 1.0
        return sum(1 for ca, cb in zip(a, b) if ca == cb) / max_len


# ──────────────────────── Feedback Collector ───────────────────────────────


class NLFeedbackCollector:
    """Collects user feedback on generated SQL to improve future generation."""

    def record_feedback(
        self,
        tenant_id: str,
        nl_text: str,
        generated_sql: str,
        was_correct: bool,
        correction: Optional[str],
        engine: Any,
    ) -> None:
        """Record feedback on a generated SQL query."""
        entry_id = hashlib.md5(
            f"{tenant_id}:{nl_text}:{generated_sql}".encode()
        ).hexdigest()
        logger.info(
            "Recording NL feedback: tenant=%s correct=%s entry=%s",
            tenant_id, was_correct, entry_id,
        )
        if engine is None:
            logger.debug("No engine; feedback not persisted")
            return
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                conn.execute(
                    text(
                        "INSERT INTO mm_nl_query_mappings "
                        "(entry_id, tenant_id, nl_text, generated_sql, corrected_sql, "
                        "was_correct, created_at) "
                        "VALUES (:eid, :tid, :nl, :sql, :corr, :ok, :ts) "
                        "ON CONFLICT (entry_id) DO UPDATE SET "
                        "corrected_sql = :corr, was_correct = :ok"
                    ),
                    {"eid": entry_id, "tid": tenant_id, "nl": nl_text,
                     "sql": generated_sql, "corr": correction or "",
                     "ok": was_correct, "ts": datetime.now(timezone.utc).isoformat()},
                )
                conn.commit()
        except Exception as exc:
            logger.warning("Failed to persist NL feedback: %s", exc)

    def get_verified_examples(
        self, tenant_id: str, limit: int = 20, engine: Any = None
    ) -> list[dict[str, str]]:
        """Return verified (nl → sql) pairs for few-shot prompting."""
        if engine is None:
            return []
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT nl_text, COALESCE(corrected_sql, generated_sql) as sql "
                        "FROM mm_nl_query_mappings "
                        "WHERE tenant_id = :tid AND was_correct = true "
                        "ORDER BY created_at DESC LIMIT :lim"
                    ),
                    {"tid": tenant_id, "lim": limit},
                ).fetchall()
                return [{"nl": r[0], "sql": r[1]} for r in rows]
        except Exception as exc:
            logger.warning("Failed to load verified examples: %s", exc)
            return []


# ──────────────────────── NL Query Generator ───────────────────────────────


class NLQueryGenerator:
    """Multi-provider NL-to-SQL generator with auto-discovery and feedback loop."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gpt-4o",
        provider: str = "openai",
        catalog: Optional[MetadataCatalog] = None,
        db_engine: Any = None,
        temperature: float = 0.0,
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.provider = provider.lower()
        self.catalog = catalog
        self.db_engine = db_engine
        self.temperature = temperature
        self._discovery = SchemaAutoDiscovery(catalog) if catalog else None
        self._feedback = NLFeedbackCollector()

    def generate(
        self,
        nl_text: str,
        tenant_id: str,
        table_hints: Optional[list[str]] = None,
        conversation: Optional[ConversationManager] = None,
        use_auto_discovery: bool = True,
        use_verified_examples: bool = True,
    ) -> NLQueryResult:
        """Generate SQL from natural language."""
        start = time.monotonic()

        if table_hints:
            relevant_tables = table_hints
        elif use_auto_discovery and self._discovery:
            relevant_tables = self._discovery.discover_tables(nl_text, tenant_id)
        else:
            relevant_tables = (
                [t.table_name for t in self.catalog.list_tables(tenant_id)]
                if self.catalog else []
            )

        examples: list[dict[str, str]] = []
        if use_verified_examples:
            examples = self._feedback.get_verified_examples(tenant_id, engine=self.db_engine)

        schema_ctx = format_schema_context(relevant_tables, self.catalog, tenant_id)
        conv_ctx = conversation.get_context_window() if conversation else []
        prompt_messages = build_prompt_messages(nl_text, schema_ctx, conv_ctx, examples)

        try:
            raw_sql = self._call_llm(prompt_messages)
        except Exception as exc:
            logger.error("LLM call failed: %s", exc)
            return NLQueryResult(
                sql="", confidence=0.0, tables_used=relevant_tables,
                explanation=f"LLM call failed: {exc}", was_validated=False,
                validation_error=str(exc),
            )

        sql = self._extract_sql(raw_sql)
        is_valid, validation_error = self._validate_sql(sql)
        confidence = self._estimate_confidence(sql, schema_ctx, is_valid, bool(examples))

        if conversation:
            conversation.add_turn("user", nl_text)
            conversation.add_turn("assistant", raw_sql, sql=sql)

        elapsed = (time.monotonic() - start) * 1000.0
        logger.info(
            "NL→SQL generated in %.0fms: confidence=%.2f valid=%s tables=%s",
            elapsed, confidence, is_valid, relevant_tables,
        )
        return NLQueryResult(
            sql=sql, confidence=confidence, tables_used=relevant_tables,
            explanation=raw_sql, was_validated=is_valid, validation_error=validation_error,
        )

    def _call_llm(self, messages: list[dict[str, str]]) -> str:
        if self.provider == "openai":
            return self._call_openai(messages)
        elif self.provider == "anthropic":
            return self._call_anthropic(messages)
        elif self.provider == "ollama":
            return self._call_ollama(messages)
        raise ValueError(f"Unknown LLM provider: {self.provider}")

    def _call_openai(self, messages: list[dict[str, str]]) -> str:
        if not self.api_key:
            raise ValueError("OpenAI API key is required (set METAMIND_LLM_API_KEY)")
        import urllib.request
        payload = json.dumps({"model": self.model, "messages": messages,
                              "temperature": self.temperature, "max_tokens": 2048}).encode()
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions", data=payload,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
        return str(result["choices"][0]["message"]["content"])

    def _call_anthropic(self, messages: list[dict[str, str]]) -> str:
        if not self.api_key:
            raise ValueError("Anthropic API key is required")
        import urllib.request
        system_msgs = [m["content"] for m in messages if m["role"] == "system"]
        user_msgs = [m for m in messages if m["role"] != "system"]
        payload = json.dumps({"model": self.model, "max_tokens": 2048,
                              "system": "\n\n".join(system_msgs), "messages": user_msgs}).encode()
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages", data=payload,
            headers={"Content-Type": "application/json", "x-api-key": self.api_key,
                     "anthropic-version": "2023-06-01"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
        return str(result["content"][0]["text"])

    def _call_ollama(self, messages: list[dict[str, str]]) -> str:
        import urllib.request
        payload = json.dumps({"model": self.model, "messages": messages, "stream": False}).encode()
        req = urllib.request.Request(
            "http://localhost:11434/api/chat", data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode())
        return str(result["message"]["content"])

    def _extract_sql(self, raw: str) -> str:
        sql = raw.strip()
        if "```sql" in sql:
            match = re.search(r"```sql\s*(.*?)\s*```", sql, re.DOTALL)
            if match:
                return match.group(1).strip()
        elif "```" in sql:
            match = re.search(r"```\s*(.*?)\s*```", sql, re.DOTALL)
            if match:
                return match.group(1).strip()
        return sql

    def _validate_sql(self, sql: str) -> tuple[bool, Optional[str]]:
        if not sql:
            return False, "Empty SQL"
        try:
            import sqlglot
            sqlglot.parse_one(sql)
            return True, None
        except ImportError:
            logger.debug("sqlglot not available; skipping validation")
            if any(kw in sql.upper() for kw in ("SELECT", "INSERT", "UPDATE", "DELETE", "WITH")):
                return True, None
            return False, "Cannot validate without sqlglot"
        except Exception as exc:
            return False, str(exc)

    def _estimate_confidence(
        self, sql: str, schema_ctx: str, is_valid: bool, has_verified_example: bool
    ) -> float:
        if not sql:
            return 0.0
        score = 0.0
        tables_in_sql = re.findall(r"\bFROM\s+(\w+)", sql, re.IGNORECASE)
        tables_in_sql += re.findall(r"\bJOIN\s+(\w+)", sql, re.IGNORECASE)
        if tables_in_sql and all(t.lower() in schema_ctx.lower() for t in tables_in_sql):
            score += 0.3
        col_refs = re.findall(r"\b(\w+)\s*[=<>!]", sql)
        if col_refs:
            valid_cols = sum(1 for c in col_refs if c.lower() in schema_ctx.lower())
            score += 0.3 * (valid_cols / len(col_refs))
        if is_valid:
            score += 0.2
        if re.search(r"\bWHERE\b", sql, re.IGNORECASE):
            score += 0.1
        if has_verified_example:
            score += 0.1
        return min(score, 1.0)
