"""Conversation session management — extracted from generator.py.

File: metamind/core/nl_interface/conversation_manager.py
Feature: F28_nl_interface
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class NLConversationTurn:
    """A single turn in a multi-turn NL conversation."""

    role: str  # "user" or "assistant"
    content: str
    generated_sql: Optional[str] = None
    executed: bool = False
    was_corrected: bool = False
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class ConversationManager:
    """Manages multi-turn NL conversations for iterative query refinement."""

    def __init__(self, max_turns: int = 10) -> None:
        self.max_turns = max_turns
        self.session_id: str = uuid.uuid4().hex
        self.turns: list[NLConversationTurn] = []

    def add_turn(self, role: str, content: str, sql: Optional[str] = None) -> None:
        """Add a conversation turn."""
        turn = NLConversationTurn(role=role, content=content, generated_sql=sql)
        self.turns.append(turn)
        if len(self.turns) > self.max_turns * 2:
            self.turns = self.turns[-(self.max_turns * 2):]

    def get_context_window(self) -> list[dict[str, str]]:
        """Return recent turns formatted for LLM prompt context."""
        window: list[dict[str, str]] = []
        recent = self.turns[-(self.max_turns * 2):]
        for turn in recent:
            entry: dict[str, str] = {"role": turn.role, "content": turn.content}
            if turn.generated_sql:
                entry["generated_sql"] = turn.generated_sql
            window.append(entry)
        return window

    def save(self, tenant_id: str, session_id: str, engine: Any) -> None:
        """Persist conversation to database."""
        data = json.dumps(
            [
                {
                    "role": t.role,
                    "content": t.content,
                    "generated_sql": t.generated_sql,
                    "executed": t.executed,
                    "was_corrected": t.was_corrected,
                    "timestamp": t.timestamp,
                }
                for t in self.turns
            ]
        )
        logger.info(
            "Saved conversation %s for tenant %s (%d turns)",
            session_id, tenant_id, len(self.turns),
        )
        self._persist(tenant_id, session_id, data, engine)

    def load(self, session_id: str, engine: Any) -> None:
        """Restore conversation from database."""
        data = self._load(session_id, engine)
        if data:
            parsed = json.loads(data)
            self.turns = [
                NLConversationTurn(
                    role=t["role"],
                    content=t["content"],
                    generated_sql=t.get("generated_sql"),
                    executed=t.get("executed", False),
                    was_corrected=t.get("was_corrected", False),
                    timestamp=t.get("timestamp", ""),
                )
                for t in parsed
            ]
            self.session_id = session_id

    def _persist(self, tenant_id: str, session_id: str, data: str, engine: Any) -> None:
        """Store conversation data via engine (SQLAlchemy Core)."""
        if engine is None:
            logger.debug("No engine provided; skipping persistence")
            return
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                conn.execute(
                    text(
                        "INSERT INTO mm_nl_sessions (session_id, tenant_id, conversation_data, updated_at) "
                        "VALUES (:sid, :tid, :data, :ts) "
                        "ON CONFLICT (session_id) DO UPDATE SET conversation_data = :data, updated_at = :ts"
                    ),
                    {"sid": session_id, "tid": tenant_id, "data": data,
                     "ts": datetime.now(timezone.utc).isoformat()},
                )
                conn.commit()
        except Exception as exc:
            logger.warning("Failed to persist conversation: %s", exc)

    def _load(self, session_id: str, engine: Any) -> Optional[str]:
        """Load conversation data from engine."""
        if engine is None:
            return None
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT conversation_data FROM mm_nl_sessions WHERE session_id = :sid"),
                    {"sid": session_id},
                ).fetchone()
                if row:
                    return str(row[0])
        except Exception as exc:
            logger.warning("Failed to load conversation: %s", exc)
        return None
