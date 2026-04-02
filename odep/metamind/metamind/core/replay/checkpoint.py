"""Checkpoint serialization utilities — extracted from recorder.py.

File: metamind/core/replay/checkpoint.py
Feature: F30_optimization_replay
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class CheckpointSerializer:
    """Serializes and deserializes query optimization checkpoints for persistence."""

    @staticmethod
    def to_json(optimization_context: dict[str, Any]) -> str:
        """Serialize optimization context to JSON string for database storage.

        Args:
            optimization_context: Dict containing plan metadata, cost estimates, etc.

        Returns:
            JSON string safe for database TEXT columns.
        """
        try:
            return json.dumps(optimization_context, default=str)
        except (TypeError, ValueError) as exc:
            logger.error("CheckpointSerializer.to_json failed: %s", exc)
            return json.dumps({"_error": str(exc)})

    @staticmethod
    def from_json(raw: Optional[str]) -> dict[str, Any]:
        """Deserialize JSON string back to optimization context dict.

        Args:
            raw: JSON string from database, or None.

        Returns:
            Deserialized dict, or empty dict on failure.
        """
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("CheckpointSerializer.from_json failed: %s", exc)
            return {"_error": str(exc), "_raw": raw[:200]}

    @staticmethod
    def to_bytes(optimization_context: dict[str, Any]) -> bytes:
        """Serialize to UTF-8 bytes (for Redis or blob storage).

        Args:
            optimization_context: Dict containing plan metadata.

        Returns:
            UTF-8 encoded bytes.
        """
        return CheckpointSerializer.to_json(optimization_context).encode("utf-8")

    @staticmethod
    def from_bytes(data: Optional[bytes]) -> dict[str, Any]:
        """Deserialize from UTF-8 bytes.

        Args:
            data: Bytes from Redis or blob storage.

        Returns:
            Deserialized dict, or empty dict on failure.
        """
        if not data:
            return {}
        try:
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("CheckpointSerializer.from_bytes failed: %s", exc)
            return {"_error": str(exc)}
