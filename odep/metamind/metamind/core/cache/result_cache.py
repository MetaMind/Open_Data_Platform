"""Result-level cache for deterministic queries."""
from __future__ import annotations
import json, hashlib, logging
from typing import Optional, Any
logger = logging.getLogger(__name__)
class ResultCache:
    def __init__(self, redis: Optional[object], ttl: int = 300) -> None:
        self._redis = redis; self._ttl = ttl
    def get(self, sql: str, tenant_id: str) -> Optional[list[dict[str, Any]]]:
        key = self._key(sql, tenant_id)
        if self._redis:
            try:
                raw = self._redis.get(key)
                return json.loads(raw) if raw else None
            except Exception as exc:
                logger.warning("ResultCache get failed: %s", exc)
        return None
    def put(self, sql: str, tenant_id: str, rows: list[dict[str, Any]]) -> None:
        key = self._key(sql, tenant_id)
        if self._redis:
            try: self._redis.setex(key, self._ttl, json.dumps(rows, default=str))
            except Exception as e: logger.warning("ResultCache put failed: %s", e)
    def _key(self, sql: str, tenant_id: str) -> str:
        h = hashlib.sha256(f"{tenant_id}:{sql}".encode()).hexdigest()[:24]
        return f"{tenant_id}:result:{h}"
