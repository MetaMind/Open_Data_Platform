"""CDC Outbound Webhook Dispatcher.

Dispatches change events (INSERT/UPDATE/DELETE) detected by CDCMonitor
to external HTTP subscribers, signed with HMAC-SHA256.

Retries: up to 3 attempts with exponential backoff.
Delivery attempts are logged to mm_webhook_delivery_log.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BASE_BACKOFF_SECONDS = 1.0


@dataclass
class WebhookSub:
    """A single CDC webhook subscription."""

    sub_id: str
    table_name: str
    url: str
    secret: str
    is_active: bool = True


@dataclass
class CDCEvent:
    """Normalized CDC change event."""

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    table_name: str = ""
    operation: str = ""   # INSERT | UPDATE | DELETE
    tenant_id: str = ""
    changed_at: str = ""
    primary_keys: dict = field(default_factory=dict)
    before: Optional[dict] = None
    after: Optional[dict] = None


class CDCWebhookDispatcher:
    """Dispatch CDC events to external HTTP subscribers.

    Args:
        db_engine: SQLAlchemy Engine for loading subscriptions and logging.
        redis_client: Redis client (reserved for future rate-limiting use).
    """

    def __init__(self, db_engine: Engine, redis_client: object) -> None:
        self._engine = db_engine
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Subscription loading
    # ------------------------------------------------------------------

    async def load_subscriptions(self, table_name: str) -> list[WebhookSub]:
        """Load active subscriptions for the given table."""
        query = text(
            "SELECT sub_id, table_name, url, secret, is_active "
            "FROM mm_cdc_webhook_subs "
            "WHERE table_name = :tbl AND is_active = TRUE"
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(query, {"tbl": table_name}).fetchall()
            return [
                WebhookSub(
                    sub_id=str(r.sub_id),
                    table_name=r.table_name,
                    url=r.url,
                    secret=r.secret or "",
                    is_active=bool(r.is_active),
                )
                for r in rows
            ]
        except Exception as exc:
            logger.error(
                "CDCWebhookDispatcher.load_subscriptions failed table=%s: %s",
                table_name,
                exc,
            )
            return []

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    async def dispatch(self, event: CDCEvent) -> None:
        """Dispatch event to all matching active subscriptions.

        Each delivery is attempted up to _MAX_RETRIES times with
        exponential backoff. All attempts are logged.
        """
        subscriptions = await self.load_subscriptions(event.table_name)
        if not subscriptions:
            logger.debug(
                "No webhooks for table=%s event=%s", event.table_name, event.event_id
            )
            return

        payload = self._build_payload(event)
        tasks = [self._deliver(sub, payload, event.event_id) for sub in subscriptions]
        await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_payload(self, event: CDCEvent) -> dict:
        return {
            "event_id": event.event_id,
            "table_name": event.table_name,
            "operation": event.operation,
            "tenant_id": event.tenant_id,
            "changed_at": event.changed_at,
            "primary_keys": event.primary_keys,
            "before": event.before,
            "after": event.after,
        }

    def _sign(self, payload_bytes: bytes, secret: str) -> str:
        """Return hex HMAC-SHA256 signature for the payload."""
        return hmac.new(
            secret.encode(),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()

    async def _deliver(
        self, sub: WebhookSub, payload: dict, event_id: str
    ) -> None:
        """Attempt delivery with retries; log every attempt."""
        body = json.dumps(payload, default=str).encode()
        signature = self._sign(body, sub.secret)
        headers = {
            "Content-Type": "application/json",
            "X-MetaMind-Signature": f"sha256={signature}",
            "X-MetaMind-Event": payload.get("operation", ""),
        }

        last_status: Optional[int] = None
        last_error: Optional[str] = None

        async with httpx.AsyncClient(timeout=10.0) as client:
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    resp = await client.post(sub.url, content=body, headers=headers)
                    last_status = resp.status_code
                    if resp.is_success:
                        self._log_delivery(sub.sub_id, event_id, last_status, None)
                        logger.info(
                            "Webhook delivered sub=%s event=%s status=%d",
                            sub.sub_id,
                            event_id,
                            last_status,
                        )
                        return
                    last_error = f"HTTP {last_status}"
                except Exception as exc:
                    last_error = str(exc)
                    last_status = 0
                    logger.error(
                        "Webhook attempt %d/%d failed sub=%s event=%s: %s",
                        attempt,
                        _MAX_RETRIES,
                        sub.sub_id,
                        event_id,
                        exc,
                    )

                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))

        # All retries exhausted
        self._log_delivery(sub.sub_id, event_id, last_status or 0, last_error)
        logger.error(
            "Webhook delivery failed after %d attempts sub=%s event=%s",
            _MAX_RETRIES,
            sub.sub_id,
            event_id,
        )

    def _log_delivery(
        self,
        sub_id: str,
        event_id: str,
        status_code: int,
        error: Optional[str],
    ) -> None:
        """Insert a delivery log record into mm_webhook_delivery_log."""
        try:
            ins = text(
                "INSERT INTO mm_webhook_delivery_log "
                "(sub_id, event_id, status_code, error_message, attempted_at) "
                "VALUES (:sid, :eid, :sc, :err, NOW())"
            )
            with self._engine.begin() as conn:
                conn.execute(
                    ins,
                    {
                        "sid": sub_id,
                        "eid": event_id,
                        "sc": status_code,
                        "err": error,
                    },
                )
        except Exception as exc:
            logger.error("CDCWebhookDispatcher._log_delivery failed: %s", exc)
