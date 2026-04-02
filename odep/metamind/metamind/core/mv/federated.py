"""F15 Federated materialized views."""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
class FederatedMVManager:
    def sync(self, tenant_id: str, mv_name: str) -> None:
        logger.info("Syncing federated MV %s for tenant %s", mv_name, tenant_id)
