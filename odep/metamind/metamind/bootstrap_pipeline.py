"""AppContext pipeline mixin — holds QueryEngine + UnifiedQueryPipeline properties.

Separated from bootstrap.py to keep that file under 500 lines.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class PipelineMixin:
    """Mixin that adds query_engine and unified_pipeline to AppContext."""

    @property
    def query_engine(self) -> Any:
        """Get or create QueryEngine (Cascades optimizer + execution pipeline)."""
        if self._query_engine is None:  # type: ignore[attr-defined]
            from metamind.core.query_engine import QueryEngine
            self._query_engine = QueryEngine(  # type: ignore[attr-defined]
                db_engine=self.sync_db_engine,  # type: ignore[attr-defined]
                redis=self._redis_client,  # type: ignore[attr-defined]
                settings=self.settings,  # type: ignore[attr-defined]
            )
            logger.debug("Created QueryEngine")
        return self._query_engine  # type: ignore[attr-defined]

    @property
    def unified_pipeline(self) -> Any:
        """W-06: Unified pipeline — QueryRouter → QueryEngine (single API entry-point)."""
        if self._unified_pipeline is None:  # type: ignore[attr-defined]
            from metamind.core.pipeline import UnifiedQueryPipeline
            self._unified_pipeline = UnifiedQueryPipeline(  # type: ignore[attr-defined]
                query_router=self._query_router,  # type: ignore[attr-defined]
                query_engine=self.query_engine,
            )
            logger.debug("Created UnifiedQueryPipeline")
        return self._unified_pipeline  # type: ignore[attr-defined]
