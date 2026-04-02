"""Redis cache backend wrapper."""
from __future__ import annotations
import logging
from typing import Optional
logger = logging.getLogger(__name__)
def get_redis_client(url: str) -> Optional[object]:
    try:
        import redis
        return redis.from_url(url, decode_responses=True)
    except ImportError:
        logger.warning("redis package not installed")
        return None
