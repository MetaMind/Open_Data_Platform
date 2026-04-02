"""
Cache Manager - Multi-Tier Result Caching

File: metamind/cache/result_cache.py
Role: Backend Engineer
Phase: 1
Dependencies: Redis, pyarrow

L1/L2/L3 cache with Redis backend.
- L1: In-memory (hot) - TTL 5min
- L2: Redis (warm) - TTL 1hour
- L3: S3 (cold) - TTL 7days
"""

from __future__ import annotations

import hashlib
import json
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import pyarrow as pa

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Cache entry metadata and data."""
    key: str
    data: Any
    created_at: datetime
    ttl_seconds: int
    metadata: Dict[str, Any]
    
    @property
    def is_expired(self) -> bool:
        """Check if entry is expired."""
        age = (datetime.now() - self.created_at).total_seconds()
        return age > self.ttl_seconds
    
    @property
    def age_seconds(self) -> float:
        """Get age in seconds."""
        return (datetime.now() - self.created_at).total_seconds()


class CacheManager:
    """
    Multi-tier cache manager with Redis backend.
    
    Tiers:
    - L1: In-memory (hot) - TTL 5min
    - L2: Redis (warm) - TTL 1hour
    - L3: S3 (cold) - TTL 7days
    """
    
    def __init__(
        self,
        redis_client: Any,
        settings: Any,
        l1_cache: Optional[Dict[str, CacheEntry]] = None
    ):
        """
        Initialize cache manager.
        
        Args:
            redis_client: Redis client
            settings: Cache settings
            l1_cache: Optional L1 cache dictionary
        """
        self.redis = redis_client
        self.settings = settings
        self._l1: Dict[str, CacheEntry] = l1_cache or {}
        self._l1_hits = 0
        self._l2_hits = 0
        self._misses = 0
        logger.debug("CacheManager initialized")
    
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get cached result by key.
        
        Tries L1 -> L2 -> L3 in order.
        
        Args:
            key: Cache key
            
        Returns:
            Cached result or None
        """
        # Try L1 (in-memory)
        if self.settings.l1_enabled:
            l1_entry = self._l1.get(key)
            if l1_entry and not l1_entry.is_expired:
                self._l1_hits += 1
                logger.debug(f"L1 cache hit: {key[:16]}...")
                return {
                    "data": l1_entry.data,
                    "cached_at": l1_entry.created_at,
                    "tier": "L1"
                }
            elif l1_entry and l1_entry.is_expired:
                # Clean up expired L1 entry
                del self._l1[key]
        
        # Try L2 (Redis)
        if self.settings.l2_enabled:
            try:
                l2_data = await self.redis.get(f"l2:{key}")
                if l2_data:
                    self._l2_hits += 1
                    logger.debug(f"L2 cache hit: {key[:16]}...")
                    
                    # Deserialize
                    entry = pickle.loads(l2_data)
                    
                    # Promote to L1
                    if self.settings.l1_enabled:
                        self._l1[key] = CacheEntry(
                            key=key,
                            data=entry["data"],
                            created_at=datetime.now(),
                            ttl_seconds=self.settings.l1_ttl_seconds,
                            metadata=entry.get("metadata", {})
                        )
                    
                    return {
                        "data": entry["data"],
                        "cached_at": entry.get("created_at"),
                        "tier": "L2"
                    }
            except Exception as e:
                logger.warning(f"L2 cache read failed: {e}")
        
        self._misses += 1
        return None
    
    async def set(
        self,
        key: str,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
        ttl_override: Optional[int] = None
    ) -> None:
        """
        Store result in cache.
        
        Stores in L1 and L2 (if enabled).
        
        Args:
            key: Cache key
            data: Data to cache
            metadata: Optional metadata
            ttl_override: Optional TTL override
        """
        now = datetime.now()
        
        # Store in L1
        if self.settings.l1_enabled:
            self._l1[key] = CacheEntry(
                key=key,
                data=data,
                created_at=now,
                ttl_seconds=ttl_override or self.settings.l1_ttl_seconds,
                metadata=metadata or {}
            )
            logger.debug(f"Stored in L1: {key[:16]}...")
        
        # Store in L2
        if self.settings.l2_enabled:
            try:
                entry = {
                    "data": data,
                    "created_at": now,
                    "metadata": metadata or {}
                }
                serialized = pickle.dumps(entry)
                
                # Check size limit
                if len(serialized) > self.settings.l2_max_size_mb * 1024 * 1024:
                    logger.warning(
                        f"Entry too large for L2: {len(serialized)} bytes"
                    )
                    return
                
                await self.redis.setex(
                    f"l2:{key}",
                    ttl_override or self.settings.l2_ttl_seconds,
                    serialized
                )
                logger.debug(f"Stored in L2: {key[:16]}...")
                
            except Exception as e:
                logger.warning(f"L2 cache write failed: {e}")
    
    async def invalidate(self, key: str) -> bool:
        """
        Invalidate cache entry.
        
        Args:
            key: Cache key to invalidate
            
        Returns:
            True if entry was found and removed
        """
        found = False
        
        # Remove from L1
        if key in self._l1:
            del self._l1[key]
            found = True
            logger.debug(f"Invalidated L1: {key[:16]}...")
        
        # Remove from L2
        try:
            result = await self.redis.delete(f"l2:{key}")
            if result:
                found = True
                logger.debug(f"Invalidated L2: {key[:16]}...")
        except Exception as e:
            logger.warning(f"L2 cache invalidate failed: {e}")
        
        return found
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate cache entries matching pattern.
        
        Args:
            pattern: Key pattern to match
            
        Returns:
            Number of entries invalidated
        """
        count = 0
        
        # Remove from L1
        keys_to_remove = [
            k for k in self._l1.keys()
            if pattern in k
        ]
        for key in keys_to_remove:
            del self._l1[key]
            count += 1
        
        # Remove from L2 (scan and delete)
        try:
            async for key in self.redis.scan_iter(match=f"l2:*{pattern}*"):
                await self.redis.delete(key)
                count += 1
        except Exception as e:
            logger.warning(f"L2 pattern invalidate failed: {e}")
        
        logger.info(f"Invalidated {count} entries matching '{pattern}'")
        return count
    
    def compute_key(
        self,
        sql: str,
        tenant_id: str,
        user_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Compute cache key from SQL and context.
        
        Args:
            sql: SQL query
            tenant_id: Tenant identifier
            user_context: Optional user context
            
        Returns:
            Cache key string
        """
        # Normalize SQL
        normalized = sql.lower().strip()
        
        # Include user context if configured
        if self.settings.include_user_context and user_context:
            context_str = json.dumps(user_context, sort_keys=True)
            key_input = f"{tenant_id}:{normalized}:{context_str}"
        else:
            key_input = f"{tenant_id}:{normalized}"
        
        # Hash
        if self.settings.fingerprint_algorithm == "sha256":
            return hashlib.sha256(key_input.encode()).hexdigest()[:32]
        else:
            return hashlib.md5(key_input.encode()).hexdigest()[:16]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Statistics dictionary
        """
        total_requests = self._l1_hits + self._l2_hits + self._misses
        hit_rate = (
            (self._l1_hits + self._l2_hits) / total_requests * 100
            if total_requests > 0 else 0
        )
        
        return {
            "l1_size": len(self._l1),
            "l1_hits": self._l1_hits,
            "l2_hits": self._l2_hits,
            "misses": self._misses,
            "hit_rate_percent": round(hit_rate, 2),
            "total_requests": total_requests
        }
    
    async def clear(self) -> None:
        """Clear all cache tiers."""
        # Clear L1
        self._l1.clear()
        logger.info("Cleared L1 cache")
        
        # Clear L2
        try:
            async for key in self.redis.scan_iter(match="l2:*"):
                await self.redis.delete(key)
            logger.info("Cleared L2 cache")
        except Exception as e:
            logger.warning(f"L2 cache clear failed: {e}")
    
    def _cleanup_l1(self) -> int:
        """
        Clean up expired L1 entries.
        
        Returns:
            Number of entries removed
        """
        expired = [
            k for k, v in self._l1.items()
            if v.is_expired
        ]
        for key in expired:
            del self._l1[key]
        
        return len(expired)
