"""
Execution Limits - Resource Limits for Query Execution

File: metamind/safety/execution_limits.py
Role: Security Engineer
Phase: 1
Dependencies: None

Enforces resource limits during query execution.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


@dataclass
class ExecutionLimits:
    """Execution resource limits."""
    max_rows_returned: int = 100000
    max_bytes_scanned: int = 10 * 1024 * 1024 * 1024  # 10 GB
    max_execution_time_seconds: int = 300  # 5 minutes
    max_concurrent_queries_per_user: int = 5
    max_concurrent_queries_global: int = 100


class ExecutionLimiter:
    """
    Enforces resource limits during query execution.
    
    Tracks and limits:
    - Rows returned
    - Bytes scanned
    - Execution time
    - Concurrent queries
    """
    
    def __init__(self, limits: Optional[ExecutionLimits] = None):
        """
        Initialize execution limiter.
        
        Args:
            limits: Execution limits (uses defaults if not provided)
        """
        self.limits = limits or ExecutionLimits()
        self._concurrent_queries: Dict[str, int] = {}  # user_id -> count
        self._global_concurrent = 0
        logger.debug("ExecutionLimiter initialized")
    
    def check_limits(self, user_id: str) -> Dict[str, Any]:
        """
        Check if user can execute a new query.
        
        Args:
            user_id: User identifier
            
        Returns:
            Dict with 'allowed' boolean and 'reason' if rejected
        """
        # Check global limit
        if self._global_concurrent >= self.limits.max_concurrent_queries_global:
            return {
                "allowed": False,
                "reason": (
                    f"Global concurrent query limit reached "
                    f"({self._global_concurrent}/{self.limits.max_concurrent_queries_global})"
                )
            }
        
        # Check per-user limit
        user_count = self._concurrent_queries.get(user_id, 0)
        if user_count >= self.limits.max_concurrent_queries_per_user:
            return {
                "allowed": False,
                "reason": (
                    f"Per-user concurrent query limit reached "
                    f"({user_count}/{self.limits.max_concurrent_queries_per_user})"
                )
            }
        
        return {"allowed": True}
    
    def acquire_slot(self, user_id: str) -> bool:
        """
        Acquire a query execution slot.
        
        Args:
            user_id: User identifier
            
        Returns:
            True if slot acquired
        """
        result = self.check_limits(user_id)
        if not result["allowed"]:
            return False
        
        self._global_concurrent += 1
        self._concurrent_queries[user_id] = self._concurrent_queries.get(user_id, 0) + 1
        
        return True
    
    def release_slot(self, user_id: str) -> None:
        """
        Release a query execution slot.
        
        Args:
            user_id: User identifier
        """
        self._global_concurrent = max(0, self._global_concurrent - 1)
        
        if user_id in self._concurrent_queries:
            self._concurrent_queries[user_id] = max(
                0, self._concurrent_queries[user_id] - 1
            )
    
    def check_result_limits(
        self,
        rows_returned: int,
        bytes_scanned: int
    ) -> Dict[str, Any]:
        """
        Check if result is within limits.
        
        Args:
            rows_returned: Number of rows returned
            bytes_scanned: Bytes scanned
            
        Returns:
            Dict with 'allowed' boolean and 'reason' if exceeded
        """
        if rows_returned > self.limits.max_rows_returned:
            return {
                "allowed": False,
                "reason": (
                    f"Row limit exceeded ({rows_returned}, "
                    f"max {self.limits.max_rows_returned})"
                )
            }
        
        if bytes_scanned > self.limits.max_bytes_scanned:
            return {
                "allowed": False,
                "reason": (
                    f"Bytes scanned limit exceeded ({bytes_scanned}, "
                    f"max {self.limits.max_bytes_scanned})"
                )
            }
        
        return {"allowed": True}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current execution statistics."""
        return {
            "global_concurrent": self._global_concurrent,
            "max_global_concurrent": self.limits.max_concurrent_queries_global,
            "user_concurrent": self._concurrent_queries.copy(),
            "max_per_user": self.limits.max_concurrent_queries_per_user
        }
