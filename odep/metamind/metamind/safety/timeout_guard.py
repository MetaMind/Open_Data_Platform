"""
Timeout Guard - Query Timeout Handling

File: metamind/safety/timeout_guard.py
Role: Security Engineer
Phase: 1
Dependencies: asyncio

Enforces query timeouts using asyncio.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, Callable, Any

logger = logging.getLogger(__name__)


class QueryTimeoutError(Exception):
    """Query exceeded timeout."""
    pass


class TimeoutGuard:
    """
    Enforces query timeouts.
    
    Wraps query execution with timeout handling.
    """
    
    def __init__(self, default_timeout_seconds: int = 300):
        """
        Initialize timeout guard.
        
        Args:
            default_timeout_seconds: Default timeout in seconds
        """
        self.default_timeout = default_timeout_seconds
        logger.debug(f"TimeoutGuard initialized with {default_timeout_seconds}s timeout")
    
    @asynccontextmanager
    async def timeout(self, timeout_seconds: Optional[int] = None):
        """
        Context manager for timeout handling.
        
        Args:
            timeout_seconds: Timeout in seconds (uses default if not provided)
            
        Yields:
            When entered
            
        Raises:
            QueryTimeoutError: If timeout is exceeded
        """
        timeout = timeout_seconds or self.default_timeout
        
        try:
            async with asyncio.timeout(timeout):
                yield
        except asyncio.TimeoutError:
            raise QueryTimeoutError(f"Query timed out after {timeout} seconds")
    
    async def execute_with_timeout(
        self,
        coro: Callable[..., Any],
        timeout_seconds: Optional[int] = None,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute coroutine with timeout.
        
        Args:
            coro: Coroutine to execute
            timeout_seconds: Timeout in seconds
            *args: Arguments for coroutine
            **kwargs: Keyword arguments for coroutine
            
        Returns:
            Result of coroutine
            
        Raises:
            QueryTimeoutError: If timeout is exceeded
        """
        timeout = timeout_seconds or self.default_timeout
        
        try:
            return await asyncio.wait_for(
                coro(*args, **kwargs),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            raise QueryTimeoutError(f"Query timed out after {timeout} seconds")
