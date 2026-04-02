"""
Storage Abstraction - Unified Storage Interface

File: metamind/storage/storage.py
Role: Storage Engineer
Phase: 1
Dependencies: None

Abstracts storage operations across different backends.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional, BinaryIO, List, Dict, Any

logger = logging.getLogger(__name__)


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    async def read(self, path: str) -> bytes:
        """Read data from storage."""
        pass
    
    @abstractmethod
    async def write(self, path: str, data: bytes) -> None:
        """Write data to storage."""
        pass
    
    @abstractmethod
    async def delete(self, path: str) -> None:
        """Delete data from storage."""
        pass
    
    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Check if path exists."""
        pass
    
    @abstractmethod
    async def list(self, prefix: str) -> List[str]:
        """List paths with prefix."""
        pass


class StorageManager:
    """
    Unified storage manager.
    
    Provides a single interface for all storage operations.
    """
    
    def __init__(self):
        """Initialize storage manager."""
        self._backends: Dict[str, StorageBackend] = {}
        self._default_backend: Optional[str] = None
        logger.debug("StorageManager initialized")
    
    def register_backend(
        self,
        name: str,
        backend: StorageBackend,
        default: bool = False
    ) -> None:
        """
        Register a storage backend.
        
        Args:
            name: Backend name
            backend: Backend instance
            default: Whether this is the default backend
        """
        self._backends[name] = backend
        if default or self._default_backend is None:
            self._default_backend = name
        logger.debug(f"Registered storage backend: {name}")
    
    def get_backend(self, name: Optional[str] = None) -> StorageBackend:
        """
        Get a storage backend.
        
        Args:
            name: Backend name (uses default if not provided)
            
        Returns:
            Storage backend
        """
        backend_name = name or self._default_backend
        if backend_name is None:
            raise ValueError("No storage backend configured")
        
        if backend_name not in self._backends:
            raise ValueError(f"Unknown storage backend: {backend_name}")
        
        return self._backends[backend_name]
    
    async def read(
        self,
        path: str,
        backend: Optional[str] = None
    ) -> bytes:
        """
        Read data from storage.
        
        Args:
            path: Storage path
            backend: Backend to use
            
        Returns:
            Data as bytes
        """
        return await self.get_backend(backend).read(path)
    
    async def write(
        self,
        path: str,
        data: bytes,
        backend: Optional[str] = None
    ) -> None:
        """
        Write data to storage.
        
        Args:
            path: Storage path
            data: Data to write
            backend: Backend to use
        """
        await self.get_backend(backend).write(path, data)
    
    async def delete(self, path: str, backend: Optional[str] = None) -> None:
        """
        Delete data from storage.
        
        Args:
            path: Storage path
            backend: Backend to use
        """
        await self.get_backend(backend).delete(path)
    
    async def exists(self, path: str, backend: Optional[str] = None) -> bool:
        """
        Check if path exists.
        
        Args:
            path: Storage path
            backend: Backend to use
            
        Returns:
            True if exists
        """
        return await self.get_backend(backend).exists(path)
    
    async def list(
        self,
        prefix: str,
        backend: Optional[str] = None
    ) -> List[str]:
        """
        List paths with prefix.
        
        Args:
            prefix: Path prefix
            backend: Backend to use
            
        Returns:
            List of paths
        """
        return await self.get_backend(backend).list(prefix)
