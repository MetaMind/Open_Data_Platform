"""
Local Storage Backend

File: metamind/storage/local.py
Role: Storage Engineer
Phase: 1
Dependencies: None

Local filesystem storage backend for development.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List

from metamind.storage.storage import StorageBackend

logger = logging.getLogger(__name__)


class LocalStorageBackend(StorageBackend):
    """
    Local filesystem storage backend.
    
    For development and testing only.
    """
    
    def __init__(self, base_path: str = "./data"):
        """
        Initialize local storage backend.
        
        Args:
            base_path: Base directory for storage
        """
        self.base_path = Path(base_path).resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"LocalStorageBackend initialized: {self.base_path}")
    
    def _get_full_path(self, path: str) -> Path:
        """Get full path for storage path."""
        # Security: prevent directory traversal
        full_path = (self.base_path / path.lstrip("/")).resolve()
        
        if not str(full_path).startswith(str(self.base_path)):
            raise ValueError("Path traversal detected")
        
        return full_path
    
    async def read(self, path: str) -> bytes:
        """Read data from local storage."""
        full_path = self._get_full_path(path)
        
        if not full_path.exists():
            raise FileNotFoundError(f"Path not found: {path}")
        
        return full_path.read_bytes()
    
    async def write(self, path: str, data: bytes) -> None:
        """Write data to local storage."""
        full_path = self._get_full_path(path)
        
        # Create parent directories
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        full_path.write_bytes(data)
        logger.debug(f"Wrote to local storage: {path}")
    
    async def delete(self, path: str) -> None:
        """Delete data from local storage."""
        full_path = self._get_full_path(path)
        
        if full_path.exists():
            full_path.unlink()
            logger.debug(f"Deleted from local storage: {path}")
    
    async def exists(self, path: str) -> bool:
        """Check if path exists."""
        full_path = self._get_full_path(path)
        return full_path.exists()
    
    async def list(self, prefix: str) -> List[str]:
        """List paths with prefix."""
        full_prefix = self._get_full_path(prefix)
        
        if not full_prefix.exists():
            return []
        
        paths = []
        base_str = str(self.base_path)
        
        if full_prefix.is_dir():
            for item in full_prefix.rglob("*"):
                if item.is_file():
                    path_str = str(item.relative_to(self.base_path))
                    paths.append(path_str)
        
        return paths
