"""Storage backend abstract base class and local filesystem implementation."""
from __future__ import annotations

import abc
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class StorageBackend(abc.ABC):
    """Abstract base class for storage backends."""

    @abc.abstractmethod
    def read(self, path: str) -> bytes:
        ...

    @abc.abstractmethod
    def write(self, path: str, data: bytes) -> None:
        ...

    @abc.abstractmethod
    def exists(self, path: str) -> bool:
        ...

    @abc.abstractmethod
    def delete(self, path: str) -> None:
        ...

    @abc.abstractmethod
    def list_keys(self, prefix: str = "") -> list[str]:
        ...


class LocalStorage(StorageBackend):
    """Local filesystem storage backend."""

    def __init__(self, base_path: str = "./data") -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _resolve(self, path: str) -> Path:
        return self.base_path / path

    def read(self, path: str) -> bytes:
        full = self._resolve(path)
        if not full.exists():
            raise FileNotFoundError(f"Storage key not found: {path}")
        return full.read_bytes()

    def write(self, path: str, data: bytes) -> None:
        full = self._resolve(path)
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_bytes(data)
        logger.debug("Wrote %d bytes to %s", len(data), path)

    def exists(self, path: str) -> bool:
        return self._resolve(path).exists()

    def delete(self, path: str) -> None:
        full = self._resolve(path)
        if full.exists():
            full.unlink()

    def list_keys(self, prefix: str = "") -> list[str]:
        search_dir = self._resolve(prefix) if prefix else self.base_path
        if not search_dir.exists():
            return []
        keys: list[str] = []
        for root, _dirs, files in os.walk(search_dir):
            for f in files:
                full = Path(root) / f
                rel = str(full.relative_to(self.base_path))
                keys.append(rel)
        return sorted(keys)
