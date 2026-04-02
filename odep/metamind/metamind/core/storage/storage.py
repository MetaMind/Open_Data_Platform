"""Storage abstraction layer."""
from __future__ import annotations
import abc
class StorageBackend(abc.ABC):
    @abc.abstractmethod
    def read(self, path: str) -> bytes: ...
    @abc.abstractmethod
    def write(self, path: str, data: bytes) -> None: ...
    @abc.abstractmethod
    def exists(self, path: str) -> bool: ...
