"""Local filesystem storage."""
from __future__ import annotations
import os
from metamind.core.storage.storage import StorageBackend
class LocalStorage(StorageBackend):
    def __init__(self, base_path: str = "./data") -> None:
        self._base = base_path; os.makedirs(base_path, exist_ok=True)
    def read(self, path: str) -> bytes:
        with open(os.path.join(self._base, path), "rb") as f: return f.read()
    def write(self, path: str, data: bytes) -> None:
        full = os.path.join(self._base, path); os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f: f.write(data)
    def exists(self, path: str) -> bool:
        return os.path.exists(os.path.join(self._base, path))
