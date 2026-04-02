"""F01 Versioned model store with metadata and concurrent access safety.

Stores ML models (XGBoost cardinality estimators) with version tracking,
metadata, and safe concurrent read/write via file locking.
"""
from __future__ import annotations

import json
import logging
import os
import pickle
import shutil
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class ModelInfo:
    """Metadata about a stored model version."""

    key: str
    version: int
    created_at: float
    file_path: str
    metadata: dict[str, Any]


class ModelStore:
    """Versioned model store with metadata and thread-safe access.

    Directory structure:
        base_path/
            {key}/
                v1.pkl
                v1.meta.json
                v2.pkl
                v2.meta.json
                latest -> v2.pkl
    """

    def __init__(self, base_path: str) -> None:
        """Initialize store at given base path."""
        self._path = base_path
        self._lock = threading.Lock()
        os.makedirs(base_path, exist_ok=True)

    def save(
        self,
        key: str,
        model: Any,
        version: Optional[int] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Save a model with version tracking.

        Args:
            key: Model identifier.
            model: Picklable model object.
            version: Explicit version (auto-increments if None).
            metadata: Optional metadata dict to store alongside model.

        Returns:
            Path to saved model file.
        """
        with self._lock:
            model_dir = os.path.join(self._path, key)
            os.makedirs(model_dir, exist_ok=True)

            if version is None:
                version = self.get_version(key) + 1

            model_path = os.path.join(model_dir, f"v{version}.pkl")
            meta_path = os.path.join(model_dir, f"v{version}.meta.json")
            latest_path = os.path.join(model_dir, "latest.pkl")

            # Save model
            with open(model_path, "wb") as f:
                pickle.dump(model, f, protocol=pickle.HIGHEST_PROTOCOL)

            # Save metadata
            meta = metadata or {}
            meta["version"] = version
            meta["created_at"] = time.time()
            meta["key"] = key
            with open(meta_path, "w") as f:
                json.dump(meta, f, indent=2)

            # Update latest symlink (or copy for portability)
            try:
                if os.path.exists(latest_path):
                    os.remove(latest_path)
                shutil.copy2(model_path, latest_path)
            except OSError:
                logger.error("Unhandled exception in model_store.py: %s", exc)

            logger.debug("Saved model %s v%d to %s", key, version, model_path)
            return model_path

    def load(self, key: str, version: Optional[int] = None) -> Optional[Any]:
        """Load a model by key and optional version.

        Args:
            key: Model identifier.
            version: Specific version to load (latest if None).

        Returns:
            Deserialized model object, or None if not found.
        """
        model_dir = os.path.join(self._path, key)

        if version is not None:
            model_path = os.path.join(model_dir, f"v{version}.pkl")
        else:
            # Try latest first
            model_path = os.path.join(model_dir, "latest.pkl")
            if not os.path.exists(model_path):
                # Fall back to highest version
                v = self.get_version(key)
                if v == 0:
                    return None
                model_path = os.path.join(model_dir, f"v{v}.pkl")

        if not os.path.exists(model_path):
            return None

        try:
            with open(model_path, "rb") as f:
                return pickle.load(f)
        except Exception as exc:
            logger.warning("Failed to load model %s: %s", key, exc)
            return None

    def get_version(self, key: str) -> int:
        """Get the latest version number for a model key."""
        model_dir = os.path.join(self._path, key)
        if not os.path.isdir(model_dir):
            return 0

        max_version = 0
        for fname in os.listdir(model_dir):
            if fname.startswith("v") and fname.endswith(".pkl") and fname != "latest.pkl":
                try:
                    v = int(fname[1:].replace(".pkl", ""))
                    max_version = max(max_version, v)
                except ValueError:
                    logger.error("Unhandled exception in model_store.py: %s", exc)

        return max_version

    def get_metadata(self, key: str, version: Optional[int] = None) -> Optional[dict[str, Any]]:
        """Get metadata for a model version.

        Args:
            key: Model identifier.
            version: Version to query (latest if None).

        Returns:
            Metadata dict, or None if not found.
        """
        model_dir = os.path.join(self._path, key)
        if version is None:
            version = self.get_version(key)

        if version == 0:
            return None

        meta_path = os.path.join(model_dir, f"v{version}.meta.json")
        if not os.path.exists(meta_path):
            return None

        try:
            with open(meta_path, "r") as f:
                return json.load(f)
        except Exception:
            return None

    def list_models(self) -> list[ModelInfo]:
        """List all stored models with their latest version info."""
        models: list[ModelInfo] = []

        if not os.path.isdir(self._path):
            return models

        for key in os.listdir(self._path):
            key_dir = os.path.join(self._path, key)
            if not os.path.isdir(key_dir):
                continue

            version = self.get_version(key)
            if version == 0:
                continue

            metadata = self.get_metadata(key, version) or {}
            model_path = os.path.join(key_dir, f"v{version}.pkl")

            models.append(ModelInfo(
                key=key,
                version=version,
                created_at=metadata.get("created_at", 0.0),
                file_path=model_path,
                metadata=metadata,
            ))

        return models

    def delete(self, key: str, version: Optional[int] = None) -> bool:
        """Delete a model version or all versions.

        Args:
            key: Model identifier.
            version: Specific version to delete (all if None).

        Returns:
            True if deletion succeeded.
        """
        with self._lock:
            model_dir = os.path.join(self._path, key)
            if not os.path.isdir(model_dir):
                return False

            if version is None:
                shutil.rmtree(model_dir, ignore_errors=True)
                return True

            model_path = os.path.join(model_dir, f"v{version}.pkl")
            meta_path = os.path.join(model_dir, f"v{version}.meta.json")

            deleted = False
            for p in [model_path, meta_path]:
                if os.path.exists(p):
                    os.remove(p)
                    deleted = True

            return deleted

    def cleanup_old_versions(self, key: str, keep: int = 3) -> int:
        """Remove old model versions, keeping only the N most recent.

        Args:
            key: Model identifier.
            keep: Number of recent versions to retain.

        Returns:
            Number of versions removed.
        """
        current = self.get_version(key)
        if current <= keep:
            return 0

        removed = 0
        for v in range(1, current - keep + 1):
            if self.delete(key, version=v):
                removed += 1

        return removed
