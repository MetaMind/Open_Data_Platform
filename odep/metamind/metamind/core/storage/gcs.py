"""Google Cloud Storage backend for MetaMind.

Provides object storage via GCS for model persistence, exports, and recordings.

Requires: google-cloud-storage
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from metamind.core.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class GCSStorage(StorageBackend):
    """Google Cloud Storage backend."""

    def __init__(
        self,
        bucket: str,
        project: str,
        credentials_path: Optional[str] = None,
        prefix: str = "metamind/",
    ) -> None:
        self.bucket_name = bucket
        self.project = project
        self.credentials_path = credentials_path
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._client: Any = None
        self._bucket: Any = None

    @property
    def client(self) -> Any:
        """Lazy-initialize the GCS client."""
        if self._client is None:
            try:
                from google.cloud import storage as gcs_storage

                kwargs: dict[str, Any] = {"project": self.project}
                if self.credentials_path:
                    kwargs["credentials"] = self._load_credentials()

                self._client = gcs_storage.Client(**kwargs)
                self._bucket = self._client.bucket(self.bucket_name)
                logger.info(
                    "Initialized GCS client: bucket=%s project=%s",
                    self.bucket_name, self.project,
                )
            except ImportError:
                raise RuntimeError(
                    "google-cloud-storage is required. Install with: "
                    "pip install google-cloud-storage"
                )
        return self._client

    @property
    def bucket(self) -> Any:
        """Get the GCS bucket object."""
        if self._bucket is None:
            _ = self.client  # triggers initialization
        return self._bucket

    def _load_credentials(self) -> Any:
        """Load credentials from a service account JSON file."""
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_file(
            self.credentials_path
        )

    def _full_key(self, path: str) -> str:
        return f"{self.prefix}{path}"

    def read(self, path: str) -> bytes:
        """Download an object from GCS.

        Args:
            path: Relative path within the prefix.

        Returns:
            Object contents as bytes.
        """
        key = self._full_key(path)
        blob = self.bucket.blob(key)
        if not blob.exists():
            raise FileNotFoundError(f"GCS key not found: gs://{self.bucket_name}/{key}")

        data: bytes = blob.download_as_bytes()
        logger.debug("Read %d bytes from gs://%s/%s", len(data), self.bucket_name, key)
        return data

    def write(self, path: str, data: bytes) -> None:
        """Upload data to GCS.

        Args:
            path: Relative path within the prefix.
            data: Bytes to write.
        """
        key = self._full_key(path)
        blob = self.bucket.blob(key)
        blob.upload_from_string(data)
        logger.debug("Wrote %d bytes to gs://%s/%s", len(data), self.bucket_name, key)

    def exists(self, path: str) -> bool:
        """Check if a GCS object exists.

        Args:
            path: Relative path within the prefix.

        Returns:
            True if the object exists.
        """
        key = self._full_key(path)
        blob = self.bucket.blob(key)
        return bool(blob.exists())

    def delete(self, path: str) -> None:
        """Delete an object from GCS.

        Args:
            path: Relative path within the prefix.
        """
        key = self._full_key(path)
        blob = self.bucket.blob(key)
        if blob.exists():
            blob.delete()
            logger.debug("Deleted gs://%s/%s", self.bucket_name, key)

    def list_keys(self, prefix: str = "") -> list[str]:
        """List all objects under a prefix.

        Args:
            prefix: Additional prefix filter.

        Returns:
            List of relative key paths.
        """
        full_prefix = self._full_key(prefix)
        blobs = self.client.list_blobs(self.bucket_name, prefix=full_prefix)

        keys: list[str] = []
        for blob in blobs:
            rel_key = blob.name
            if self.prefix and rel_key.startswith(self.prefix):
                rel_key = rel_key[len(self.prefix):]
            keys.append(rel_key)

        return sorted(keys)
