"""Azure Blob Storage backend for MetaMind.

Provides object storage via Azure Blob Storage for model persistence,
result exports, and replay recordings.

Requires: azure-storage-blob
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from metamind.core.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class AzureBlobStorage(StorageBackend):
    """Azure Blob Storage backend."""

    def __init__(
        self,
        connection_string: str,
        container: str,
        prefix: str = "metamind/",
    ) -> None:
        self.connection_string = connection_string
        self.container_name = container
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._service_client: Any = None
        self._container_client: Any = None

    @property
    def container_client(self) -> Any:
        """Lazy-initialize the Azure container client."""
        if self._container_client is None:
            try:
                from azure.storage.blob import BlobServiceClient

                self._service_client = BlobServiceClient.from_connection_string(
                    self.connection_string
                )
                self._container_client = self._service_client.get_container_client(
                    self.container_name
                )
                logger.info(
                    "Initialized Azure Blob client: container=%s",
                    self.container_name,
                )
            except ImportError:
                raise RuntimeError(
                    "azure-storage-blob is required. Install with: "
                    "pip install azure-storage-blob"
                )
        return self._container_client

    def _full_key(self, path: str) -> str:
        return f"{self.prefix}{path}"

    def _blob_client(self, path: str) -> Any:
        """Get a blob client for a specific path."""
        key = self._full_key(path)
        return self.container_client.get_blob_client(key)

    def read(self, path: str) -> bytes:
        """Download a blob from Azure.

        Args:
            path: Relative path within the prefix.

        Returns:
            Blob contents as bytes.
        """
        blob = self._blob_client(path)
        try:
            data: bytes = blob.download_blob().readall()
            logger.debug(
                "Read %d bytes from azure://%s/%s",
                len(data), self.container_name, self._full_key(path),
            )
            return data
        except Exception as exc:
            error_code = getattr(exc, "error_code", "")
            if error_code == "BlobNotFound" or "BlobNotFound" in str(exc):
                raise FileNotFoundError(
                    f"Azure blob not found: {self.container_name}/{self._full_key(path)}"
                )
            raise

    def write(self, path: str, data: bytes) -> None:
        """Upload data to Azure Blob Storage.

        Args:
            path: Relative path within the prefix.
            data: Bytes to write.
        """
        blob = self._blob_client(path)
        blob.upload_blob(data, overwrite=True)
        logger.debug(
            "Wrote %d bytes to azure://%s/%s",
            len(data), self.container_name, self._full_key(path),
        )

    def exists(self, path: str) -> bool:
        """Check if an Azure blob exists.

        Args:
            path: Relative path within the prefix.

        Returns:
            True if the blob exists.
        """
        blob = self._blob_client(path)
        try:
            blob.get_blob_properties()
            return True
        except Exception:
            return False

    def delete(self, path: str) -> None:
        """Delete a blob from Azure.

        Args:
            path: Relative path within the prefix.
        """
        blob = self._blob_client(path)
        try:
            blob.delete_blob()
            logger.debug(
                "Deleted azure://%s/%s",
                self.container_name, self._full_key(path),
            )
        except Exception as exc:
            logger.error("Azure delete error for %s: %s", path, exc)
            raise

    def list_keys(self, prefix: str = "") -> list[str]:
        """List all blobs under a prefix.

        Args:
            prefix: Additional prefix filter.

        Returns:
            List of relative key paths.
        """
        full_prefix = self._full_key(prefix)
        keys: list[str] = []

        blobs = self.container_client.list_blobs(name_starts_with=full_prefix)
        for blob in blobs:
            rel_key = blob.name
            if self.prefix and rel_key.startswith(self.prefix):
                rel_key = rel_key[len(self.prefix):]
            keys.append(rel_key)

        return sorted(keys)
