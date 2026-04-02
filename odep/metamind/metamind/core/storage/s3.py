"""AWS S3 storage backend for MetaMind.

Provides object storage for ML model persistence, result exports,
replay recordings, and other binary artifacts.

Requires: boto3
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from metamind.core.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class S3Storage(StorageBackend):
    """AWS S3 storage backend with KMS encryption."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "metamind/",
        region: str = "us-east-1",
        profile: Optional[str] = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.region = region
        self.profile = profile
        self._client: Any = None

    @property
    def client(self) -> Any:
        """Lazy-initialize the S3 client."""
        if self._client is None:
            try:
                import boto3

                session_kwargs: dict[str, Any] = {"region_name": self.region}
                if self.profile:
                    session_kwargs["profile_name"] = self.profile

                session = boto3.Session(**session_kwargs)
                self._client = session.client("s3")
                logger.info(
                    "Initialized S3 client: bucket=%s region=%s",
                    self.bucket, self.region,
                )
            except ImportError:
                raise RuntimeError(
                    "boto3 is required for S3 storage. Install with: pip install boto3"
                )
        return self._client

    def _full_key(self, path: str) -> str:
        """Build full S3 key from relative path."""
        return f"{self.prefix}{path}"

    def read(self, path: str) -> bytes:
        """Read an object from S3.

        Args:
            path: Relative path within the prefix.

        Returns:
            Object contents as bytes.

        Raises:
            FileNotFoundError: If the key does not exist.
        """
        key = self._full_key(path)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            data: bytes = response["Body"].read()
            logger.debug("Read %d bytes from s3://%s/%s", len(data), self.bucket, key)
            return data
        except self.client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"S3 key not found: s3://{self.bucket}/{key}")
        except Exception as exc:
            logger.error("S3 read error for %s: %s", key, exc)
            raise

    def write(self, path: str, data: bytes) -> None:
        """Write data to S3 with server-side KMS encryption.

        Args:
            path: Relative path within the prefix.
            data: Bytes to write.
        """
        key = self._full_key(path)
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ServerSideEncryption="aws:kms",
            )
            logger.debug("Wrote %d bytes to s3://%s/%s", len(data), self.bucket, key)
        except Exception as exc:
            logger.error("S3 write error for %s: %s", key, exc)
            raise

    def exists(self, path: str) -> bool:
        """Check if an S3 key exists.

        Args:
            path: Relative path within the prefix.

        Returns:
            True if the key exists, False otherwise.
        """
        key = self._full_key(path)
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def delete(self, path: str) -> None:
        """Delete an object from S3.

        Args:
            path: Relative path within the prefix.
        """
        key = self._full_key(path)
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
            logger.debug("Deleted s3://%s/%s", self.bucket, key)
        except Exception as exc:
            logger.error("S3 delete error for %s: %s", key, exc)
            raise

    def list_keys(self, prefix: str = "") -> list[str]:
        """List all keys under a prefix.

        Args:
            prefix: Additional prefix to filter by.

        Returns:
            List of relative key paths.
        """
        full_prefix = self._full_key(prefix)
        keys: list[str] = []

        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
                for obj in page.get("Contents", []):
                    rel_key = obj["Key"]
                    if self.prefix and rel_key.startswith(self.prefix):
                        rel_key = rel_key[len(self.prefix):]
                    keys.append(rel_key)
        except Exception as exc:
            logger.error("S3 list error for prefix %s: %s", full_prefix, exc)
            raise

        return sorted(keys)

    def get_presigned_url(self, path: str, expires_in: int = 3600) -> str:
        """Generate a presigned URL for temporary access.

        Args:
            path: Relative path within the prefix.
            expires_in: URL validity in seconds (default: 1 hour).

        Returns:
            Presigned URL string.
        """
        key = self._full_key(path)
        url: str = self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_in,
        )
        return url
