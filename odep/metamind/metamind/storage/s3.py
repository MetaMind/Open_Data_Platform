"""
S3 Storage Backend

File: metamind/storage/s3.py
Role: Storage Engineer
Phase: 1
Dependencies: boto3, aiobotocore

S3-compatible storage backend (AWS S3, MinIO, etc.)
"""

from __future__ import annotations

import logging
from typing import Optional, List

from metamind.storage.storage import StorageBackend

logger = logging.getLogger(__name__)


class S3StorageBackend(StorageBackend):
    """
    S3-compatible storage backend.
    
    Supports AWS S3, MinIO, and other S3-compatible services.
    """
    
    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        prefix: str = ""
    ):
        """
        Initialize S3 backend.
        
        Args:
            bucket: S3 bucket name
            region: AWS region
            endpoint_url: Custom endpoint URL (for MinIO)
            access_key_id: AWS access key
            secret_access_key: AWS secret key
            prefix: Key prefix
        """
        self.bucket = bucket
        self.region = region
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.prefix = prefix.rstrip("/")
        
        self._client: Optional[Any] = None
        logger.debug(f"S3StorageBackend initialized: {bucket}")
    
    def _get_client(self) -> Any:
        """Get or create S3 client."""
        if self._client is None:
            try:
                import boto3
                
                session = boto3.Session(
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.secret_access_key,
                    region_name=self.region
                )
                
                kwargs = {}
                if self.endpoint_url:
                    kwargs["endpoint_url"] = self.endpoint_url
                
                self._client = session.client("s3", **kwargs)
                logger.debug("Created S3 client")
            except ImportError:
                raise ImportError("boto3 is required for S3 storage")
        
        return self._client
    
    def _get_key(self, path: str) -> str:
        """Get full S3 key for path."""
        if self.prefix:
            return f"{self.prefix}/{path.lstrip('/')}"
        return path.lstrip("/")
    
    async def read(self, path: str) -> bytes:
        """Read data from S3."""
        import asyncio
        
        client = self._get_client()
        key = self._get_key(path)
        
        def _read():
            response = client.get_object(Bucket=self.bucket, Key=key)
            return response["Body"].read()
        
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _read)
    
    async def write(self, path: str, data: bytes) -> None:
        """Write data to S3."""
        import asyncio
        
        client = self._get_client()
        key = self._get_key(path)
        
        def _write():
            client.put_object(Bucket=self.bucket, Key=key, Body=data)
        
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _write)
        logger.debug(f"Wrote to S3: {key}")
    
    async def delete(self, path: str) -> None:
        """Delete data from S3."""
        import asyncio
        
        client = self._get_client()
        key = self._get_key(path)
        
        def _delete():
            client.delete_object(Bucket=self.bucket, Key=key)
        
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _delete)
        logger.debug(f"Deleted from S3: {key}")
    
    async def exists(self, path: str) -> bool:
        """Check if key exists in S3."""
        import asyncio
        from botocore.exceptions import ClientError
        
        client = self._get_client()
        key = self._get_key(path)
        
        def _exists():
            try:
                client.head_object(Bucket=self.bucket, Key=key)
                return True
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return False
                raise
        
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _exists)
    
    async def list(self, prefix: str) -> List[str]:
        """List keys with prefix."""
        import asyncio
        
        client = self._get_client()
        full_prefix = self._get_key(prefix)
        
        def _list():
            response = client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=full_prefix
            )
            return [
                obj["Key"][len(self.prefix) + 1:] if self.prefix else obj["Key"]
                for obj in response.get("Contents", [])
            ]
        
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _list)
