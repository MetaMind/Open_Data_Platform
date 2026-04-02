"""Configuration settings for the MetaMind platform."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class DatabaseSettings:
    url: str = "sqlite:///:memory:"
    pool_size: int = 5
    max_overflow: int = 10
    echo: bool = False


@dataclass
class RedisSettings:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None


@dataclass
class StorageSettings:
    provider: str = "local"  # "local", "s3", "gcs", "azure"
    local_base_path: str = "./data"
    s3_bucket: Optional[str] = None
    s3_region: str = "us-east-1"
    s3_prefix: str = "metamind/"
    s3_profile: Optional[str] = None
    gcs_bucket: Optional[str] = None
    gcs_project: Optional[str] = None
    gcs_credentials_path: Optional[str] = None
    azure_connection_string: Optional[str] = None
    azure_container: Optional[str] = None


@dataclass
class LLMSettings:
    provider: str = "openai"  # "openai", "anthropic", "ollama"
    api_key: Optional[str] = None
    model: str = "gpt-4o"
    base_url: Optional[str] = None
    temperature: float = 0.0
    max_tokens: int = 2048


@dataclass
class FeatureFlagsSettings:
    flags: dict[str, bool] = field(default_factory=lambda: {
        "F01_learned_cardinality": True,
        "F04_dpccp_join": True,
        "F09_cache": True,
        "F11_compiled_execution": True,
        "F12_basic_optimization": True,
        "F19_vector_search": True,
        "F20_regret_minimization": True,
        "F28_nl_interface": True,
        "F29_query_rewrite": True,
        "F30_optimization_replay": True,
    })

    def is_enabled(self, feature: str) -> bool:
        return self.flags.get(feature, False)


@dataclass
class MetaMindSettings:
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    redis: RedisSettings = field(default_factory=RedisSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    llm: LLMSettings = field(default_factory=LLMSettings)
    features: FeatureFlagsSettings = field(default_factory=FeatureFlagsSettings)
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> MetaMindSettings:
        settings = cls()
        settings.database.url = os.environ.get("METAMIND_DB_URL", settings.database.url)
        settings.redis.host = os.environ.get("METAMIND_REDIS_HOST", settings.redis.host)
        settings.storage.provider = os.environ.get("METAMIND_STORAGE_PROVIDER", "local")
        settings.storage.s3_bucket = os.environ.get("METAMIND_S3_BUCKET")
        settings.storage.s3_region = os.environ.get("METAMIND_S3_REGION", "us-east-1")
        settings.storage.gcs_bucket = os.environ.get("METAMIND_GCS_BUCKET")
        settings.storage.gcs_project = os.environ.get("METAMIND_GCS_PROJECT")
        settings.storage.azure_connection_string = os.environ.get("METAMIND_AZURE_CONN_STR")
        settings.storage.azure_container = os.environ.get("METAMIND_AZURE_CONTAINER")
        settings.llm.provider = os.environ.get("METAMIND_LLM_PROVIDER", "openai")
        settings.llm.api_key = os.environ.get("METAMIND_LLM_API_KEY")
        settings.llm.model = os.environ.get("METAMIND_LLM_MODEL", "gpt-4o")
        settings.log_level = os.environ.get("METAMIND_LOG_LEVEL", "INFO")
        return settings


def get_storage_backend(settings: StorageSettings) -> Any:
    """Factory function returning the appropriate storage backend."""
    provider = settings.provider.lower()
    if provider == "local":
        from metamind.core.storage.base import LocalStorage
        return LocalStorage(base_path=settings.local_base_path)
    elif provider == "s3":
        from metamind.core.storage.s3 import S3Storage
        if not settings.s3_bucket:
            raise ValueError("s3_bucket is required for S3 storage provider")
        return S3Storage(
            bucket=settings.s3_bucket,
            prefix=settings.s3_prefix,
            region=settings.s3_region,
            profile=settings.s3_profile,
        )
    elif provider == "gcs":
        from metamind.core.storage.gcs import GCSStorage
        if not settings.gcs_bucket or not settings.gcs_project:
            raise ValueError("gcs_bucket and gcs_project required for GCS provider")
        return GCSStorage(
            bucket=settings.gcs_bucket,
            project=settings.gcs_project,
            credentials_path=settings.gcs_credentials_path,
        )
    elif provider == "azure":
        from metamind.core.storage.azure import AzureBlobStorage
        if not settings.azure_connection_string or not settings.azure_container:
            raise ValueError("azure_connection_string and azure_container required")
        return AzureBlobStorage(
            connection_string=settings.azure_connection_string,
            container=settings.azure_container,
        )
    else:
        raise ValueError(f"Unknown storage provider: {provider}")
